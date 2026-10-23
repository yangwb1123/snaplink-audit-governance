#!/usr/bin/env python3
"""Read-only audit fact probe for an externally supplied event id.

This is deliberately independent of the platform integration/ingest scripts:
it never obtains a token, never writes an event, and never reads a database.
It performs exactly two HTTP GETs against an explicitly authorized loopback
Audit API:

  /api/v1/events/{event_id}?tenant_id=...
  /api/v1/events/{event_id}/receipt?tenant_id=...

A pass proves only that the audit side can read the supplied event and its
ledger receipt with matching tenant/event/stream/sequence/hash coordinates,
the expected source/event/schema identity, a ledger-backed receipt status, a
valid ledgered_at, and no-store on both responses. It does not prove the
external business operation, outbox/relay delivery, Kafka, ClickHouse, or
whole-chain completeness.

The Bearer credential is read only from AUDIT_FACT_PROBE_BEARER. It is never
accepted as an argument and never printed.

Exit codes: 0 verified, 1 failed evidence/authorization/read, 2 not executed
(missing input/credential or unavailable service).
"""

from __future__ import annotations

import argparse
import datetime
import http.client
import ipaddress
import json
import math
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

TOKEN_ENV = "AUDIT_FACT_PROBE_BEARER"
DEFAULT_TIMEOUT_SECONDS = 5.0
MAX_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_BODY_BYTES = 1024 * 1024
MIN_MAX_BODY_BYTES = 128
MAX_MAX_BODY_BYTES = 8 * 1024 * 1024
LEDGER_BACKED_STATUSES = frozenset({"ledgered", "indexed", "archived"})
CONTROL_OR_SPACE = re.compile(r"[\x00-\x1f\x7f\s/\\]")
# RFC3339 date-time with a mandatory zone and at most nanosecond precision.
# datetime.fromisoformat is deliberately not used for validation: recent Python
# versions also accept an arbitrary date/time separator (for example "X"),
# offsets wider than any real timezone, and offsets without a colon. The regex
# binds the exact structure; the datetime constructor then validates the real
# calendar date and clock fields. Go's time.RFC3339Nano output uses uppercase
# T/Z, numeric offsets, and 1-9 fractional digits, all of which remain valid.
RFC3339_LEDGERED_AT = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})T"
    r"([0-9]{2}):([0-9]{2}):([0-9]{2})"
    r"(?:\.([0-9]{1,9}))?"
    r"(Z|[+-][0-9]{2}:[0-9]{2})"
)


class NotExecuted(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class Failed(Exception):
    def __init__(self, reason: str, check: str = "") -> None:
        super().__init__(reason)
        self.reason = reason
        self.check = check


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        # Never echo argparse text: it can contain a caller-supplied URL.
        raise NotExecuted("invalid_input")


class _RefuseRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _require_text(value: str | None, *, path_safe: bool = False) -> str:
    if value is None or value == "":
        raise NotExecuted("missing_input")
    if value != value.strip() or any(ord(char) < 0x20 or ord(char) == 0x7F for char in value):
        raise NotExecuted("invalid_input")
    if path_safe and CONTROL_OR_SPACE.search(value):
        raise NotExecuted("invalid_input")
    return value


def _loopback_host(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    return address.is_loopback


def _base_url(raw: str) -> str:
    if not raw or any(ord(char) <= 0x20 or ord(char) == 0x7F for char in raw):
        raise NotExecuted("invalid_audit_url")
    if "?" in raw or "#" in raw or "\\" in raw:
        raise NotExecuted("invalid_audit_url")
    try:
        parts = urllib.parse.urlsplit(raw)
        port = parts.port
    except ValueError:
        raise NotExecuted("invalid_audit_url") from None
    if parts.scheme not in ("http", "https"):
        raise NotExecuted("invalid_audit_url")
    if parts.username is not None or parts.password is not None or "@" in parts.netloc:
        raise NotExecuted("invalid_audit_url")
    if not parts.hostname or not _loopback_host(parts.hostname):
        raise NotExecuted("invalid_audit_url")
    if port is not None and not 0 < port <= 65535:
        raise NotExecuted("invalid_audit_url")
    path = parts.path.rstrip("/")
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def _config(argv: list[str] | None) -> dict:
    parser = _Parser(description="Verify an externally supplied audit event and receipt read-only")
    parser.add_argument("--audit-url", help="loopback Audit API base URL (required, no query/fragment/userinfo)")
    parser.add_argument("--tenant", help="existing event tenant id (required, sent explicitly)")
    parser.add_argument("--event-id", help="existing event id from the external operation (required)")
    parser.add_argument("--source-system", help="expected exact source_system (required)")
    parser.add_argument("--event-type", help="expected exact event_type (required)")
    parser.add_argument("--schema-id", help="expected exact schema_id (required)")
    parser.add_argument("--schema-version", type=int, help="expected exact schema_version (required)")
    parser.add_argument("--operation-id", help="optional exact operation_id")
    parser.add_argument("--correlation-id", help="optional exact correlation_id")
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--max-body-bytes", type=int, default=DEFAULT_MAX_BODY_BYTES)
    args = parser.parse_args(argv)

    tenant = _require_text(args.tenant)
    event_id = _require_text(args.event_id, path_safe=True)
    source_system = _require_text(args.source_system)
    event_type = _require_text(args.event_type)
    schema_id = _require_text(args.schema_id)
    if args.schema_version is None or args.schema_version < 1:
        raise NotExecuted("missing_input" if args.schema_version is None else "invalid_input")
    if args.audit_url is None or args.audit_url == "":
        raise NotExecuted("missing_input")
    base_url = _base_url(args.audit_url)
    if not math.isfinite(args.timeout_seconds) or not 0 < args.timeout_seconds <= MAX_TIMEOUT_SECONDS:
        raise NotExecuted("invalid_input")
    if not MIN_MAX_BODY_BYTES <= args.max_body_bytes <= MAX_MAX_BODY_BYTES:
        raise NotExecuted("invalid_input")

    token = os.environ.get(TOKEN_ENV, "")
    if not token:
        raise NotExecuted("missing_credentials")
    if not token.isascii() or token != token.strip() or any(
        ord(char) < 0x20 or ord(char) == 0x7F for char in token
    ):
        raise NotExecuted("invalid_credentials")

    operation_id = None if args.operation_id is None else _require_text(args.operation_id)
    correlation_id = None if args.correlation_id is None else _require_text(args.correlation_id)
    return {
        "base_url": base_url,
        "tenant": tenant,
        "event_id": event_id,
        "source_system": source_system,
        "event_type": event_type,
        "schema_id": schema_id,
        "schema_version": args.schema_version,
        "operation_id": operation_id,
        "correlation_id": correlation_id,
        "timeout_seconds": args.timeout_seconds,
        "max_body_bytes": args.max_body_bytes,
        "token": token,
    }


def _endpoint(base_url: str, event_id: str, receipt: bool, tenant: str) -> str:
    suffix = "/receipt" if receipt else ""
    path = "/api/v1/events/" + urllib.parse.quote(event_id, safe="") + suffix
    return base_url + path + "?" + urllib.parse.urlencode({"tenant_id": tenant})


def _has_no_store(headers) -> bool:
    values = headers.get_all("Cache-Control") or []
    return len(values) == 1 and " ".join(values[0].split()).lower() == "no-store"


def _read_json(url: str, token: str, timeout: float, max_body: int) -> dict:
    request = urllib.request.Request(
        url,
        method="GET",
        headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
    )
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        _RefuseRedirect(),
    )
    try:
        response = opener.open(request, timeout=timeout)
    except urllib.error.HTTPError as error:
        status = error.code
        error.close()
        if 300 <= status < 400:
            raise Failed("redirect_refused") from None
        raise Failed(f"http_status_{status}") from None
    except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException):
        raise NotExecuted("audit_unavailable") from None

    with response:
        status = response.status
        if 300 <= status < 400:
            raise Failed("redirect_refused")
        if status != 200:
            raise Failed(f"http_status_{status}")
        if not _has_no_store(response.headers):
            raise Failed("no_store_missing")
        try:
            raw = response.read(max_body + 1)
        except (TimeoutError, OSError, http.client.HTTPException):
            raise Failed("response_read_failed") from None

    if len(raw) > max_body:
        raise Failed("response_too_large")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError, RecursionError):
        raise Failed("invalid_json") from None
    if not isinstance(value, dict):
        raise Failed("invalid_json")
    return value


def _match_text(value, expected: str, reason: str, check: str) -> None:
    if not isinstance(value, str) or value != expected:
        raise Failed(reason, check)


def _nonempty_text(value, reason: str, check: str) -> str:
    if not isinstance(value, str) or not value:
        raise Failed(reason, check)
    return value


def _positive_sequence(value, check: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise Failed("chain_mismatch", check)
    return value


def _optional_text(value, expected: str | None, reason: str, check: str) -> None:
    if expected is not None and (not isinstance(value, str) or value != expected):
        raise Failed(reason, check)


def _verify_event(event: dict, config: dict) -> tuple[str, int, str]:
    _match_text(event.get("tenant_id"), config["tenant"], "tenant_mismatch", "event.tenant_id")
    _match_text(event.get("event_id"), config["event_id"], "event_mismatch", "event.event_id")
    _match_text(event.get("source_system"), config["source_system"], "source_mismatch", "event.source_system")
    _match_text(event.get("event_type"), config["event_type"], "event_type_mismatch", "event.event_type")
    _match_text(event.get("schema_id"), config["schema_id"], "schema_mismatch", "event.schema_id")
    version = event.get("schema_version")
    if isinstance(version, bool) or not isinstance(version, int) or version != config["schema_version"]:
        raise Failed("schema_mismatch", "event.schema_version")
    _optional_text(event.get("operation_id"), config["operation_id"], "operation_mismatch", "event.operation_id")
    _optional_text(
        event.get("correlation_id"), config["correlation_id"], "correlation_mismatch", "event.correlation_id"
    )
    stream_id = _nonempty_text(event.get("stream_id"), "chain_mismatch", "event.stream_id")
    sequence = _positive_sequence(event.get("sequence"), "event.sequence")
    event_hash = _nonempty_text(event.get("hash"), "chain_mismatch", "event.hash")
    return stream_id, sequence, event_hash


def _valid_ledgered_at(value) -> bool:
    if not isinstance(value, str):
        return False
    match = RFC3339_LEDGERED_AT.fullmatch(value)
    if match is None:
        return False
    year, month, day, hour, minute, second = (
        int(match.group(index)) for index in range(1, 7)
    )
    try:
        datetime.datetime(year, month, day, hour, minute, second)
    except ValueError:
        return False
    zone = match.group(8)
    if zone != "Z":
        offset_hour = int(zone[1:3])
        offset_minute = int(zone[4:6])
        if offset_minute > 59:
            return False
        # No real UTC offset exceeds 14:00; reject syntactically plausible but
        # nonexistent offsets such as +15:00 or +14:30.
        if offset_hour > 14 or (offset_hour == 14 and offset_minute != 0):
            return False
    # Go's zero time (year 1) and any pre-ledger date are not evidence.
    return year >= 1900


def _verify_receipt(receipt: dict, config: dict, stream_id: str, sequence: int, event_hash: str) -> str:
    _match_text(
        receipt.get("tenant_id"), config["tenant"], "receipt_identity_mismatch", "receipt.tenant_id"
    )
    _match_text(
        receipt.get("event_id"), config["event_id"], "receipt_identity_mismatch", "receipt.event_id"
    )
    status = receipt.get("status")
    if not isinstance(status, str) or status not in LEDGER_BACKED_STATUSES:
        raise Failed("receipt_status", "receipt.status")
    if not _valid_ledgered_at(receipt.get("ledgered_at")):
        raise Failed("ledgered_at_invalid", "receipt.ledgered_at")
    _match_text(receipt.get("stream_id"), stream_id, "chain_mismatch", "receipt.stream_id")
    receipt_sequence = _positive_sequence(receipt.get("sequence"), "receipt.sequence")
    if receipt_sequence != sequence:
        raise Failed("chain_mismatch", "receipt.sequence")
    _match_text(receipt.get("hash"), event_hash, "chain_mismatch", "receipt.hash")
    return status


def _observe(config: dict) -> dict:
    headers = {"token": config["token"], "timeout": config["timeout_seconds"], "max_body": config["max_body_bytes"]}
    event = _read_json(
        _endpoint(config["base_url"], config["event_id"], False, config["tenant"]),
        **headers,
    )
    stream_id, sequence, event_hash = _verify_event(event, config)
    receipt = _read_json(
        _endpoint(config["base_url"], config["event_id"], True, config["tenant"]),
        **headers,
    )
    status = _verify_receipt(receipt, config, stream_id, sequence, event_hash)
    return {
        "status": "ok",
        "reason": "verified",
        "tenant": config["tenant"],
        "event_id": config["event_id"],
        "web_event_hash": "#events/event/" + urllib.parse.quote(config["event_id"], safe=""),
        "source_system": event["source_system"],
        "event_type": event["event_type"],
        "schema_id": event["schema_id"],
        "schema_version": event["schema_version"],
        "receipt_status": status,
        "ledgered_at": receipt["ledgered_at"],
        "chain": {"stream_id": stream_id, "sequence": sequence, "hash": event_hash},
    }


def _emit(result: dict) -> None:
    print(json.dumps(result, sort_keys=True, separators=(",", ":"), ensure_ascii=True))


def main(argv: list[str] | None = None) -> int:
    try:
        _emit(_observe(_config(argv)))
        return 0
    except NotExecuted as error:
        _emit({"status": "not_executed", "reason": error.reason})
        return 2
    except Failed as error:
        result = {"status": "failed", "reason": error.reason}
        if error.check:
            result["check"] = error.check
        _emit(result)
        return 1
    except (BrokenPipeError, KeyboardInterrupt):
        return 1
    except Exception:
        # Fail closed without rendering an exception, response, or credential.
        _emit({"status": "failed", "reason": "internal_error"})
        return 1


if __name__ == "__main__":
    sys.exit(main())
