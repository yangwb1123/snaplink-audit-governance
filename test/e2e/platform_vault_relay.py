#!/usr/bin/env python3
"""Real, opt-in Vault relay acceptance against an already running local stack.

Requires AUDIT_VAULT_RELAY_AUTHORIZED=true before config/credential access.
Only Python's standard library is used. PLATFORM_ENV_FILE is opened read-only
in place; only SECRET_KEYS are retained, with no shell execution or env export.
No credentials, raw request IDs, object paths/content, pseudonyms or response
bodies are logged or written. The only business writes are one unique small
Vault upload and its soft delete. Audit calls only query/read existing facts.

Exit codes: 0 verified; 1 operation/evidence failure; 2 unmet precondition/config.
A pass is not Kafka/ClickHouse confirmation, completeness or release approval.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import hmac
import http.client
import json
import os
import re
import shlex
import stat
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import platform_fact_probe as probe

SECRET_KEYS = frozenset({
    "SNAPLINK_CLIENT_SECRET_AERO_IM_VAULT",
    "SNAPLINK_CLIENT_SECRET_PLATFORM_AUDIT_BOOTSTRAP",
    "AERO_VAULT_AUDIT_HMAC_KEY",
})
DOMAIN = "aero-vault/audit-governance/v1"
SCHEMA = "aero.vault.security"
BUSINESS_SCOPE = "read write vault.file.delete"
AUDIT_SCOPE = "audit:platform:cross_tenant"
REQUEST_TIMEOUT = 5.0
POLL_SECONDS = 60.0
POLL_INTERVAL = 1.0
MAX_POLLS = 60
MAX_BODY = 1024 * 1024
MAX_ENV = 1024 * 1024
PAGE_SIZE = 100
Failed = probe.Failed
NotExecuted = probe.NotExecuted


def private_env(path: str) -> dict[str, str]:
    """Read only literal allow-listed assignments; never source/copy the file."""
    if not path:
        raise NotExecuted("missing_env_file")
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, encoding="utf-8") as handle:
            info = os.fstat(handle.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or info.st_mode & 0o077 or info.st_nlink != 1 or info.st_size > MAX_ENV):
                raise NotExecuted("unsafe_env_file")
            selected = {}
            consumed = 0
            while True:
                line = handle.readline(MAX_ENV + 1)
                if not line:
                    break
                consumed += len(line.encode("utf-8"))
                if consumed > MAX_ENV:
                    raise NotExecuted("invalid_env_file")
                match = re.match(r"\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)", line)
                if match is None or match[1] not in SECRET_KEYS:
                    continue
                name, literal = match[1], match[2]
                if name in selected or "$" in literal or "`" in literal:
                    raise NotExecuted("invalid_env_file")
                values = shlex.split(literal, comments=True, posix=True)
                if len(values) != 1 or not values[0] or values[0] != values[0].strip():
                    raise NotExecuted("invalid_env_file")
                value = values[0]
                if len(value.encode("utf-8")) > 4096 or any(ord(c) < 32 or ord(c) == 127 for c in value):
                    raise NotExecuted("invalid_env_file")
                selected[name] = value
    except (OSError, ValueError, UnicodeError):
        raise NotExecuted("unreadable_env_file") from None
    if selected.keys() != SECRET_KEYS:
        raise NotExecuted("missing_credentials")
    if len(selected["AERO_VAULT_AUDIT_HMAC_KEY"].encode("utf-8")) < 32:
        raise NotExecuted("invalid_hmac_key")
    return selected


def config(argv: list[str] | None) -> dict:
    if os.environ.get("AUDIT_VAULT_RELAY_AUTHORIZED") != "true":
        raise NotExecuted("not_authorized")
    parser = probe._Parser(description="Real local Vault upload/read/soft-delete relay acceptance")
    parser.add_argument("--vault-url", default="http://127.0.0.1:28081")
    parser.add_argument("--audit-url", default="http://127.0.0.1:28089")
    parser.add_argument("--token-url", default="http://127.0.0.1:28080/token")
    parser.add_argument("--tenant", default="platform-local")
    args = parser.parse_args(argv)
    # All endpoints must be explicit loopback HTTP(S), without URL credentials,
    # query/fragment or proxy inheritance. Validate before opening the env.
    result = {name: probe._base_url(getattr(args, name))
              for name in ("vault_url", "audit_url", "token_url")}
    result["tenant"] = probe._require_text(args.tenant, path_safe=True)
    result["secrets"] = private_env(os.environ.get("PLATFORM_ENV_FILE", ""))
    return result


def pseudonym(key: str, tenant: str, field: str, value: str) -> str:
    # Vault redaction.go: each UTF-8 field, including the last, is NUL terminated.
    frame = "\x00".join((DOMAIN, tenant, field, value, "")).encode("utf-8")
    digest = hmac.new(key.encode("utf-8"), frame, hashlib.sha256).digest()
    return "hmac-sha256:" + base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def json_object(raw: bytes) -> dict:
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeError, RecursionError):
        raise Failed("invalid_json") from None
    if not isinstance(value, dict):
        raise Failed("invalid_json")
    return value


class HTTP:
    def __init__(self) -> None:
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), probe._RefuseRedirect())
        self.last_status = 0
        self.last_body = b""

    def call(self, method: str, url: str, *, token: str = "", data: bytes | None = None,
             headers: dict | None = None, expected: int = 200, audit: bool = False,
             deadline: float | None = None, check: str = "http"):
        # A failed/partial request must never inherit another response's status
        # or body when token() classifies its precondition failure.
        self.last_status, self.last_body = 0, b""
        timeout = REQUEST_TIMEOUT
        if deadline is not None:
            timeout = min(timeout, deadline - time.monotonic())
            if timeout <= 0:
                raise Failed("relay_timeout")
        request_headers = {"Accept": "application/json", **(headers or {})}
        if token:
            request_headers["Authorization"] = "Bearer " + token
        request = urllib.request.Request(url, method=method, data=data, headers=request_headers)
        try:
            try:
                response = self.opener.open(request, timeout=timeout)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                if 300 <= response.status < 400:
                    raise Failed("redirect_refused", check)
                raw = response.read(MAX_BODY + 1)
                if len(raw) > MAX_BODY:
                    raise Failed("response_too_large")
                self.last_status = response.status
                self.last_body = raw
                if response.status != expected:
                    raise Failed("http_status_" + str(response.status), check)
                if audit and not probe._has_no_store(response.headers):
                    raise Failed("no_store_missing", check)
                if deadline is not None and time.monotonic() >= deadline:
                    raise Failed("relay_timeout")
                return response.headers, raw
        except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException):
            raise Failed("service_unavailable", check) from None

    def token(self, url: str, client: str, secret: str, scope: str, resource: str) -> str:
        form = urllib.parse.urlencode({"grant_type": "client_credentials", "scope": scope,
                                      "resource": resource}).encode("ascii")
        basic = base64.b64encode((client + ":" + secret).encode("utf-8")).decode("ascii")
        try:
            _, raw = self.call("POST", url, data=form, headers={
                "Authorization": "Basic " + basic, "Content-Type": "application/x-www-form-urlencoded"},
                check="business_token" if resource == "aero-vault" else "audit_token")
        except Failed as error:
            # Only this business token response can establish the missing-grant
            # precondition. Descriptions, nested errors and partial replies do
            # not authorize a skip, nor does any later business/audit failure.
            if (resource == "aero-vault" and error.reason == "http_status_400"
                    and self.last_status == 400):
                try:
                    failure = json_object(self.last_body)
                except Failed:
                    failure = {}
                if failure.get("error") == "invalid_scope":
                    raise NotExecuted("missing_delete_scope") from None
            raise
        value = json_object(raw)
        token = value.get("access_token")
        if (not isinstance(token, str) or not re.fullmatch(r"[\x21-\x7e]+", token)
                or str(value.get("token_type", "")).lower() != "bearer"):
            raise Failed("invalid_token_response")
        granted = value.get("scope")
        if not isinstance(granted, str) or not set(scope.split()) <= set(granted.split()):
            raise Failed("insufficient_token_scope")
        return token


def timestamp(value: str) -> dt.datetime:
    if not probe._valid_ledgered_at(value):
        raise Failed("invalid_fact_time")
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def rfc3339(value: dt.datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def query(http: HTTP, cfg: dict, token: str, operation: str, start: dt.datetime,
          end: dt.datetime, deadline: float | None = None) -> list[dict]:
    # HMAC correlation and source filters travel in a JSON read-query body, not
    # URLs/access logs. Each poll starts a NEW fixed-member query (no old cursor).
    body = {"from": rfc3339(start), "to": rfc3339(end), "source_system": cfg["source"],
            "event_type": SCHEMA, "operation_id": operation, "page_size": PAGE_SIZE}
    url = cfg["audit_url"] + "/api/v1/events:query?" + urllib.parse.urlencode({"tenant_id": cfg["tenant"]})
    _, raw = http.call("POST", url, token=token, data=json.dumps(body).encode("utf-8"),
                       headers={"Content-Type": "application/json"}, audit=True, deadline=deadline, check="audit_query")
    value = json_object(raw)
    items, count = value.get("items"), value.get("count")
    if (not isinstance(items, list) or not all(isinstance(item, dict) for item in items)
            or type(count) is not int or count != len(items) or count > PAGE_SIZE or value.get("next_cursor")):
        raise Failed("audit_result_limit_or_shape")
    return items


def verify_fact(http: HTTP, cfg: dict, token: str, event: dict, action: str,
                operation: str, start: dt.datetime, end: dt.datetime, deadline: float) -> dict:
    event_id = event.get("event_id")
    if not isinstance(event_id, str) or not re.fullmatch(r"[0-9a-f]{32}", event_id):
        raise Failed("invalid_event_id")
    expected = {"tenant": cfg["tenant"], "event_id": event_id, "source_system": cfg["source"],
                "event_type": SCHEMA, "schema_id": SCHEMA, "schema_version": 1,
                "operation_id": operation, "correlation_id": None}
    coordinates = probe._verify_event(event, expected)
    payload = event.get("payload")
    if (event.get("action") != action or event.get("outcome") != "success"
            or event.get("aggregate_type") != "file" or event.get("aggregate_id") != cfg["target"]
            or event.get("targets") != [{"type": "file", "id": cfg["target"]}]
            or event.get("data_classification") != "confidential" or event.get("retention_class") != "security"
            or not isinstance(payload, dict) or payload.get("fact_kind") != "file"
            or payload.get("request_id") != operation):
        raise Failed("fact_contract_mismatch")
    if not start <= timestamp(event.get("occurred_at")) <= end:
        raise Failed("fact_not_fresh")
    url = probe._endpoint(cfg["audit_url"], event_id, True, cfg["tenant"])
    _, raw = http.call("GET", url, token=token, audit=True, deadline=deadline, check="audit_receipt")
    receipt = json_object(raw)
    status = probe._verify_receipt(receipt, expected, *coordinates)
    observed = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=2)
    if receipt.get("conflict") or not start <= timestamp(receipt.get("ledgered_at")) <= observed:
        raise Failed("receipt_not_fresh_or_conflict")
    return {"action": action, "event_id": event_id, "receipt_status": status,
            "web_event_hash": "#events/event/" + event_id}


def poll(http: HTTP, cfg: dict, token: str, operations: dict[str, str],
         start: dt.datetime, end: dt.datetime) -> list[dict]:
    deadline = time.monotonic() + POLL_SECONDS
    found = {}
    for _ in range(MAX_POLLS):
        for action, operation in operations.items():
            if action in found:
                continue
            items = query(http, cfg, token, operation, start, end, deadline)
            if not items:
                continue
            if len(items) != 1:
                raise Failed("ambiguous_request_facts")
            found[action] = verify_fact(http, cfg, token, items[0], action, operation, start, end, deadline)
        if len(found) == len(operations):
            facts = list(found.values())
            if len({fact["event_id"] for fact in facts}) != len(facts):
                raise Failed("event_identity_reused")
            return facts
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(POLL_INTERVAL, remaining))
    raise Failed("relay_timeout")


def exercise(http: HTTP, cfg: dict, token: str, key: str, content: bytes,
             upload_request: str, delete_request: str) -> None:
    url = cfg["vault_url"] + "/v1/files/" + urllib.parse.quote(key, safe="/")
    tenant_header = {"X-Aero-Tenant": cfg["tenant"]}
    # Never retry an unknown business write. If upload acknowledgement succeeds,
    # even a malformed response/readback failure still triggers one soft delete.
    headers, raw = http.call("PUT", url, token=token, data=content, expected=201, check="vault_upload", headers={
        **tenant_header, "Content-Type": "application/octet-stream", "If-None-Match": "*",
        "X-Request-ID": upload_request})
    try:
        obj = json_object(raw)
        if (headers.get("X-Request-ID") != upload_request or obj.get("key") != key
                or obj.get("bucket") != "default" or type(obj.get("size")) is not int
                or obj["size"] != len(content)):
            raise Failed("upload_response_mismatch")
        _, downloaded = http.call("GET", url, token=token, check="vault_readback",
                                  headers={**tenant_header, "X-Request-ID": str(uuid.uuid4())})
        if downloaded != content:
            raise Failed("readback_mismatch")
    finally:
        headers, _ = http.call("DELETE", url + "?hard=0", token=token, expected=204, check="vault_soft_delete",
                               headers={**tenant_header, "X-Request-ID": delete_request})
        if headers.get("X-Request-ID") != delete_request:
            raise Failed("delete_response_mismatch")
    http.call("GET", url, token=token, headers=tenant_header, expected=404, check="vault_deleted_read")


def run(cfg: dict) -> dict:
    http = HTTP()
    secrets = cfg["secrets"]
    business = http.token(cfg["token_url"], "aero-im-vault", secrets["SNAPLINK_CLIENT_SECRET_AERO_IM_VAULT"],
                          BUSINESS_SCOPE, "aero-vault")
    audit = http.token(cfg["token_url"], "platform-audit-bootstrap",
                       secrets["SNAPLINK_CLIENT_SECRET_PLATFORM_AUDIT_BOOTSTRAP"],
                       AUDIT_SCOPE, "audit-governance")
    hmac_key, tenant = secrets["AERO_VAULT_AUDIT_HMAC_KEY"], cfg["tenant"]
    cfg["source"] = "aero-vault." + pseudonym(hmac_key, tenant, "source-system", tenant).split(":", 1)[1]
    key = "audit-relay-acceptance/" + str(uuid.uuid4()) + ".bin"
    cfg["target"] = pseudonym(hmac_key, tenant, "file-target", "default\x00" + key).replace(":", ".", 1)
    upload_request, delete_request = str(uuid.uuid4()), str(uuid.uuid4())
    operations = {"file.created": pseudonym(hmac_key, tenant, "request", upload_request),
                  "file.deleted": pseudonym(hmac_key, tenant, "request", delete_request)}
    start = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=2)
    horizon = start + dt.timedelta(seconds=120)
    # Prove both unique correlation IDs are absent before any business writes.
    for operation in operations.values():
        if query(http, cfg, audit, operation, start, horizon):
            raise Failed("preexisting_request_fact")
    exercise(http, cfg, business, key, b"vault relay acceptance\n" + uuid.uuid4().bytes,
             upload_request, delete_request)
    end = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=2)
    facts = poll(http, cfg, audit, operations, start, end)
    return {"status": "ok", "reason": "verified", "service": "aero-vault",
            "upload": "verified", "readback": "verified", "soft_delete": "verified", "facts": facts}


def main(argv: list[str] | None = None) -> int:
    try:
        probe._emit(run(config(argv)))
        return 0
    except NotExecuted as error:
        probe._emit({"status": "not_executed", "reason": error.reason})
        return 2
    except Failed as error:
        result = {"status": "failed", "reason": error.reason}
        if error.check:
            result["check"] = error.check
        probe._emit(result)
        return 1
    except (BrokenPipeError, KeyboardInterrupt):
        return 1
    except Exception:
        # No traceback/exception rendering: upstream bodies/config can be secret.
        probe._emit({"status": "failed", "reason": "internal_error"})
        return 1


if __name__ == "__main__":
    sys.exit(main())
