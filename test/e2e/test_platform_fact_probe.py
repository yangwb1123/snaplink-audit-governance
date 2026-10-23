#!/usr/bin/env python3
"""HTTP-fixture regression tests for platform_fact_probe.py.

The fixture is a local read-only HTTP server. It records every request so the
tests can prove the probe only performs the two explicitly tenant-scoped GETs
and never falls back to another tenant, a write, a redirect, or a proxy.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlsplit

TOOL = Path(__file__).with_name("platform_fact_probe.py")
TOKEN_ENV = "AUDIT_FACT_PROBE_BEARER"

TENANT = "tenant-a"
EVENT_ID = "evt-fact-1"
SOURCE = "aero.vault.security"
EVENT_TYPE = "aero.vault.security"
SCHEMA_ID = "aero.vault.security"
SCHEMA_VERSION = 1
STREAM = "stream-fact-1"
SEQUENCE = 7
HASH = "a" * 64

TOKEN = "probe-test-token-never-print"
URL_SECRET = "url-userinfo-secret-never-print"
WRONG_TOKEN = "wrong-token-never-print"
PAYLOAD_SECRET = "payload-secret-never-print"
ACTOR_SECRET = "actor-secret-never-print"
BACKEND_SECRET = "backend-error-secret-never-print"
SECRETS = (TOKEN, URL_SECRET, WRONG_TOKEN, PAYLOAD_SECRET, ACTOR_SECRET, BACKEND_SECRET)

ACCEPTED_AT = "2026-10-23T03:12:00Z"
LEDGERED_AT = "2026-10-23T03:12:01Z"


def default_event() -> dict:
    return {
        "event_id": EVENT_ID,
        "tenant_id": TENANT,
        "source_system": SOURCE,
        "event_type": EVENT_TYPE,
        "schema_id": SCHEMA_ID,
        "schema_version": SCHEMA_VERSION,
        "stream_id": STREAM,
        "sequence": SEQUENCE,
        "hash": HASH,
        "operation_id": "op-1",
        "correlation_id": "corr-1",
        "occurred_at": ACCEPTED_AT,
        "payload": {"secret": PAYLOAD_SECRET},
        "actor": {"id": ACTOR_SECRET, "name": ACTOR_SECRET},
    }


def default_receipt() -> dict:
    return {
        "event_id": EVENT_ID,
        "tenant_id": TENANT,
        "status": "ledgered",
        "accepted_at": ACCEPTED_AT,
        "ledgered_at": LEDGERED_AT,
        "stream_id": STREAM,
        "sequence": SEQUENCE,
        "hash": HASH,
    }


class Fixture:
    """Mutable read-only server state; reset() keeps subtests independent."""

    def __init__(self) -> None:
        self.tenant = TENANT
        self.event_id = EVENT_ID
        self.requests: list[dict] = []
        self.reset()

    def reset(self) -> None:
        self.event = default_event()
        self.receipt = default_receipt()
        self.event_status = 200
        self.receipt_status = 200
        self.event_body = None
        self.receipt_body = None
        self.event_no_store = True
        self.receipt_no_store = True
        self.event_redirect = False
        self.receipt_redirect = False
        self.event_disconnect = False
        self.receipt_disconnect = False
        self.event_delay = 0.0
        self.receipt_delay = 0.0
        self.requests = []

    def event_path(self) -> str:
        return "/api/v1/events/" + quote(self.event_id, safe="")

    def receipt_path(self) -> str:
        return self.event_path() + "/receipt"

    def serve(self, handler: BaseHTTPRequestHandler) -> None:
        split = urlsplit(handler.path)
        self.requests.append(
            {
                "method": handler.command,
                "path": split.path,
                "query": parse_qs(split.query, keep_blank_values=True),
                "authorized": handler.headers.get("Authorization") == "Bearer " + TOKEN,
            }
        )
        if split.path == self.event_path():
            self._serve_kind(handler, "event")
        elif split.path == self.receipt_path():
            self._serve_kind(handler, "receipt")
        else:
            self._reply(handler, 404, b"{}", True)

    def _serve_kind(self, handler: BaseHTTPRequestHandler, kind: str) -> None:
        if getattr(self, kind + "_disconnect"):
            handler.close_connection = True
            try:
                handler.connection.shutdown(socket.SHUT_RDWR)
                handler.connection.close()
            except OSError:
                pass
            return
        delay = getattr(self, kind + "_delay")
        if delay:
            time.sleep(delay)
        if getattr(self, kind + "_redirect"):
            self._redirect(handler)
            return
        status = getattr(self, kind + "_status")
        body = getattr(self, kind + "_body")
        if body is None:
            raw = json.dumps(
                self.event if kind == "event" else self.receipt,
                separators=(",", ":"),
            ).encode()
        elif isinstance(body, bytes):
            raw = body
        else:
            raw = json.dumps(body, separators=(",", ":")).encode()
        self._reply(handler, status, raw, getattr(self, kind + "_no_store"))

    def _redirect(self, handler: BaseHTTPRequestHandler) -> None:
        try:
            handler.send_response(302)
            handler.send_header("Location", "/redirect-target")
            handler.send_header("Content-Length", "0")
            handler.send_header("Connection", "close")
            handler.end_headers()
            handler.close_connection = True
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def _reply(
        self,
        handler: BaseHTTPRequestHandler,
        status: int,
        body: bytes,
        no_store: bool,
    ) -> None:
        try:
            handler.send_response(status)
            if no_store:
                handler.send_header("Cache-Control", "no-store")
            handler.send_header("Content-Type", "application/json")
            handler.send_header("Content-Length", str(len(body)))
            handler.send_header("Connection", "close")
            handler.end_headers()
            handler.wfile.write(body)
            handler.close_connection = True
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass


class FixtureHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self.server.fixture.serve(self)

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        split = urlsplit(self.path)
        self.server.fixture.requests.append(
            {
                "method": self.command,
                "path": split.path,
                "query": parse_qs(split.query, keep_blank_values=True),
                "authorized": self.headers.get("Authorization") == "Bearer " + TOKEN,
            }
        )
        self.server.fixture._reply(self, 405, b"{}", True)

    def log_message(self, _format: str, *args) -> None:
        pass


class PlatformFactProbeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = Fixture()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
        self.server.daemon_threads = True
        self.server.fixture = self.fixture
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def default_args(self, **overrides) -> list[str]:
        values = {
            "audit_url": self.base_url,
            "tenant": TENANT,
            "event_id": EVENT_ID,
            "source_system": SOURCE,
            "event_type": EVENT_TYPE,
            "schema_id": SCHEMA_ID,
            "schema_version": SCHEMA_VERSION,
            "operation_id": None,
            "correlation_id": None,
            "timeout_seconds": None,
            "max_body_bytes": None,
        }
        values.update(overrides)
        flags = (
            ("audit_url", "--audit-url"),
            ("tenant", "--tenant"),
            ("event_id", "--event-id"),
            ("source_system", "--source-system"),
            ("event_type", "--event-type"),
            ("schema_id", "--schema-id"),
            ("schema_version", "--schema-version"),
            ("operation_id", "--operation-id"),
            ("correlation_id", "--correlation-id"),
            ("timeout_seconds", "--timeout-seconds"),
            ("max_body_bytes", "--max-body-bytes"),
        )
        args: list[str] = []
        for key, flag in flags:
            value = values[key]
            if value is None:
                continue
            args.extend((flag, str(value)))
        return args

    def run_probe(
        self,
        args: list[str] | None = None,
        token: str | None = TOKEN,
        env_extra: dict | None = None,
    ) -> tuple[subprocess.CompletedProcess, dict]:
        env = {
            key: value
            for key, value in os.environ.items()
            if key.upper() not in {"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"}
            and key != TOKEN_ENV
        }
        if token is not None:
            env[TOKEN_ENV] = token
        if env_extra:
            env.update(env_extra)
        command = [sys.executable, "-B", str(TOOL), *(self.default_args() if args is None else args)]
        proc = subprocess.run(
            command,
            cwd=str(TOOL.parents[2]),
            env=env,
            text=True,
            capture_output=True,
            timeout=15,
            check=False,
        )
        self.assertNotIn("Traceback", proc.stderr)
        self._assert_clean(proc)
        try:
            result = json.loads(proc.stdout)
        except json.JSONDecodeError:
            self.fail("probe stdout was not one JSON object")
        self.assertIsInstance(result, dict)
        return proc, result

    def _assert_clean(self, proc: subprocess.CompletedProcess) -> None:
        combined = proc.stdout + proc.stderr
        for secret in SECRETS:
            self.assertFalse(secret in combined, "probe output leaked a test secret")

    def _assert_failed(self, proc: subprocess.CompletedProcess, result: dict) -> None:
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(result.get("status"), "failed")

    def _assert_not_executed(self, proc: subprocess.CompletedProcess, result: dict) -> None:
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(result.get("status"), "not_executed")

    def _assert_only_tenant_gets(self) -> None:
        for request in self.fixture.requests:
            self.assertEqual(request["method"], "GET")
            self.assertEqual(request["query"], {"tenant_id": [TENANT]})
            self.assertTrue(request["authorized"], "probe did not send the dedicated Bearer credential")

    def test_verified_fact_and_receipt(self) -> None:
        proc, result = self.run_probe()

        self.assertEqual(proc.returncode, 0)
        self.assertEqual(result.get("status"), "ok")
        self.assertEqual(result.get("event_id"), EVENT_ID)
        self.assertEqual(result.get("web_event_hash"), "#events/event/" + EVENT_ID)
        self.assertEqual(result.get("receipt_status"), "ledgered")
        self.assertEqual(len(self.fixture.requests), 2)
        self.assertEqual(
            {request["path"] for request in self.fixture.requests},
            {self.fixture.event_path(), self.fixture.receipt_path()},
        )
        self._assert_only_tenant_gets()
        rendered = json.dumps(result)
        self.assertNotIn("payload", rendered)
        self.assertNotIn("actor", rendered)
        self.assertNotIn("raw", rendered)
        self.assertNotIn("error", rendered)

    def test_indexed_and_archived_receipts_are_ledger_backed(self) -> None:
        for status in ("indexed", "archived"):
            with self.subTest(status=status):
                self.fixture.reset()
                self.fixture.receipt["status"] = status
                proc, result = self.run_probe()
                self.assertEqual(proc.returncode, 0)
                self.assertEqual(result.get("receipt_status"), status)
                self._assert_only_tenant_gets()

    def test_optional_operation_and_correlation_must_match_when_requested(self) -> None:
        proc, result = self.run_probe(
            self.default_args(operation_id="op-1", correlation_id="corr-1")
        )
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(result.get("status"), "ok")

        for field in ("operation_id", "correlation_id"):
            with self.subTest(field=field):
                self.fixture.reset()
                proc, result = self.run_probe(
                    self.default_args(**{field: "different-" + field})
                )
                self._assert_failed(proc, result)
                self.assertEqual(len(self.fixture.requests), 1)

    def test_http_status_failures_never_pass_or_fall_back(self) -> None:
        for kind in ("event", "receipt"):
            for status in (401, 403, 404, 409, 503):
                with self.subTest(kind=kind, status=status):
                    self.fixture.reset()
                    setattr(self.fixture, kind + "_status", status)
                    setattr(
                        self.fixture,
                        kind + "_body",
                        json.dumps({"error": {"message": BACKEND_SECRET}}).encode(),
                    )
                    proc, result = self.run_probe()
                    self._assert_failed(proc, result)
                    expected_requests = 1 if kind == "event" else 2
                    self.assertEqual(len(self.fixture.requests), expected_requests)
                    self._assert_only_tenant_gets()

    def test_disconnect_and_timeout_never_pass(self) -> None:
        for kind in ("event", "receipt"):
            with self.subTest(kind=kind, case="disconnect"):
                self.fixture.reset()
                setattr(self.fixture, kind + "_disconnect", True)
                proc, result = self.run_probe()
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn(result.get("status"), {"failed", "not_executed"})

        self.fixture.reset()
        self.fixture.event_delay = 0.6
        proc, result = self.run_probe(self.default_args(timeout_seconds="0.15"))
        self._assert_not_executed(proc, result)

    def test_redirects_are_refused_on_both_reads(self) -> None:
        self.fixture.event_redirect = True
        proc, result = self.run_probe()
        self._assert_failed(proc, result)
        self.assertEqual(result.get("reason"), "redirect_refused")
        self.assertEqual(len(self.fixture.requests), 1)
        self.assertEqual(
            [request["path"] for request in self.fixture.requests],
            [self.fixture.event_path()],
        )

        self.fixture.reset()
        self.fixture.receipt_redirect = True
        proc, result = self.run_probe()
        self._assert_failed(proc, result)
        self.assertEqual(result.get("reason"), "redirect_refused")
        self.assertEqual(len(self.fixture.requests), 2)
        self.assertNotIn(
            "/redirect-target",
            [request["path"] for request in self.fixture.requests],
        )

    def test_bad_json_and_oversized_bodies_never_pass(self) -> None:
        for kind in ("event", "receipt"):
            with self.subTest(kind=kind, case="bad-json"):
                self.fixture.reset()
                setattr(
                    self.fixture,
                    kind + "_body",
                    ('{"secret":"' + BACKEND_SECRET + '"').encode(),
                )
                proc, result = self.run_probe()
                self._assert_failed(proc, result)

            with self.subTest(kind=kind, case="non-object-json"):
                self.fixture.reset()
                setattr(self.fixture, kind + "_body", b"[]")
                proc, result = self.run_probe()
                self._assert_failed(proc, result)

            with self.subTest(kind=kind, case="oversized"):
                self.fixture.reset()
                setattr(self.fixture, kind + "_body", b"x" * 4096)
                proc, result = self.run_probe(self.default_args(max_body_bytes="512"))
                self._assert_failed(proc, result)
                self.assertEqual(result.get("reason"), "response_too_large")

    def test_no_store_is_required_on_both_reads(self) -> None:
        for kind in ("event", "receipt"):
            with self.subTest(kind=kind):
                self.fixture.reset()
                setattr(self.fixture, kind + "_no_store", False)
                proc, result = self.run_probe()
                self._assert_failed(proc, result)
                self.assertEqual(result.get("reason"), "no_store_missing")

    def test_identity_schema_and_chain_mismatches_fail_closed(self) -> None:
        cases = (
            ("event", "tenant_id", "tenant-b", 1),
            ("event", "event_id", "other-event", 1),
            ("event", "source_system", "aero.other", 1),
            ("event", "event_type", "aero.other.event", 1),
            ("event", "schema_id", "other.schema", 1),
            ("event", "schema_version", 2, 1),
            ("event", "stream_id", "", 1),
            ("event", "sequence", 0, 1),
            ("event", "hash", "", 1),
            ("receipt", "tenant_id", "tenant-b", 2),
            ("receipt", "event_id", "other-event", 2),
            ("receipt", "stream_id", "other-stream", 2),
            ("receipt", "stream_id", "", 2),
            ("receipt", "sequence", SEQUENCE + 1, 2),
            ("receipt", "sequence", 0, 2),
            ("receipt", "hash", "b" * 64, 2),
            ("receipt", "hash", "", 2),
        )
        for kind, field, value, expected_requests in cases:
            with self.subTest(kind=kind, field=field, value=value):
                self.fixture.reset()
                getattr(self.fixture, kind)[field] = value
                proc, result = self.run_probe()
                self._assert_failed(proc, result)
                self.assertEqual(len(self.fixture.requests), expected_requests)
                self._assert_only_tenant_gets()

    def test_receipt_status_and_ledgered_time_fail_closed(self) -> None:
        cases = (
            ("status", "accepted"),
            ("status", "queued"),
            ("status", "mystery"),
            ("status", "LEDGERED"),
            ("status", None),
            ("ledgered_at", "0001-01-01T00:00:00Z"),
            ("ledgered_at", ""),
            ("ledgered_at", "not-a-time"),
            ("ledgered_at", "2026-10-23T03:12:01"),
            ("ledgered_at", "1899-12-31T23:59:59Z"),
            ("ledgered_at", 123),
        )
        for field, value in cases:
            with self.subTest(field=field, value=value):
                self.fixture.reset()
                self.fixture.receipt[field] = value
                proc, result = self.run_probe()
                self._assert_failed(proc, result)

        for field in ("status", "ledgered_at"):
            with self.subTest(field=field, case="missing"):
                self.fixture.reset()
                self.fixture.receipt.pop(field)
                proc, result = self.run_probe()
                self._assert_failed(proc, result)

    def test_rfc3339_x_separator_counterexample_fails(self) -> None:
        # The old datetime.fromisoformat check accepted this shape and printed
        # ok when every other field matched. A CLI-level fixture regression is
        # kept here so that permissive ISO parsing cannot silently return.
        invalid = "2026-10-23X03:12:01+00:00"
        self.fixture.receipt["ledgered_at"] = invalid
        proc, result = self.run_probe()
        self._assert_failed(proc, result)
        self.assertEqual(result.get("reason"), "ledgered_at_invalid")
        self.assertEqual(result.get("check"), "receipt.ledgered_at")
        self.assertEqual(len(self.fixture.requests), 2)
        self._assert_only_tenant_gets()
        self.assertNotIn(invalid, proc.stdout + proc.stderr)

    def test_rfc3339_structural_invalid_values_fail_closed(self) -> None:
        invalid_values = (
            ("2026-10-23X03:12:01+00:00", "non-RFC3339 date/time separator"),
            ("2026-10-23 03:12:01+00:00", "space date/time separator"),
            ("2026-10-23T03:12:01", "missing timezone"),
            ("2026-10-23T03:12:01+0800", "offset without colon"),
            ("2026-10-23T03:12:01+24:00", "offset hour out of range"),
            ("2026-10-23T03:12:01+15:00", "offset beyond real timezone range"),
            ("2026-10-23T03:12:01-14:01", "negative offset minute beyond range"),
            ("2026-02-30T03:12:01Z", "nonexistent February day"),
            ("2026-04-31T03:12:01Z", "nonexistent April day"),
            ("2026-13-01T03:12:01Z", "month out of range"),
            ("2026-10-23T24:00:00Z", "hour out of range"),
            ("2026-10-23T03:60:00Z", "minute out of range"),
            ("2026-10-23T03:12:60Z", "second out of range"),
            ("0000-01-01T00:00:00Z", "year zero"),
            ("0001-01-01T00:00:00Z", "Go zero time"),
        )
        for value, label in invalid_values:
            with self.subTest(label=label, value=value):
                self.fixture.reset()
                self.fixture.receipt["ledgered_at"] = value
                proc, result = self.run_probe()
                self._assert_failed(proc, result)
                self.assertEqual(result.get("reason"), "ledgered_at_invalid")
                self.assertEqual(result.get("check"), "receipt.ledgered_at")
                self.assertEqual(len(self.fixture.requests), 2)
                self._assert_only_tenant_gets()
                self.assertNotIn(value, proc.stdout + proc.stderr)

    def test_rfc3339_valid_service_time_forms_pass(self) -> None:
        valid_values = (
            "2026-10-23T03:12:01Z",
            "2026-10-23T03:12:01+00:00",
            "2026-10-23T11:12:01+08:00",
            "2026-10-22T22:12:01-05:00",
            "2026-10-23T03:12:01.1Z",
            "2026-10-23T03:12:01.123456Z",
            "2026-10-23T03:12:01.123456789Z",
            "2028-02-29T23:59:59.999999999+00:00",
            "2026-10-23T03:12:01+14:00",
            "2026-10-23T03:12:01-14:00",
        )
        for value in valid_values:
            with self.subTest(ledgered_at=value):
                self.fixture.reset()
                self.fixture.receipt["ledgered_at"] = value
                proc, result = self.run_probe()
                self.assertEqual(proc.returncode, 0)
                self.assertEqual(result.get("status"), "ok")
                self.assertEqual(result.get("ledgered_at"), value)
                self.assertEqual(len(self.fixture.requests), 2)
                self._assert_only_tenant_gets()

    def test_base_url_rejects_userinfo_query_fragment_and_non_loopback(self) -> None:
        urls = (
            "http://user:" + URL_SECRET + "@127.0.0.1:%d" % self.server.server_port,
            self.base_url + "/?tenant_id=tenant-b",
            self.base_url + "/#fragment",
            "ftp://127.0.0.1:%d" % self.server.server_port,
            "http://192.0.2.1:%d" % self.server.server_port,
        )
        for url in urls:
            with self.subTest(url=url.split("@")[-1]):
                self.fixture.reset()
                proc, result = self.run_probe(self.default_args(audit_url=url))
                self._assert_not_executed(proc, result)
                self.assertEqual(self.fixture.requests, [])

    def test_missing_inputs_credentials_or_service_are_not_executed(self) -> None:
        proc, result = self.run_probe(token=None)
        self._assert_not_executed(proc, result)
        self.assertEqual(self.fixture.requests, [])

        for field in (
            "audit_url",
            "tenant",
            "event_id",
            "source_system",
            "event_type",
            "schema_id",
            "schema_version",
        ):
            with self.subTest(missing=field):
                self.fixture.reset()
                proc, result = self.run_probe(self.default_args(**{field: None}))
                self._assert_not_executed(proc, result)
                self.assertEqual(self.fixture.requests, [])

        closed = socket.socket()
        closed.bind(("127.0.0.1", 0))
        unused_port = closed.getsockname()[1]
        closed.close()
        proc, result = self.run_probe(
            self.default_args(audit_url=f"http://127.0.0.1:{unused_port}")
        )
        self._assert_not_executed(proc, result)
        self.assertEqual(result.get("reason"), "audit_unavailable")

    def test_proxy_environment_is_not_inherited(self) -> None:
        proc, result = self.run_probe(
            env_extra={
                "HTTP_PROXY": "http://127.0.0.1:1",
                "HTTPS_PROXY": "http://127.0.0.1:1",
                "ALL_PROXY": "http://127.0.0.1:1",
                "NO_PROXY": "",
            }
        )
        self.assertEqual(proc.returncode, 0)
        self.assertEqual(result.get("status"), "ok")
        self._assert_only_tenant_gets()


if __name__ == "__main__":
    unittest.main()
