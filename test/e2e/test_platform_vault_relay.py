#!/usr/bin/env python3
"""Standard-library HTTP fixtures, NOT evidence of a real platform relay run."""

from __future__ import annotations

import base64
import contextlib
import copy
import datetime as dt
import hashlib
import hmac
import io
import json
import os
from pathlib import Path
import stat
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock
from urllib.parse import parse_qs, unquote, urlsplit

import platform_vault_relay as relay

BUSINESS_SECRET = "business-fixture-secret-never-log"
AUDIT_SECRET = "audit-fixture-secret-never-log"
HMAC_KEY = "K" * 32
BUSINESS_TOKEN = "business-fixture-token-never-log"
AUDIT_TOKEN = "audit-fixture-token-never-log"
BACKEND_SECRET = "backend-fixture-secret-never-log"
SECRETS = (BUSINESS_SECRET, AUDIT_SECRET, HMAC_KEY, BUSINESS_TOKEN, AUDIT_TOKEN, BACKEND_SECRET)


def fixture_mac(field, value):
    # Independent Vault contract replica, deliberately not relay.pseudonym().
    frame = b"aero-vault/audit-governance/v1\0platform-local\0" + field.encode() + b"\0" + value.encode() + b"\0"
    digest = hmac.new(HMAC_KEY.encode(), frame, hashlib.sha256).digest()
    return "hmac-sha256:" + base64.urlsafe_b64encode(digest).decode().rstrip("=")


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


class Fixture:
    def reset(self):
        self.requests = []
        self.events = []
        self.key = ""
        self.content = b""
        self.deleted = False
        self.delay_queries = 0
        self.post_write_queries = 0
        self.no_relay = False
        self.preexisting = False
        self.readback_bad = False
        self.deleted_read_status = 404
        self.upload_bad = False
        self.upload_json_bad = False
        self.upload_status = 201
        self.upload_error = {"error": BACKEND_SECRET}
        self.readback_status = 200
        self.delete_status = 204
        self.audit_status = 200
        self.token_status = 200
        self.token_error = {"error": BACKEND_SECRET}
        self.token_error_client = ""
        self.token_scope = None
        self.mutate_token = lambda value, client: None
        self.no_store = ""
        self.redirect = ""
        self.oversized = ""
        self.malformed = ""
        self.next_cursor = False
        self.mutate_event = lambda value: None
        self.mutate_receipt = lambda value: None

    def event(self, action, request_id):
        event_id = format(len(self.events) + 1, "032x")
        target = fixture_mac("file-target", "default\0" + self.key).replace(":", ".", 1)
        self.events.append({
            "tenant_id": "platform-local", "event_id": event_id,
            "source_system": "aero-vault." + fixture_mac("source-system", "platform-local").split(":")[1],
            "event_type": relay.SCHEMA, "schema_id": relay.SCHEMA, "schema_version": 1,
            "operation_id": fixture_mac("request", request_id),
            "action": action, "outcome": "success", "aggregate_type": "file", "aggregate_id": target,
            "targets": [{"type": "file", "id": target}],
            "payload": {"fact_kind": "file", "request_id": fixture_mac("request", request_id)},
            "data_classification": "confidential", "retention_class": "security",
            "occurred_at": now(), "stream_id": "fixture-stream", "sequence": len(self.events) + 1,
            "hash": "a" * 64,
        })

    def serve(self, handler):
        split = urlsplit(handler.path)
        raw = handler.rfile.read(int(handler.headers.get("Content-Length", "0")))
        self.requests.append({"method": handler.command, "path": split.path, "query": parse_qs(split.query),
                              "headers": dict(handler.headers), "body": raw})
        category = "token" if split.path == "/token" else (
            "query" if split.path == "/api/v1/events:query" else (
                "receipt" if split.path.endswith("/receipt") else "vault"))
        if self.redirect == category:
            self.reply(handler, 307, b"", {"Location": "/credential-sink"})
            return
        if self.oversized == category:
            self.reply(handler, 200, b"x" * (relay.MAX_BODY + 1), {"Cache-Control": "no-store"})
            return
        if self.malformed == category:
            self.reply(handler, 200, b"not-json-" + BACKEND_SECRET.encode(), {"Cache-Control": "no-store"})
            return
        if category == "token":
            self.token(handler, raw)
        elif category == "vault":
            self.vault(handler, split.path, parse_qs(split.query), raw)
        else:
            self.audit(handler, category, split.path, parse_qs(split.query), raw)

    def token(self, handler, raw):
        form = parse_qs(raw.decode())
        client, secret = base64.b64decode(handler.headers["Authorization"][6:]).decode().split(":", 1)
        business = client == "aero-im-vault"
        expected_secret = BUSINESS_SECRET if business else AUDIT_SECRET
        expected_scope = relay.BUSINESS_SCOPE if business else relay.AUDIT_SCOPE
        expected_resource = "aero-vault" if business else "audit-governance"
        if (secret != expected_secret or form != {"grant_type": ["client_credentials"],
                "resource": [expected_resource], "scope": [expected_scope]}):
            self.reply(handler, 401, {"error": BACKEND_SECRET})
            return
        if self.token_status != 200 and self.token_error_client in ("", client):
            self.reply(handler, self.token_status, self.token_error)
            return
        value = {"access_token": BUSINESS_TOKEN if business else AUDIT_TOKEN,
                 "token_type": "Bearer", "scope": self.token_scope or expected_scope}
        self.mutate_token(value, client)
        self.reply(handler, 200, value)

    def vault(self, handler, path, query, raw):
        if (handler.headers.get("Authorization") != "Bearer " + BUSINESS_TOKEN
                or handler.headers.get("X-Aero-Tenant") != "platform-local"):
            self.reply(handler, 401, {"error": BACKEND_SECRET})
            return
        request_id = handler.headers.get("X-Request-ID", "")
        if handler.command == "PUT":
            if self.upload_status != 201:
                self.reply(handler, self.upload_status, self.upload_error)
                return
            self.key = unquote(path.removeprefix("/v1/files/"))
            self.content = raw
            self.event("file.created", request_id)
            body = (b"not-json-" + BACKEND_SECRET.encode() if self.upload_json_bad else
                    {"bucket": "default", "key": "wrong" if self.upload_bad else self.key, "size": len(raw)})
            self.reply(handler, 201, body, {"X-Request-ID": request_id})
        elif handler.command == "DELETE":
            if query != {"hard": ["0"]}:
                self.reply(handler, 400, {"error": BACKEND_SECRET})
                return
            if self.delete_status == 204:
                self.deleted = True
                self.event("file.deleted", request_id)
            self.reply(handler, self.delete_status, b"", {"X-Request-ID": request_id})
        elif handler.command == "GET":
            if self.deleted:
                self.reply(handler, self.deleted_read_status, {"error": BACKEND_SECRET})
            else:
                self.reply(handler, self.readback_status, b"wrong" if self.readback_bad else self.content)
        else:
            self.reply(handler, 405, {})

    def audit(self, handler, category, path, query, raw):
        if (handler.headers.get("Authorization") != "Bearer " + AUDIT_TOKEN
                or query != {"tenant_id": ["platform-local"]}):
            self.reply(handler, 403, {"error": BACKEND_SECRET})
            return
        if self.events and self.audit_status != 200:
            self.reply(handler, self.audit_status, {"error": "invalid_scope", "error_description": BACKEND_SECRET})
            return
        headers = {} if self.no_store == category else {"Cache-Control": "no-store"}
        if category == "query":
            body = json.loads(raw)
            if self.preexisting:
                items = [{"event_id": "preexisting"}]
            else:
                items = [copy.deepcopy(event) for event in self.events
                         if event["operation_id"] == body["operation_id"]]
            if self.events:
                self.post_write_queries += 1
                if self.no_relay or self.post_write_queries <= self.delay_queries:
                    items = []
            for event in items:
                self.mutate_event(event)
            result = {"items": items, "count": len(items)}
            if self.next_cursor:
                result["next_cursor"] = "never-silently-drop-this-page"
            self.reply(handler, 200, result, headers)
        else:
            event = next(event for event in self.events if event["event_id"] in path)
            receipt = {key: event[key] for key in ("event_id", "tenant_id", "stream_id", "sequence", "hash")}
            receipt.update({"status": "ledgered", "ledgered_at": now()})
            self.mutate_receipt(receipt)
            self.reply(handler, 200, receipt, headers)

    @staticmethod
    def reply(handler, status, body, headers=None):
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        handler.send_response(status)
        for key, value in (headers or {}).items():
            handler.send_header(key, value)
        handler.send_header("Content-Length", str(len(raw)))
        handler.send_header("Connection", "close")
        handler.end_headers()
        try:
            handler.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError):
            pass


class RelayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = Fixture()
        fixture = cls.fixture

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                fixture.serve(self)
            do_POST = do_GET
            do_PUT = do_GET
            do_DELETE = do_GET

            def log_message(self, *args):
                pass

        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = "http://127.0.0.1:" + str(cls.server.server_port)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def setUp(self):
        self.fixture.reset()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.env = Path(self.tmp.name) / "private.env"
        self.env.write_text(
            "export SNAPLINK_CLIENT_SECRET_AERO_IM_VAULT='" + BUSINESS_SECRET + "'\n"
            "SNAPLINK_CLIENT_SECRET_PLATFORM_AUDIT_BOOTSTRAP=\"" + AUDIT_SECRET + "\" # literal\n"
            "AERO_VAULT_AUDIT_HMAC_KEY=" + HMAC_KEY + "\n"
            "UNRELATED_SECRET=$(touch should-never-run)\n", encoding="utf-8")
        self.env.chmod(0o600)

    def invoke(self, *extra, authorized="true"):
        output, errors = io.StringIO(), io.StringIO()
        env = {"PLATFORM_ENV_FILE": str(self.env), "HTTP_PROXY": self.url + "/proxy", "NO_PROXY": ""}
        if authorized is not None:
            env["AUDIT_VAULT_RELAY_AUTHORIZED"] = authorized
        argv = ["--vault-url", self.url, "--audit-url", self.url, "--token-url", self.url + "/token", *extra]
        with mock.patch.dict(os.environ, env, clear=True), contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            # No real-service timeout is changed: the fixture's synthetic waits
            # are elided, while MAX_POLLS and all evidence assertions stay live.
            with mock.patch.object(relay.time, "sleep"):
                code = relay.main(argv)
        text = output.getvalue() + errors.getvalue()
        for secret in SECRETS:
            self.assertNotIn(secret, text)
        self.assertNotIn(str(self.env), text)
        self.assertEqual(errors.getvalue(), "")
        return code, json.loads(output.getvalue())

    def assert_single_soft_delete(self):
        writes = [item for item in self.fixture.requests if item["method"] in ("PUT", "DELETE")]
        self.assertEqual([item["method"] for item in writes], ["PUT", "DELETE"])
        self.assertEqual(writes[1]["query"], {"hard": ["0"]})
        self.assertEqual(writes[0]["path"], writes[1]["path"])

    def test_authorization_is_exact_and_precedes_env_and_network(self):
        for authorized in (None, "", "True", "TRUE", "tRuE", "false", "1", " true", "true ",
                           "\ttrue", "true\n", "true\u00a0"):
            with self.subTest(authorized=authorized):
                with mock.patch.object(relay, "private_env") as private, \
                        mock.patch.object(relay, "run", return_value={}) as run, \
                        mock.patch.object(relay, "HTTP") as http:
                    code, result = self.invoke(authorized=authorized)
                self.assertEqual((private.call_count, run.call_count, http.call_count), (0, 0, 0))
                self.assertEqual((code, result), (2, {"status": "not_executed", "reason": "not_authorized"}))
                self.assertFalse(self.fixture.requests)

    def test_success_and_request_boundary(self):
        before = self.env.stat(), self.env.read_bytes()
        self.fixture.delay_queries = 3
        code, result = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(result["reason"], "verified")
        self.assertEqual({fact["action"] for fact in result["facts"]}, {"file.created", "file.deleted"})
        self.assertEqual(len({fact["event_id"] for fact in result["facts"]}), 2)
        requests = self.fixture.requests
        writes = [item for item in requests if item["method"] in ("PUT", "DELETE")]
        self.assertEqual([item["method"] for item in writes], ["PUT", "DELETE"])
        self.assertEqual(writes[0]["headers"]["If-None-Match"], "*")
        self.assertNotEqual(writes[0]["headers"]["X-Request-Id"], writes[1]["headers"]["X-Request-Id"])
        self.assertEqual(writes[1]["query"], {"hard": ["0"]})
        for item in requests:
            if item["path"].startswith("/v1/files/"):
                self.assertEqual(item["headers"]["X-Aero-Tenant"], "platform-local")
        self.assertLess(len(self.fixture.content), 128)
        self.assertTrue(self.fixture.deleted)
        self.assertNotIn(self.fixture.key, json.dumps(result))
        self.assertEqual(len([item for item in requests if item["path"] == "/token"]), 2)
        audit_requests = [item for item in requests if item["path"].startswith("/api/")]
        for item in audit_requests:
            self.assertEqual(item["query"], {"tenant_id": ["platform-local"]})
            self.assertTrue(item["path"].endswith("events:query") or item["path"].endswith("/receipt"))
            self.assertNotIn("operation_id", item["query"])
            if item["method"] == "POST":
                body = json.loads(item["body"])
                self.assertNotIn("cursor", body)
                self.assertEqual(body["event_type"], relay.SCHEMA)
                self.assertTrue(body["operation_id"].startswith("hmac-sha256:"))
        self.assertEqual(self.env.stat().st_ino, before[0].st_ino)
        self.assertEqual(self.env.stat().st_mtime_ns, before[0].st_mtime_ns)
        self.assertEqual(stat.S_IMODE(self.env.stat().st_mode), 0o600)
        self.assertEqual(self.env.read_bytes(), before[1])
        self.assertEqual(list(Path(self.tmp.name).iterdir()), [self.env])

    def test_missing_delete_permission_stops_before_upload(self):
        self.fixture.token_status = 400
        code, result = self.invoke()
        self.assertEqual(code, 1)
        self.assertEqual(result["check"], "business_token")
        self.assertEqual(len(self.fixture.requests), 1)

    def test_reduced_granted_scope_stops_before_upload(self):
        self.fixture.token_scope = "read write"
        code, result = self.invoke()
        self.assertEqual((code, result["reason"]), (1, "insufficient_token_scope"))
        self.assertFalse(self.fixture.events)

    def test_only_business_token_http400_top_level_invalid_scope_is_precondition(self):
        cases = [
            ("aero-im-vault", 400, {"error": "invalid_scope"}, 2),
            ("aero-im-vault", 400, b'{"error":"invalid_\\u0073cope"}', 2),
            ("aero-im-vault", 400, {"error": "other", "error_description": "invalid_scope " + BACKEND_SECRET}, 1),
            ("aero-im-vault", 400, {"error": {"error": "invalid_scope"}}, 1),
            ("aero-im-vault", 400, {"nested": {"error": "invalid_scope"}}, 1),
            ("aero-im-vault", 400, {"error": ["invalid_scope"]}, 1),
            ("aero-im-vault", 400, [{"error": "invalid_scope"}], 1),
            ("aero-im-vault", 400, b'{"error":"invalid_scope",', 1),
            ("aero-im-vault", 400, b'invalid_scope ' + BACKEND_SECRET.encode(), 1),
            ("aero-im-vault", 400, {"error": "INVALID_SCOPE"}, 1),
            ("aero-im-vault", 400, {"error": " invalid_scope"}, 1),
            ("aero-im-vault", 400, {"error": "invalid_scope "}, 1),
            ("aero-im-vault", 400, {"error": "invalid_scope_extra"}, 1),
            ("aero-im-vault", 401, {"error": "invalid_scope"}, 1),
            ("aero-im-vault", 403, {"error": "invalid_scope"}, 1),
            ("aero-im-vault", 500, {"error": "invalid_scope"}, 1),
            ("platform-audit-bootstrap", 400, {"error": "invalid_scope"}, 1),
        ]
        for client, status, body, expected in cases:
            with self.subTest(client=client, status=status, body=body):
                self.fixture.reset()
                self.fixture.token_status = status
                self.fixture.token_error = body
                self.fixture.token_error_client = client
                code, result = self.invoke()
                self.assertEqual(code, expected)
                if expected == 2:
                    self.assertEqual(result, {"status": "not_executed", "reason": "missing_delete_scope"})
                else:
                    self.assertEqual(result, {"status": "failed", "reason": "http_status_" + str(status),
                                             "check": "business_token" if client == "aero-im-vault" else "audit_token"})
                self.assertEqual(len(self.fixture.requests), 1 if client == "aero-im-vault" else 2)
                self.assertTrue(all(item["path"] == "/token" for item in self.fixture.requests))

    def test_successful_http_status_with_invalid_scope_is_not_skip(self):
        self.fixture.mutate_token = lambda value, client: (value.clear(), value.update({"error": "invalid_scope"}))
        self.assertEqual(self.invoke(), (1, {"status": "failed", "reason": "invalid_token_response"}))
        self.assertEqual(len(self.fixture.requests), 1)

    def test_stale_invalid_scope_response_never_classifies_current_failure(self):
        for fault, reason in (("timeout", "service_unavailable"), ("redirect", "redirect_refused"),
                              ("oversized", "response_too_large"), ("truncated", "service_unavailable")):
            with self.subTest(fault=fault):
                self.fixture.reset()
                self.fixture.token_status = 400
                self.fixture.token_error = {"error": "invalid_scope"}
                http = relay.HTTP()
                args = (self.url + "/token", "aero-im-vault", BUSINESS_SECRET, relay.BUSINESS_SCOPE, "aero-vault")
                with self.assertRaises(relay.NotExecuted):
                    http.token(*args)
                self.fixture.token_status = 200
                if fault in ("redirect", "oversized"):
                    setattr(self.fixture, fault, "token")
                    failure = contextlib.nullcontext()
                else:
                    error = (TimeoutError(BACKEND_SECRET) if fault == "timeout" else
                             relay.http.client.IncompleteRead(b"invalid_scope " + BACKEND_SECRET.encode()))
                    failure = mock.patch.object(http.opener, "open", side_effect=error)
                with failure, self.assertRaises(relay.Failed) as raised:
                    http.token(*args)
                self.assertEqual(raised.exception.reason, reason)
                self.assertEqual(raised.exception.check, "business_token" if fault != "oversized" else "")

    def test_missing_empty_or_wrong_type_granted_scope_stops_before_put(self):
        for client in ("aero-im-vault", "platform-audit-bootstrap"):
            for scope in ("omitted", None, "", " \t", [], [relay.BUSINESS_SCOPE], {}, 1, True, "read"):
                with self.subTest(client=client, scope=scope):
                    self.fixture.reset()
                    def mutate(value, actual_client):
                        if actual_client == client:
                            if scope == "omitted":
                                value.pop("scope")
                            else:
                                value["scope"] = scope
                    self.fixture.mutate_token = mutate
                    self.assertEqual(self.invoke(), (1, {"status": "failed", "reason": "insufficient_token_scope"}))
                    self.assertEqual(len(self.fixture.requests), 1 if client == "aero-im-vault" else 2)
                    self.assertTrue(all(item["path"] == "/token" for item in self.fixture.requests))
                    self.assertFalse(self.fixture.events)

    def test_business_and_audit_errors_after_start_are_failures_not_skip(self):
        for target in ("upload_status", "readback_status", "delete_status", "audit_status"):
            for status in (400, 403):
                with self.subTest(target=target, status=status):
                    self.fixture.reset()
                    setattr(self.fixture, target, status)
                    self.fixture.upload_error = {"error": "invalid_scope", "error_description": BACKEND_SECRET}
                    code, result = self.invoke()
                    self.assertEqual((code, result["status"], result["reason"]),
                                     (1, "failed", "http_status_" + str(status)))
                    self.assertEqual(len([item for item in self.fixture.requests if item["method"] == "PUT"]), 1)
                    if target == "upload_status":
                        self.assertFalse(any(item["method"] == "DELETE" for item in self.fixture.requests))
                    else:
                        self.assert_single_soft_delete()

    def test_no_facts_is_bounded_failure_not_pass(self):
        self.fixture.no_relay = True
        code, result = self.invoke()
        self.assertEqual((code, result["reason"]), (1, "relay_timeout"))
        self.assertEqual(self.fixture.post_write_queries, 2 * relay.MAX_POLLS)
        self.assertTrue(self.fixture.deleted)

    def test_preexisting_fact_stops_before_upload(self):
        self.fixture.preexisting = True
        self.assertEqual(self.invoke()[1]["reason"], "preexisting_request_fact")
        self.assertFalse(self.fixture.events)

    def test_no_pagination_truncation_success(self):
        self.fixture.next_cursor = True
        self.assertEqual(self.invoke()[1]["reason"], "audit_result_limit_or_shape")
        self.assertFalse(self.fixture.events)

    def test_readback_failure_still_soft_deletes(self):
        self.fixture.readback_bad = True
        code, result = self.invoke()
        self.assertEqual((code, result["status"], result["reason"]), (1, "failed", "readback_mismatch"))
        self.assertTrue(self.fixture.deleted)
        self.assert_single_soft_delete()
        self.assertFalse(any(item["path"].endswith("/receipt") for item in self.fixture.requests))

    def test_upload_metadata_failure_still_soft_deletes(self):
        self.fixture.upload_bad = True
        code, result = self.invoke()
        self.assertEqual((code, result["status"], result["reason"]), (1, "failed", "upload_response_mismatch"))
        self.assertTrue(self.fixture.deleted)
        self.assert_single_soft_delete()

    def test_acknowledged_upload_with_bad_json_still_soft_deletes_once(self):
        self.fixture.upload_json_bad = True
        self.assertEqual(self.invoke(), (1, {"status": "failed", "reason": "invalid_json"}))
        self.assertTrue(self.fixture.deleted)
        self.assert_single_soft_delete()
        self.assertFalse(any(item["method"] == "GET" for item in self.fixture.requests))

    def test_transport_failures_never_retry_or_guess_rollback(self):
        real_open = relay.urllib.request.OpenerDirector.open
        cases = (("PUT", "/v1/files/", "vault_upload"),
                 ("GET", "/v1/files/", "vault_readback"),
                 ("DELETE", "/v1/files/", "vault_soft_delete"),
                 ("POST", "/api/v1/events:query", "audit_query"),
                 ("GET", "/api/v1/events/", "audit_receipt"))
        for method, path, check in cases:
            with self.subTest(method=method, check=check):
                self.fixture.reset()
                failures = []
                def lose_response(opener, request, *args, **kwargs):
                    # The synthetic server applies the operation, but its reply
                    # never reaches the caller. No waits or real platform I/O.
                    response = real_open(opener, request, *args, **kwargs)
                    if (request.get_method() == method and urlsplit(request.full_url).path.startswith(path)
                            and (check != "audit_query" or self.fixture.events)):
                        response.close()
                        failures.append(request)
                        raise TimeoutError(BACKEND_SECRET)
                    return response
                with mock.patch.object(relay.urllib.request.OpenerDirector, "open", autospec=True,
                                       side_effect=lose_response):
                    self.assertEqual(self.invoke(), (1, {"status": "failed", "reason": "service_unavailable", "check": check}))
                self.assertEqual(len(failures), 1)
                puts = [item for item in self.fixture.requests if item["method"] == "PUT"]
                self.assertEqual(len(puts), 1)
                if method == "PUT":
                    self.assertFalse(self.fixture.deleted)
                    self.assertEqual([event["action"] for event in self.fixture.events], ["file.created"])
                    self.assertFalse(any(item["method"] in ("GET", "DELETE") for item in self.fixture.requests))
                else:
                    self.assertTrue(self.fixture.deleted)
                    self.assert_single_soft_delete()
                    if method == "DELETE":
                        deletes = [i for i, item in enumerate(self.fixture.requests) if item["method"] == "DELETE"]
                        self.assertEqual(deletes, [len(self.fixture.requests) - 1])

    def test_upload_rejection_does_not_delete_or_retry(self):
        self.fixture.upload_status = 409
        self.assertEqual(self.invoke()[0], 1)
        self.assertEqual(len([item for item in self.fixture.requests if item["method"] == "PUT"]), 1)
        self.assertFalse(any(item["method"] == "DELETE" for item in self.fixture.requests))

    def test_soft_delete_denial_and_postdelete_read_fail(self):
        for attr, value in (("delete_status", 403), ("deleted_read_status", 200)):
            with self.subTest(attr=attr):
                self.fixture.reset()
                setattr(self.fixture, attr, value)
                self.assertEqual(self.invoke()[0], 1)
                self.assert_single_soft_delete()
                self.assertFalse(any(item["path"].endswith("/receipt") for item in self.fixture.requests))

    def test_fact_identity_contract_and_freshness_fail_closed(self):
        mutations = {
            "tenant_id": "other-tenant", "source_system": "aero-im.source", "event_type": "other",
            "schema_id": "other", "schema_version": True, "operation_id": "wrong-request",
            "action": "file.accessed", "outcome": "failure", "aggregate_type": "admin",
            "aggregate_id": "wrong-target", "targets": [], "data_classification": "public",
            "retention_class": "other", "payload": {"fact_kind": "file", "request_id": "wrong"},
            "occurred_at": "2000-01-01T00:00:00Z", "sequence": True, "hash": "", "event_id": "unsafe/id",
        }
        for field, value in mutations.items():
            with self.subTest(field=field):
                self.fixture.reset()
                self.fixture.mutate_event = lambda event, key=field, val=value: event.update({key: val})
                self.assertEqual(self.invoke()[0], 1)

    def test_receipt_identity_chain_and_ledger_status_fail_closed(self):
        mutations = {"event_id": "other", "tenant_id": "other", "stream_id": "other", "sequence": 99,
                     "hash": "b" * 64, "status": "accepted", "ledgered_at": "0001-01-01T00:00:00Z", "conflict": True}
        for field, value in mutations.items():
            with self.subTest(field=field):
                self.fixture.reset()
                self.fixture.mutate_receipt = lambda receipt, key=field, val=value: receipt.update({key: val})
                self.assertEqual(self.invoke()[0], 1)

    def test_audit_no_store_required(self):
        for category in ("query", "receipt"):
            with self.subTest(category=category):
                self.fixture.reset()
                self.fixture.no_store = category
                self.assertEqual(self.invoke()[1]["reason"], "no_store_missing")

    def test_redirects_refused_at_every_service(self):
        for category in ("token", "vault", "query", "receipt"):
            with self.subTest(category=category):
                self.fixture.reset()
                self.fixture.redirect = category
                self.assertEqual(self.invoke()[1]["reason"], "redirect_refused")
                self.assertFalse(any(item["path"] == "/credential-sink" for item in self.fixture.requests))

    def test_bounded_invalid_and_secret_bodies(self):
        for attr, reason in (("oversized", "response_too_large"), ("malformed", "invalid_json")):
            for category in ("token", "query", "receipt"):
                with self.subTest(attr=attr, category=category):
                    self.fixture.reset()
                    setattr(self.fixture, attr, category)
                    code, result = self.invoke()
                    self.assertEqual((code, result["reason"]), (1, reason))

    def test_invalid_endpoints_never_contact_service(self):
        for option in ("--vault-url", "--audit-url", "--token-url"):
            for url in ("http://example.com", "http://user:password@localhost", "http://localhost/?secret=x",
                        "http://localhost/#secret", "file:///tmp/private", "http://localhost:0"):
                with self.subTest(option=option, url=url):
                    self.fixture.reset()
                    self.assertEqual(self.invoke(option, url)[0], 2)
                    self.assertFalse(self.fixture.requests)

    def test_missing_and_unsafe_env_never_contact_service(self):
        for mode in (0o644, 0o660):
            self.env.chmod(mode)
            self.assertEqual(self.invoke()[1]["reason"], "unsafe_env_file")
            self.assertFalse(self.fixture.requests)
        self.env.chmod(0o600)
        self.env.unlink()
        self.assertEqual(self.invoke()[0], 2)
        self.assertFalse(self.fixture.requests)

    def test_env_whitelist_literals_and_no_execution_or_export(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            values = relay.private_env(str(self.env))
            self.assertEqual(values.keys(), relay.SECRET_KEYS)
            self.assertFalse(set(values) & os.environ.keys())
        self.assertFalse((Path.cwd() / "should-never-run").exists())

    def test_invalid_whitelist_values(self):
        original = self.env.read_text()
        suffixes = ["AERO_VAULT_AUDIT_HMAC_KEY=duplicate\n", "AERO_VAULT_AUDIT_HMAC_KEY=$(false)\n"]
        for suffix in suffixes:
            self.env.write_text(original + suffix)
            self.assertEqual(self.invoke()[0], 2)
            self.assertFalse(self.fixture.requests)
        self.env.write_text(original.replace(HMAC_KEY, "short"))
        self.assertEqual(self.invoke()[1]["reason"], "invalid_hmac_key")

    def test_env_symlink_and_hardlink_rejected(self):
        moved = self.env.with_name("original.env")
        self.env.rename(moved)
        self.env.symlink_to(moved)
        self.assertEqual(self.invoke()[0], 2)
        self.env.unlink()
        os.link(moved, self.env)
        self.assertEqual(self.invoke()[1]["reason"], "unsafe_env_file")
        self.assertFalse(self.fixture.requests)

    def test_hmac_contract_vectors_and_domains(self):
        vectors = {("request", "request-fixture"): "-92wSX1TH7wurJ9fzbI8pcyEeIOIF3vP20U3hv37DHs",
                   ("source-system", "tenant-fixture"): "Xf3Kafq63wZK-IyeKyk_9GX28O25I7Lbk4ktMASWLqA",
                   ("file-target", "default\0fixture.bin"): "TS0ScW4z5OeDmO6pD5ap2JPOEv79z9MDoKU25lzUMDE"}
        for (field, value), expected in vectors.items():
            self.assertEqual(relay.pseudonym(HMAC_KEY, "tenant-fixture", field, value), "hmac-sha256:" + expected)
        original = relay.pseudonym(HMAC_KEY, "tenant-fixture", "request", "same")
        self.assertNotEqual(original, relay.pseudonym(HMAC_KEY, "other-tenant", "request", "same"))
        self.assertNotEqual(original, relay.pseudonym(HMAC_KEY, "tenant-fixture", "file-target", "same"))

    def test_poll_deadline_not_just_round_limit(self):
        with mock.patch.object(relay, "query", return_value=[]) as query:
            with mock.patch.object(relay.time, "monotonic", side_effect=[0, 61]):
                with self.assertRaisesRegex(relay.Failed, "relay_timeout"):
                    relay.poll(None, {}, "", {"file.created": "one", "file.deleted": "two"}, None, None)
        self.assertEqual(query.call_count, 2)
        self.assertTrue(all(call.args[-1] == relay.POLL_SECONDS for call in query.call_args_list))

    def test_expired_http_deadline_makes_no_request(self):
        with mock.patch.object(relay.time, "monotonic", return_value=10):
            with self.assertRaisesRegex(relay.Failed, "relay_timeout"):
                relay.HTTP().call("GET", self.url, deadline=9)
        self.assertFalse(self.fixture.requests)

    def test_missing_oversize_or_special_env(self):
        with self.assertRaisesRegex(relay.NotExecuted, "missing_env_file"):
            relay.private_env("")
        self.env.write_bytes(b"x" * (relay.MAX_ENV + 1))
        self.assertEqual(self.invoke()[1]["reason"], "unsafe_env_file")
        self.env.unlink()
        os.mkfifo(self.env, 0o600)
        self.assertEqual(self.invoke()[1]["reason"], "unsafe_env_file")
        self.assertFalse(self.fixture.requests)

    def test_make_target_phony_and_help_are_wired(self):
        makefile = (Path(__file__).resolve().parents[2] / "Makefile").read_text()
        target = "e2e-platform-vault-relay"
        phony = next(line for line in makefile.splitlines() if line.startswith(".PHONY:"))
        help_line = next(line for line in makefile.splitlines() if 'echo "make targets:' in line)
        self.assertIn(target, phony.split())
        self.assertIn(target, help_line.split())
        self.assertIn(target + ":\n\tpython3 test/e2e/platform_vault_relay.py\n", makefile)

    def test_unexpected_exception_is_sanitized(self):
        with mock.patch.object(relay, "run", side_effect=RuntimeError(BACKEND_SECRET)):
            self.assertEqual(self.invoke(), (1, {"status": "failed", "reason": "internal_error"}))


if __name__ == "__main__":
    unittest.main()
