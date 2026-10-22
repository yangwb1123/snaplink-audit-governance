#!/usr/bin/env python3
"""Platform integration checks against a running local Aero platform.

This verifies the *contract* the Aero services rely on, from the outside:
Snaplink client_credentials -> audit-governance JWKS; the platform audit
bootstrap tenant/source/schema registrations; per-source ingest; idempotency,
tenant mismatch, schema-field and cross-source rejection; tenant query; and the
Snaplink Console compatibility read.

Required environment (secrets are read from env only, never argv/logs):
  PLATFORM_BOOTSTRAP_CLIENT_SECRET
  AERO_ID_AUDIT_SECRET
  AERO_IM_AUDIT_SECRET
  AERO_VAULT_AUDIT_SECRET
Optional:
  AUDIT_API_URL       default http://127.0.0.1:28089
  SNAPLINK_TOKEN_URL  default http://127.0.0.1:28080/token
  AUDIT_TENANT        default platform-local
  AUDIT_REPORT        optional JSON report path

Exit code 0 means every check passed. It is protocol-level evidence, not a
release approval; real Aero service relays are verified separately.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

API = os.environ.get("AUDIT_API_URL", "http://127.0.0.1:28089")
TOKEN_URL = os.environ.get("SNAPLINK_TOKEN_URL", "http://127.0.0.1:28080/token")
TENANT = os.environ.get("AUDIT_TENANT", "platform-local")

CLIENTS = {
    "aero-id-audit": ("AERO_ID_AUDIT_SECRET", "aero.id.audit-fact", "personal", {}),
    "aero-im-audit": ("AERO_IM_AUDIT_SECRET", "aero.im.security", "confidential", {}),
    "aero-vault-audit": (
        "AERO_VAULT_AUDIT_SECRET",
        "aero.vault.security",
        "confidential",
        {
            "fact_kind": "object.download",
            "object_size_bytes": 123,
            "request_id": "req-integration",
            "storage_backend": "s3",
            "detail_sha256": "a" * 64,
        },
    ),
}
PREFIXES = {
    "aero-id-audit": lambda value: value.startswith("aero-id."),
    "aero-im-audit": lambda value: value == "aero-im.source",
    "aero-vault-audit": lambda value: value.startswith("aero-vault."),
}


def env(name: str, default: str = "") -> str:
    value = os.environ.get(name, default).strip()
    if not value:
        raise SystemExit(f"missing required environment variable {name}")
    return value


def call(method: str, url: str, token: str = "", body=None):
    data = None if body is None else json.dumps(body, separators=(",", ":")).encode()
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Accept", "application/json")
    if body is not None:
        request.add_header("Content-Type", "application/json")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            raw = response.read(1 << 20)
            return response.status, dict(response.headers), json.loads(raw or b"{}")
    except urllib.error.HTTPError as error:
        raw = error.read(1 << 20)
        try:
            payload = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            payload = {}
        return error.code, dict(error.headers), payload


def err_code(body) -> str:
    return body.get("code") or (body.get("error") or {}).get("code", "")


def token(client_id: str, secret: str, scopes: str) -> str:
    form = urllib.parse.urlencode(
        {"grant_type": "client_credentials", "scope": scopes, "resource": "audit-governance"}
    ).encode()
    request = urllib.request.Request(TOKEN_URL, data=form, method="POST")
    request.add_header(
        "Authorization",
        "Basic " + base64.b64encode(f"{client_id}:{secret}".encode()).decode(),
    )
    request.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            value = json.load(response)
    except urllib.error.HTTPError as error:
        raise SystemExit(f"Snaplink token request failed for {client_id}: HTTP {error.code}") from None
    if not value.get("access_token"):
        raise SystemExit(f"Snaplink returned no access token for {client_id}")
    return value["access_token"]


def event(source: str, schema: str, classification: str, event_id: str, payload=None):
    return {
        "event_id": event_id,
        "source_system": source,
        "event_type": schema,
        "schema_id": schema,
        "schema_version": 1,
        "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "actor": {"id": "platform-integration"},
        "action": "verify",
        "outcome": "success",
        "data_classification": classification,
        "retention_class": "standard",
        "payload": payload or {},
        "idempotency_key": f"idem-{event_id}",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Aero platform audit integration checks")
    parser.add_argument("--report", default=os.environ.get("AUDIT_REPORT", ""))
    args = parser.parse_args()
    results: list[dict] = []

    def check(name: str, condition: bool, detail: str = "") -> None:
        results.append({"check": name, "ok": bool(condition), "detail": detail})
        print(("PASS " if condition else "FAIL ") + name + (f" :: {detail}" if detail else ""), flush=True)

    bootstrap = token(
        os.environ.get("PLATFORM_BOOTSTRAP_CLIENT_ID", "platform-audit-bootstrap"),
        env("PLATFORM_BOOTSTRAP_CLIENT_SECRET"),
        "audit:platform:cross_tenant audit:policy:read audit:policy:write",
    )
    status, _, tenants = call("GET", f"{API}/api/v1/tenants", bootstrap)
    check(
        "platform tenant is registered",
        status == 200 and any(item.get("id") == TENANT for item in tenants.get("items", [])),
        f"status={status}",
    )
    status, _, sources = call("GET", f"{API}/api/v1/sources?tenant_id={TENANT}", bootstrap)
    source_by_client = {
        client: source["id"]
        for source in sources.get("items", [])
        for client in source.get("allowed_client_ids", [])
    }
    check(
        "three Aero sources are registered",
        status == 200 and all(client in source_by_client for client in CLIENTS),
        json.dumps(sorted(source_by_client)),
    )
    check(
        "source ids follow the platform contract",
        all(PREFIXES[client](source_by_client.get(client, "")) for client in CLIENTS),
        json.dumps(source_by_client),
    )
    status, _, schemas = call("GET", f"{API}/api/v1/schemas?tenant_id={TENANT}", bootstrap)
    registered = {item.get("schema_id") for item in schemas.get("items", [])}
    check(
        "three Aero schemas are registered",
        status == 200 and {value[1] for value in CLIENTS.values()} <= registered,
        json.dumps(sorted(registered)),
    )

    created = {}
    for client, (secret_env, schema, classification, payload) in CLIENTS.items():
        if client not in source_by_client:
            check(f"{client} source exists", False)
            continue
        access = token(client, env(secret_env), "audit:event:write")
        event_id = f"it-{client}-{uuid.uuid4().hex[:12]}"
        body = event(source_by_client[client], schema, classification, event_id, payload)
        status, _, receipt = call("POST", f"{API}/api/v1/events?wait_for=ledgered", access, body)
        created[client] = (event_id, body, access)
        receipt_body = receipt.get("receipt", {})
        check(
            f"{client} ingests {schema}",
            status in (200, 201, 202) and receipt_body.get("status") in ("ledgered", "indexed", "archived"),
            f"status={status} receipt={receipt_body.get('status')}",
        )

    client = "aero-vault-audit"
    event_id, body, access = created[client]
    status, _, duplicate = call("POST", f"{API}/api/v1/events?wait_for=ledgered", access, body)
    check(
        "duplicate ingest stays idempotent",
        status in (200, 201, 202) and duplicate.get("receipt", {}).get("duplicate") is True,
        f"status={status}",
    )
    conflict = dict(body, payload=dict(body["payload"], object_size_bytes=999))
    status, _, conflict_body = call("POST", f"{API}/api/v1/events?wait_for=ledgered", access, conflict)
    check("same event id with different content conflicts", status == 409 and err_code(conflict_body) == "conflict", f"status={status} code={err_code(conflict_body)}")

    mismatch = event(source_by_client[client], "aero.vault.security", "confidential", f"it-mm-{uuid.uuid4().hex[:8]}", CLIENTS[client][3])
    mismatch["tenant_id"] = "other-tenant"
    status, _, mismatch_body = call("POST", f"{API}/api/v1/events?wait_for=ledgered", access, mismatch)
    check("tenant mismatch is rejected", status == 422 and err_code(mismatch_body) == "tenant_mismatch", f"status={status} code={err_code(mismatch_body)}")

    bad = event(source_by_client[client], "aero.vault.security", "confidential", f"it-bad-{uuid.uuid4().hex[:8]}", {"fact_kind": "x", "not_allowed": True})
    status, _, bad_body = call("POST", f"{API}/api/v1/events?wait_for=ledgered", access, bad)
    check("schema field rejection is enforced", status in (400, 422) and err_code(bad_body) == "invalid_request", f"status={status} code={err_code(bad_body)}")

    cross = event(source_by_client[client], "aero.vault.security", "confidential", f"it-cx-{uuid.uuid4().hex[:8]}", CLIENTS[client][3])
    status, _, cross_body = call("POST", f"{API}/api/v1/events?wait_for=ledgered", created["aero-im-audit"][2], cross)
    check("a client cannot use another source", status == 403 and err_code(cross_body) == "forbidden", f"status={status} code={err_code(cross_body)}")

    ids = [value[0] for value in created.values()]
    since = "2000-01-01T00:00:00Z"
    until = "2300-01-01T00:00:00Z"
    status, _, listing = call("GET", f"{API}/api/v1/events?from={since}&to={until}&tenant_id={TENANT}&page_size=100", bootstrap)
    listed = {item.get("event_id") for item in listing.get("items", [])}
    check("tenant query returns Aero facts", status == 200 and set(ids) <= listed, f"status={status} listed={len(listed)}")

    status, headers, compat = call("GET", f"{API}/api/v1/compat/snaplink/audit/events?since={since}&until={until}&limit=100&tenant_id={TENANT}", bootstrap)
    compat_ids = {item.get("event_id") for item in compat.get("events", [])}
    check("Snaplink Console compat read returns the facts", status == 200 and set(ids) <= compat_ids, f"status={status} count={compat.get('count')}")
    check("compat read is no-store", headers.get("Cache-Control") == "no-store", headers.get("Cache-Control", ""))
    status, _, filtered = call("GET", f"{API}/api/v1/compat/snaplink/audit/events?since={since}&until={until}&limit=100&client_id=aero-im.source", bootstrap)
    filtered_items = filtered.get("events", [])
    check("compat client_id narrows to the source", status == 200 and bool(filtered_items) and all(item.get("source_system") == "aero-im.source" for item in filtered_items), f"status={status} n={len(filtered_items)}")

    status, _, receipt_view = call("GET", f"{API}/api/v1/events/{ids[0]}/receipt?tenant_id={TENANT}", bootstrap)
    check("receipt metadata is readable", status == 200 and bool(receipt_view.get("hash")), f"status={status}")

    failures = [item for item in results if not item["ok"]]
    summary = {"checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "tenant": TENANT, "sources": source_by_client, "checks": results}
    if args.report:
        with open(args.report, "w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2)
            handle.write("\n")
    print(f"summary: {len(results) - len(failures)}/{len(results)} checks passed")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
