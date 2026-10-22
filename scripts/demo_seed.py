#!/usr/bin/env python3
"""Register the exact demo source/schema and ingest the byte-stable seed."""

from __future__ import annotations

import sys
from typing import Any

import demo_common as demo


def ensure_tenant() -> None:
    tenants = demo.request("GET", "/api/v1/tenants", demo.platform_token()).json()
    if any(item.get("id") == demo.tenant() for item in tenants.get("items", [])):
        return
    body = {
        "id": demo.tenant(),
        "name": "Isolated demo tenant",
        "home_region": "local",
        "data_region": "local",
        "active": True,
    }
    response = demo.request("POST", "/api/v1/tenants", demo.platform_token(), body)
    demo.expect_status(response, 201, "create demo tenant")


def exact_source(spec: dict[str, Any]) -> None:
    wanted = spec["source"]
    page = demo.request("GET", "/api/v1/sources", demo.tenant_admin_token()).json()
    current = next((item for item in page.get("items", []) if item.get("id") == wanted["id"]), None)
    if current is None:
        body = dict(wanted, tenant_id=demo.tenant())
        response = demo.request("POST", "/api/v1/sources", demo.tenant_admin_token(), body)
        demo.expect_status(response, 201, "register exact source binding")
        current = response.json()
    checks = {
        "id": wanted["id"],
        "tenant_id": demo.tenant(),
        "name": wanted["name"],
        "allowed_client_ids": wanted["allowed_client_ids"],
        "active": wanted["active"],
    }
    for key, value in checks.items():
        if current.get(key) != value:
            raise demo.DemoError(
                f"source {wanted['id']} does not match the exact seed binding: "
                f"{key}={current.get(key)!r}, want {value!r}"
            )
    demo.log(f"source binding exact: {wanted['id']} -> {wanted['allowed_client_ids']}")


def exact_schema(spec: dict[str, Any]) -> None:
    wanted = spec["schema"]
    page = demo.request("GET", "/api/v1/schemas", demo.tenant_admin_token()).json()
    current = next(
        (
            item
            for item in page.get("items", [])
            if item.get("schema_id") == wanted["schema_id"] and item.get("version") == wanted["version"]
        ),
        None,
    )
    if current is None:
        body = dict(wanted, tenant_id=demo.tenant())
        response = demo.request("POST", "/api/v1/schemas", demo.tenant_admin_token(), body)
        demo.expect_status(response, 201, "register exact schema")
        current = response.json()
    checks = {
        "schema_id": wanted["schema_id"],
        "version": wanted["version"],
        "event_type": wanted["event_type"],
        "required_fields": wanted["required_fields"],
        "allowed_fields": wanted["allowed_fields"],
        "classification": wanted["classification"],
        "active": wanted["active"],
    }
    for key, value in checks.items():
        if current.get(key) != value:
            raise demo.DemoError(
                f"schema {wanted['schema_id']}@v{wanted['version']} does not match the exact seed: "
                f"{key}={current.get(key)!r}, want {value!r}"
            )
    demo.log(f"schema exact: {wanted['schema_id']}@v{wanted['version']}")


def ingest(spec: dict[str, Any]) -> None:
    receipts: dict[str, dict[str, Any]] = {}
    duplicate_count = 0
    for event in spec["events"]:
        response = demo.request(
            "POST",
            "/api/v1/events?wait_for=archived",
            demo.writer_token(),
            event,
        )
        demo.expect_status(response, 202, f"ingest {event['event_id']}")
        receipt = response.json().get("receipt", {})
        if receipt.get("event_id") != event["event_id"]:
            raise demo.DemoError(f"receipt event mismatch for {event['event_id']}: {receipt}")
        if receipt.get("tenant_id") != demo.tenant():
            raise demo.DemoError(f"receipt tenant mismatch for {event['event_id']}: {receipt}")
        if receipt.get("status") != "archived":
            raise demo.DemoError(
                f"seed event {event['event_id']} did not reach archived: {receipt.get('status')!r}"
            )
        for key in ("stream_id", "sequence", "hash", "ledgered_at", "archived_at"):
            if not receipt.get(key):
                raise demo.DemoError(f"receipt {event['event_id']} is missing {key}: {receipt}")
        if receipt.get("duplicate"):
            duplicate_count += 1
        coords = (receipt["stream_id"], receipt["sequence"], receipt["hash"])
        previous = receipts.setdefault(event["event_id"], receipt)
        if (previous["stream_id"], previous["sequence"], previous["hash"]) != coords:
            raise demo.DemoError(f"receipt coordinates changed inside one seed run: {event['event_id']}")
    demo.log(f"seed accepted: events={len(spec['events'])} duplicates={duplicate_count}")
    if duplicate_count not in (0, len(spec["events"])):
        raise demo.DemoError(
            f"partial pre-existing seed: {duplicate_count} duplicates of {len(spec['events'])} events"
        )


def main() -> int:
    try:
        demo.wait_http(demo.api_url() + "/readyz")
        spec = demo.seed_spec()
        demo.persist_or_check_manifest(spec)
        demo.log(f"seed manifest stable: sha256={spec['fingerprint']}")
        ensure_tenant()
        exact_source(spec)
        exact_schema(spec)
        ingest(spec)
        demo.log("PASS: demo-seed")
        return 0
    except demo.DemoError as exc:
        demo.fail(str(exc))
    return 1


if __name__ == "__main__":
    sys.exit(main())
