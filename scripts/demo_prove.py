#!/usr/bin/env python3
"""Prove the real demo API path, including a one-off copied-volume tamper test."""

from __future__ import annotations

import json
import os
import re
import sys
import time
from typing import Any

import demo_common as demo


REQUIRED_MEMBERS = set(demo.expected_seed_ids())
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def assert_health() -> None:
    demo.wait_http(demo.api_url() + "/readyz")
    health = demo.request("GET", "/healthz", demo.compliance_token()).json()
    if health.get("status") != "ok":
        raise demo.DemoError(f"healthz is not ok: {health}")
    ready = demo.request("GET", "/readyz", demo.compliance_token()).json()
    if ready.get("status") != "ready":
        raise demo.DemoError(f"readyz is not ready: {ready}")
    demo.log("PASS: healthz/readyz")


def assert_no_dev_fallback() -> None:
    response = demo.request(
        "GET",
        "/api/v1/events?from=2026-01-15T00%3A00%3A00Z&to=2026-01-16T00%3A00%3A00Z",
        "dev:demo:service",
        allow=(401,),
    )
    demo.expect_status(response, 401, "development-token rejection")
    demo.log("PASS: development-token fallback disabled")


def assert_ingest_and_idempotency(spec: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    first_event = spec["events"][0]
    original = demo.request(
        "GET",
        "/api/v1/events/" + first_event["event_id"] + "/receipt",
        demo.compliance_token(),
    ).json()
    if original.get("status") != "archived" or not original.get("hash"):
        raise demo.DemoError(f"seeded receipt is not archived: {original}")
    demo.assert_nonzero_time(original.get("ledgered_at"), "receipt.ledgered_at")

    duplicate = demo.request(
        "POST",
        "/api/v1/events?wait_for=archived",
        demo.writer_token(),
        first_event,
    ).json().get("receipt", {})
    if not duplicate.get("duplicate"):
        raise demo.DemoError(f"same-content ingest was not a duplicate: {duplicate}")
    for key in ("stream_id", "sequence", "hash"):
        if duplicate.get(key) != original.get(key):
            raise demo.DemoError(
                f"duplicate changed chain coordinate {key}: {duplicate.get(key)!r} != {original.get(key)!r}"
            )
    after_duplicate = demo.request(
        "GET",
        "/api/v1/events/" + first_event["event_id"] + "/receipt",
        demo.compliance_token(),
    ).json()
    if after_duplicate.get("hash") != original.get("hash") or after_duplicate.get("sequence") != original.get("sequence"):
        raise demo.DemoError("duplicate rewrite changed the durable receipt")

    changed = json.loads(json.dumps(first_event))
    changed["outcome"] = "denied"
    changed["payload"]["status"] = "cancelled"
    conflict = demo.request(
        "POST",
        "/api/v1/events?wait_for=archived",
        demo.writer_token(),
        changed,
        allow=(409,),
    )
    demo.expect_status(conflict, 409, "different-content conflict")
    error = conflict.json().get("error", {})
    if error.get("code") != "conflict":
        raise demo.DemoError(f"different-content ingest returned the wrong error: {error}")
    after_conflict = demo.request(
        "GET",
        "/api/v1/events/" + first_event["event_id"] + "/receipt",
        demo.compliance_token(),
    ).json()
    if after_conflict.get("hash") != original.get("hash") or after_conflict.get("sequence") != original.get("sequence"):
        raise demo.DemoError("conflicting ingest changed the original receipt")
    demo.log("PASS: archived receipt, duplicate coordinates unchanged, content conflict")
    return original, after_conflict


def assert_precise_query() -> None:
    result = demo.request(
        "POST",
        "/api/v1/events:query",
        demo.compliance_token(),
        demo.query_body(),
    ).json()
    if result.get("count") != len(REQUIRED_MEMBERS):
        raise demo.DemoError(f"precise query count={result.get('count')}, want {len(REQUIRED_MEMBERS)}")
    ids = {item.get("event_id") for item in result.get("items", [])}
    if ids != REQUIRED_MEMBERS:
        raise demo.DemoError(f"precise query members={sorted(ids)}, want {sorted(REQUIRED_MEMBERS)}")
    snapshot = result.get("snapshot") or {}
    if snapshot.get("source") != "ledger" or snapshot.get("completeness") != "not_asserted":
        raise demo.DemoError(f"query snapshot is not the expected ledger observation: {snapshot}")

    control = demo.request(
        "POST",
        "/api/v1/events:query",
        demo.compliance_token(),
        demo.query_body(demo.NOISE_OPERATION_ID),
    ).json()
    if control.get("count") != 1 or [item.get("event_id") for item in control.get("items", [])] != [
        "demo-seed-noise-0001"
    ]:
        raise demo.DemoError(f"precision-control query leaked or lost members: {control}")
    demo.log("PASS: precise fixed-membership query and precision control")


def create_export() -> dict[str, Any]:
    response = demo.request(
        "POST",
        "/api/v1/exports",
        demo.compliance_token(),
        demo.query_body(),
    )
    demo.expect_status(response, 202, "create export")
    job_id = response.json().get("id")
    if not job_id:
        raise demo.DemoError(f"create export returned no id: {response.text()}")
    deadline = time.time() + 60
    while time.time() < deadline:
        job = demo.request("GET", "/api/v1/exports/" + job_id, demo.compliance_token()).json()
        if job.get("status") == "completed":
            return job
        if job.get("status") in {"failed", "cancelled"}:
            raise demo.DemoError(f"export {job_id} ended as {job.get('status')}: {job}")
        time.sleep(0.5)
    raise demo.DemoError(f"export {job_id} did not complete within 60s")


def assert_export(job: dict[str, Any]) -> None:
    if job.get("event_count") != len(REQUIRED_MEMBERS):
        raise demo.DemoError(f"export count={job.get('event_count')}, want {len(REQUIRED_MEMBERS)}")
    if not SHA256_RE.fullmatch(str(job.get("digest", ""))):
        raise demo.DemoError(f"export digest is not a plain SHA-256: {job.get('digest')!r}")
    if not job.get("object_path"):
        raise demo.DemoError("completed export has no object_path")

    download = demo.request("GET", "/api/v1/exports/" + job["id"] + "/download", demo.compliance_token())
    if demo.sha256_hex(download.body) != job["digest"]:
        raise demo.DemoError("download body does not match the export digest")
    members = []
    for line in download.body.splitlines():
        if line.strip():
            members.append(json.loads(line))
    ids = {item.get("event_id") for item in members}
    if ids != REQUIRED_MEMBERS:
        raise demo.DemoError(f"export JSONL members={sorted(ids)}, want {sorted(REQUIRED_MEMBERS)}")
    if len(members) != len(REQUIRED_MEMBERS):
        raise demo.DemoError(f"export JSONL has {len(members)} lines, want {len(REQUIRED_MEMBERS)}")
    demo.log(f"PASS: export {job['id']} JSONL members={len(members)} digest={job['digest']}")


def assert_hold_gate(completed: dict[str, Any]) -> None:
    hold_response = demo.request(
        "POST",
        "/api/v1/legal-holds",
        demo.compliance_token(),
        {
            "name": "demo proof hold",
            "reason": "demonstrate current Hold export access gate",
            "filter": demo.query_body(),
        },
    )
    demo.expect_status(hold_response, 201, "create legal hold")
    hold = hold_response.json()
    hold_id = hold.get("id")
    if not hold_id:
        raise demo.DemoError(f"legal hold returned no id: {hold}")

    for surface, path in (
        ("create", "/api/v1/exports"),
        ("detail", "/api/v1/exports/" + completed["id"]),
        ("download", "/api/v1/exports/" + completed["id"] + "/download"),
    ):
        method = "POST" if surface == "create" else "GET"
        response = demo.request(
            method,
            path,
            demo.compliance_token(),
            demo.query_body() if surface == "create" else None,
            allow=(403,),
        )
        demo.expect_status(response, 403, f"Hold blocks export {surface}")
        error = response.json().get("error", {})
        if error.get("code") != "forbidden" or error.get("reason") != "legal_hold" or error.get("hold_id") != hold_id:
            raise demo.DemoError(f"Hold {surface} returned the wrong denial: {error}")

    release = demo.request(
        "POST",
        "/api/v1/legal-holds/" + hold_id + "/release",
        demo.compliance_token(),
        {},
    )
    demo.expect_status(release, 200, "release legal hold")
    if not release.json().get("released_at"):
        raise demo.DemoError("released Hold has no released_at")

    detail = demo.request("GET", "/api/v1/exports/" + completed["id"], demo.compliance_token())
    demo.expect_status(detail, 200, "export detail after release")
    if detail.json().get("digest") != completed["digest"]:
        raise demo.DemoError("released Hold did not restore the original detail")

    download = demo.request(
        "GET",
        "/api/v1/exports/" + completed["id"] + "/download",
        demo.compliance_token(),
    )
    demo.expect_status(download, 200, "export download after release")
    if demo.sha256_hex(download.body) != completed["digest"]:
        raise demo.DemoError("released Hold download digest changed")

    recovered = create_export()
    assert_export(recovered)
    demo.log("PASS: later Hold blocks create/detail/download; release restores all three")


def assert_integrity(base: str | None = None) -> dict[str, Any]:
    response = demo.request(
        "POST",
        "/api/v1/integrity/verify",
        demo.compliance_token(),
        {},
        base=base,
    )
    demo.expect_status(response, 200, "integrity verify")
    result = response.json()
    return result


def assert_healthy_integrity() -> None:
    result = assert_integrity()
    if result.get("valid") is not True:
        raise demo.DemoError(f"healthy integrity is invalid: {result}")
    if result.get("event_count", 0) < len(REQUIRED_MEMBERS):
        raise demo.DemoError(f"healthy integrity saw too few events: {result}")
    if result.get("segment_count", 0) < 1:
        raise demo.DemoError(f"healthy integrity saw no signed segments: {result}")
    if result.get("errors"):
        raise demo.DemoError(f"healthy integrity reported errors: {result}")
    demo.log(
        f"PASS: healthy integrity valid events={result.get('event_count')} "
        f"segments={result.get('segment_count')}"
    )


def tamper_one_off_copy() -> None:
    profile = demo.env("DEMO_PROFILE", "core")
    if profile != "core":
        demo.log("SKIP: local-volume tamper proof is core-profile only (advanced uses MinIO WORM)")
        return
    service = "audit-api-core"
    port = demo.free_loopback_port()
    os.environ["DEMO_TAMPER_API_PORT"] = str(port)

    demo.log(f"stopping the only core writer ({service}) before copying its volume")
    demo.compose("stop", service)
    running = demo.compose("ps", "-q", "--status", "running", service).strip()
    if running:
        raise demo.DemoError(f"writer {service} is still running before the copy: {running}")

    copy_script = r"""set -eu
rm -rf /target/* /target/.[!.]* /target/..?*
cp -a /source/. /target/
ledger="$(find /target/ledger -type f -name '*.jsonl' | head -n 1)"
[ -n "$ledger" ]
grep -q '"record_type":"segment"' "$ledger"
awk 'BEGIN { done=0 } !done && /"record_type":"segment"/ { sub(/"signature":"[^"]*"/, "\"signature\":\"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\""); done=1 } { print } END { if (!done) exit 2 }' "$ledger" > "$ledger.tampered"
mv "$ledger.tampered" "$ledger"
chown -R audit:audit /target
chmod 600 "$ledger"
grep -q 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' "$ledger"
"""
    demo.compose(
        "--profile",
        "tools",
        "run",
        "--rm",
        "--no-deps",
        "demo-copy",
        copy_script,
    )
    demo.log("own disposable copy created; changed one segment signature in the copy only")

    try:
        demo.compose(
            "--profile",
            "tools",
            "up",
            "-d",
            "--no-build",
            "--force-recreate",
            "audit-api-tamper",
        )
        tamper_base = f"http://127.0.0.1:{port}"
        demo.wait_http(tamper_base + "/readyz", tries=45)
        result = assert_integrity(tamper_base)
        if result.get("valid") is not False:
            raise demo.DemoError(f"tampered copy still verified as valid: {result}")
        errors = " ".join(result.get("errors") or [])
        if "signature mismatch" not in errors:
            raise demo.DemoError(f"tamper was not detected as a signature failure: {result}")
        demo.compose("stop", "audit-api-tamper")
    except Exception:
        demo.log("FAIL: leaving the stopped original writer and the tamper container in place for inspection")
        raise

    demo.log("restarting the untouched original writer")
    demo.compose("start", service)
    demo.wait_http(demo.api_url() + "/readyz")
    healthy = assert_integrity()
    if healthy.get("valid") is not True:
        raise demo.DemoError(f"original volume was not the healthy control after tamper test: {healthy}")
    demo.log("PASS: tampered copied disk was detected as invalid; original volume stayed healthy")


def assert_advanced_worker() -> None:
    if demo.env("DEMO_PROFILE", "core") != "advanced":
        return
    services = {
        line.strip()
        for line in demo.compose("ps", "--status", "running", "--services").splitlines()
        if line.strip()
    }
    if "audit-governance-worker" not in services:
        raise demo.DemoError(f"advanced governance worker is not running: {sorted(services)}")
    demo.log("PASS: advanced PostgreSQL/MinIO API and governance worker are running")


def main() -> int:
    try:
        spec = demo.seed_spec()
        manifest = demo.read_manifest()
        if manifest.get("fingerprint") != spec["fingerprint"]:
            raise demo.DemoError("seed manifest fingerprint does not match the current deterministic seed")
        if demo.canonical(manifest) != demo.canonical(spec):
            raise demo.DemoError("seed manifest bytes differ from the current deterministic seed")

        assert_health()
        assert_no_dev_fallback()
        assert_ingest_and_idempotency(spec)
        assert_precise_query()
        completed = create_export()
        assert_export(completed)
        assert_hold_gate(completed)
        assert_advanced_worker()
        assert_healthy_integrity()
        tamper_one_off_copy()
        demo.log(f"PASS: demo-prove profile={demo.env('DEMO_PROFILE', 'core')}")
        return 0
    except demo.DemoError as exc:
        demo.fail(str(exc))
    except Exception as exc:
        demo.fail(f"unexpected {type(exc).__name__}: {exc}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
