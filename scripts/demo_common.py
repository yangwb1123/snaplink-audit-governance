#!/usr/bin/env python3
"""Shared, dependency-free helpers for the isolated demo seed and proof."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SEED_VERSION = 1
TENANT_DEFAULT = "demo"
SOURCE_ID = "demo.crm"
SCHEMA_ID = "demo.order"
SCHEMA_VERSION = 1
EVENT_TYPE = "demo.order.changed"
WRITER_CLIENT = "demo-writer"
OPERATION_ID = "demo-op-order-0001"
NOISE_OPERATION_ID = "demo-op-noise-0001"
SEED_FROM = "2026-01-15T09:00:00Z"
SEED_TO = "2026-01-15T09:10:00Z"


class DemoError(RuntimeError):
    pass


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def tenant() -> str:
    return env("DEMO_TENANT", TENANT_DEFAULT)


def api_url() -> str:
    value = env("DEMO_API_URL")
    if not value:
        value = f"http://127.0.0.1:{env('DEMO_API_PORT', '18089')}"
    return value.rstrip("/")


def run_dir() -> Path:
    value = env("DEMO_RUN_DIR")
    if not value:
        raise DemoError("DEMO_RUN_DIR is not set; run make demo-up first")
    return Path(value)


def manifest_path() -> Path:
    return run_dir() / "seed.manifest.json"


def log(message: str) -> None:
    print(f"[demo] {message}", flush=True)


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_hex(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def mint_token(subject: str, client_id: str, roles: list[str], tenant_id: str | None = None) -> str:
    """Mint an explicit local HS256 token. There is no development-token fallback."""
    secret = env("DEMO_JWT_SECRET")
    if len(secret) < 32:
        raise DemoError("DEMO_JWT_SECRET is missing or shorter than 32 bytes")
    now = int(time.time())
    claims: dict[str, Any] = {
        "sub": subject,
        "client_id": client_id,
        "azp": client_id,
        "roles": roles,
        "iss": env("DEMO_JWT_ISSUER", "snaplink-audit-demo"),
        "aud": env("DEMO_JWT_AUDIENCE", "audit-governance"),
        "iat": now - 30,
        "exp": now + 3600,
    }
    if tenant_id:
        claims["tenant_id"] = tenant_id
    header = {"alg": "HS256", "typ": "JWT"}
    signing_input = f"{_b64url(canonical(header))}.{_b64url(canonical(claims))}".encode("ascii")
    signature = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return signing_input.decode("ascii") + "." + _b64url(signature)


def tenant_admin_token() -> str:
    return mint_token("demo-admin", "demo-admin", ["tenant-admin"], tenant())


def platform_token() -> str:
    return mint_token("demo-platform", "demo-platform", ["platform-admin"])


def writer_token() -> str:
    return mint_token("demo-writer", WRITER_CLIENT, ["service"], tenant())


def compliance_token() -> str:
    return mint_token("demo-compliance", "demo-compliance", ["compliance"], tenant())


class Response:
    def __init__(self, status: int, headers: dict[str, str], body: bytes):
        self.status = status
        self.headers = headers
        self.body = body

    def json(self) -> Any:
        try:
            return json.loads(self.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DemoError(f"response is not JSON (status {self.status}): {self.body[:200]!r}") from exc

    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")


def request(
    method: str,
    path: str,
    token: str,
    body: Any | None = None,
    *,
    timeout: float = 30.0,
    allow: tuple[int, ...] = (),
    base: str | None = None,
) -> Response:
    url = (base or api_url()).rstrip("/") + path
    data = None
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    if body is not None:
        data = canonical(body)
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            result = Response(response.status, dict(response.headers), response.read())
    except urllib.error.HTTPError as exc:
        result = Response(exc.code, dict(exc.headers), exc.read())
    except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
        raise DemoError(f"{method} {path} failed before an HTTP response: {exc}") from exc
    if result.status >= 500 or (result.status not in allow and not 200 <= result.status < 300):
        raise DemoError(f"{method} {path} returned HTTP {result.status}: {result.text()[:500]}")
    return result


def expect_status(response: Response, status: int, what: str) -> None:
    if response.status != status:
        raise DemoError(f"{what}: expected HTTP {status}, got {response.status}: {response.text()[:500]}")


def query_body(operation_id: str = OPERATION_ID) -> dict[str, Any]:
    return {
        "from": SEED_FROM,
        "to": SEED_TO,
        "event_type": EVENT_TYPE,
        "source_system": SOURCE_ID,
        "actor_id": "demo-user",
        "operation_id": operation_id,
    }


def seed_spec() -> dict[str, Any]:
    events = [
        {
            "event_id": "demo-seed-order-0001",
            "source_system": SOURCE_ID,
            "event_type": EVENT_TYPE,
            "schema_id": SCHEMA_ID,
            "schema_version": SCHEMA_VERSION,
            "occurred_at": "2026-01-15T09:00:00Z",
            "operation_id": OPERATION_ID,
            "correlation_id": "demo-correlation-order-0001",
            "actor": {"id": "demo-user", "type": "user", "name": "Demo User"},
            "targets": [{"type": "order", "id": "order-1001"}],
            "aggregate_type": "order",
            "aggregate_id": "order-1001",
            "aggregate_version": 1,
            "action": "order.created",
            "outcome": "success",
            "reason": "stable demo seed",
            "payload": {"order_id": "order-1001", "status": "created", "amount_minor": 1250},
            "data_classification": "internal",
            "retention_class": "standard",
            "idempotency_key": "demo-idem-order-0001",
        },
        {
            "event_id": "demo-seed-order-0002",
            "source_system": SOURCE_ID,
            "event_type": EVENT_TYPE,
            "schema_id": SCHEMA_ID,
            "schema_version": SCHEMA_VERSION,
            "occurred_at": "2026-01-15T09:01:00Z",
            "operation_id": OPERATION_ID,
            "correlation_id": "demo-correlation-order-0001",
            "actor": {"id": "demo-user", "type": "user", "name": "Demo User"},
            "targets": [{"type": "order", "id": "order-1001"}],
            "aggregate_type": "order",
            "aggregate_id": "order-1001",
            "aggregate_version": 2,
            "action": "order.paid",
            "outcome": "success",
            "reason": "stable demo seed",
            "payload": {"order_id": "order-1001", "status": "paid", "amount_minor": 1250},
            "data_classification": "internal",
            "retention_class": "standard",
            "idempotency_key": "demo-idem-order-0002",
        },
        {
            "event_id": "demo-seed-order-0003",
            "source_system": SOURCE_ID,
            "event_type": EVENT_TYPE,
            "schema_id": SCHEMA_ID,
            "schema_version": SCHEMA_VERSION,
            "occurred_at": "2026-01-15T09:02:00Z",
            "operation_id": OPERATION_ID,
            "correlation_id": "demo-correlation-order-0001",
            "actor": {"id": "demo-user", "type": "user", "name": "Demo User"},
            "targets": [{"type": "order", "id": "order-1001"}],
            "aggregate_type": "order",
            "aggregate_id": "order-1001",
            "aggregate_version": 3,
            "action": "order.fulfilled",
            "outcome": "success",
            "reason": "stable demo seed",
            "payload": {"order_id": "order-1001", "status": "fulfilled", "amount_minor": 1250},
            "data_classification": "internal",
            "retention_class": "standard",
            "idempotency_key": "demo-idem-order-0003",
        },
        {
            "event_id": "demo-seed-noise-0001",
            "source_system": SOURCE_ID,
            "event_type": EVENT_TYPE,
            "schema_id": SCHEMA_ID,
            "schema_version": SCHEMA_VERSION,
            "occurred_at": "2026-01-15T09:05:00Z",
            "operation_id": NOISE_OPERATION_ID,
            "correlation_id": "demo-correlation-noise-0001",
            "actor": {"id": "demo-user", "type": "user", "name": "Demo User"},
            "targets": [{"type": "order", "id": "order-2002"}],
            "aggregate_type": "order",
            "aggregate_id": "order-2002",
            "aggregate_version": 1,
            "action": "order.created",
            "outcome": "success",
            "reason": "stable precision-control seed",
            "payload": {"order_id": "order-2002", "status": "created", "amount_minor": 800},
            "data_classification": "internal",
            "retention_class": "standard",
            "idempotency_key": "demo-idem-noise-0001",
        },
    ]
    spec = {
        "version": SEED_VERSION,
        "tenant_id": tenant(),
        "source": {
            "id": SOURCE_ID,
            "name": "Demo CRM",
            "allowed_client_ids": [WRITER_CLIENT],
            "active": True,
        },
        "schema": {
            "schema_id": SCHEMA_ID,
            "version": SCHEMA_VERSION,
            "event_type": EVENT_TYPE,
            "required_fields": ["order_id", "status"],
            "allowed_fields": ["order_id", "status", "amount_minor", "note"],
            "classification": "internal",
            "active": True,
        },
        "events": events,
    }
    spec["fingerprint"] = sha256_hex(canonical(events))
    return spec


def persist_or_check_manifest(spec: dict[str, Any]) -> None:
    """Keep one durable record of the stable seed; never rewrite equal bytes."""
    path = manifest_path()
    current = canonical(spec)
    if path.exists():
        raw = path.read_bytes()
        try:
            previous = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DemoError(f"seed manifest is unreadable: {path}") from exc
        if previous.get("fingerprint") != spec["fingerprint"] or canonical(previous) != current:
            raise DemoError(
                "seed manifest differs from the current deterministic seed; "
                f"refusing to change seed bytes at {path}"
            )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_bytes(current + b"\n")
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def expected_seed_ids() -> list[str]:
    return [event["event_id"] for event in seed_spec()["events"][:3]]


def read_manifest() -> dict[str, Any]:
    path = manifest_path()
    if not path.exists():
        raise DemoError(f"seed manifest is missing: {path}; run make demo-seed")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DemoError(f"seed manifest is unreadable: {path}") from exc


def parse_time(value: str) -> datetime:
    # Go RFC3339Nano may carry 9 fractional digits; Python 3.10's
    # fromisoformat accepts at most 6. Trim the excess, not the instant.
    text = value.replace("Z", "+00:00")
    text = re.sub(r"(\.\d{6})\d+(?=[+-]\d\d:\d\d$)", r"\1", text)
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def assert_nonzero_time(value: Any, name: str) -> None:
    if not value:
        raise DemoError(f"{name} is empty")
    parsed = parse_time(str(value))
    if parsed.year < 2000:
        raise DemoError(f"{name} is not a real timestamp: {value}")


def compose(*args: str, check: bool = True) -> str:
    """Run a compose command without ever printing environment values."""
    command = [
        "docker",
        "compose",
        "-p",
        env("DEMO_PROJECT", "snaplink-audit-demo"),
        "-f",
        env("DEMO_COMPOSE_FILE", "deploy/docker-compose.demo.verify.yml"),
        "--env-file",
        env("DEMO_ENV_FILE", ""),
        *args,
    ]
    import subprocess

    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    if check and completed.returncode != 0:
        raise DemoError(
            f"compose {' '.join(args)} failed (exit {completed.returncode}): "
            f"{(completed.stderr or completed.stdout).strip()[:1000]}"
        )
    return (completed.stdout or "") + (completed.stderr or "")


def free_loopback_port() -> int:
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


def wait_http(url: str, tries: int = 60, delay: float = 1.0) -> None:
    last = ""
    for _ in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                if 200 <= response.status < 300:
                    return
                last = f"HTTP {response.status}"
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            last = str(exc)
        time.sleep(delay)
    raise DemoError(f"service did not become ready at {url}: {last}")


def fail(message: str) -> None:
    print(f"[demo] FAIL: {message}", file=sys.stderr, flush=True)
    raise SystemExit(1)
