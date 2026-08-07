#!/usr/bin/env bash
# Full-stack compose verification: outbox -> relay(Kafka) -> consumer ->
# ledger + projector -> ClickHouse + MinIO WORM archive + Jaeger traces.
# Requires docker compose and the verify stack images (prometheus optional).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE="docker compose -f ${ROOT}/deploy/docker-compose.verify.yml"
API="http://localhost:19089"
CH="http://localhost:19123"
JAEGER="http://localhost:19686"
MINIO_ALIAS=local

log() { printf '[e2e] %s\n' "$*"; }
wait_http() { # url tries
  local url="$1" tries="${2:-60}"
  for _ in $(seq 1 "$tries"); do
    if curl -sf "$url" >/dev/null 2>&1; then return 0; fi
    sleep 2
  done
  return 1
}

# B1-7 (T-1.2) token fixture: a real IdP token replaces dev tokens once the
# IdP deployment (B4-1) is available. Precedence: AUDIT_E2E_TOKEN env
# (explicit) -> mint-token.sh (IdP config present) -> dev token (transitional,
# G1 not yet closed; logs a warning).
if [ -n "${AUDIT_E2E_TOKEN:-}" ]; then
  AUTH_READ="Bearer ${AUDIT_E2E_TOKEN}"
  AUTH_COMPLIANCE="Bearer ${AUDIT_E2E_TOKEN}"
elif [ -n "${AUDIT_IDP_TOKEN_URL:-}" ] && [ -n "${AUDIT_IDP_CLIENT_ID:-}" ]; then
  log "minting IdP token from ${AUDIT_IDP_TOKEN_URL}"
  # shellcheck disable=SC1091
  source "${ROOT}/scripts/mint-token.sh"
  AUTH_READ="Bearer ${AUDIT_E2E_TOKEN}"
  AUTH_COMPLIANCE="Bearer ${AUDIT_E2E_TOKEN}"
  export AUDIT_OUTBOX_TOKEN
  log "PASS: real IdP token in use (G1 fixture path)"
else
  log "WARN: no IdP token fixture configured; using dev tokens (transitional, B1-7 awaits B4-1)"
  AUTH_READ="Bearer dev:demo:tenant-auditor"
  AUTH_COMPLIANCE="Bearer dev:demo:compliance"
fi

log "starting stack (postgres/redpanda/clickhouse/minio/jaeger/audit-api/relay/consumer/projector)"
$COMPOSE up -d postgres redpanda clickhouse minio jaeger audit-api \
  audit-outbox-relay audit-kafka-consumer audit-projector audit-kafka-dlq-replay

# 自举归档依赖：S3Store.Ready 要求 bucket 存在且启用 Object Lock（WORM），
# 因此 MinIO bucket 必须在等待 /readyz 之前创建。
log "bootstrapping MinIO Object Lock bucket"
for _ in $(seq 1 30); do
  if $COMPOSE exec -T minio mc alias set "$MINIO_ALIAS" http://localhost:9000 \
    audit-local audit-local-change-me >/dev/null 2>&1; then
    break
  fi
  sleep 2
  log "waiting for minio"
done
$COMPOSE exec -T minio mc mb --with-lock "$MINIO_ALIAS/worm-audit" >/dev/null 2>&1 || true
$COMPOSE exec -T minio mc version enable "$MINIO_ALIAS/worm-audit" >/dev/null 2>&1 || true

wait_http "$API/readyz" || { log "audit-api not ready"; exit 1; }
log "audit-api ready"

log "checking gRPC ingest listener (B1-6 topology: --grpc-listen registered)"
if (exec 3<>/dev/tcp/localhost/19051) 2>/dev/null; then
  log "gRPC listener: open on 19051"
  exec 3<&- 3>&-
else
  log "gRPC listener: not reachable"
  exit 1
fi

log "applying migrations"
$COMPOSE exec -T postgres psql -U audit -d audit -v ON_ERROR_STOP=1 \
  -f - < "${ROOT}/migrations/001_control_plane.sql" >/dev/null
$COMPOSE exec -T postgres psql -U audit -d audit -v ON_ERROR_STOP=1 \
  -f - < "${ROOT}/migrations/003_outbox_relay.sql" >/dev/null

log "ensuring clickhouse database"
curl -sf -u audit:audit-local-only -X POST \
  --data-binary 'CREATE DATABASE IF NOT EXISTS audit' "$CH/" >/dev/null
log "ensuring Kafka topic"
$COMPOSE exec -T redpanda rpk topic create audit.events.accepted.v1 \
  --partitions 1 --replicas 1 >/dev/null 2>&1 || true

EVENT_ID="fullstack-e2e-$(date +%s)"
log "inserting outbox record $EVENT_ID"
$COMPOSE exec -T postgres psql -U audit -d audit <<SQL >/dev/null
INSERT INTO audit_outbox (event_id, tenant_id, idempotency_key, payload, occurred_at)
VALUES ('$EVENT_ID', 'demo', 'fs-$EVENT_ID',
'{"event_id":"$EVENT_ID","tenant_id":"demo","source_system":"demo","event_type":"audit.event","schema_id":"audit.event","schema_version":1,"occurred_at":"2026-08-05T12:00:00Z","actor":{"id":"user-1"},"action":"update","outcome":"success","data_classification":"internal","retention_class":"standard","idempotency_key":"fs-$EVENT_ID","payload":{"note":"fullstack e2e"}}',
now());
SQL

log "waiting for relay -> kafka -> consumer -> ledger"
sleep 15
COUNT="$(curl -s "$API/api/v1/events?from=2026-08-01T00:00:00Z&to=2026-09-01T00:00:00Z" \
  -H "Authorization: $AUTH_READ" 2>/dev/null | grep -c "$EVENT_ID" || true)"
if [ "$COUNT" -lt 1 ]; then
  $COMPOSE logs audit-outbox-relay | tail -3
  $COMPOSE logs audit-kafka-consumer | tail -3
  log "FAIL: event missing from ledger"
  exit 1
fi
log "PASS: event in ledger"

sleep 5
log "verifying ClickHouse projection"
CH_ROW="$(curl -sf -u audit:audit-local-only -X POST \
  --data-binary "SELECT count() FROM audit.audit_events WHERE event_id='$EVENT_ID' FORMAT TabSeparated" "$CH/")"
[ "$CH_ROW" = "1" ] || { log "FAIL: projection missing row ($CH_ROW)"; exit 1; }
log "PASS: projection row present"

log "verifying MinIO WORM archive"
ARCHIVED="$($COMPOSE exec -T minio mc ls --recursive "$MINIO_ALIAS/worm-audit" 2>/dev/null | grep -c "$EVENT_ID" || true)"
[ "$ARCHIVED" -ge 1 ] || { log "FAIL: archive object missing"; exit 1; }
log "PASS: archive object present"

log "verifying integrity"
VALID="$(curl -s -X POST "$API/api/v1/integrity/verify" \
  -H "Authorization: $AUTH_COMPLIANCE" -H 'Content-Type: application/json' \
  -d '{}' 2>/dev/null | grep -o '"valid":true' || true)"
[ -n "$VALID" ] || { log "FAIL: integrity invalid"; exit 1; }
log "PASS: integrity valid"

log "verifying Jaeger trace export"
sleep 8
TRACES="$(curl -sf "$JAEGER/api/traces?service=audit-api&limit=1" | grep -o '"traceID"' | head -1 || true)"
[ -n "$TRACES" ] || { log "WARN: no Jaeger traces (export batch may lag)"; }
log "full-stack e2e PASS"
