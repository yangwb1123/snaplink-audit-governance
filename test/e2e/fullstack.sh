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

log "starting stack (postgres/redpanda/clickhouse/minio/jaeger/audit-api/relay/consumer/projector)"
$COMPOSE up -d postgres redpanda clickhouse minio jaeger audit-api \
  audit-outbox-relay audit-kafka-consumer audit-projector

wait_http "$API/readyz" || { log "audit-api not ready"; exit 1; }
log "audit-api ready"

log "applying migrations"
$COMPOSE exec -T postgres psql -U audit -d audit -v ON_ERROR_STOP=1 \
  -f - < "${ROOT}/migrations/001_control_plane.sql" >/dev/null
$COMPOSE exec -T postgres psql -U audit -d audit -v ON_ERROR_STOP=1 \
  -f - < "${ROOT}/migrations/003_outbox_relay.sql" >/dev/null

log "ensuring clickhouse database"
curl -sf -u audit:audit-local-only -X POST \
  --data-binary 'CREATE DATABASE IF NOT EXISTS audit' "$CH/" >/dev/null

log "ensuring MinIO Object Lock bucket"
$COMPOSE exec -T minio mc alias set "$MINIO_ALIAS" http://localhost:9000 \
  audit-local audit-local-change-me >/dev/null
$COMPOSE exec -T minio mc mb --with-lock "$MINIO_ALIAS/worm-audit" >/dev/null 2>&1 || true

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
  -H 'Authorization: Bearer dev:demo:tenant-auditor' 2>/dev/null | grep -c "$EVENT_ID" || true)"
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
  -H 'Authorization: Bearer dev:demo:compliance' -H 'Content-Type: application/json' \
  -d '{}' 2>/dev/null | grep -o '"valid": true' || true)"
[ -n "$VALID" ] || { log "FAIL: integrity invalid"; exit 1; }
log "PASS: integrity valid"

log "verifying Jaeger trace export"
sleep 8
TRACES="$(curl -sf "$JAEGER/api/traces?service=audit-api&limit=1" | grep -o '"traceID"' | head -1 || true)"
[ -n "$TRACES" ] || { log "WARN: no Jaeger traces (export batch may lag)"; }
log "full-stack e2e PASS"
