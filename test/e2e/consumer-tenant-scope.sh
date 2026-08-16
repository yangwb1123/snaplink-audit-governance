#!/usr/bin/env bash
# Tenant-scope e2e for the Kafka consumer: with AUDIT_KAFKA_TENANT set
# (verify stack default demo), a foreign-tenant event on the shared accepted
# topic is skipped and committed — never ingested, never dead-lettered, never
# replayed into a permanent closure — and the partition keeps advancing for
# the instance's own tenant. Run via `make e2e-tenant-scope` (Docker-gated,
# outside cli.py quality, like fullstack.sh).
#
# Assumes the verify stack defaults: AUDIT_OUTBOX_TOKEN=dev:<TENANT>:service
# and AUDIT_KAFKA_TENANT=<TENANT> aligned (both default demo). Overriding
# AUDIT_KAFKA_TENANT requires overriding AUDIT_OUTBOX_TOKEN to the same
# tenant, and the audit-api must know that tenant (AUDIT_BOOTSTRAP_TENANT).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE="docker compose -f ${ROOT}/deploy/docker-compose.verify.yml"
API="http://localhost:19089"

TENANT="${AUDIT_KAFKA_TENANT:-demo}"
FOREIGN="other"
if [ "$FOREIGN" = "$TENANT" ]; then FOREIGN="other-tenant"; fi

log() { printf '[e2e-tenant-scope] %s\n' "$*"; }

wait_http() { # url tries
  local url="$1" tries="${2:-60}"
  for _ in $(seq 1 "$tries"); do
    if curl -sf "$url" >/dev/null 2>&1; then return 0; fi
    sleep 2
  done
  return 1
}

# The consumer publishes /metrics on :9092 INSIDE the container only (the
# compose service publishes no host port; host 19092 belongs to Redpanda), so
# the scrape uses busybox wget inside the container.
consumer_metric() { # metric_name -> value (or empty)
  $COMPOSE exec -T audit-kafka-consumer wget -qO- http://127.0.0.1:9092/metrics 2>/dev/null \
    | awk -v m="$1" '$1 == m { print $2 }'
}

log "starting infrastructure (postgres/redpanda/minio/jaeger)"
$COMPOSE up -d postgres redpanda minio jaeger

for _ in $(seq 1 30); do
  if $COMPOSE exec -T postgres pg_isready -U audit -d audit >/dev/null 2>&1; then
    break
  fi
  sleep 2
  log "waiting for postgres"
done

log "applying migrations before starting audit-api (bootstrap writes the PG snapshot)"
$COMPOSE exec -T postgres psql -U audit -d audit -v ON_ERROR_STOP=1 \
  -f - < "${ROOT}/migrations/001_control_plane.sql" >/dev/null
$COMPOSE exec -T postgres psql -U audit -d audit -v ON_ERROR_STOP=1 \
  -f - < "${ROOT}/migrations/003_outbox_relay.sql" >/dev/null
$COMPOSE exec -T postgres psql -U audit -d audit -v ON_ERROR_STOP=1 \
  -f - < "${ROOT}/migrations/004_state_snapshot.sql" >/dev/null

# audit-api probes the archive destination at boot (probeArchiveReady), so the
# WORM bucket must be fully bootstrapped before it starts (fullstack.sh parity).
log "bootstrapping MinIO Object Lock bucket"
for _ in $(seq 1 30); do
  if $COMPOSE exec -T minio mc alias set local http://localhost:9000 \
    audit-local audit-local-change-me >/dev/null 2>&1; then
    break
  fi
  sleep 2
  log "waiting for minio"
done
$COMPOSE exec -T minio mc mb --with-lock local/worm-audit >/dev/null 2>&1 || true
$COMPOSE exec -T minio mc version enable local/worm-audit >/dev/null 2>&1 || true
$COMPOSE exec -T minio mc retention set --default compliance 365d local/worm-audit >/dev/null

log "starting audit-api (AUDIT_KAFKA_TENANT=${TENANT})"
$COMPOSE up -d audit-api
wait_http "$API/readyz" || { log "audit-api not ready"; exit 1; }
log "audit-api ready"

$COMPOSE up -d audit-kafka-consumer

log "ensuring Kafka topics"
$COMPOSE exec -T redpanda rpk topic create audit.events.accepted.v1 \
  --partitions 1 --replicas 1 >/dev/null 2>&1 || true
$COMPOSE exec -T redpanda rpk topic create audit.events.dlq.v1 \
  --partitions 1 --replicas 1 >/dev/null 2>&1 || true

# Wait for the consumer's in-container metrics listener (bounded, then fail).
for _ in $(seq 1 30); do
  if [ -n "$(consumer_metric audit_consumer_foreign_tenant_skipped_total)" ]; then
    break
  fi
  sleep 1
  log "waiting for consumer metrics"
done
if [ -z "$(consumer_metric audit_consumer_foreign_tenant_skipped_total)" ]; then
  log "FAIL: consumer /metrics not reachable (AUDIT_KAFKA_METRICS set?)"
  exit 1
fi

# M1: counters are cumulative across restarts and Redpanda persists — assert
# DELTAS, never absolutes, so a non-fresh stack cannot flake.
SKIP_BEFORE="$(consumer_metric audit_consumer_foreign_tenant_skipped_total)"
DEAD_BEFORE="$(consumer_metric audit_consumer_dead_lettered_total)"

FOREIGN_ID="tenant-scope-foreign-$(date +%s)"
DEMO_ID="tenant-scope-demo-$(date +%s)"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

log "producing foreign-tenant event $FOREIGN_ID to partition 0"
echo "{\"event_id\":\"$FOREIGN_ID\",\"tenant_id\":\"$FOREIGN\",\"source_system\":\"demo\",\"event_type\":\"audit.event\",\"schema_id\":\"audit.event\",\"schema_version\":1,\"occurred_at\":\"$NOW\",\"actor\":{\"id\":\"user-1\"},\"action\":\"update\",\"outcome\":\"success\",\"data_classification\":\"internal\",\"retention_class\":\"standard\",\"idempotency_key\":\"ts-$FOREIGN_ID\",\"payload\":{\"note\":\"tenant scope e2e foreign\"}}" \
  | $COMPOSE exec -T redpanda rpk topic produce audit.events.accepted.v1 -k "$FOREIGN_ID" -p 0 >/dev/null

log "producing demo-tenant event $DEMO_ID to the same partition"
echo "{\"event_id\":\"$DEMO_ID\",\"tenant_id\":\"$TENANT\",\"source_system\":\"demo\",\"event_type\":\"audit.event\",\"schema_id\":\"audit.event\",\"schema_version\":1,\"occurred_at\":\"$NOW\",\"actor\":{\"id\":\"user-1\"},\"action\":\"update\",\"outcome\":\"success\",\"data_classification\":\"internal\",\"retention_class\":\"standard\",\"idempotency_key\":\"ts-$DEMO_ID\",\"payload\":{\"note\":\"tenant scope e2e own\"}}" \
  | $COMPOSE exec -T redpanda rpk topic produce audit.events.accepted.v1 -k "$DEMO_ID" -p 0 >/dev/null

log "waiting for the foreign skip (bounded)"
SKIP_OK=0
for _ in $(seq 1 30); do
  SKIP_NOW="$(consumer_metric audit_consumer_foreign_tenant_skipped_total)"
  if [ -n "$SKIP_NOW" ] && [ $((SKIP_NOW - SKIP_BEFORE)) -ge 1 ]; then
    SKIP_OK=1
    break
  fi
  sleep 1
done
if [ "$SKIP_OK" != 1 ]; then
  log "FAIL: audit_consumer_foreign_tenant_skipped_total did not advance"
  exit 1
fi

DEAD_NOW="$(consumer_metric audit_consumer_dead_lettered_total)"
if [ $((DEAD_NOW - DEAD_BEFORE)) -ne 0 ]; then
  log "FAIL: audit_consumer_dead_lettered_total advanced by $((DEAD_NOW - DEAD_BEFORE)), want 0 (skip must not dead-letter)"
  exit 1
fi
log "PASS: skip counter advanced by $((SKIP_NOW - SKIP_BEFORE)), dead_lettered delta 0"

log "asserting the skip log line carries both tenants (M3/FM-3 diagnostics)"
if ! $COMPOSE logs audit-kafka-consumer 2>/dev/null | grep -q "skipped foreign-tenant .*event_id=$FOREIGN_ID .*tenant=$FOREIGN .*consumer_tenant=$TENANT"; then
  $COMPOSE logs audit-kafka-consumer | tail -5
  log "FAIL: skip log line missing or diagnostic fields absent"
  exit 1
fi
log "PASS: skip log line carries tenant=$FOREIGN consumer_tenant=$TENANT"

log "waiting for the demo event to be ingested after the foreign skip (bounded)"
DEMO_OK=0
for _ in $(seq 1 30); do
  if $COMPOSE logs audit-kafka-consumer 2>/dev/null | grep -q "ingested .*event_id=$DEMO_ID"; then
    DEMO_OK=1
    break
  fi
  sleep 1
done
if [ "$DEMO_OK" != 1 ]; then
  $COMPOSE logs audit-kafka-consumer | tail -5
  log "FAIL: demo event not ingested (partition stalled by the skip?)"
  exit 1
fi
log "PASS: demo event ingested after the foreign skip (partition advanced)"

log "asserting the DLQ holds no Failure for the foreign event (bounded read, H1)"
DLQ_OUT="$(mktemp)"
# rpk topic consume without a bound blocks forever awaiting new messages, so
# the whole read is wrapped in a host-side timeout and captured to a file;
# absence is proven by grep -c == 0, never by grep -q on an unbounded stream.
if timeout 15 $COMPOSE exec -T redpanda rpk topic consume audit.events.dlq.v1 -p 0 -o 0 >"$DLQ_OUT" 2>/dev/null; then
  :
fi
if grep -q "$FOREIGN_ID" "$DLQ_OUT"; then
  log "FAIL: foreign event found on the DLQ:"
  grep "$FOREIGN_ID" "$DLQ_OUT" | head -3
  rm -f "$DLQ_OUT"
  exit 1
fi
rm -f "$DLQ_OUT"
log "PASS: foreign event absent from the DLQ (no permanent_error closure)"

log "tenant-scope e2e PASS"
