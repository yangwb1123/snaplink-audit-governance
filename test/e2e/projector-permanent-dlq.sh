#!/usr/bin/env bash
# Docker-gated e2e (AC-3 / REQ-4): cmd/audit-projector pointed at the
# accepted topic (AUDIT_KAFKA_TOPIC=audit.events.accepted.v1 — the documented
# misconfiguration window) dead-letters a pre-ledger event as
# error_code=permanent_error on the FIRST ingest attempt — never
# attempts_exhausted, zero retry/backoff log lines. Run via
# `make e2e-projector-permanent-dlq` (outside cli.py quality, like
# consumer-tenant-scope.sh / fullstack.sh).
#
# The compose audit-projector service keeps its ledgered default; this script
# runs a ONE-OFF container (audit-projector-accepted) with only the topic
# overridden (it inherits AUDIT_KAFKA_BROKERS/AUDIT_CLICKHOUSE_DSN from the
# service), asserts against its logs by container name, then stops and
# removes it. ClickHouse must be up (the projector pings at boot and runs
# EnsureSchema); migrations/audit-api are NOT required — the ingest guard
# (projection.go) rejects before any DB write.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE="docker compose -f ${ROOT}/deploy/docker-compose.verify.yml"
CH="http://localhost:19123"
PROJ="audit-projector-accepted"

log() { printf '[e2e-projector-permanent-dlq] %s\n' "$*"; }

# The one-off container must run the CURRENT source: rebuild the projector
# image (layer-cached; stale verify-stack images predate the classification
# branch and would silently exercise the old retry path).
log "building the audit-projector image from the current tree"
$COMPOSE build audit-projector

# Defensive cleanup: an interrupted previous run can leave the named one-off
# container behind (set -e exits before the trailing cleanup).
docker rm -f "$PROJ" >/dev/null 2>&1 || true

log "starting redpanda + clickhouse (subset; no migrations/audit-api needed)"
$COMPOSE up -d redpanda clickhouse

for _ in $(seq 1 30); do
  if curl -sf -u audit:audit-local-only "$CH/ping" >/dev/null 2>&1; then
    break
  fi
  sleep 2
  log "waiting for clickhouse"
done
curl -sf -u audit:audit-local-only -X POST \
  --data-binary 'CREATE DATABASE IF NOT EXISTS audit' "$CH/" >/dev/null

log "ensuring Kafka topics"
$COMPOSE exec -T redpanda rpk topic create audit.events.accepted.v1 \
  --partitions 1 --replicas 1 >/dev/null 2>&1 || true
$COMPOSE exec -T redpanda rpk topic create audit.events.dlq.v1 \
  --partitions 1 --replicas 1 >/dev/null 2>&1 || true

EVENT_ID="projector-perm-$(date +%s)"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "producing one pre-ledger event $EVENT_ID (no stream_id/sequence/hash) to partition 0"
echo "{\"event_id\":\"$EVENT_ID\",\"tenant_id\":\"demo\",\"source_system\":\"demo\",\"event_type\":\"audit.event\",\"schema_id\":\"audit.event\",\"schema_version\":1,\"occurred_at\":\"$NOW\",\"actor\":{\"id\":\"user-1\"},\"action\":\"update\",\"outcome\":\"success\",\"data_classification\":\"internal\",\"retention_class\":\"standard\",\"idempotency_key\":\"perm-$EVENT_ID\",\"payload\":{\"note\":\"projector permanent dlq e2e\"}}" \
  | $COMPOSE exec -T redpanda rpk topic produce audit.events.accepted.v1 -k "$EVENT_ID" -p 0 >/dev/null

log "starting the projector as a one-off container pointed at the accepted topic"
# Unique consumer group: a fresh group starts at the first retained offset and
# owns the partition deterministically, so a pre-existing audit-projector
# service container (e.g. a stale verify-stack member on the same default
# group) can never steal the partition under this run.
$COMPOSE run -d --rm --name "$PROJ" \
  -e AUDIT_KAFKA_TOPIC=audit.events.accepted.v1 \
  -e AUDIT_KAFKA_GROUP="audit-projector-e2e-$(date +%s)" \
  audit-projector

log "waiting for the dead-letter log line (bounded)"
DL_OK=0
for _ in $(seq 1 30); do
  if docker logs "$PROJ" 2>/dev/null | grep -q "dead-lettered .*event_id=$EVENT_ID .*code=permanent_error"; then
    DL_OK=1
    break
  fi
  sleep 1
done
if [ "$DL_OK" != 1 ]; then
  docker logs "$PROJ" | tail -10
  log "FAIL: no dead-lettered ... code=permanent_error line for $EVENT_ID"
  exit 1
fi
log "PASS: projector dead-lettered the pre-ledger event"

log "asserting a single ingest attempt (no retry/backoff lines for the event)"
RETRIES="$(docker logs "$PROJ" 2>/dev/null | grep -c "ingest failed .*event_id=$EVENT_ID" || true)"
if [ "$RETRIES" != 0 ]; then
  docker logs "$PROJ" | tail -10
  log "FAIL: $RETRIES retry/backoff lines for $EVENT_ID, want 0 (must dead-letter on the first attempt)"
  exit 1
fi
DEAD_LINES="$(docker logs "$PROJ" 2>/dev/null | grep -c "dead-lettered .*event_id=$EVENT_ID .*code=permanent_error" || true)"
if [ "$DEAD_LINES" != 1 ]; then
  docker logs "$PROJ" | tail -10
  log "FAIL: $DEAD_LINES dead-letter lines for $EVENT_ID, want exactly 1"
  exit 1
fi
log "PASS: exactly one dead-letter line, zero retry lines (single ingest attempt)"

log "reading the DLQ (bounded read, H1) and asserting the Failure record"
DLQ_OUT="$(mktemp)"
# rpk topic consume without a bound blocks forever awaiting new messages, so
# the whole read is wrapped in a host-side timeout and captured to a file.
if timeout 15 $COMPOSE exec -T redpanda rpk topic consume audit.events.dlq.v1 -p 0 -o 0 >"$DLQ_OUT" 2>/dev/null; then
  :
fi
# rpk (v24.x) prints each record as a pretty-printed JSON object with the
# key/value as JSON-escaped strings; the value line carries the whole
# Failure payload. The key line is unique per record, so grep -A1 binds the
# record's key + value lines together for the assertions below.
RECORD="$(grep -A1 '"key": "'"$EVENT_ID"'"' "$DLQ_OUT" || true)"
COUNT="$(printf '%s\n' "$RECORD" | grep -c '"key": "'"$EVENT_ID"'"' || true)"
if [ "$COUNT" != 1 ]; then
  log "FAIL: expected exactly 1 DLQ record for $EVENT_ID, found $COUNT:"
  grep "$EVENT_ID" "$DLQ_OUT" | head -3
  rm -f "$DLQ_OUT"
  exit 1
fi
if ! printf '%s\n' "$RECORD" | grep -q 'permanent_error'; then
  log "FAIL: DLQ record for $EVENT_ID lacks error_code=permanent_error:"
  printf '%s\n' "$RECORD"
  rm -f "$DLQ_OUT"
  exit 1
fi
if ! printf '%s\n' "$RECORD" | grep -q 'lacks ledger-assigned chain state'; then
  log "FAIL: DLQ record for $EVENT_ID lacks the self-describing error message:"
  printf '%s\n' "$RECORD"
  rm -f "$DLQ_OUT"
  exit 1
fi
if printf '%s\n' "$RECORD" | grep -q 'attempts_exhausted'; then
  log "FAIL: DLQ record for $EVENT_ID carries attempts_exhausted:"
  printf '%s\n' "$RECORD"
  rm -f "$DLQ_OUT"
  exit 1
fi
rm -f "$DLQ_OUT"
log "PASS: exactly one DLQ record with error_code=permanent_error, never attempts_exhausted"

log "cleaning up the one-off projector"
docker stop "$PROJ" >/dev/null 2>&1 || true
docker rm -f "$PROJ" >/dev/null 2>&1 || true

log "projector-permanent-dlq e2e PASS"
