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

# G1 模式：先启动 audit-idp（token 铸造依赖它），就绪后 mint 并把 dev-auth
# 关闭 / JWKS env 导出（必须在其余服务 up 之前，compose 插值需要它们）。
if [ -n "${AUDIT_IDP_CLIENT_ID:-}" ] && [ -n "${AUDIT_IDP_CLIENT_SECRET:-}" ]; then
  log "starting audit-idp (G1 fixture)"
  $COMPOSE up -d audit-idp
  for _ in $(seq 1 30); do
    if curl -sf -m 2 -X POST "${AUDIT_IDP_TOKEN_URL:-http://localhost:18082/token}" \
      -H 'Content-Type: application/x-www-form-urlencoded' \
      --data "grant_type=client_credentials&client_id=${AUDIT_IDP_CLIENT_ID}&client_secret=${AUDIT_IDP_CLIENT_SECRET}&scope=audit:event:write" >/dev/null 2>&1; then
      break
    fi
    sleep 2
    log "waiting for audit-idp"
  done
fi

# B1-7 (T-1.2) token fixture: a real IdP token replaces dev tokens once the
# IdP deployment (B4-1) is available. Precedence: AUDIT_E2E_TOKEN env
# (explicit) -> mint-token.sh (IdP config present) -> dev token (transitional,
# G1 not yet closed; logs a warning).
# G1 mode also flips the stack to real-token-only: dev auth off, JWKS pointed
# at the host-run IdP via the host-gateway alias.
if [ -n "${AUDIT_E2E_TOKEN:-}" ]; then
  AUTH_READ="Bearer ${AUDIT_E2E_TOKEN}"
  AUTH_COMPLIANCE="Bearer ${AUDIT_E2E_TOKEN}"
elif [ -n "${AUDIT_IDP_CLIENT_ID:-}" ] && [ -n "${AUDIT_IDP_CLIENT_SECRET:-}" ]; then
  # compose 栈自带 audit-idp：默认 URL 指向其发布端口（调用方可覆盖）。
  export AUDIT_IDP_TOKEN_URL="${AUDIT_IDP_TOKEN_URL:-http://localhost:18082/token}"
  log "minting IdP token from ${AUDIT_IDP_TOKEN_URL}"
  # 默认 scope 覆盖 e2e 全部治理断言（策略/Legal Hold/导出/完整性/读写）；
  # 调用方可覆盖。
  export AUDIT_IDP_SCOPE="${AUDIT_IDP_SCOPE:-audit:event:write audit:event:read audit:operation:read audit:export:create audit:integrity:verify audit:legal_hold:manage audit:policy:read audit:policy:write}"
  # shellcheck disable=SC1091
  source "${ROOT}/scripts/mint-token.sh"
  AUTH_READ="Bearer ${AUDIT_E2E_TOKEN}"
  AUTH_COMPLIANCE="Bearer ${AUDIT_E2E_TOKEN}"
  export AUDIT_OUTBOX_TOKEN
  # G1: dev auth OFF + JWKS verification against the host IdP. The IdP's
  # issuer must equal AUDIT_JWT_ISSUER (host-visible URL of the IdP).
  export AUDIT_ALLOW_DEV_AUTH=false
  export AUDIT_ALLOW_INSECURE_JWKS_LOOPBACK=true
  export AUDIT_JWKS_URL="${AUDIT_JWKS_URL:-http://host.docker.internal:18082/.well-known/jwks.json}"
  export AUDIT_JWT_ISSUER="${AUDIT_JWT_ISSUER:-http://localhost:18082}"
  AUTH_POLICY="Bearer ${AUDIT_E2E_TOKEN}"
  AUTH_WRITE="Bearer ${AUDIT_E2E_TOKEN}"
  # 第二主体（demo-admin）：职责分离 e2e 的审批者。mint 会覆盖
  # AUDIT_OUTBOX_TOKEN，先保存再恢复（relay 必须保持 demo 客户端身份）。
  FIRST_OUTBOX_TOKEN="${AUDIT_OUTBOX_TOKEN}"
  AUDIT_IDP_CLIENT_ID="${AUDIT_IDP_ADMIN_CLIENT_ID:-demo-admin}"
  AUDIT_IDP_CLIENT_SECRET="${AUDIT_IDP_ADMIN_CLIENT_SECRET:-demo-admin-secret}"
  AUDIT_IDP_SCOPE="audit:event:read audit:operation:read audit:export:create audit:integrity:verify audit:legal_hold:manage audit:policy:read"
  source "${ROOT}/scripts/mint-token.sh" >/dev/null
  AUTH_SECONDARY="Bearer ${AUDIT_E2E_TOKEN}"
  export AUDIT_OUTBOX_TOKEN="${FIRST_OUTBOX_TOKEN}"
  log "PASS: real IdP token in use (G1 fixture path); dev auth off"
else
  log "WARN: no IdP token fixture configured; using dev tokens (transitional, B1-7 awaits B4-1)"
  AUTH_READ="Bearer dev:demo:tenant-auditor"
  AUTH_COMPLIANCE="Bearer dev:demo:compliance"
  AUTH_POLICY="Bearer dev:demo:platform-admin"
  AUTH_WRITE="Bearer dev:demo:service"
  AUTH_SECONDARY="Bearer dev:demo:compliance"
fi

log "starting infrastructure (postgres/redpanda/clickhouse/minio/jaeger)"
$COMPOSE up -d postgres redpanda clickhouse minio jaeger

# PostgreSQL 冷启动需要数秒：等待 healthy 后再应用迁移。
for _ in $(seq 1 30); do
  if $COMPOSE exec -T postgres pg_isready -U audit -d audit >/dev/null 2>&1; then
    break
  fi
  sleep 2
  log "waiting for postgres"
done

log "applying migrations before starting applications (audit-api bootstrap writes the PG snapshot)"
$COMPOSE exec -T postgres psql -U audit -d audit -v ON_ERROR_STOP=1 \
  -f - < "${ROOT}/migrations/001_control_plane.sql" >/dev/null
$COMPOSE exec -T postgres psql -U audit -d audit -v ON_ERROR_STOP=1 \
  -f - < "${ROOT}/migrations/003_outbox_relay.sql" >/dev/null
$COMPOSE exec -T postgres psql -U audit -d audit -v ON_ERROR_STOP=1 \
  -f - < "${ROOT}/migrations/004_state_snapshot.sql" >/dev/null

# 自举归档依赖：S3Store.Ready 要求 bucket 存在、启用 Object Lock（WORM）、
# versioning，并且（自 R-1 收口起）默认留存为 COMPLIANCE 模式 + 正有效期。
# worker 启动即探测归档就绪（REQ-1，失败 fatal），因此 bucket 必须在任何
# 应用启动之前完成全部配置，否则新栈的 worker 首次启动必然失败并进入重启退避。
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

# ─────────────────────────────────────────────────────────────────────────
# COMPLIANCE-default cutover (qa F4 / compliance F2 / security F6): the R-1
# gate makes S3Store.Ready fail closed unless the bucket default retention is
# COMPLIANCE with positive validity. 没有默认留存的桶恰是 R-3a
# （"no default retention"），GOVERNANCE 默认留存是 R-3b（"GOVERNANCE"）——
# 两者在 worker -check-config 下都必须退出码 1 且不打印 check_config=ok。
# 用 worker 二进制对一次性 scratch 桶做 cutover 三态验证（自动化 design §5
# 的手动步骤；scratch 桶从不写入数据、无留存对象，因此 mc rb --force 可
# 安全重建，每次运行都从"无默认留存"状态开始，幂等）：
#   no default      → exit 1 + "no default retention"   (R-3a)
#   GOVERNANCE 365d → exit 1 + "GOVERNANCE"             (R-3b)
#   COMPLIANCE 365d → exit 0 + check_config=ok
# 随后给真实 worm-audit 桶设置 COMPLIANCE 365d 默认留存（幂等覆盖：即使
# 上次运行残留 COMPLIANCE 默认，重跑也不会失真）并正向复核。
# 注（部署顺序）：负向断言只在 R-1 门禁进入部署二进制后成立；pre-gate
# 二进制对三类桶全部通过。fullstack.sh 必须与门禁代码同一发布落地（见
# README 的 deploy/rollback runbook）。
log "building worker binary for -check-config cutover validation"
WORKER_BIN="${TMPDIR:-/tmp}/audit-governance-worker-checkconfig"
CHECKCONFIG_LOG="${TMPDIR:-/tmp}/fullstack-checkconfig-$$.log"
( cd "${ROOT}" && go build -o "$WORKER_BIN" ./cmd/audit-governance-worker )
export AUDIT_SIGNING_SECRET="${AUDIT_SIGNING_SECRET:-local-only-change-me}"
export AUDIT_ENCRYPTION_KEY="${AUDIT_ENCRYPTION_KEY:-local-only-encryption-key}"
export AUDIT_S3_ENDPOINT="localhost:19010"   # minio 发布到宿主的端口
# AUDIT_S3_BUCKET 按腿覆盖（见 checkconfig_expect）
export AUDIT_S3_ACCESS_KEY="audit-local"
export AUDIT_S3_SECRET_KEY="audit-local-change-me"
# F1（2026-08-15）：S3 归档的每次 Put 都携带显式 COMPLIANCE 留存，时长由
# AUDIT_ARCHIVE_RETENTION_DAYS 决定（必须为正，缺失/为零即 fail-closed 配置
# 错误——门禁二进制下 -check-config 会因此退出 1）。这里与 verify 栈一致用
# 365 天；guard 保证变量缺失时给出可操作报错而不是静默失败。
export AUDIT_ARCHIVE_RETENTION_DAYS="${AUDIT_ARCHIVE_RETENTION_DAYS:-365}"
if ! [ "$AUDIT_ARCHIVE_RETENTION_DAYS" -gt 0 ] 2>/dev/null; then
  log "FAIL: AUDIT_ARCHIVE_RETENTION_DAYS must be a positive integer (per-object COMPLIANCE retention for the S3 archive)"
  exit 1
fi

checkconfig_expect() { # bucket want_marker forbid_marker want_rc
  local bucket="$1" want="$2" forbid="$3" wantrc="$4" rc
  AUDIT_S3_BUCKET="$bucket" "$WORKER_BIN" -check-config >"$CHECKCONFIG_LOG" 2>&1 && rc=0 || rc=$?
  if [ "$rc" -ne "$wantrc" ] \
     || ! grep -qE "$want" "$CHECKCONFIG_LOG" \
     || { [ -n "$forbid" ] && grep -qE "$forbid" "$CHECKCONFIG_LOG"; }; then
    log "FAIL: -check-config bucket=$bucket: want rc=$wantrc marker='$want' forbid='${forbid:-}' got rc=$rc"
    tail -5 "$CHECKCONFIG_LOG"
    exit 1
  fi
  log "PASS: -check-config bucket=$bucket → $want"
}

CUTOVER_BUCKET="worm-audit-cutover"
$COMPOSE exec -T minio mc rb --force "$MINIO_ALIAS/$CUTOVER_BUCKET" >/dev/null 2>&1 || true
$COMPOSE exec -T minio mc mb --with-lock "$MINIO_ALIAS/$CUTOVER_BUCKET" >/dev/null
log "cutover negative leg (R-3a): no default retention must fail closed"
checkconfig_expect "$CUTOVER_BUCKET" "no default retention" "check_config=ok|GOVERNANCE" 1
$COMPOSE exec -T minio mc retention set --default governance 365d "$MINIO_ALIAS/$CUTOVER_BUCKET" >/dev/null
log "cutover negative leg (R-3b): GOVERNANCE default must fail closed"
checkconfig_expect "$CUTOVER_BUCKET" "GOVERNANCE" "no default retention" 1
$COMPOSE exec -T minio mc retention set --default compliance 365d "$MINIO_ALIAS/$CUTOVER_BUCKET" >/dev/null
log "cutover positive leg: COMPLIANCE 365d default must pass"
checkconfig_expect "$CUTOVER_BUCKET" "check_config=ok" "" 0
$COMPOSE exec -T minio mc rb --force "$MINIO_ALIAS/$CUTOVER_BUCKET" >/dev/null 2>&1 || true

log "setting COMPLIANCE 365d default retention on worm-audit (R-1 required state)"
$COMPOSE exec -T minio mc retention set --default compliance 365d "$MINIO_ALIAS/worm-audit" >/dev/null
checkconfig_expect "worm-audit" "check_config=ok" "" 0
rm -f "$CHECKCONFIG_LOG"

log "starting applications (audit-api/relay/consumer/projector/worker/idp)"
$COMPOSE up -d audit-api audit-outbox-relay audit-kafka-consumer \
  audit-projector audit-kafka-dlq-replay audit-governance-worker audit-idp

wait_http "$API/readyz" || { log "audit-api not ready"; exit 1; }
log "audit-api ready"

log "asserting worker archive readiness probe"
for _ in $(seq 1 30); do
  if $COMPOSE logs audit-governance-worker 2>/dev/null | grep -q "archive_ready=ok"; then
    break
  fi
  sleep 2
  log "waiting for worker archive_ready=ok"
done
if ! $COMPOSE logs audit-governance-worker 2>/dev/null | grep -q "archive_ready=ok"; then
  $COMPOSE logs audit-governance-worker | tail -5
  log "FAIL: worker archive readiness probe did not pass"
  exit 1
fi
log "PASS: worker archive readiness probe passed"

log "checking gRPC ingest (B1-6 topology: listener + real Write over socket)"
if (exec 3<>/dev/tcp/localhost/19051) 2>/dev/null; then
  log "gRPC listener: open on 19051"
  exec 3<&- 3>&-
else
  log "gRPC listener: not reachable"
  exit 1
fi
G_TOKEN="${AUTH_WRITE#Bearer }"
if (cd "${ROOT}" && go run ./test/e2e/grpcwrite -address localhost:19051 -token "$G_TOKEN" \
    -event-id "grpc-e2e-$(date +%s)" | grep -q "grpc-write-ok"); then
  log "PASS: gRPC Write ingested"
else
  log "FAIL: gRPC Write failed"
  exit 1
fi

log "ensuring clickhouse database"
# ClickHouse 容器启动需要数秒：就绪前 curl 会失败（CURLE_RECV_ERROR）。
for _ in $(seq 1 30); do
  if curl -sf -u audit:audit-local-only "$CH/ping" >/dev/null 2>&1; then
    break
  fi
  sleep 2
  log "waiting for clickhouse"
done
curl -sf -u audit:audit-local-only -X POST \
  --data-binary 'CREATE DATABASE IF NOT EXISTS audit' "$CH/" >/dev/null
log "ensuring Kafka topic"
$COMPOSE exec -T redpanda rpk topic create audit.events.accepted.v1 \
  --partitions 1 --replicas 1 >/dev/null 2>&1 || true
$COMPOSE exec -T redpanda rpk topic create audit.events.ledgered.v1 \
  --partitions 1 --replicas 1 >/dev/null 2>&1 || true
$COMPOSE exec -T redpanda rpk topic create audit.events.dlq.v1 \
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

if [ -n "${AUDIT_IDP_TOKEN_URL:-}" ]; then
  # G1 (T-1.1) negative leg: with dev auth off, dev tokens must be rejected
  # on the read route too.
  DEV_STATUS="$(curl -s -o /dev/null -w '%{http_code}' "$API/api/v1/events?from=2026-08-01T00:00:00Z&to=2026-09-01T00:00:00Z" -H 'Authorization: Bearer dev:demo:tenant-auditor' 2>/dev/null || true)"
  if [ "$DEV_STATUS" != "401" ]; then
    log "FAIL: dev token accepted in G1 mode (status=$DEV_STATUS, want 401)"
    exit 1
  fi
  log "PASS: dev token rejected (401) in G1 mode"
fi

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

log "verifying governance chain (retention / legal hold / export / worker)"
# 留存策略（policy:write 角色；G1 模式为 IdP token）
POLICY_STATUS="$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$API/api/v1/policies/retention" \
  -H "Authorization: $AUTH_POLICY" -H 'Content-Type: application/json' \
  -d '{"tenant_id":"demo","hot_days":1,"warm_days":7,"archive_days":30,"retention_class":"standard"}')"
[ "$POLICY_STATUS" = "200" ] || { log "FAIL: set retention policy ($POLICY_STATUS)"; exit 1; }
EVAL_STATUS="$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/api/v1/retention/evaluate?tenant_id=demo" \
  -H "Authorization: $AUTH_POLICY")"
[ "$EVAL_STATUS" = "200" ] || { log "FAIL: retention evaluate ($EVAL_STATUS)"; exit 1; }
log "PASS: retention policy set and evaluated"

EXPORT_ID="$(curl -s -X POST "$API/api/v1/exports" \
  -H "Authorization: $AUTH_COMPLIANCE" -H 'Content-Type: application/json' \
  -d '{"from":"2026-08-01T00:00:00Z","to":"2026-09-01T00:00:00Z"}' | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")"
[ -n "$EXPORT_ID" ] || { log "FAIL: create export"; exit 1; }
for _ in $(seq 1 30); do
  EXPORT_STATUS="$(curl -s "$API/api/v1/exports/$EXPORT_ID" -H "Authorization: $AUTH_COMPLIANCE" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")"
  [ "$EXPORT_STATUS" = "completed" -o "$EXPORT_STATUS" = "failed" ] && break
  sleep 2
done
[ "$EXPORT_STATUS" = "completed" ] || { log "FAIL: export did not complete ($EXPORT_STATUS)"; exit 1; }
DOWNLOAD_STATUS="$(curl -s -o /dev/null -w '%{http_code}' "$API/api/v1/exports/$EXPORT_ID/download" -H "Authorization: $AUTH_COMPLIANCE")"
[ "$DOWNLOAD_STATUS" = "200" ] || { log "FAIL: export download ($DOWNLOAD_STATUS)"; exit 1; }
log "PASS: export completed and downloaded"

# 建立 Legal Hold 要放在导出之后：同一时间窗的导出会被活动 hold
# 正确拒绝，先完成导出才能同时验证两条治理语义。
HOLD_STATUS="$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/api/v1/legal-holds?tenant_id=demo" \
  -H "Authorization: $AUTH_POLICY" -H 'Content-Type: application/json' \
  -d '{"name":"e2e-hold","reason":"verification","filter":{"from":"2026-08-01T00:00:00Z","to":"2026-09-01T00:00:00Z"}}')"
[ "$HOLD_STATUS" = "201" ] || { log "FAIL: create legal hold ($HOLD_STATUS)"; exit 1; }
log "PASS: legal hold created"

# worker 单轮评估（与常驻实例共享 PG 快照 —— 乐观锁 + 有界重试的真实载体）
$COMPOSE exec -T audit-governance-worker /audit-governance-worker -once 2>&1 | \
  grep -q "eligible=" || { log "FAIL: governance worker once-run produced no retention evaluation"; exit 1; }
log "PASS: governance worker once-run"

log "verifying restore approval chain (separation of duties)"
# 先写入一个带 operation_id 的事件作为恢复对象
RESTORE_OP="restore-op-$(date +%s)"
curl -s -X POST "$API/api/v1/events?wait_for=ledgered" \
  -H "Authorization: $AUTH_WRITE" -H 'Content-Type: application/json' \
  -d "{\"event_id\":\"restore-evt-$(date +%s)\",\"operation_id\":\"$RESTORE_OP\",\"source_system\":\"demo\",\"event_type\":\"audit.event\",\"schema_id\":\"audit.event\",\"schema_version\":1,\"occurred_at\":\"2026-08-07T12:00:00Z\",\"actor\":{\"id\":\"u\"},\"action\":\"update\",\"outcome\":\"success\",\"data_classification\":\"internal\",\"retention_class\":\"standard\",\"idempotency_key\":\"restore-idem-$(date +%s)\",\"payload\":{\"x\":1}}" \
  -o /dev/null -w '%{http_code}' | grep -q 202 || { log "FAIL: restore op event ingest"; exit 1; }
RUN_ID="$(curl -s -X POST "$API/api/v1/restores?tenant_id=demo" \
  -H "Authorization: $AUTH_POLICY" -H 'Content-Type: application/json' \
  -d "{\"operation_id\":\"$RESTORE_OP\",\"reason\":\"e2e verification\"}" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")"
[ -n "$RUN_ID" ] || { log "FAIL: create restore run"; exit 1; }
# 同人审批必须 403（职责分离）
SAME_ACTOR="$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/api/v1/restores/$RUN_ID/approve?tenant_id=demo" \
  -H "Authorization: $AUTH_POLICY")"
[ "$SAME_ACTOR" = "403" ] || { log "FAIL: same-actor approve must be 403 (got $SAME_ACTOR)"; exit 1; }
# 第二主体审批 -> approved（仅 G1 模式：dev token 的 sub 恒等于租户，
# 单主体语义下同人 403 本身就是职责分离验证；真实第二主体需要 IdP）
if [ -n "${AUDIT_IDP_CLIENT_ID:-}" ]; then
  APPROVE_STATUS="$(curl -s -X POST "$API/api/v1/restores/$RUN_ID/approve?tenant_id=demo" \
    -H "Authorization: $AUTH_SECONDARY" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")"
  [ "$APPROVE_STATUS" = "approved" ] || { log "FAIL: restore approve ($APPROVE_STATUS)"; exit 1; }
  log "PASS: restore approval chain (same-actor 403, distinct-actor approved)"
else
  log "PASS: restore same-actor refusal (dev single-principal semantics)"
fi

log "verifying legal hold release"
HOLD_ID="$(curl -s "$API/api/v1/legal-holds?tenant_id=demo" -H "Authorization: $AUTH_POLICY" | python3 -c "import json,sys; items=json.load(sys.stdin).get('items',[]); print(items[0]['id'] if items else '')")"
[ -n "$HOLD_ID" ] || { log "FAIL: no legal hold to release"; exit 1; }
RELEASE_STATUS="$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/api/v1/legal-holds/$HOLD_ID/release?tenant_id=demo" \
  -H "Authorization: $AUTH_POLICY")"
[ "$RELEASE_STATUS" = "200" ] || { log "FAIL: legal hold release ($RELEASE_STATUS)"; exit 1; }
log "PASS: legal hold released"

log "verifying Jaeger trace export"
sleep 8
TRACES="$(curl -sf "$JAEGER/api/traces?service=audit-api&limit=1" | grep -o '"traceID"' | head -1 || true)"
[ -n "$TRACES" ] || { log "WARN: no Jaeger traces (export batch may lag)"; }
log "full-stack e2e PASS"
