#!/usr/bin/env bash
# Isolated demo orchestrator: core (files only) and advanced (PostgreSQL +
# MinIO + governance worker). Every project/volume/network is demo-owned.
set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT/deploy/docker-compose.demo.verify.yml"
PROFILE="${DEMO_PROFILE:-core}"
PROJECT="${DEMO_PROJECT:-snaplink-audit-demo}"
API_PORT="${DEMO_API_PORT:-18089}"

fail() {
  printf '[demo] FAIL: %s\n' "$*" >&2
  exit 1
}

case "$PROFILE" in
  core|advanced) ;;
  *) fail "DEMO_PROFILE must be core or advanced, got '$PROFILE'" ;;
esac
[[ "$PROJECT" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || fail "DEMO_PROJECT must match [a-z0-9][a-z0-9_-]*"
[[ "$API_PORT" =~ ^[0-9]+$ ]] || fail "DEMO_API_PORT must be an integer"
(( API_PORT >= 1 && API_PORT <= 65535 )) || fail "DEMO_API_PORT must be between 1 and 65535"

DEFAULT_RUN_DIR="${XDG_STATE_HOME:-${HOME:-/tmp}/.local/state}/snaplink-audit-demo/$PROJECT"
RUN_DIR="$(python3 -c 'import os,sys; print(os.path.abspath(os.path.expanduser(sys.argv[1])))' "${DEMO_RUN_DIR:-$DEFAULT_RUN_DIR}")"
case "$RUN_DIR/" in
  "$ROOT/"*) fail "DEMO_RUN_DIR must be outside the repository: $RUN_DIR" ;;
esac
ENV_FILE="$RUN_DIR/runtime.env"

export DEMO_PROFILE="$PROFILE"
export DEMO_PROJECT="$PROJECT"
export DEMO_RUN_DIR="$RUN_DIR"
export DEMO_ENV_FILE="$ENV_FILE"
export DEMO_COMPOSE_FILE="$COMPOSE_FILE"
export DEMO_API_PORT="$API_PORT"
export DEMO_API_URL="${DEMO_API_URL:-http://127.0.0.1:${API_PORT}}"

compose() {
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" --env-file "$ENV_FILE" "$@"
}

generate_env() {
  mkdir -p "$RUN_DIR"
  chmod 700 "$RUN_DIR"
  if [[ ! -s "$ENV_FILE" ]]; then
    python3 - "$ENV_FILE" <<'PY'
import os
import secrets
import sys

path = sys.argv[1]
tenant = os.environ.get("DEMO_TENANT", "demo")
issuer = os.environ.get("DEMO_JWT_ISSUER", "snaplink-audit-demo")
audience = os.environ.get("DEMO_JWT_AUDIENCE", "audit-governance")
jwt_secret = secrets.token_hex(48)
signing_secret = secrets.token_hex(48)
encryption_key = secrets.token_hex(48)
if len({jwt_secret, signing_secret, encryption_key}) != 3:
    raise SystemExit("generated secrets were not independent")
postgres_password = secrets.token_hex(24)
minio_user = "demo-" + secrets.token_hex(6)
minio_password = secrets.token_hex(24)
lines = [
    f"DEMO_TENANT={tenant}",
    f"DEMO_JWT_ISSUER={issuer}",
    f"DEMO_JWT_AUDIENCE={audience}",
    f"DEMO_JWT_SECRET={jwt_secret}",
    f"DEMO_SIGNING_SECRET={signing_secret}",
    f"DEMO_ENCRYPTION_KEY={encryption_key}",
    f"DEMO_POSTGRES_PASSWORD={postgres_password}",
    f"DEMO_POSTGRES_DSN=postgres://audit:{postgres_password}@postgres:5432/audit?sslmode=disable",
    f"DEMO_MINIO_USER={minio_user}",
    f"DEMO_MINIO_PASSWORD={minio_password}",
    "DEMO_S3_ENDPOINT=minio:9000",
    "DEMO_S3_BUCKET=worm-audit",
    "DEMO_IMAGE=snaplink-audit-demo",
]
fd = os.open(path + ".tmp", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, "w", encoding="utf-8") as stream:
    stream.write("\n".join(lines) + "\n")
os.replace(path + ".tmp", path)
PY
  fi
  chmod 600 "$ENV_FILE"
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
  export DEMO_TENANT DEMO_JWT_ISSUER DEMO_JWT_AUDIENCE DEMO_JWT_SECRET
  export DEMO_SIGNING_SECRET DEMO_ENCRYPTION_KEY
  export DEMO_POSTGRES_PASSWORD DEMO_POSTGRES_DSN
  export DEMO_MINIO_USER DEMO_MINIO_PASSWORD DEMO_S3_ENDPOINT DEMO_S3_BUCKET DEMO_IMAGE
}

require_env() {
  [[ -s "$ENV_FILE" ]] || fail "private runtime file is missing; run make demo-up first"
  generate_env
}

wait_ready() {
  local service="$1"
  for attempt in $(seq 1 90); do
    if curl -fsS "$DEMO_API_URL/readyz" >/dev/null 2>&1; then
      printf '[demo] API ready: %s\n' "$DEMO_API_URL"
      return 0
    fi
    sleep 1
  done
  compose logs --no-color "$service" | tail -n 40 || true
  fail "$service did not become ready at $DEMO_API_URL/readyz"
}

wait_postgres() {
  for _ in $(seq 1 60); do
    if compose exec -T postgres pg_isready -U audit -d audit >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  compose logs --no-color postgres | tail -n 40 || true
  fail "PostgreSQL did not become ready"
}

apply_migrations() {
  local migration
  for migration in "$ROOT"/migrations/*.sql; do
    printf '[demo] migration %s\n' "$(basename "$migration")"
    compose exec -T postgres psql -v ON_ERROR_STOP=1 -U audit -d audit -f - <"$migration" >/dev/null
  done
}

init_minio() {
  local attempt output
  for attempt in $(seq 1 60); do
    if compose exec -T minio mc alias set local http://localhost:9000 \
      "$DEMO_MINIO_USER" "$DEMO_MINIO_PASSWORD" >/dev/null 2>&1; then
      break
    fi
    if [[ "$attempt" -eq 60 ]]; then
      compose logs --no-color minio | tail -n 40 || true
      fail "MinIO did not accept the generated credentials"
    fi
    sleep 1
  done
  compose exec -T minio mc mb --with-lock --ignore-existing "local/$DEMO_S3_BUCKET" >/dev/null
  compose exec -T minio mc version enable "local/$DEMO_S3_BUCKET" >/dev/null
  compose exec -T minio mc retention set --default compliance 365d "local/$DEMO_S3_BUCKET" >/dev/null
  output="$(compose exec -T minio mc retention info --default "local/$DEMO_S3_BUCKET")"
  if ! grep -qi 'COMPLIANCE' <<<"$output"; then
    fail "MinIO bucket default retention is not COMPLIANCE: $output"
  fi
  printf '[demo] MinIO bucket %s: Object Lock + versioning + COMPLIANCE 365d\n' "$DEMO_S3_BUCKET"
}

wait_worker() {
  for _ in $(seq 1 60); do
    if compose logs --no-color audit-governance-worker 2>/dev/null | grep -q 'archive_ready=ok' \
      && [[ -n "$(compose ps -q --status running audit-governance-worker)" ]]; then
      printf '[demo] governance worker ready (archive_ready=ok)\n'
      return 0
    fi
    sleep 1
  done
  compose logs --no-color audit-governance-worker | tail -n 60 || true
  fail "governance worker did not reach archive_ready=ok"
}

up() {
  generate_env
  printf '[demo] profile=%s project=%s api=%s\n' "$PROFILE" "$PROJECT" "$DEMO_API_URL"
  printf '[demo] private runtime files: %s (credentials are not printed)\n' "$RUN_DIR"
  if [[ "$PROFILE" == "core" ]]; then
    compose --profile core up -d --build audit-api-core
    wait_ready audit-api-core
  else
    compose --profile advanced up -d --build postgres minio
    wait_postgres
    apply_migrations
    init_minio
    compose --profile advanced up -d --build audit-api-advanced audit-governance-worker
    wait_ready audit-api-advanced
    wait_worker
  fi
  printf '[demo] PASS: demo-up profile=%s\n' "$PROFILE"
}

seed() {
  require_env
  python3 "$ROOT/scripts/demo_seed.py"
}

prove() {
  require_env
  python3 "$ROOT/scripts/demo_prove.py"
}

down() {
  require_env
  compose --profile core --profile advanced --profile tools down --remove-orphans
  printf '[demo] stopped project=%s; named volumes were preserved\n' "$PROJECT"
}

usage() {
  cat <<'EOF'
usage: scripts/demo.sh <up|seed|prove|down>
environment: DEMO_PROFILE=core|advanced DEMO_PROJECT=<compose project>
             DEMO_API_PORT=<loopback port> DEMO_RUN_DIR=<outside-repo dir>
EOF
}

case "${1:-}" in
  up) up ;;
  seed) seed ;;
  prove) prove ;;
  down) down ;;
  *) usage; exit 2 ;;
esac
