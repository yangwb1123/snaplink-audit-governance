#!/usr/bin/env bash
# Bootstrap the real-token audit verification stack, then add the Iris UI Web
# and Snaplink Console Hosted Login profile and exercise the PKCE contract.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/deploy/docker-compose.verify.yml" --profile web)

wait_http() {
  local url="$1"
  for _ in $(seq 1 90); do
    if curl --fail --silent --max-time 2 "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  printf 'web stack: timed out waiting for %s\n' "$url" >&2
  return 1
}

if [ "${AUDIT_WEB_REUSE_STACK:-false}" != "true" ]; then
  AUDIT_IDP_CLIENT_ID="${AUDIT_IDP_CLIENT_ID:-demo}" \
  AUDIT_IDP_CLIENT_SECRET="${AUDIT_IDP_CLIENT_SECRET:-demo-secret}" \
    bash "$ROOT/test/e2e/fullstack.sh"
fi

"${COMPOSE[@]}" up -d --build audit-web audit-console
wait_http "${AUDIT_WEB_URL:-http://localhost:15178}/"
wait_http "${SNAPLINK_HOSTED_LOGIN_URL:-http://localhost:4444/login/}"
bash "$ROOT/test/e2e/web-auth.sh"
AUDIT_WEB_USERNAME=audit-approver bash "$ROOT/test/e2e/web-auth.sh"

printf '%s\n' \
  'web stack PASS' \
  '  Audit Governance Web: http://localhost:15178' \
  '  Snaplink Hosted Login: http://localhost:4444/login/' \
  '  requester: audit-operator / audit-demo-password' \
  '  approver: audit-approver / audit-demo-password'
