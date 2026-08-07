#!/usr/bin/env bash
# mint-token.sh — B1-7 (T-1.2) token fixture: mints a client_credentials JWT
# from the Snaplink IdP (B4-1 deployment) carrying the tenant_id/roles/scope
# claims the audit API verifies, and prints shell-exportable assignments for
# AUDIT_OUTBOX_TOKEN / AUDIT_E2E_TOKEN.
#
# Configuration (all required):
#   AUDIT_IDP_TOKEN_URL    IdP token endpoint, e.g. https://idp.example/token
#   AUDIT_IDP_CLIENT_ID    client registered with audit:event:write scope
#   AUDIT_IDP_CLIENT_SECRET
# Optional:
#   AUDIT_IDP_SCOPE        default "audit:event:write"
#
# Fail-closed: without the three configuration variables the script exits
# non-zero with a message instead of producing a bogus token. This script
# cannot be verified inside this repository — the IdP endpoint is owned by
# the snaplink deployment repo (B4-1); it exists so the e2e suite has a
# single, auditable fixture path once B4-1 lands.
set -euo pipefail

: "${AUDIT_IDP_TOKEN_URL:?AUDIT_IDP_TOKEN_URL is required (IdP token endpoint, B4-1)}"
: "${AUDIT_IDP_CLIENT_ID:?AUDIT_IDP_CLIENT_ID is required}"
: "${AUDIT_IDP_CLIENT_SECRET:?AUDIT_IDP_CLIENT_SECRET is required}"
SCOPE="${AUDIT_IDP_SCOPE:-audit:event:write}"

if ! command -v curl >/dev/null 2>&1; then
  echo "mint-token: curl is required" >&2
  exit 1
fi

RESPONSE="$(curl -sfS \
  -X POST "${AUDIT_IDP_TOKEN_URL}" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode "grant_type=client_credentials" \
  --data-urlencode "client_id=${AUDIT_IDP_CLIENT_ID}" \
  --data-urlencode "client_secret=${AUDIT_IDP_CLIENT_SECRET}" \
  --data-urlencode "scope=${SCOPE}")"

TOKEN="$(printf '%s' "$RESPONSE" | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')"
if [ -z "${TOKEN}" ] || [ "${TOKEN}" = "None" ]; then
  echo "mint-token: IdP response contained no access_token" >&2
  exit 1
fi

# Shell-exportable output; works both when sourced (`source mint-token.sh`)
# and when evaluated (`eval "$(./mint-token.sh)"`).
export AUDIT_OUTBOX_TOKEN="${TOKEN}"
export AUDIT_E2E_TOKEN="${TOKEN}"
printf 'export AUDIT_OUTBOX_TOKEN=%q\nexport AUDIT_E2E_TOKEN=%q\n' "${TOKEN}" "${TOKEN}"
