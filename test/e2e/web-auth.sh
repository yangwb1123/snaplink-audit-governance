#!/usr/bin/env bash
# Real browser-client contract without browser automation:
# Snaplink password login -> authorization code + PKCE -> public token exchange
# -> JWT claim assertions -> authenticated Audit API and governance workflows.
set -euo pipefail

IDP_BASE="${SNAPLINK_IDP_URL:-http://localhost:18082}"
AUDIT_BASE="${AUDIT_API_URL:-http://localhost:19089}"
CLIENT_ID="${AUDIT_WEB_CLIENT_ID:-audit-governance-web}"
REDIRECT_URI="${AUDIT_WEB_REDIRECT_URI:-http://localhost:15178/auth/callback}"
USERNAME="${AUDIT_WEB_USERNAME:-audit-operator}"
PASSWORD="${AUDIT_WEB_PASSWORD:-audit-demo-password}"
RESOURCE="${AUDIT_WEB_RESOURCE:-audit-governance}"

log() {
  printf '[web-auth] %s\n' "$1" >&2
}

authorize_user() {
  local username="$1"
  local password="$2"
  local verifier challenge state nonce login_payload login_response code token_response
  verifier="$(openssl rand -hex 48)"
  challenge="$(printf '%s' "$verifier" | openssl dgst -sha256 -binary | openssl base64 -A | tr '+/' '-_' | tr -d '=')"
  state="$(openssl rand -hex 16)"
  nonce="$(openssl rand -hex 16)"
  login_payload="$(CLIENT_ID="$CLIENT_ID" REDIRECT_URI="$REDIRECT_URI" USERNAME="$username" PASSWORD="$password" RESOURCE="$RESOURCE" CHALLENGE="$challenge" STATE="$state" NONCE="$nonce" python3 -c '
import json, os
print(json.dumps({
    "provider": "password",
    "client_id": os.environ["CLIENT_ID"],
    "redirect_uri": os.environ["REDIRECT_URI"],
    "response_type": "code",
    "scope": [
        "openid", "profile", "audit:event:read", "audit:event:write",
        "audit:operation:read", "audit:export:create", "audit:integrity:verify",
        "audit:legal_hold:manage", "audit:policy:read", "audit:policy:write",
        "audit:platform:cross_tenant",
    ],
    "resource": [os.environ["RESOURCE"]],
    "state": os.environ["STATE"],
    "nonce": os.environ["NONCE"],
    "code_challenge": os.environ["CHALLENGE"],
    "code_challenge_method": "S256",
    "credential": {
        "username": os.environ["USERNAME"],
        "password": os.environ["PASSWORD"],
    },
}))
')"
  login_response="$(curl --fail-with-body --silent --show-error \
    -X POST "$IDP_BASE/auth/login" \
    -H 'Content-Type: application/json' \
    --data "$login_payload")"
  code="$(printf '%s' "$login_response" | STATE="$state" IDP_BASE="$IDP_BASE" python3 -c '
import json, os, sys
body = json.load(sys.stdin)
assert body.get("state") == os.environ["STATE"], "authorization state mismatch"
assert body.get("iss") == os.environ["IDP_BASE"], "authorization issuer mismatch"
code = body.get("code", "")
assert code, "authorization response missing code"
print(code)
')"
  token_response="$(curl --fail-with-body --silent --show-error \
    -X POST "$IDP_BASE/token" \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode 'grant_type=authorization_code' \
    --data-urlencode "code=$code" \
    --data-urlencode "redirect_uri=$REDIRECT_URI" \
    --data-urlencode "client_id=$CLIENT_ID" \
    --data-urlencode "code_verifier=$verifier")"
  TOKEN_RESPONSE="$token_response" RESOURCE="$RESOURCE" CLIENT_ID="$CLIENT_ID" NONCE="$nonce" IDP_BASE="$IDP_BASE" USERNAME="$username" python3 -c '
import base64, json, os

def claims(token):
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))

def audiences(value):
    return [value] if isinstance(value, str) else value

body = json.loads(os.environ["TOKEN_RESPONSE"])
assert body.get("token_type", "").lower() == "bearer", "token_type is not Bearer"
access_token = body.get("access_token", "")
id_token = body.get("id_token", "")
assert access_token, "token response missing access_token"
assert id_token, "token response missing id_token"
access = claims(access_token)
identity = claims(id_token)
assert os.environ["RESOURCE"] in audiences(access.get("aud", [])), "access-token audience mismatch"
assert access.get("iss") == os.environ["IDP_BASE"], "access-token issuer mismatch"
assert access.get("tenant_id") == "demo", "access-token tenant mismatch"
assert access.get("sub") == os.environ["USERNAME"], "access-token subject mismatch"
assert os.environ["CLIENT_ID"] in audiences(identity.get("aud", [])), "ID-token audience mismatch"
assert identity.get("iss") == os.environ["IDP_BASE"], "ID-token issuer mismatch"
assert identity.get("sub") == os.environ["USERNAME"], "ID-token subject mismatch"
assert identity.get("nonce") == os.environ["NONCE"], "ID-token nonce mismatch"
scopes = set(access.get("scope", "").split())
assert "audit:platform:cross_tenant" in scopes, "platform scope missing"
assert "audit:event:read" in scopes, "audit read scope missing"
assert "audit:event:write" in scopes, "audit write scope missing"
print(access_token)
'
}

log "authorizing $USERNAME with public PKCE"
access_token="$(authorize_user "$USERNAME" "$PASSWORD")"
run_suffix="$(openssl rand -hex 12)"

if [ "${AUDIT_WEB_SKIP_API:-false}" = "true" ]; then
  printf 'web auth IdP PASS: public PKCE client -> Snaplink JWT\n'
  exit 0
fi

log "checking authenticated events, facets, and aggregate history"
events_response="$(curl --fail-with-body --silent --show-error \
  "$AUDIT_BASE/api/v1/events?tenant_id=demo&from=2026-01-01T00%3A00%3A00Z&to=2027-01-01T00%3A00%3A00Z&correlation_id=fullstack-web-contract&page_size=100" \
  -H "Authorization: Bearer $access_token")"

aggregate_path="$(printf '%s' "$events_response" | python3 -c '
import json, sys, urllib.parse
body = json.load(sys.stdin)
assert isinstance(body.get("items"), list), "event response missing items"
assert isinstance(body.get("count"), int), "event response missing count"
assert all(event.get("correlation_id") == "fullstack-web-contract" for event in body["items"]), "correlation filter leaked unrelated events"
for event in body["items"]:
    if event.get("aggregate_type") and event.get("aggregate_id"):
        print("/api/v1/aggregates/%s/%s/timeline" % (
            urllib.parse.quote(event["aggregate_type"], safe=""),
            urllib.parse.quote(event["aggregate_id"], safe=""),
        ))
        break
else:
    print("")
')"

log "checking Snaplink-compatible event facets"
facets_response="$(curl --fail-with-body --silent --show-error \
  "$AUDIT_BASE/api/v1/compat/snaplink/audit/facets?tenant_id=demo&since=2026-01-01T00%3A00%3A00Z&until=2027-01-01T00%3A00%3A00Z" \
  -H "Authorization: Bearer $access_token")"
printf '%s' "$facets_response" | python3 -c '
import json, sys
facets = json.load(sys.stdin).get("facets", {})
assert isinstance(facets.get("total"), int), "facet response missing total"
for dimension in ("outcomes", "types", "clients", "providers"):
    assert isinstance(facets.get(dimension), dict), "facet response missing %s" % dimension
assert sum(facets["outcomes"].values()) == facets["total"], "outcome facets do not cover total"
'

if [ -n "$aggregate_path" ]; then
  log "checking aggregate timeline from the authenticated event result"
  aggregate_response="$(curl --fail-with-body --silent --show-error \
    "$AUDIT_BASE$aggregate_path?tenant_id=demo&page_size=1" \
    -H "Authorization: Bearer $access_token")"
  printf '%s' "$aggregate_response" | python3 -c '
import json, sys
body = json.load(sys.stdin)
assert isinstance(body.get("items"), list), "aggregate response missing items"
assert isinstance(body.get("count"), int), "aggregate response missing count"
'
else
  log "checking the empty aggregate contract"
  # A freshly bootstrapped API has no aggregate events. The route is still
  # authenticated and contract-checked through its stable not-found result;
  # fullstack.sh seeds a real verification aggregate for the positive leg.
  aggregate_status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
    "$AUDIT_BASE/api/v1/aggregates/web-contract/missing/timeline?tenant_id=demo&page_size=1" \
    -H "Authorization: Bearer $access_token")"
  if [ "$aggregate_status" != "404" ]; then
    printf 'web auth e2e: empty aggregate returned %s, want 404\n' "$aggregate_status" >&2
    exit 1
  fi
fi

log "checking source bindings and schema catalog"
sources_response="$(curl --fail-with-body --silent --show-error \
  "$AUDIT_BASE/api/v1/sources?tenant_id=demo" \
  -H "Authorization: Bearer $access_token")"
source_update="$(printf '%s' "$sources_response" | CLIENT_ID="$CLIENT_ID" python3 -c '
import json, os, sys
body = json.load(sys.stdin)
source = next((item for item in body.get("items", []) if item.get("id") == "demo"), None)
assert source, "bootstrap source demo missing"
allowed = list(source.get("allowed_client_ids", []))
for client_id in (source["id"], os.environ["CLIENT_ID"]):
    if client_id not in allowed:
        allowed.append(client_id)
print(json.dumps({
    "name": source["name"],
    "allowed_client_ids": allowed,
    "active": source["active"],
}))
')"
curl --fail-with-body --silent --show-error --output /dev/null \
  -X PUT "$AUDIT_BASE/api/v1/sources/demo?tenant_id=demo" \
  -H "Authorization: Bearer $access_token" \
  -H 'Content-Type: application/json' \
  --data "$source_update"

schemas_response="$(curl --fail-with-body --silent --show-error \
  "$AUDIT_BASE/api/v1/schemas?tenant_id=demo" \
  -H "Authorization: Bearer $access_token")"
printf '%s' "$schemas_response" | python3 -c '
import json, sys
body = json.load(sys.stdin)
items = body.get("items")
assert isinstance(items, list), "schema response missing items"
assert all(item.get("tenant_id") == "demo" for item in items), "schema response crossed tenant scope"
'

log "checking restore preview and two-subject approval"
restore_operation="web-restore-$run_suffix"
restore_reason="web PKCE restore verification"
restore_event_payload="$(RESTORE_OPERATION="$restore_operation" RUN_SUFFIX="$run_suffix" USERNAME="$USERNAME" python3 -c '
import datetime, json, os
print(json.dumps({
    "event_id": "web-restore-event-" + os.environ["RUN_SUFFIX"],
    "operation_id": os.environ["RESTORE_OPERATION"],
    "source_system": "demo",
    "event_type": "audit.event",
    "schema_id": "audit.event",
    "schema_version": 1,
    "occurred_at": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
    "actor": {"id": os.environ["USERNAME"], "type": "user"},
    "action": "restore-test",
    "outcome": "success",
    "data_classification": "internal",
    "retention_class": "standard",
    "idempotency_key": "web-restore-idem-" + os.environ["RUN_SUFFIX"],
    "payload": {"verification": "snaplink-pkce"},
}))
')"
restore_receipt="$(curl --fail-with-body --silent --show-error \
  -X POST "$AUDIT_BASE/api/v1/events?wait_for=ledgered" \
  -H "Authorization: Bearer $access_token" \
  -H 'Content-Type: application/json' \
  --data "$restore_event_payload")"
printf '%s' "$restore_receipt" | python3 -c '
import json, sys
receipt = json.load(sys.stdin).get("receipt", {})
assert receipt.get("event_id"), "restore event receipt missing event_id"
assert receipt.get("status") in ("ledgered", "indexed", "archived"), "restore event was not ledgered"
assert receipt.get("ledgered_at"), "restore event receipt missing ledgered_at"
'

restore_request="$(RESTORE_OPERATION="$restore_operation" RESTORE_REASON="$restore_reason" python3 -c '
import json, os
print(json.dumps({
    "operation_id": os.environ["RESTORE_OPERATION"],
    "reason": os.environ["RESTORE_REASON"],
}))
')"
restore_preview="$(curl --fail-with-body --silent --show-error \
  -X POST "$AUDIT_BASE/api/v1/restores/preview?tenant_id=demo" \
  -H "Authorization: Bearer $access_token" \
  -H 'Content-Type: application/json' \
  --data "$restore_request")"
printf '%s' "$restore_preview" | RESTORE_OPERATION="$restore_operation" python3 -c '
import json, os, sys
preview = json.load(sys.stdin)
assert preview.get("id"), "restore preview missing id"
assert preview.get("tenant_id") == "demo", "restore preview tenant mismatch"
assert preview.get("operation_id") == os.environ["RESTORE_OPERATION"], "restore preview operation mismatch"
assert preview.get("requires_approval") is True, "restore preview must require approval"
assert isinstance(preview.get("proposed_state"), dict), "restore preview state missing"
assert isinstance(preview.get("external_calls"), list), "restore preview external_calls missing"
'

restore_response="$(curl --fail-with-body --silent --show-error \
  -X POST "$AUDIT_BASE/api/v1/restores?tenant_id=demo" \
  -H "Authorization: Bearer $access_token" \
  -H 'Content-Type: application/json' \
  --data "$restore_request")"
restore_id="$(printf '%s' "$restore_response" | USERNAME="$USERNAME" RESTORE_OPERATION="$restore_operation" RESTORE_REASON="$restore_reason" python3 -c '
import json, os, sys
run = json.load(sys.stdin)
assert run.get("status") == "pending_approval", "restore run is not pending approval"
assert run.get("tenant_id") == "demo", "restore run tenant mismatch"
assert run.get("operation_id") == os.environ["RESTORE_OPERATION"], "restore run operation mismatch"
assert run.get("reason") == os.environ["RESTORE_REASON"], "restore run reason mismatch"
assert run.get("created_by") == os.environ["USERNAME"], "restore requester subject mismatch"
run_id = run.get("id", "")
assert run_id, "restore run missing id"
print(run_id)
')"
same_actor_status="$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
  -X POST "$AUDIT_BASE/api/v1/restores/$restore_id/approve?tenant_id=demo" \
  -H "Authorization: Bearer $access_token")"
if [ "$same_actor_status" != "403" ]; then
  printf 'web auth e2e: same-subject restore approval returned %s, want 403\n' "$same_actor_status" >&2
  exit 1
fi
if [ "$USERNAME" = "${AUDIT_WEB_APPROVER_USERNAME:-audit-approver}" ]; then
  secondary_username="${AUDIT_WEB_REQUESTER_USERNAME:-audit-operator}"
else
  secondary_username="${AUDIT_WEB_APPROVER_USERNAME:-audit-approver}"
fi
secondary_password="${AUDIT_WEB_SECONDARY_PASSWORD:-$PASSWORD}"
secondary_token="$(authorize_user "$secondary_username" "$secondary_password")"
approved_restore="$(curl --fail-with-body --silent --show-error \
  -X POST "$AUDIT_BASE/api/v1/restores/$restore_id/approve?tenant_id=demo" \
  -H "Authorization: Bearer $secondary_token")"
printf '%s' "$approved_restore" | SECONDARY_USERNAME="$secondary_username" python3 -c '
import json, os, sys
run = json.load(sys.stdin)
assert run.get("status") == "approved", "restore run was not approved"
assert run.get("approved_by") == os.environ["SECONDARY_USERNAME"], "restore approver subject mismatch"
assert run.get("approved_at"), "restore approval timestamp missing"
'

log "checking scoped legal hold and export"
evidence_query='{"from":"2298-01-01T00:00:00Z","to":"2298-01-02T00:00:00Z","source_system":"aero-im.source","event_type":"aero.im.security","operation_id":"web-evidence-contract","correlation_id":"web-evidence-contract","page_size":1000}'
hold_payload="$(EVIDENCE_QUERY="$evidence_query" HOLD_SUFFIX="$run_suffix" python3 -c '
import json, os
print(json.dumps({
    "name": "web-evidence-" + os.environ["HOLD_SUFFIX"],
    "reason": "web scoped evidence contract",
    "filter": json.loads(os.environ["EVIDENCE_QUERY"]),
}))
')"
hold_response="$(curl --fail-with-body --silent --show-error \
  -X POST "$AUDIT_BASE/api/v1/legal-holds?tenant_id=demo" \
  -H "Authorization: Bearer $access_token" \
  -H 'Content-Type: application/json' \
  --data "$hold_payload")"
hold_id="$(printf '%s' "$hold_response" | python3 -c '
import json, sys
hold = json.load(sys.stdin)
assert hold.get("filter", {}).get("source_system") == "aero-im.source", "legal-hold source filter missing"
assert hold.get("filter", {}).get("operation_id") == "web-evidence-contract", "legal-hold operation filter missing"
hold_id = hold.get("id", "")
assert hold_id, "legal-hold response missing id"
print(hold_id)
')"
released_hold="$(curl --fail-with-body --silent --show-error \
  -X POST "$AUDIT_BASE/api/v1/legal-holds/$hold_id/release?tenant_id=demo" \
  -H "Authorization: Bearer $access_token")"
printf '%s' "$released_hold" | python3 -c '
import json, sys
hold = json.load(sys.stdin)
assert hold.get("released_at"), "legal hold was not released"
'

export_response="$(curl --fail-with-body --silent --show-error \
  -X POST "$AUDIT_BASE/api/v1/exports?tenant_id=demo" \
  -H "Authorization: Bearer $access_token" \
  -H 'Content-Type: application/json' \
  --data "$evidence_query")"
export_id="$(printf '%s' "$export_response" | python3 -c '
import json, sys
job = json.load(sys.stdin)
assert job.get("query", {}).get("correlation_id") == "web-evidence-contract", "export scope missing"
job_id = job.get("id", "")
assert job_id, "export response missing id"
print(job_id)
')"
export_status="pending"
for _attempt in $(seq 1 30); do
  export_response="$(curl --fail-with-body --silent --show-error \
    "$AUDIT_BASE/api/v1/exports/$export_id?tenant_id=demo" \
    -H "Authorization: Bearer $access_token")"
  export_status="$(printf '%s' "$export_response" | python3 -c 'import json, sys; print(json.load(sys.stdin).get("status", ""))')"
  if [ "$export_status" = "completed" ] || [ "$export_status" = "failed" ]; then
    break
  fi
  sleep 0.2
done
if [ "$export_status" != "completed" ]; then
  printf 'web auth e2e: scoped export ended in %s\n' "$export_status" >&2
  exit 1
fi
printf '%s' "$export_response" | python3 -c '
import json, sys
job = json.load(sys.stdin)
assert job.get("event_count") == 0, "future-window export unexpectedly selected events"
'
curl --fail-with-body --silent --show-error --output /dev/null \
  "$AUDIT_BASE/api/v1/exports/$export_id/download?tenant_id=demo" \
  -H "Authorization: Bearer $access_token"

printf 'web auth e2e PASS: public PKCE -> Snaplink JWT/ID token -> events/facets + aggregate history + source/schema + restore separation + scoped evidence governance\n'
