import re
import sys
from pathlib import Path

from checks.asyncapi_channels import AsyncAPIError, parse_spec
from checks.proto_sync import ProtoSyncParseError, envelope_fields


# Architecture plan §19 requires OpenAPI/Protobuf/AsyncAPI contract checks.
# This guard verifies that the JSON-serialized domain.Event fields stay
# aligned with the three contracts so payload fields cannot drift silently
# (a regression this project hit before).
#
# Rules:
#   - OpenAPI Event schema and AsyncAPI envelope must cover every business
#     field (all domain fields except server-assigned ledger state).
#   - The gRPC envelope must cover every field a business system can supply.
SERVER_ASSIGNED = {
    "tenant_id", "received_at", "source_digest", "stream_id", "sequence",
    "prev_hash", "hash", "server_version",
}
# Fields only meaningful on the internal ledger, not on the ingest envelope.
INGEST_ONLY = {"tenant_id"}

# The ingest envelope deliberately excludes server-derived tenant authority
# from its required set.  The authenticated source resolves the tenant and
# the service stamps it after validation; an optional tenant_id is still
# accepted so a mismatching claim can be rejected explicitly (DS-08).
ENVELOPE_REQUIRED = {
    "event_id", "source_system", "event_type", "schema_id", "schema_version",
    "occurred_at", "actor", "action", "outcome", "data_classification",
    "retention_class", "idempotency_key",
}
ENVELOPE_ACTOR_PIN = "actor"
ENVELOPE_ACTOR_ID_PIN = "actor.id"
ENVELOPE_ANYOF_PIN = "[{ required: [payload] }, { required: [payload_ref] }]"


def _event_envelope_block(root: Path) -> str:
    spec = (root / "api/asyncapi/asyncapi.yaml").read_text(encoding="utf-8")
    match = re.search(
        r"^    EventEnvelope:\n(.*?)(?=^    \w+:\s*$|\Z)",
        spec,
        re.MULTILINE | re.DOTALL,
    )
    return match.group(1) if match else ""


def _flow_required(value: str) -> set[str]:
    match = re.search(r"required:\s*\[([^]]*)\]", value)
    if not match:
        return set()
    return {item.strip() for item in match.group(1).split(",") if item.strip()}


def asyncapi_envelope_semantics(root: Path | None = None) -> list[str]:
    """Validate the security-sensitive EventEnvelope shape.

    The repository intentionally uses a small YAML subset for contract gates,
    so this check stays text-based and fail-closed.  It pins the producer
    contract without introducing a PyYAML dependency: tenant authority is not
    a required client field, actor.id is mandatory, and payload/payload_ref is
    an at-least-one disjunction rather than an exclusive oneOf.
    """
    root = Path(root) if root is not None else Path(__file__).resolve().parents[1]
    block = _event_envelope_block(root)
    if not block:
        return ["AsyncAPI EventEnvelope schema not found"]
    failures: list[str] = []

    required_match = re.search(r"^      required:\s*\[([^]]*)\]", block, re.MULTILINE)
    required = _flow_required(required_match.group(0)) if required_match else set()
    if required != ENVELOPE_REQUIRED:
        failures.append(
            "EventEnvelope required set = %s, want exactly %s"
            % (sorted(required), sorted(ENVELOPE_REQUIRED))
        )

    actor_match = re.search(r"^        actor:\s*(\{.*\})\s*$", block, re.MULTILINE)
    actor = actor_match.group(1) if actor_match else ""
    if not actor:
        failures.append(f"EventEnvelope missing {ENVELOPE_ACTOR_PIN} property")
    elif "required: [id]" not in actor or "id:" not in actor:
        failures.append(
            f"EventEnvelope {ENVELOPE_ACTOR_PIN} must require nested {ENVELOPE_ACTOR_ID_PIN}"
        )

    anyof_match = re.search(r"^      anyOf:\s*(\[.*\])\s*$", block, re.MULTILINE)
    if not anyof_match:
        if re.search(r"^      oneOf:\s*", block, re.MULTILINE):
            failures.append(
                "EventEnvelope uses oneOf; anyOf is required for payload/payload_ref"
            )
        else:
            failures.append("EventEnvelope anyOf must require payload or payload_ref")
    else:
        branches = [
            _flow_required(branch)
            for branch in re.findall(r"\{[^{}]*required:\s*\[[^]]*\][^{}]*\}", anyof_match.group(1))
        ]
        if branches != [{"payload"}, {"payload_ref"}]:
            failures.append(
                "EventEnvelope anyOf branches = %s, want payload and payload_ref"
                % branches
            )
    return failures


def domain_event_fields(root: Path) -> set[str]:
    model = (root / "internal/domain/models.go").read_text(encoding="utf-8")
    match = re.search(r"type Event struct \{(.*?)\n\}", model, re.S)
    if not match:
        raise SystemExit("FAIL: domain Event struct not found")
    fields = set(re.findall(r'json:"(\w+)', match.group(1)))
    if not fields:
        raise SystemExit("FAIL: no json tags found in Event struct")
    return fields


def openapi_event_props(root: Path) -> set[str]:
    spec = (root / "api/openapi/openapi.yaml").read_text(encoding="utf-8")
    match = re.search(r"^    Event: \{.*\}$", spec, re.MULTILINE)
    if not match:
        raise SystemExit("FAIL: OpenAPI Event schema not found")
    return set(re.findall(r"(\w+): \{", match.group(0)))


def asyncapi_envelope_props(root: Path) -> set[str]:
    spec = (root / "api/asyncapi/asyncapi.yaml").read_text(encoding="utf-8")
    match = re.search(r"EventEnvelope:\n(.*?)(?=\n  \S|\Z)", spec, re.S)
    if not match:
        raise SystemExit("FAIL: AsyncAPI EventEnvelope not found")
    return set(re.findall(r"^\s+(\w+): \{", match.group(1), re.MULTILINE))


def asyncapi_failure_payload(root: Path) -> tuple[set[str], str] | None:
    """Parse components.messages.Failure.payload with the strict YAML-subset
    parser (checks.asyncapi_channels). Returns (property key set, required
    string) or None when the block is missing or not a mapping. parse_spec
    raises AsyncAPIError on unclassifiable input — the caller reports it."""
    spec = parse_spec((root / "api/asyncapi/asyncapi.yaml").read_text(encoding="utf-8"))
    try:
        payload = spec["components"]["messages"]["Failure"]["payload"]
    except (KeyError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    properties = payload.get("properties")
    if not isinstance(properties, dict):
        return None
    required = payload.get("required")
    return set(properties), str(required) if required is not None else ""


def run(root=None) -> int:
    if root is None:
        root = Path(__file__).resolve().parents[1]
    domain_fields = domain_event_fields(root)
    business_fields = domain_fields - SERVER_ASSIGNED - INGEST_ONLY

    failures = []
    openapi_props = openapi_event_props(root)
    for field in sorted(business_fields):
        if field not in openapi_props:
            failures.append(f"OpenAPI Event missing field {field}")

    asyncapi_props = asyncapi_envelope_props(root)
    for field in sorted(business_fields):
        if field not in asyncapi_props:
            failures.append(f"AsyncAPI EventEnvelope missing field {field}")
    failures.extend(asyncapi_envelope_semantics(root))

    # The gRPC envelope field set comes from the GENERATED descriptor
    # (checks.proto_sync.envelope_fields), never from the .proto source text:
    # a field added only to audit.proto is not part of the compiled runtime
    # and cannot satisfy the parity loop (R4).
    try:
        proto_fields = envelope_fields(root)
    except ProtoSyncParseError as error:
        print(f"FAIL: contract fields: proto descriptor parse error: {error}")
        return 1
    # The gRPC envelope carries the payload as bytes named payload_json.
    proto_lookup = set(proto_fields)
    if "payload_json" in proto_lookup:
        proto_lookup.add("payload")
    for field in sorted(business_fields):
        if field not in proto_lookup:
            failures.append(f"proto EventEnvelope missing field {field}")

    # Failure (DLQ) payload: pin the AsyncAPI components.messages.Failure key
    # set exactly (event_id, error_code, error_message + optional tenant_id)
    # and require that required stays [event_id, error_code, error_message] —
    # tenant_id must remain optional (the unparsable dead-letter path cannot
    # recover a tenant). The Go-side pin is
    # TestFailurePayloadMatchesAsyncAPISchema; this rule pins the YAML side so
    # a schema edit without the struct update (or vice versa) cannot pass the
    # gate silently (design FM-6).
    try:
        failure = asyncapi_failure_payload(root)
    except AsyncAPIError as error:
        print(f"FAIL: contract fields: asyncapi Failure parse error: {error}")
        return 1
    if failure is None:
        failures.append("AsyncAPI Failure payload not found or unparsable")
    else:
        failure_keys, failure_required = failure
        want_keys = {"event_id", "error_code", "error_message", "tenant_id"}
        if failure_keys != want_keys:
            failures.append(
                f"AsyncAPI Failure property set = {sorted(failure_keys)}, "
                f"want exactly {sorted(want_keys)}")
        if failure_required != "[event_id, error_code, error_message]":
            failures.append(
                f"AsyncAPI Failure required = {failure_required}, want unchanged "
                "[event_id, error_code, error_message] (tenant_id must stay optional)")

    if failures:
        print("FAIL: contract fields", *failures, sep="\n  ")
        return 1
    print(f"PASS: contract fields (domain {len(domain_fields)} fields aligned with OpenAPI/AsyncAPI/Proto; Failure payload pinned)")
    return 0


if __name__ == "__main__":
    sys.exit(run())
