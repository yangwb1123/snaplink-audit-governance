import re
import sys
from pathlib import Path

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

    if failures:
        print("FAIL: contract fields", *failures, sep="\n  ")
        return 1
    print(f"PASS: contract fields (domain {len(domain_fields)} fields aligned with OpenAPI/AsyncAPI/Proto)")
    return 0


if __name__ == "__main__":
    sys.exit(run())
