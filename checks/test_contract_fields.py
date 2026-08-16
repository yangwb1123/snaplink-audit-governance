"""Unit tests for checks/contract_fields.py EventEnvelope semantics pins
(REQ-1/2/3: required-set equality, nested actor.id, anyOf payload|payload_ref).

Each case builds an in-memory minimal AsyncAPI spec with the exact subset the
check's parser reads (api/asyncapi/asyncapi.yaml EventEnvelope block), writes
it to a temp tree, and runs checks.contract_fields.asyncapi_envelope_semantics
against it. The real repo spec is never mutated; the repo's current state is
pinned by checks/contract_fields.run() inside cmd_quality.
"""

import tempfile
import unittest
from pathlib import Path

from checks.contract_fields import (
    ENVELOPE_ACTOR_ID_PIN,
    ENVELOPE_ACTOR_PIN,
    ENVELOPE_ANYOF_PIN,
    ENVELOPE_REQUIRED,
    asyncapi_envelope_semantics,
)

# The D-1/D-2/D-3 control shape (what the fixed spec must look like).
CONTROL_REQUIRED = ", ".join(sorted(ENVELOPE_REQUIRED))
CONTROL_ACTOR = "{ type: object, required: [id], properties: { id: { type: string } } }"


def minimal_spec(required: str, actor: str, anyof: str | None = None, oneof: str | None = None) -> str:
    """Minimal AsyncAPI text that parse_spec can read: EventEnvelope with the
    three pinned aspects variable. ``anyof`` and ``oneof`` are mutually
    exclusive keyword shapes (a real spec has at most one at schema level)."""
    lines = [
        "asyncapi: 3.0.0",
        "components:",
        "  schemas:",
        "    EventEnvelope:",
        "      type: object",
        f"      required: [{required}]",
        "      properties:",
        f"        actor: {actor}",
        "        action: { type: string }",
    ]
    if anyof is not None:
        lines.append(f"      anyOf: {anyof}")
    if oneof is not None:
        lines.append(f"      oneOf: {oneof}")
    return "\n".join(lines) + "\n"


class EventEnvelopeSemanticsTest(unittest.TestCase):
    def root(self, spec_text: str) -> Path:
        tree = tempfile.TemporaryDirectory()
        self.addCleanup(tree.cleanup)
        api = Path(tree.name) / "api" / "asyncapi"
        api.mkdir(parents=True)
        (api / "asyncapi.yaml").write_text(spec_text, encoding="utf-8")
        return Path(tree.name)

    def failures(self, spec_text: str) -> list[str]:
        return asyncapi_envelope_semantics(self.root(spec_text))

    # -- control ----------------------------------------------------------

    def test_control_d1_d2_d3_shape_passes(self):
        spec = minimal_spec(CONTROL_REQUIRED, CONTROL_ACTOR, ENVELOPE_ANYOF_PIN)
        failures = self.failures(spec)
        self.assertEqual(failures, [], failures)

    # -- REQ-1 required-set -------------------------------------------------

    def test_plus_tenant_id_fails(self):
        # HEAD shape: 13 tokens including tenant_id. Errors the ==-comparison.
        extra = sorted(ENVELOPE_REQUIRED | {"tenant_id"})
        failures = self.failures(minimal_spec(", ".join(extra), CONTROL_ACTOR, ENVELOPE_ANYOF_PIN))
        self.assertTrue(any("required" in failure for failure in failures), failures)
        self.assertTrue(any("tenant_id" in failure for failure in failures), failures)

    def test_minus_actor_fails(self):
        reduced = sorted(ENVELOPE_REQUIRED - {"actor"})
        failures = self.failures(minimal_spec(", ".join(reduced), CONTROL_ACTOR, ENVELOPE_ANYOF_PIN))
        self.assertTrue(any("required" in failure for failure in failures), failures)

    def test_plus_server_assigned_token_fails(self):
        # received_at is server-stamped after validation and must never be
        # added to required; the exact-set equality blocks it.
        extra = sorted(ENVELOPE_REQUIRED | {"received_at"})
        failures = self.failures(minimal_spec(", ".join(extra), CONTROL_ACTOR, ENVELOPE_ANYOF_PIN))
        self.assertTrue(any("required" in failure for failure in failures), failures)
        self.assertTrue(any("received_at" in failure for failure in failures), failures)

    # -- REQ-2 actor nested id ----------------------------------------------

    def test_actor_bare_object_fails(self):
        # HEAD shape: actor: { type: object } — no nested required.
        spec = minimal_spec(CONTROL_REQUIRED, "{ type: object }", ENVELOPE_ANYOF_PIN)
        failures = self.failures(spec)
        self.assertTrue(any(ENVELOPE_ACTOR_PIN in failure and ENVELOPE_ACTOR_ID_PIN in failure for failure in failures), failures)

    # -- REQ-3 anyOf (payload|payload_ref at-least-one) ----------------------

    def test_anyof_removed_fails(self):
        spec = minimal_spec(CONTROL_REQUIRED, CONTROL_ACTOR, anyof=None)
        failures = self.failures(spec)
        self.assertTrue(any("anyOf" in failure for failure in failures), failures)

    def test_oneof_instead_of_anyof_fails(self):
        # oneOf would reject runtime-conformant both-set envelopes; the check
        # must fail loudly with the keyword-level message.
        spec = minimal_spec(CONTROL_REQUIRED, CONTROL_ACTOR, oneof="[{ required: [payload] }, { required: [payload_ref] }]")
        failures = self.failures(spec)
        self.assertTrue(any("oneOf" in failure and "anyOf" in failure for failure in failures), failures)
        self.assertTrue(any("anyOf" in failure for failure in failures), failures)

    def test_anyof_branch_drift_fails(self):
        # exact-branch pin: dropping the payload_ref branch must red.
        spec = minimal_spec(CONTROL_REQUIRED, CONTROL_ACTOR, anyof="[{ required: [payload] }]")
        failures = self.failures(spec)
        self.assertTrue(any("anyOf" in failure for failure in failures), failures)


if __name__ == "__main__":
    unittest.main()