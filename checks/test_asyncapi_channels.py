"""Regression tests for checks/asyncapi_channels.py (AsyncAPI channel <-> Go
Kafka symbol drift guard).

Covers AC-1 (ghost fixture fail/pass/value-alignment), AC-2 (regression
shape, no-exemption behavioral + static proofs, current tree), AC-3 (tuple
order), the Rule B suite, the fail-closed suite (missing/unknown action,
orphan channel, parser robustness), and the Go scan hardening (const blocks,
cross-package duplicates, comment/raw-string fakes).
"""

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

GHOST_SPEC = """\
asyncapi: 3.0.0
info: { title: Test, version: 1.0.0 }
channels:
  ghost:
    address: audit.events.ghost.v1
    messages:
      auditEvent: { $ref: '#/components/messages/AuditEvent' }
operations:
  publishGhost:
    action: send
    channel: { $ref: '#/channels/ghost' }
"""

AC2_SPEC = """\
asyncapi: 3.0.0
info: { title: Test, version: 1.0.0 }
channels:
  accepted:
    address: audit.events.accepted.v1
  dlq:
    address: audit.events.dlq.v1
operations:
  publishAccepted:
    action: send
    channel: { $ref: '#/channels/accepted' }
  publishFailure:
    action: send
    channel: { $ref: '#/channels/dlq' }
"""

AC2_KAFKA_GO = """\
package kafka

const TopicAccepted = "audit.events.accepted.v1"
const TopicDLQ = "audit.events.dlq.v1"

type Producer struct{ topic string }

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{topic: topic}
}
"""

AC2_OUTBOX_MAIN = """\
package main

import "example/kafka"

func main() {
	brokers := []string{"localhost:9092"}
	_ = kafka.NewProducer(brokers, kafka.TopicAccepted)
}
"""

AC2_CONSUMER_MAIN = """\
package main

import (
	"flag"

	"example/kafka"
)

func main() {
	dlqTopic := flag.String("dlq-topic", kafka.TopicDLQ, "dead-letter topic")
	_ = kafka.NewProducer(nil, *dlqTopic)
}
"""


def run_capture(check, *args, **kwargs):
    stream = io.StringIO()
    with redirect_stdout(stream):
        code = check(*args, **kwargs)
    return code, stream.getvalue()


class AsyncApiChannelsCheckTest(unittest.TestCase):
    """asyncapi_channels: channel <-> symbol alignment, fail closed."""

    def make_tree(self, files):
        tree = tempfile.TemporaryDirectory()
        root = Path(tree.name)
        for rel, content in files.items():
            path = root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
        self.addCleanup(tree.cleanup)
        return root

    def run_check(self, root):
        from checks.asyncapi_channels import run
        return run_capture(run, root)

    # -- AC-1: ghost fixture ------------------------------------------------

    def test_ghost_channel_without_symbol_fails(self):
        root = self.make_tree({"api/asyncapi/asyncapi.yaml": GHOST_SPEC})
        code, out = self.run_check(root)
        self.assertEqual(code, 1)
        self.assertIn("audit.events.ghost.v1", out)
        self.assertIn("TopicGhost", out)

    def test_ghost_channel_with_symbol_passes(self):
        files = {
            "api/asyncapi/asyncapi.yaml": GHOST_SPEC,
            "internal/kafka/ghost.go": (
                "package kafka\n\n"
                "// TopicGhost is the AsyncAPI topic for ghost events.\n"
                'const TopicGhost = "audit.events.ghost.v1"\n'),
            "cmd/ghost/main.go": (
                "package main\n\n"
                'import "example/kafka"\n\n'
                "func main() {\n"
                "\t_ = kafka.NewProducer(nil, kafka.TopicGhost)\n"
                "}\n"),
        }
        root = self.make_tree(files)
        code, out = self.run_check(root)
        self.assertEqual(code, 0, out)
        self.assertIn("PASS: asyncapi channels", out)

    def test_value_alignment_mismatch_fails(self):
        files = {
            "api/asyncapi/asyncapi.yaml": GHOST_SPEC,
            "internal/kafka/ghost.go": (
                "package kafka\n\n"
                'const TopicGhost = "audit.events.other.v1"\n'),
        }
        root = self.make_tree(files)
        code, out = self.run_check(root)
        self.assertEqual(code, 1)
        self.assertIn("audit.events.ghost.v1", out)
        self.assertIn("TopicGhost", out)

    # -- AC-2: regression shape + no exemptions -----------------------------

    def test_current_tree_passes(self):
        from checks.asyncapi_channels import run
        self.assertEqual(run(ROOT), 0)

    def test_regression_shape_accepted_and_dlq_passes(self):
        files = {
            "api/asyncapi/asyncapi.yaml": AC2_SPEC,
            "internal/kafka/kafka.go": AC2_KAFKA_GO,
            "cmd/audit-outbox-relay/main.go": AC2_OUTBOX_MAIN,
            "cmd/audit-kafka-consumer/main.go": AC2_CONSUMER_MAIN,
        }
        root = self.make_tree(files)
        code, out = self.run_check(root)
        self.assertEqual(code, 0, out)
        self.assertIn("PASS: asyncapi channels", out)

    def test_regression_shape_plus_ghost_fails_no_exemption(self):
        # AC-2.2: the regression fixture plus a ghost channel with no Go
        # symbol must go red naming the ghost address -- proving the AC-2.1
        # pass comes from symbol analysis, not a fixture/address allowlist.
        combined = """\
asyncapi: 3.0.0
info: { title: Test, version: 1.0.0 }
channels:
  accepted:
    address: audit.events.accepted.v1
  dlq:
    address: audit.events.dlq.v1
  ghost:
    address: audit.events.ghost.v1
operations:
  publishAccepted:
    action: send
    channel: { $ref: '#/channels/accepted' }
  publishFailure:
    action: send
    channel: { $ref: '#/channels/dlq' }
  publishGhost:
    action: send
    channel: { $ref: '#/channels/ghost' }
"""
        files = {
            "api/asyncapi/asyncapi.yaml": combined,
            "internal/kafka/kafka.go": AC2_KAFKA_GO,
            "cmd/audit-outbox-relay/main.go": AC2_OUTBOX_MAIN,
            "cmd/audit-kafka-consumer/main.go": AC2_CONSUMER_MAIN,
        }
        root = self.make_tree(files)
        code, out = self.run_check(root)
        self.assertEqual(code, 1)
        self.assertIn("audit.events.ghost.v1", out)

    def test_no_exemption_allowlist_static(self):
        text = (ROOT / "checks" / "asyncapi_channels.py").read_text(encoding="utf-8")
        for token in ("EXEMPT", "ALLOWLIST", "SKIPLIST", "CARVEOUT", "FIXTURE_ONLY"):
            self.assertNotRegex(text, rf"\b{token}\b",
                                f"module must not contain an exemption mechanism ({token})")

    # -- AC-3: cmd_quality wiring ------------------------------------------

    def test_quality_tuple_order(self):
        text = (ROOT / "cli.py").read_text(encoding="utf-8")
        self.assertIn("from checks.asyncapi_channels import run as asyncapi_channels", text)
        self.assertRegex(
            text,
            r"contract_fields,\s*\n\s*asyncapi_channels,.*\n\s*proto_sync,",
            "asyncapi_channels must sit between contract_fields and proto_sync")

    # -- Rule B (Go -> spec) ------------------------------------------------

    def test_rule_b_undeclared_topic_fails(self):
        files = {
            "api/asyncapi/asyncapi.yaml": GHOST_SPEC,
            "internal/kafka/kafka.go": (
                "package kafka\n\n"
                'const TopicGhost = "audit.events.ghost.v1"\n'
                'const TopicPhantom = "audit.events.phantom.v1"\n'),
        }
        root = self.make_tree(files)
        code, out = self.run_check(root)
        self.assertEqual(code, 1)
        self.assertIn("TopicPhantom", out)
        self.assertIn("audit.events.phantom.v1", out)

    def test_rule_b_non_dotted_value_ignored(self):
        files = {
            "api/asyncapi/asyncapi.yaml": GHOST_SPEC,
            "internal/kafka/kafka.go": (
                "package kafka\n\n"
                'const TopicGhost = "audit.events.ghost.v1"\n'
                'const TopicTmp = "tmp"\n'),
        }
        root = self.make_tree(files)
        code, out = self.run_check(root)
        self.assertEqual(code, 0, out)

    # -- Go scan hardening (const blocks, duplicates, fakes) ----------------

    def test_const_block_declaration_recognized(self):
        files = {
            "api/asyncapi/asyncapi.yaml": GHOST_SPEC,
            "internal/kafka/kafka.go": (
                "package kafka\n\n"
                "const (\n"
                '\tTopicGhost = "audit.events.ghost.v1"\n'
                ")\n"),
        }
        root = self.make_tree(files)
        code, out = self.run_check(root)
        self.assertEqual(code, 0, out)

    def test_const_block_wrong_value_fails(self):
        files = {
            "api/asyncapi/asyncapi.yaml": GHOST_SPEC,
            "internal/kafka/kafka.go": (
                "package kafka\n\n"
                "const (\n"
                '\tTopicGhost = "audit.events.other.v1"\n'
                ")\n"),
        }
        root = self.make_tree(files)
        code, out = self.run_check(root)
        self.assertEqual(code, 1)
        self.assertIn("audit.events.ghost.v1", out)

    def test_cross_package_duplicate_symbol_fails(self):
        files = {
            "api/asyncapi/asyncapi.yaml": GHOST_SPEC,
            "internal/a/a.go": (
                "package a\n\n"
                'const TopicGhost = "audit.events.ghost.v1"\n'),
            "internal/b/b.go": (
                "package b\n\n"
                'const TopicGhost = "audit.events.other.v1"\n'),
        }
        root = self.make_tree(files)
        code, out = self.run_check(root)
        self.assertEqual(code, 1)
        self.assertIn("audit.events.other.v1", out)

    def test_duplicate_same_value_passes(self):
        files = {
            "api/asyncapi/asyncapi.yaml": GHOST_SPEC,
            "internal/a/a.go": (
                "package a\n\n"
                'const TopicGhost = "audit.events.ghost.v1"\n'),
            "internal/b/b.go": (
                "package b\n\n"
                'const TopicGhost = "audit.events.ghost.v1"\n'),
        }
        root = self.make_tree(files)
        code, out = self.run_check(root)
        self.assertEqual(code, 0, out)

    def test_comment_fake_const_ignored(self):
        files = {
            "api/asyncapi/asyncapi.yaml": GHOST_SPEC,
            "internal/kafka/kafka.go": (
                "package kafka\n\n"
                '// const TopicGhost = "audit.events.ghost.v1"\n'),
        }
        root = self.make_tree(files)
        code, out = self.run_check(root)
        self.assertEqual(code, 1)
        self.assertIn("audit.events.ghost.v1", out)

    def test_raw_string_fake_const_ignored(self):
        files = {
            "api/asyncapi/asyncapi.yaml": GHOST_SPEC,
            "internal/kafka/kafka.go": (
                "package kafka\n\n"
                "const doc = `\n"
                'const TopicGhost = "audit.events.ghost.v1"\n'
                "`\n"),
        }
        root = self.make_tree(files)
        code, out = self.run_check(root)
        self.assertEqual(code, 1)
        self.assertIn("audit.events.ghost.v1", out)

    def test_test_file_const_excluded(self):
        files = {
            "api/asyncapi/asyncapi.yaml": GHOST_SPEC,
            "internal/kafka/ghost_test.go": (
                "package kafka\n\n"
                'const TopicGhost = "audit.events.ghost.v1"\n'),
        }
        root = self.make_tree(files)
        code, out = self.run_check(root)
        self.assertEqual(code, 1)
        self.assertIn("audit.events.ghost.v1", out)

    # -- Symbol derivation table --------------------------------------------

    def test_derive_symbol_table(self):
        from checks.asyncapi_channels import derive_symbol
        cases = {
            "audit.events.accepted.v1": "TopicAccepted",
            "audit.events.dlq.v1": "TopicDLQ",
            "audit.events.ledgered.v1": "TopicLedgered",
            "audit.events.projection.v1": "TopicProjection",
            "audit.events.archive.v1": "TopicArchive",
            "audit.events.ghost.v1": "TopicGhost",
            "audit.events.dlq.v2": "TopicDLQ",
            "audit.events.api.v1": "TopicApi",
            "audit.events.hold.v1": "TopicHold",
            "accepted": "TopicAccepted",
        }
        for address, symbol in cases.items():
            self.assertEqual(derive_symbol(address), symbol, address)

    # -- Fail-closed suite ---------------------------------------------------

    def test_missing_spec_fails(self):
        root = self.make_tree({})
        code, out = self.run_check(root)
        self.assertEqual(code, 1)
        self.assertIn("FAIL: asyncapi channels", out)

    def test_empty_spec_fails(self):
        root = self.make_tree({"api/asyncapi/asyncapi.yaml": ""})
        code, out = self.run_check(root)
        self.assertEqual(code, 1)

    def test_unparsable_spec_fails(self):
        root = self.make_tree({"api/asyncapi/asyncapi.yaml": "this is not yaml\n- item\n"})
        code, out = self.run_check(root)
        self.assertEqual(code, 1)

    def test_non_mapping_root_fails(self):
        root = self.make_tree({"api/asyncapi/asyncapi.yaml": "- a\n- b\n"})
        code, out = self.run_check(root)
        self.assertEqual(code, 1)

    def test_missing_address_fails(self):
        spec = GHOST_SPEC.replace("    address: audit.events.ghost.v1\n", "")
        root = self.make_tree({"api/asyncapi/asyncapi.yaml": spec})
        code, out = self.run_check(root)
        self.assertEqual(code, 1)

    def test_unresolvable_ref_fails(self):
        spec = GHOST_SPEC.replace("#/channels/ghost", "#/channels/nope")
        root = self.make_tree({"api/asyncapi/asyncapi.yaml": spec})
        code, out = self.run_check(root)
        self.assertEqual(code, 1)

    def test_operations_missing_with_channels_fails(self):
        spec = GHOST_SPEC.split("operations:")[0]
        root = self.make_tree({"api/asyncapi/asyncapi.yaml": spec})
        code, out = self.run_check(root)
        self.assertEqual(code, 1)

    def test_channels_missing_fails(self):
        spec = GHOST_SPEC.split("channels:", 1)[0] + "operations: {}\n"
        root = self.make_tree({"api/asyncapi/asyncapi.yaml": spec})
        code, out = self.run_check(root)
        self.assertEqual(code, 1)

    def test_unknown_action_fails(self):
        spec = GHOST_SPEC.replace("action: send", "action: Send")
        root = self.make_tree({"api/asyncapi/asyncapi.yaml": spec})
        code, out = self.run_check(root)
        self.assertEqual(code, 1)
        self.assertIn("audit.events.ghost.v1", out)

    def test_missing_action_fails(self):
        spec = GHOST_SPEC.replace("    action: send\n", "")
        root = self.make_tree({"api/asyncapi/asyncapi.yaml": spec})
        code, out = self.run_check(root)
        self.assertEqual(code, 1)
        self.assertIn("publishGhost", out)

    def test_orphan_channel_fails(self):
        spec = GHOST_SPEC.replace(
            "  ghost:\n    address: audit.events.ghost.v1\n",
            "  ghost:\n    address: audit.events.ghost.v1\n"
            "  extra:\n    address: audit.events.extra.v1\n")
        root = self.make_tree({"api/asyncapi/asyncapi.yaml": spec})
        code, out = self.run_check(root)
        self.assertEqual(code, 1)
        self.assertIn("extra", out)

    def test_duplicate_channel_name_fails(self):
        spec = GHOST_SPEC.replace(
            "operations:", "  ghost:\n    address: audit.events.ghost.v1\noperations:", 1)
        root = self.make_tree({"api/asyncapi/asyncapi.yaml": spec})
        code, out = self.run_check(root)
        self.assertEqual(code, 1)

    def test_channels_as_list_fails(self):
        spec = "asyncapi: 3.0.0\nchannels:\n  - accepted\n"
        root = self.make_tree({"api/asyncapi/asyncapi.yaml": spec})
        code, out = self.run_check(root)
        self.assertEqual(code, 1)

    def test_tab_indentation_fails(self):
        spec = GHOST_SPEC.replace("  ghost:\n", "\tghost:\n")
        root = self.make_tree({"api/asyncapi/asyncapi.yaml": spec})
        code, out = self.run_check(root)
        self.assertEqual(code, 1)

    def test_receive_action_supported(self):
        spec = GHOST_SPEC.replace("action: send", "action: receive")
        files = {
            "api/asyncapi/asyncapi.yaml": spec,
            "internal/kafka/ghost.go": (
                "package kafka\n\n"
                'const TopicGhost = "audit.events.ghost.v1"\n'),
        }
        root = self.make_tree(files)
        code, out = self.run_check(root)
        self.assertEqual(code, 0, out)

    # -- Parser robustness (real-spec constructs) ---------------------------

    def test_folded_description_and_real_constructs(self):
        spec = """\
asyncapi: 3.0.0
info:
  title: Test
  version: 1.0.0
  description: Normalized audit event lifecycle topics.
defaultContentType: application/json
channels:
  accepted:
    address: audit.events.accepted.v1
    description: >-
      Accepted, normalized audit events. Producers MUST set the Kafka message
      key to the payload's canonical event_id; the replay recovery path also
      matches by payload event_id, so an absent or mismatched key never blocks
      recovery, but key alignment keeps dead-letter evidence and partitioning
      correct.
    messages:
      auditEvent:
        $ref: '#/components/messages/AuditEvent'
        bindings:
          kafka:
            key:
              type: string
              description: MUST equal the AuditEvent payload's event_id.
operations:
  publishAccepted:
    action: send
    channel:
      $ref: '#/channels/accepted'
components:
  messages:
    AuditEvent:
      payload:
        type: object
        required: [event_id, tenant_id]
        properties:
          event_id: { type: string, minLength: 1 }
          targets: { type: array, items: { type: object } }
          description: Server-assigned; a client-supplied value is stripped on ingest.
"""
        files = {
            "api/asyncapi/asyncapi.yaml": spec,
            "internal/kafka/kafka.go": (
                "package kafka\n\n"
                'const TopicAccepted = "audit.events.accepted.v1"\n'),
        }
        root = self.make_tree(files)
        code, out = self.run_check(root)
        self.assertEqual(code, 0, out)
        self.assertIn("PASS: asyncapi channels", out)

    def test_quoted_address_inline_comment_and_crlf(self):
        spec = (
            "asyncapi: 3.0.0\r\n"
            "info: { title: Test, version: 1.0.0 } # contract info\r\n"
            "channels:\r\n"
            "  ghost:\r\n"
            "    address: 'audit.events.ghost.v1'   # trailing whitespace\r\n"
            "operations:\r\n"
            "  publishGhost:\r\n"
            "    action: send\r\n"
            "    channel: '#/channels/ghost'\r\n"
        )
        files = {
            "api/asyncapi/asyncapi.yaml": spec,
            "internal/kafka/ghost.go": (
                "package kafka\n\n"
                'const TopicGhost = "audit.events.ghost.v1"\n'),
        }
        root = self.make_tree(files)
        code, out = self.run_check(root)
        self.assertEqual(code, 0, out)


if __name__ == "__main__":
    unittest.main()
