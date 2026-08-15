"""Regression tests for checks/prometheus_rules.py (AuditDLQUnresolvableDrop
gate on deploy/prometheus-rules.verify.yml).

Covers: missing alert (pre-change shape) fails; the pinned expr/severity/
annotations pass; a wrong expr, wrong severity, missing permanent-loss
annotations, and wrong group each fail; and the current tree passes (the
REQ-4 rule is in place)."""

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from checks import prometheus_rules

ROOT = Path(__file__).resolve().parents[1]

PRE_FIX_RULES = """\
groups:
  - name: audit-dlq
    rules:
      - alert: AuditDLQTraffic
        expr: increase(audit_consumer_dlq_published_total[5m]) > 0
        labels:
          severity: warning
        annotations:
          summary: Audit events are being dead-lettered
      - alert: AuditDLQBacklog
        expr: audit_dlq_pending > 0
        labels:
          severity: warning
        annotations:
          summary: Dead-letter backlog not draining
      - alert: AuditDLQRepublishFailures
        expr: increase(audit_dlq_republish_failures_total[15m]) > 0
        labels:
          severity: warning
        annotations:
          summary: Dead-letter replay is failing
"""

FIXED_RULES = """\
groups:
  - name: audit-dlq
    rules:
      - alert: AuditDLQTraffic
        expr: increase(audit_consumer_dlq_published_total[5m]) > 0
        labels:
          severity: warning
        annotations:
          summary: Audit events are being dead-lettered
      - alert: AuditDLQUnresolvableDrop
        expr: increase(audit_dlq_unresolvable_total[15m]) > 0
        labels:
          severity: warning
        annotations:
          summary: Dead-lettered events dropped as unresolvable (permanent loss)
          description: "{{ $value }} dead-lettered events were resolved as unresolvable in the last 15m. This is a permanent loss of ledger evidence."
"""

WRONG_EXPR_RULES = """\
groups:
  - name: audit-dlq
    rules:
      - alert: AuditDLQUnresolvableDrop
        expr: increase(audit_dlq_unresolvable_total[5m]) > 0
        labels:
          severity: warning
        annotations:
          summary: Dead-lettered events dropped as unresolvable (permanent loss)
          description: "This is a permanent loss of ledger evidence."
"""

WRONG_SEVERITY_RULES = """\
groups:
  - name: audit-dlq
    rules:
      - alert: AuditDLQUnresolvableDrop
        expr: increase(audit_dlq_unresolvable_total[15m]) > 0
        labels:
          severity: critical
        annotations:
          summary: Dead-lettered events dropped as unresolvable (permanent loss)
          description: "This is a permanent loss of ledger evidence."
"""

MISSING_ANNOTATIONS_RULES = """\
groups:
  - name: audit-dlq
    rules:
      - alert: AuditDLQUnresolvableDrop
        expr: increase(audit_dlq_unresolvable_total[15m]) > 0
        labels:
          severity: warning
        annotations:
          summary: Unresolvable drops detected
          description: "Unresolvable drops in the last 15m."
"""

WRONG_GROUP_RULES = """\
groups:
  - name: audit-slo
    rules:
      - alert: AuditDLQUnresolvableDrop
        expr: increase(audit_dlq_unresolvable_total[15m]) > 0
        labels:
          severity: warning
        annotations:
          summary: Dead-lettered events dropped as unresolvable (permanent loss)
          description: "This is a permanent loss of ledger evidence."
"""


def _write_fixture(root: Path, body: str) -> None:
    deploy = root / "deploy"
    deploy.mkdir(parents=True, exist_ok=True)
    (deploy / "prometheus-rules.verify.yml").write_text(body, encoding="utf-8")


class PrometheusRulesTest(unittest.TestCase):
    def _run(self, body: str) -> int:
        with tempfile.TemporaryDirectory() as td:
            _write_fixture(Path(td), body)
            with redirect_stdout(io.StringIO()):
                return prometheus_rules.run(root=Path(td))

    def test_missing_alert_fails(self):
        """Pre-change shape (no unresolvable alert) must fail the gate."""
        self.assertEqual(self._run(PRE_FIX_RULES), 1)

    def test_pinned_alert_passes(self):
        """REQ-4 shape (exact expr, severity warning, permanent-loss
        annotations, group audit-dlq) must pass the gate."""
        self.assertEqual(self._run(FIXED_RULES), 0)

    def test_wrong_expr_fails(self):
        """A loosened/changed expr (e.g. a 5m window) must fail."""
        self.assertEqual(self._run(WRONG_EXPR_RULES), 1)

    def test_wrong_severity_fails(self):
        """A severity other than warning must fail (AC-3 pins warning)."""
        self.assertEqual(self._run(WRONG_SEVERITY_RULES), 1)

    def test_missing_permanent_loss_annotations_fails(self):
        """Annotations must document the permanent-loss meaning."""
        self.assertEqual(self._run(MISSING_ANNOTATIONS_RULES), 1)

    def test_wrong_group_fails(self):
        """The alert must live in the audit-dlq group."""
        self.assertEqual(self._run(WRONG_GROUP_RULES), 1)

    def test_current_tree_passes(self):
        """The fixed tree satisfies the gate (guards against wiring drift)."""
        with redirect_stdout(io.StringIO()):
            self.assertEqual(prometheus_rules.run(root=ROOT), 0)


if __name__ == "__main__":
    unittest.main()
