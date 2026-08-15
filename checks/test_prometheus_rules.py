"""Regression tests for checks/prometheus_rules.py.

Covers the three pinned audit-dlq alerts: missing alert (pre-change shapes)
fails; the pinned exprs/severities/annotations pass; a wrong expr, wrong
severity, missing meaning annotations, wrong group, a missing backlog leg,
and an unparsable rule file each fail; and the current tree passes (all
three rules are in place)."""

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
      - alert: AuditDLQAuthBlocked
        expr: increase(audit_consumer_unauthorized_total[15m]) > 0
        labels:
          severity: warning
        annotations:
          summary: Audit ingest token rejected (401) — dead-letters classified unauthorized
          description: "{{ $value }} dead-lettered events classified as unauthorized in the last 15m. Restore/rotate AUDIT_OUTBOX_TOKEN; records stay auth-blocked until replay is re-enabled."
      - alert: AuditDLQAuthBlockedBacklog
        expr: audit_dlq_auth_blocked > 0
        labels:
          severity: warning
        annotations:
          summary: Auth-blocked DLQ backlog not draining
          description: "{{ $value }} dead-lettered records are held auth-blocked (error_code=unauthorized). Verify the credential, then drain with -replay-auth-blocked; the gauge falls to 0 when drained."
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
      - alert: AuditDLQAuthBlocked
        expr: increase(audit_consumer_unauthorized_total[15m]) > 0
        labels:
          severity: warning
        annotations:
          summary: Audit ingest token rejected (401) — dead-letters classified unauthorized
          description: "Restore/rotate AUDIT_OUTBOX_TOKEN."
      - alert: AuditDLQAuthBlockedBacklog
        expr: audit_dlq_auth_blocked > 0
        labels:
          severity: warning
        annotations:
          summary: Auth-blocked DLQ backlog not draining
          description: "Verify the credential, then drain with -replay-auth-blocked."
"""

WRONG_SEVERITY_RULES = """\
groups:
  - name: audit-dlq
    rules:
      - alert: AuditDLQUnresolvableDrop
        expr: increase(audit_dlq_unresolvable_total[15m]) > 0
        labels:
          severity: warning
        annotations:
          summary: Dead-lettered events dropped as unresolvable (permanent loss)
          description: "This is a permanent loss of ledger evidence."
      - alert: AuditDLQAuthBlocked
        expr: increase(audit_consumer_unauthorized_total[15m]) > 0
        labels:
          severity: warning
        annotations:
          summary: Audit ingest token rejected (401) — dead-letters classified unauthorized
          description: "Restore/rotate AUDIT_OUTBOX_TOKEN."
      - alert: AuditDLQAuthBlockedBacklog
        expr: audit_dlq_auth_blocked > 0
        labels:
          severity: critical
        annotations:
          summary: Auth-blocked DLQ backlog not draining
          description: "Verify the credential, then drain with -replay-auth-blocked."
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
      - alert: AuditDLQAuthBlocked
        expr: increase(audit_consumer_unauthorized_total[15m]) > 0
        labels:
          severity: warning
        annotations:
          summary: Audit ingest token rejected (401) — dead-letters classified unauthorized
          description: "Restore/rotate AUDIT_OUTBOX_TOKEN."
      - alert: AuditDLQAuthBlockedBacklog
        expr: audit_dlq_auth_blocked > 0
        labels:
          severity: warning
        annotations:
          summary: Auth-blocked DLQ records pending
          description: "Verify the credential, then drain with -replay-auth-blocked."
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
  - name: audit-dlq
    rules:
      - alert: AuditDLQAuthBlocked
        expr: increase(audit_consumer_unauthorized_total[15m]) > 0
        labels:
          severity: warning
        annotations:
          summary: Audit ingest token rejected (401) — dead-letters classified unauthorized
          description: "Restore/rotate AUDIT_OUTBOX_TOKEN."
      - alert: AuditDLQAuthBlockedBacklog
        expr: audit_dlq_auth_blocked > 0
        labels:
          severity: warning
        annotations:
          summary: Auth-blocked DLQ backlog not draining
          description: "Verify the credential, then drain with -replay-auth-blocked."
"""

MISSING_BACKLOG_ALERT_RULES = """\
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
          description: "This is a permanent loss of ledger evidence."
      - alert: AuditDLQAuthBlocked
        expr: increase(audit_consumer_unauthorized_total[15m]) > 0
        labels:
          severity: warning
        annotations:
          summary: Audit ingest token rejected (401) — dead-letters classified unauthorized
          description: "Restore/rotate AUDIT_OUTBOX_TOKEN."
"""

# The F-03 semantic error shape: the backlog leg references the counter
# (audit_dlq_auth_blocked_total) instead of the gauge — a counter never
# falls, so a drained backlog stays firing and a static backlog is not
# distinguishable. The gate pins the gauge expr.
COUNTER_ONLY_LEG_RULES = """\
groups:
  - name: audit-dlq
    rules:
      - alert: AuditDLQUnresolvableDrop
        expr: increase(audit_dlq_unresolvable_total[15m]) > 0
        labels:
          severity: warning
        annotations:
          summary: Dead-lettered events dropped as unresolvable (permanent loss)
          description: "This is a permanent loss of ledger evidence."
      - alert: AuditDLQAuthBlocked
        expr: increase(audit_consumer_unauthorized_total[15m]) > 0
        labels:
          severity: warning
        annotations:
          summary: Audit ingest token rejected (401) — dead-letters classified unauthorized
          description: "Restore/rotate AUDIT_OUTBOX_TOKEN."
      - alert: AuditDLQAuthBlockedBacklog
        expr: audit_dlq_auth_blocked_total > 0
        labels:
          severity: warning
        annotations:
          summary: Auth-blocked DLQ backlog not draining
          description: "Verify the credential, then drain with -replay-auth-blocked."
"""

UNPARSABLE_RULES = """\
groups:
  - name: audit-dlq
    rules:
      - alert: AuditDLQUnresolvableDrop
        expr: increase(audit_dlq_unresolvable_total[15m]) > 0
        labels:
          severity: warning
        annotations:
          summary: Dead-lettered events dropped as unresolvable (permanent loss)
          description: "This is a permanent loss of ledger evidence."
\t- alert: AuditDLQAuthBlocked   # tab indentation: YAML syntax error
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
        """Pre-change shape (no unresolvable/auth-blocked alerts) must fail."""
        self.assertEqual(self._run(PRE_FIX_RULES), 1)

    def test_pinned_alerts_pass(self):
        """All three pinned alerts with exact exprs, severity warning, and
        meaning-documenting annotations must pass the gate."""
        self.assertEqual(self._run(FIXED_RULES), 0)

    def test_wrong_expr_fails(self):
        """A loosened/changed expr (e.g. a 5m window) must fail."""
        self.assertEqual(self._run(WRONG_EXPR_RULES), 1)

    def test_wrong_severity_fails(self):
        """A severity other than warning must fail."""
        self.assertEqual(self._run(WRONG_SEVERITY_RULES), 1)

    def test_missing_meaning_annotations_fails(self):
        """Annotations must document each alert's meaning."""
        self.assertEqual(self._run(MISSING_ANNOTATIONS_RULES), 1)

    def test_wrong_group_fails(self):
        """The alerts must live in the audit-dlq group."""
        self.assertEqual(self._run(WRONG_GROUP_RULES), 1)

    def test_missing_backlog_alert_fails(self):
        """Removing the gauge-leg alert (static backlog invisible) must fail."""
        self.assertEqual(self._run(MISSING_BACKLOG_ALERT_RULES), 1)

    def test_counter_only_leg_fails(self):
        """The F-03 semantic error — the backlog leg referencing the counter
        (audit_dlq_auth_blocked_total) instead of the gauge — must fail: a
        counter never falls, so a drained backlog stays firing."""
        self.assertEqual(self._run(COUNTER_ONLY_LEG_RULES), 1)

    def test_unparsable_yaml_fails(self):
        """A rule file that does not parse as YAML must fail the gate."""
        self.assertEqual(self._run(UNPARSABLE_RULES), 1)

    def test_current_tree_passes(self):
        """The fixed tree satisfies the gate (guards against wiring drift)."""
        with redirect_stdout(io.StringIO()):
            self.assertEqual(prometheus_rules.run(root=ROOT), 0)


if __name__ == "__main__":
    unittest.main()
