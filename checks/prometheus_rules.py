"""Static gate: the shipped Prometheus rules must keep the DLQ alert set
pinned so every permanent-loss / config-fixable path stays visible.

Pinned alerts (all in group `audit-dlq` of deploy/prometheus-rules.verify.yml):

- `AuditDLQUnresolvableDrop` (REQ-4/REQ-5): the permanent-loss path, expr
  `increase(audit_dlq_unresolvable_total[15m]) > 0`, severity warning, and
  annotations documenting the permanent-loss meaning. Before the split
  resolution counters landed, unresolvable drops were counted as `replayed`
  and invisible to alerting; a future change that re-conflates the paths,
  renames the alert, or loosens the expr would silently reintroduce that
  alert gap.

- `AuditDLQAuthBlocked` (campaign fail-fast-or-warn-on-empty-rotated-
  ingest-token): new 401-class dead-letters, expr
  `increase(audit_consumer_unauthorized_total[15m]) > 0`. This is the
  counter leg: it fires only while the consumer keeps dead-lettering
  401s, so it goes quiet once the stream stops or the consumer is down.

- `AuditDLQAuthBlockedBacklog` (same campaign): the static backlog leg,
  expr `audit_dlq_auth_blocked > 0`, on the replay-side gauge. Blocked
  records are excluded from `audit_dlq_pending` (so `AuditDLQBacklog`
  stays quiet) and from the wanted set (so the counter leg alone would
  leave a static backlog — including a forged `error_code=unauthorized`
  wedge — silent). A gauge is required here, not a counter: counters
  never fall, so a backlog-drained state is only observable on a gauge.

The rules file is also parsed with `yaml.safe_load` when PyYAML is
installed (guarded import: the gate must keep working without the
third-party module), so an unparsable rule file fails the gate instead of
being scanned as raw text.
"""

import re
import sys
from pathlib import Path

try:  # pragma: no cover - environment-dependent
    import yaml as _yaml
except ImportError:  # pragma: no cover
    _yaml = None

# name -> (pinned expr, pinned severity, annotation tokens that must appear
# somewhere in the alert block). Annotation tokens are checked against the
# whole block text (existing behavior): they document the alert's meaning,
# so a reword that drops them fails the gate.
PINNED_ALERTS = {
    "AuditDLQUnresolvableDrop": {
        "expr": "increase(audit_dlq_unresolvable_total[15m]) > 0",
        "severity": "warning",
        "tokens": ("unresolvable", "permanent loss"),
    },
    "AuditDLQAuthBlocked": {
        "expr": "increase(audit_consumer_unauthorized_total[15m]) > 0",
        "severity": "warning",
        "tokens": ("unauthorized", "token"),
    },
    "AuditDLQAuthBlockedBacklog": {
        "expr": "audit_dlq_auth_blocked > 0",
        "severity": "warning",
        "tokens": ("auth-blocked", "backlog"),
    },
}
EXPECTED_GROUP = "audit-dlq"

# Rule blocks are indented 6 spaces (`      - alert: ...`); group headers
# (`  - name: ...`) are indented 2. A block ends at the next line indented
# at or above the alert header (a new rule or a new group).
ALERT_HEADER = re.compile(r"^      - alert: (\S+)")
GROUP_HEADER = re.compile(r"^  - name: (\S+)")


def _alert_block(src: str, name: str) -> tuple[int, list[str]] | None:
    """Return (line index, block lines) of the named alert, or None."""
    lines = src.splitlines()
    for index, line in enumerate(lines):
        match = ALERT_HEADER.match(line)
        if match is None or match.group(1) != name:
            continue
        block = []
        for entry in lines[index + 1 :]:
            indent = len(entry) - len(entry.lstrip())
            if entry.strip() and indent <= 6:
                break
            block.append(entry)
        return index, block
    return None


def _group_of(src: str, line_index: int) -> str | None:
    """Return the name of the group containing line_index, or None."""
    group = None
    for line in src.splitlines()[:line_index]:
        match = GROUP_HEADER.match(line)
        if match:
            group = match.group(1)
    return group


def _check_alert(path: Path, src: str, name: str, pin: dict) -> list[str]:
    failures = []
    found = _alert_block(src, name)
    if found is None:
        failures.append(
            f"{path}: group {EXPECTED_GROUP} has no alert named {name} — "
            "the path it alerts on is invisible"
        )
        return failures
    index, block = found
    if _group_of(src, index) != EXPECTED_GROUP:
        failures.append(f"{path}: {name} must live in group {EXPECTED_GROUP}")
    if not any(line.strip() == f"expr: {pin['expr']}" for line in block):
        failures.append(f"{path}: {name} expr must be exactly {pin['expr']!r}")
    if not any(line.strip() == f"severity: {pin['severity']}" for line in block):
        failures.append(f"{path}: {name} severity must be exactly {pin['severity']!r}")
    block_text = " ".join(line.strip() for line in block).lower()
    for token in pin["tokens"]:
        if token not in block_text:
            failures.append(
                f"{path}: {name} block must document its meaning (contain {token!r})"
            )
    return failures


def run(root=None) -> int:
    if root is None:
        root = Path(__file__).resolve().parents[1]
    path = root / "deploy" / "prometheus-rules.verify.yml"
    src = path.read_text(encoding="utf-8")

    failures = []
    if _yaml is not None:
        try:
            _yaml.safe_load(src)
        except _yaml.YAMLError as exc:  # pragma: no cover - fixture-driven
            failures.append(f"{path}: rule file does not parse as YAML ({exc})")
    for name, pin in PINNED_ALERTS.items():
        failures.extend(_check_alert(path, src, name, pin))

    if failures:
        print("FAIL: prometheus rules gate", *failures, sep="\n  ")
        return 1
    print(f"PASS: prometheus rules gate ({path})")
    return 0


if __name__ == "__main__":
    sys.exit(run())
