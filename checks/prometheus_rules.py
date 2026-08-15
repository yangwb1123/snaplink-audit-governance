"""Static gate: the shipped Prometheus rules must alert on the
unresolvable-drop permanent-loss path.

REQ-4/REQ-5: `deploy/prometheus-rules.verify.yml` group `audit-dlq` must
contain the `AuditDLQUnresolvableDrop` alert with the exact expr
`increase(audit_dlq_unresolvable_total[15m]) > 0`, `severity: warning`, and
annotations documenting the permanent-loss meaning. Before the split
resolution counters landed, unresolvable drops were counted as `replayed`
and invisible to alerting; a future change that re-conflates the paths,
renames the alert, or loosens the expr would silently reintroduce that
alert gap. This gate fails on the pre-change rule file and passes once
REQ-4 lands.

The gate is intentionally narrow and stdlib-only: it scans the YAML text
for the alert block and asserts the pinned expr/severity/annotations. It
does not import PyYAML or compile anything.
"""

import re
import sys
from pathlib import Path

ALERT_NAME = "AuditDLQUnresolvableDrop"
EXPECTED_EXPR = "increase(audit_dlq_unresolvable_total[15m]) > 0"
EXPECTED_SEVERITY = "warning"

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


def run(root=None) -> int:
    if root is None:
        root = Path(__file__).resolve().parents[1]
    path = root / "deploy" / "prometheus-rules.verify.yml"
    src = path.read_text(encoding="utf-8")

    failures = []
    found = _alert_block(src, ALERT_NAME)
    if found is None:
        failures.append(
            f"{path}: group audit-dlq has no alert named {ALERT_NAME} — "
            "unresolvable drops are invisible to alerting"
        )
    else:
        index, block = found
        if _group_of(src, index) != "audit-dlq":
            failures.append(f"{path}: {ALERT_NAME} must live in group audit-dlq")
        if not any(line.strip() == f"expr: {EXPECTED_EXPR}" for line in block):
            failures.append(f"{path}: {ALERT_NAME} expr must be exactly {EXPECTED_EXPR!r}")
        if not any(line.strip() == f"severity: {EXPECTED_SEVERITY}" for line in block):
            failures.append(f"{path}: {ALERT_NAME} severity must be exactly {EXPECTED_SEVERITY!r}")
        annotations = " ".join(line.strip() for line in block).lower()
        if "unresolvable" not in annotations or "permanent loss" not in annotations:
            failures.append(
                f"{path}: {ALERT_NAME} annotations must document the permanent-loss "
                "meaning (contain 'unresolvable' and 'permanent loss')"
            )

    if failures:
        print("FAIL: prometheus rules gate", *failures, sep="\n  ")
        return 1
    print(f"PASS: prometheus rules gate ({path})")
    return 0


if __name__ == "__main__":
    sys.exit(run())
