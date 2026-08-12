"""Static gate: no replay round may reuse the previous round's end position.

REQ-1/REQ-6: `RunOnce` in internal/kafka/replay.go must invoke the
per-round accepted-reader reset (`resetAcceptedToFirstOffset` /
`seekAcceptedToStart`) BEFORE `scanAccepted`. kafka-go honors
`ReaderConfig.StartOffset` only at reader creation and rejects `SetOffset`
for group readers, so the only correct mechanism is per-round reader
recreation; a future change that removes or reorders the reset would
silently reintroduce the daemon-mode data-loss bug (old DLQ events marked
unresolvable and dropped). This gate fails on today's pre-fix code and
passes once REQ-1 lands.

The gate is intentionally narrow: it parses only the `RunOnce` body (brace
balanced) and checks call ordering. It does not import or compile Go.
"""

import re
import sys
from pathlib import Path

RESET_NAMES = ("resetAcceptedToFirstOffset", "seekAcceptedToStart")
RESET_CALL = re.compile(r"r\.(?:resetAcceptedToFirstOffset|seekAcceptedToStart)\(")
SCAN_CALL = re.compile(r"scanAccepted\(")
RUNONCE_HEADER = "func (r *Replayer) RunOnce("


def _func_body(src: str, header: str) -> str:
    """Return the brace-balanced body of the first func whose header contains
    `header`, without the outer braces."""
    start = src.index(header)
    brace = src.index("{", start)
    depth = 0
    for i in range(brace, len(src)):
        if src[i] == "{":
            depth += 1
        elif src[i] == "}":
            depth -= 1
            if depth == 0:
                return src[brace + 1 : i]
    raise ValueError(f"unbalanced braces after {header!r}")


def run(root=None) -> int:
    if root is None:
        root = Path(__file__).resolve().parents[1]
    path = root / "internal" / "kafka" / "replay.go"
    src = path.read_text(encoding="utf-8")

    failures = []
    try:
        body = _func_body(src, RUNONCE_HEADER)
    except ValueError as err:
        print(f"FAIL: replay round-reset gate — cannot parse RunOnce: {err}")
        return 1

    reset = RESET_CALL.search(body)
    scan = SCAN_CALL.search(body)
    if reset is None:
        failures.append(f"{path}: RunOnce has no per-round accepted-reader reset call ({'/'.join(RESET_NAMES)})")
    elif scan is None:
        failures.append(f"{path}: RunOnce does not call scanAccepted")
    elif reset.start() > scan.start():
        failures.append(f"{path}: RunOnce reset call appears AFTER scanAccepted — a round may reuse the previous round's end position")

    if failures:
        print("FAIL: replay round-reset gate", *failures, sep="\n  ")
        return 1
    print(f"PASS: replay round-reset gate ({path})")
    return 0


if __name__ == "__main__":
    sys.exit(run())
