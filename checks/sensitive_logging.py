import re
import sys
from pathlib import Path


# VALIDATION_PLAN step 6: no log line may reference event payload bodies.
# Architecture plan §14: logs, metrics and traces must never copy audit
# payloads. This guard scans logging calls and rejects argument expressions
# that hand over whole event objects or payload values.
BANNED = (r"\bpayload\b", r"\bevent\b", r"\bbody\b", r"\bdata\b")

LOG_CALL = re.compile(r"\.Printf\(")


def run() -> int:
    root = Path(__file__).resolve().parents[1]
    patterns = [re.compile(pattern) for pattern in BANNED]
    failures = []
    for path in sorted((root / "internal").rglob("*.go")):
        if path.name.endswith("_test.go"):
            continue
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not LOG_CALL.search(line):
                continue
            args = line.split("Printf(", 1)[1]
            for pattern in patterns:
                if pattern.search(args):
                    failures.append(f"{path.relative_to(root)}:{lineno}: log call references {pattern.pattern}")
    if failures:
        print("FAIL: sensitive logging", *failures, sep="\n  ")
        return 1
    print("PASS: sensitive logging")
    return 0


if __name__ == "__main__":
    sys.exit(run())
