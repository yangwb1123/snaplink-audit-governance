import re
import sys
from pathlib import Path
from .config import get_config, ROOT


FUNCTION = re.compile(r"^func (?:\([^)]*\) )?(\w+)")


def run() -> int:
    config = get_config()
    failures = 0
    for path in sorted(ROOT.rglob("*.go")):
        if "_test.go" in path.name or "/bin/" in str(path):
            continue
        lines = path.read_text(encoding="utf-8").splitlines()
        starts = [(index, match.group(1)) for index, line in enumerate(lines)
                  if (match := FUNCTION.match(line))]
        for position, (start, name) in enumerate(starts):
            end = starts[position + 1][0] if position + 1 < len(starts) else len(lines)
            body = lines[start:end]
            decisions = sum(line.count(token) for line in body
                            for token in (" if ", " for ", "switch ", "case ", "&&", "||"))
            if len(body) > config.max_function_lines or decisions > config.max_decisions:
                print(f"FAIL complexity: {path.relative_to(ROOT)}:{start + 1} {name} lines={len(body)} decisions={decisions}")
                failures += 1
    print("PASS: complexity" if not failures else f"FAIL: complexity ({failures})")
    return int(bool(failures))


if __name__ == "__main__":
    sys.exit(run())

