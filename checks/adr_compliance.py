import sys
from pathlib import Path


def run() -> int:
    root = Path(__file__).resolve().parents[1]
    required = (root / "docs/ARCHITECTURE_PLAN.md", root / "docs/VALIDATION_PLAN.md",
                root / "engineering.yaml")
    missing = [str(path.relative_to(root)) for path in required if not path.exists()]
    if missing:
        print("FAIL: engineering evidence", *missing, sep="\n  ")
        return 1
    print("PASS: engineering evidence")
    return 0


if __name__ == "__main__":
    sys.exit(run())

