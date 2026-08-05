import sys
from pathlib import Path


def run() -> int:
    root = Path(__file__).resolve().parents[1]
    banned = {"handler.go", "service.go", "store.go"}
    violations = [path.name for path in root.iterdir() if path.is_file() and path.name in banned]
    if violations:
        print("FAIL: root business files", *violations, sep="\n  ")
        return 1
    print("PASS: root business files")
    return 0


if __name__ == "__main__":
    sys.exit(run())

