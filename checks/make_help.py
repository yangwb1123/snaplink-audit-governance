import sys
from pathlib import Path


def run() -> int:
    path = Path(__file__).resolve().parents[1] / "Makefile"
    if not path.exists():
        print("FAIL: Makefile missing")
        return 1
    targets = [line.split(":", 1)[0] for line in path.read_text(encoding="utf-8").splitlines()
               if ":" in line and not line.startswith("\t")]
    print(f"PASS: Makefile ({len(targets)} targets)")
    return 0


if __name__ == "__main__":
    sys.exit(run())

