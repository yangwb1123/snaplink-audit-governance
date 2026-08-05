import sys
from pathlib import Path
from .config import ROOT


def run() -> int:
    path = ROOT / "engineering-exemptions.yaml"
    if not path.exists():
        print("PASS: exemptions (none)")
        return 0
    text = path.read_text(encoding="utf-8")
    ok = "reason:" in text and "owner:" in text
    print("PASS: exemptions" if ok else "FAIL: exemptions")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(run())

