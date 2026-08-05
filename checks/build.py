import subprocess
import sys
from pathlib import Path
from .config import ROOT


def run() -> int:
    output = ROOT / "bin"
    output.mkdir(exist_ok=True)
    targets = (("audit-api", "./cmd/audit-api"), ("audit-governance-worker", "./cmd/audit-governance-worker"), ("audit-outbox-relay", "./cmd/audit-outbox-relay"))
    for name, package in targets:
        result = subprocess.run(["go", "build", "-o", str(output / name), package], cwd=ROOT, check=False)
        if result.returncode:
            return result.returncode
    print("PASS: build")
    return 0


if __name__ == "__main__":
    sys.exit(run())

