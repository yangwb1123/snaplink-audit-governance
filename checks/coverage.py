import subprocess
import sys
import tempfile
from pathlib import Path
from .config import ROOT


def run() -> int:
    with tempfile.NamedTemporaryFile(prefix="audit-coverage-", suffix=".out") as report:
        result = subprocess.run(["go", "test", "-coverprofile=" + report.name, "./..."], cwd=ROOT, check=False)
    print("PASS: coverage" if result.returncode == 0 else "FAIL: coverage")
    return result.returncode


if __name__ == "__main__":
    sys.exit(run())

