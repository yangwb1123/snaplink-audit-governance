import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run() -> int:
    result = subprocess.run([sys.executable, str(ROOT / "cli.py"), "quality"], cwd=ROOT, check=False)
    return result.returncode


if __name__ == "__main__":
    sys.exit(run())

