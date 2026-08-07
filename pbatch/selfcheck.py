"""One-command self-check for the tool itself: `pi-batch check`.

Runs the local engineering gates that keep the rule system healthy:
- quality: code-organization scanner (functions/lines/complexity/dupes)
- registry: schema integrity of both domain rule registries (rules --check)
- eval: the rule-system regression suite (evals/, 25 cases)

Exit 0 only when everything passes (fail closed); useful after installing
or editing rules/keywords/registry to confirm the environment is sound.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def _run_step(name: str, command: list) -> bool:
    """Run one gate; print its tail; True when it passed."""
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=600)
    except FileNotFoundError:
        print(f"CHECK [{name}] command not found: {command[0]}", file=sys.stderr)
        return False
    tail = (result.stdout + result.stderr).strip().splitlines()[-3:]
    for line in tail:
        print(f"CHECK [{name}] {line}")
    return result.returncode == 0


def check_main(argv: list) -> None:
    """`pi-batch check [--skip-eval]`."""
    import argparse
    parser = argparse.ArgumentParser(
        prog="pi-batch.py check",
        description="One-command self-check: quality + registry schema + eval.")
    parser.add_argument("--skip-eval", action="store_true",
                        help="skip the eval suite (slowest step)")
    args = parser.parse_args(argv)
    steps = [
        ("quality", [sys.executable, str(Path("quality.py")), "--strict", "."]),
        ("registry", [sys.executable, "-m", "pbatch", "rules", "--check"]),
    ]
    if not args.skip_eval:
        steps.append(("eval", [sys.executable, "-m", "pbatch", "eval"]))
    failed = [name for name, cmd in steps if not _run_step(name, cmd)]
    if failed:
        print(f"CHECK: failed: {', '.join(failed)}", file=sys.stderr)
        sys.exit(1)
    print("CHECK: all gates passed")
