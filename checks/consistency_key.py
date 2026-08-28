#!/usr/bin/env python3
"""Compare the signer/archive identity emitted by the two built binaries.

The check deliberately invokes ``-consistency-key`` rather than
``-check-config``.  The former is a pure, network-free configuration read, so
this parity gate remains usable before an archive endpoint is reachable.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

from .config import ROOT


KEY_RE = re.compile(r"(?:^|\s)consistency_key=(?P<key>[^\s]+)\s*$")
BINARY_NAMES = ("audit-api", "audit-governance-worker")


def extract_key(output: str) -> str | None:
    """Return one unambiguous key from a process output, or ``None``.

    A missing, blank, or multiply-emitted key is invalid.  Failing closed on
    ambiguity prevents an unrelated diagnostic line from being mistaken for
    the process's resolved identity.
    """
    values = [match.group("key") for line in output.splitlines() for match in [KEY_RE.search(line)] if match]
    if len(values) != 1 or not values[0]:
        return None
    return values[0]


def _run(binary: str, root: Path, env: dict[str, str]) -> tuple[str | None, str | None]:
    path = root / "bin" / binary
    if not path.is_file():
        return None, f"{binary}: built binary not found at {path} (run `python3 cli.py build` first)"
    try:
        result = subprocess.run(
            [str(path), "-consistency-key"],
            cwd=str(root),
            env=dict(env),
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        return None, f"{binary}: could not execute {path}: {exc}"
    if result.returncode != 0:
        return None, f"{binary}: -consistency-key exited {result.returncode}"
    output = (result.stdout or "") + ("\n" + result.stderr if result.stderr else "")
    key = extract_key(output)
    if key is None:
        return None, f"{binary}: successful -consistency-key run did not emit exactly one consistency_key"
    return key, None


def run(root: Path = ROOT, env: dict[str, str] | None = None) -> int:
    """Run the cross-process consistency assertion.

    ``env`` is copied once and supplied unchanged to both processes.  An
    empty mapping is meaningful for tests and is not replaced by the host
    environment.
    """
    root = Path(root)
    shared_env = dict(os.environ) if env is None else dict(env)
    values: dict[str, str | None] = {}
    errors: list[str] = []
    for binary in BINARY_NAMES:
        value, error = _run(binary, root, shared_env)
        values[binary] = value
        if error is not None:
            errors.append(error)
    if errors:
        print("FAIL: consistency_key check")
        for error in errors:
            print(f"  {error}")
        for binary in BINARY_NAMES:
            print(f"  {binary}: {values[binary] if values[binary] is not None else '<missing>'}")
        return 1

    api_key = values["audit-api"]
    worker_key = values["audit-governance-worker"]
    if api_key != worker_key:
        print("FAIL: consistency_key mismatch")
        print(f"  audit-api:           {api_key}")
        print(f"  audit-governance-worker: {worker_key}")
        return 1
    print(f"PASS: consistency_key identical across processes ({api_key})")
    return 0


if __name__ == "__main__":
    sys.exit(run())
