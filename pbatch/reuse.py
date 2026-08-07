"""Reuse decision: is an existing artifact still reusable?

Centralizes the six reuse sites (cli._filter_reused, the four pipeline
task builders, and execute_stage's full-reuse short-circuit) behind one
contract: an artifact is reusable only when it exists, is non-empty, is
not a symlink, and (when validators are configured) still passes every
effective gate. A stale artifact is deleted so the caller regenerates it.

`--reuse-legacy` restores the old existence-only skip as an escape hatch.
"""

from __future__ import annotations

import hashlib
import json
import os
import shlex
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import config
from .runner import revalidate_existing
from .config import log


def reuse_decision(output_path: str, validate_cmd: Optional[str],
                   workdir: str = "", legacy: bool = False,
                   expected_fp: Optional[str] = None) -> bool:
    """True when the artifact may be reused (skipped) by --reuse.

    Contract: exists AND non-empty AND not a symlink AND (no effective
    validators OR all of them pass against the artifact). A symlink or a
    failed revalidation deletes the artifact and returns False so the
    caller regenerates it (never validated through a symlink target).

    Args:
        output_path: the artifact path (resolved, absolute preferred)
        validate_cmd: the EFFECTIVE gate for this task/stage (task.validate
            > stage.validate_cmd > CLI --validate-cmd), or None/"" for none
        workdir: cwd used for validator commands ({cwd} placeholder) and
            relative output resolution
        legacy: existence-only check (--reuse-legacy escape hatch)
        expected_fp: when set (--reuse-fingerprint), the artifact is reused
            only when its sidecar fingerprint matches (T9); a mismatch
            deletes the stale artifact + sidecar for regeneration
    """
    if legacy:
        return Path(output_path).exists()
    path = Path(output_path)
    if not path.is_file() or path.is_symlink():
        if path.is_symlink():
            log.warning("REUSE: %s is a symlink; deleting and regenerating", path)
            path.unlink(missing_ok=True)
        return False
    try:
        if path.stat().st_size == 0:
            return False
    except OSError:
        return False  # vanished between is_file and stat: fail closed
    # T9 (fingerprint reuse 2.0): sidecar must match the current inputs;
    # a missing/mismatched sidecar means the inputs changed -> regenerate.
    if expected_fp is not None and not sidecar_matches(path, expected_fp):
        log.warning("REUSE: fingerprint mismatch for %s (inputs changed); regenerating", path)
        path.unlink(missing_ok=True)
        sidecar_path(path).unlink(missing_ok=True)
        return False
    wd = workdir or os.getcwd()
    if revalidate_existing(path, validate_cmd, wd):
        log.info("REUSED+VALIDATED: %s", path)
        return True
    log.warning("REUSE: deleting stale artifact %s", path)
    path.unlink(missing_ok=True)
    return False


def fingerprint(prompt: str, output: str, validate_cmd: Optional[str],
                model: str = "", provider: str = "") -> str:
    """sha256 of the canonical task inputs (T9 / decision D1): prompt,
    output path, effective gate spec, and model/provider routing."""
    canon = json.dumps([prompt, output, validate_cmd, model, provider],
                       sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


def reuse_fingerprint(task, validate_cmd: Optional[str]) -> str:
    """Fingerprint for a Task object (T9), used by the single-batch
    reuse path."""
    return fingerprint(task.prompt, str(task.output_path()), validate_cmd,
                       task.model, task.provider)


def sidecar_path(output: str) -> Path:
    return Path(output).with_suffix(Path(output).suffix + ".meta.json")


def write_sidecar(output: str, prompt: str, validate_cmd: Optional[str],
                  model: str = "", provider: str = "") -> None:
    """Persist the artifact's input fingerprint next to it (frozen format
    version 1; only written under --reuse-fingerprint)."""
    meta = {
        "version": 1,
        "fingerprint": fingerprint(prompt, output, validate_cmd, model, provider),
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "model": model,
        "provider": provider,
    }
    try:
        sidecar_path(output).write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    except OSError as e:
        log.warning("sidecar write failed: %s", e)


def sidecar_matches(path: Path, expected_fp: str) -> bool:
    """True when the artifact's sidecar exists, is version 1, and carries
    the expected fingerprint; anything else (missing/corrupt/older format)
    means the artifact must be regenerated."""
    try:
        meta = json.loads(sidecar_path(str(path)).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return meta.get("version") == 1 and meta.get("fingerprint") == expected_fp
