"""Pipeline archive helpers (split from pbatch/pipeline.py for line budget).

完成即处理：阶段/流水线全部成功后，把交付物移入时间戳归档目录，工作区
保持整洁；git 历史保留一切。归档函数被 pipeline.py 与 cli.py 共用。
"""

from __future__ import annotations

import hashlib
import shutil
from datetime import datetime
from pathlib import Path

from .config import log


def archive_output_pairs(outputs: list, archive_dir: str,
                         label: str) -> list:
    """Move outputs and retain their original-to-archive mapping."""
    if not archive_dir or not outputs:
        return []
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    target = Path(archive_dir) / f"{label}-{stamp}"
    target.mkdir(parents=True, exist_ok=True)
    moved = []
    for o in outputs:
        p = Path(o)
        if p.exists():
            dest = _archive_destination(target, p)
            shutil.move(str(p), str(dest))
            moved.append((str(o), str(dest)))
    if moved:
        log.info("ARCHIVED %d deliverable(s) -> %s", len(moved), target)
    return moved


def archive_outputs(outputs: list, archive_dir: str, label: str) -> list:
    """Move finished deliverables into a timestamped archive subdirectory
    once the stage/pipeline completed (all tasks succeeded, gates passed).
    The worktree stays clean; git history (committed artifacts) retains
    everything, and the decision log points at the original paths. Returns
    the moved destinations for logging."""
    return [destination for _, destination in
            archive_output_pairs(outputs, archive_dir, label)]


def _archive_destination(target: Path, source: Path) -> Path:
    """Choose a destination without overwriting an earlier deliverable."""
    direct = target / source.name
    if not direct.exists():
        return direct
    digest = hashlib.sha256(str(source.resolve()).encode("utf-8")).hexdigest()[:8]
    candidate = target / f"{source.stem}-{digest}{source.suffix}"
    index = 2
    while candidate.exists():
        candidate = target / f"{source.stem}-{digest}-{index}{source.suffix}"
        index += 1
    return candidate
