"""Residue marker warnings (split from cli_batch.py for line budget).

`.pi-batch/markers` 残留扫描：被杀的 runner 会留下孤儿标记，下一轮启动
前警告并可选清理。
"""

from __future__ import annotations

from typing import Optional

from .config import log


def _warn_residue_markers(dirs: Optional[list] = None,
                          kill_orphans: bool = False) -> None:
    """Warn about leftover task markers (or clean them with --kill-orphans)."""
    from pathlib import Path
    from .triage import clear_marker, residue_markers
    roots = [Path(d) for d in (dirs or [])] or [Path.cwd()]
    found = []
    for root in roots:
        found.extend(residue_markers(root))
    if found:
        log.warning("%d residue marker(s) from an interrupted run; "
                    "rerun with --kill-orphans to clean", len(found))
    if kill_orphans:
        for marker in found:
            clear_marker(marker.stem if hasattr(marker, "stem") else marker,
                         str(marker.parent))
        if found:
            log.info("cleaned %d residue marker(s)", len(found))
