"""Bounded JSONL index scans shared by progressive-memory queries."""

from __future__ import annotations

import json
from pathlib import Path

from .config import log
from .memory_io import bounded_lines, reverse_lines


def scan_events(path: Path, maximum: int, newest_first: bool = False):
    """Yield valid objects without materializing or trusting an index line."""
    if not path.is_file() or path.is_symlink():
        return
    lines = (reverse_lines(path, maximum=maximum) if newest_first
             else bounded_lines(path, maximum))
    try:
        for number, line in enumerate(lines, 1):
            if line is None:
                log.warning("MEMORY: ignoring oversized index record %d in %s",
                            number, path)
                continue
            try:
                item = json.loads(line)
            except ValueError:
                continue
            if isinstance(item, dict):
                yield item
    except OSError:
        return
