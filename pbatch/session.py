"""pi session file access for rotation (R9 / T10).

Reads the session files pi writes under ~/.pi/agent/sessions/ (location
spike-validated in docs/spike-pi-session.md). Used to detect session
growth (rotation watermark) and compaction entries (warn), and to build
the --fork invocation that starts a fresh session from the old file.
Everything degrades gracefully: a missing/unreadable session simply means
"no rotation needed".
"""

from __future__ import annotations

import json
import os
from glob import escape as glob_escape
from pathlib import Path
from typing import Optional

from . import config
from .memory_io import bounded_lines

# test-injectable (monkeypatch), default matches pi's runtime layout
SESSIONS_DIR = Path.home() / ".pi" / "agent" / "sessions"


def workdir_key(cwd: str) -> str:
    """pi's session-dir key for a workdir: the absolute path with its
    leading '/' stripped and remaining '/' -> '-', wrapped in '--'. pi
    writes `--home-u1-project--` for /home/u1/project; dashing the leading
    slash too produces `---home-u1-project--`, which never matches a real
    pi session tree (round-4 finding P0-2)."""
    path = (cwd or os.getcwd()).lstrip("/")
    return "--" + path.replace("/", "-") + "--"


def session_file(cwd: str, session_name: str) -> Optional[Path]:
    """Newest session file for an id/name reference under *cwd*.

    Pi normally uses the session id as the filename suffix, while older
    adapters used the display name; accepting both keeps rotation portable.
    """
    d = SESSIONS_DIR / workdir_key(cwd)
    if not d.is_dir():
        return None
    try:
        escaped = glob_escape(session_name)
        candidates = sorted({path for pattern in (f"*_{escaped}.jsonl",
                                                   f"*_{escaped}-*.jsonl")
                             for path in d.glob(pattern)})
    except OSError:
        return None
    return candidates[-1] if candidates else None


def session_size_bytes(cwd: str, session_name: str) -> int:
    """Size of the active session file (0 when absent)."""
    f = session_file(cwd, session_name)
    if f is None:
        return 0
    try:
        return f.stat().st_size
    except OSError:
        return 0


def has_compaction(cwd: str, session_name: str) -> bool:
    """True when the session file contains a compaction entry (pi shrinks
    the context; worth warning the operator about)."""
    f = session_file(cwd, session_name)
    if f is None:
        return False
    try:
        for line in bounded_lines(f, config.SESSION_LINE_MAX_BYTES):
            if line is None:
                continue
            try:
                if json.loads(line).get("type") == "compaction":
                    return True
            except ValueError:
                continue
    except OSError:
        return False
    return False


def fork_flags(session_file_path: str) -> list:
    """Agent flags that fork the session file into a fresh session (the
    rotation mechanism; --fork is documented by pi 0.83)."""
    from .config import _session_flags
    return _session_flags("fork", session_file_path, "")
