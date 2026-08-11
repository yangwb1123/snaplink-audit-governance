"""Global run registry: who is running what, where, for how long.

Shared-machine lesson (LESSONS.md D3): locks only tell you a repo is
held, not WHO runs WHAT. Every pi-batch process registers itself under
~/.pi-batch/runs/<pid>.json (one file per process — no cross-process
write contention), heartbeats its last activity every 60s via a daemon
thread, and unregisters on exit. `pi-batch ps` lists live runs across
all repositories and users on this host.

Lock-free read path: list_runs() only scans and stat()s files; a stale
file (dead pid) is reported with status=dead and can be pruned.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from pathlib import Path

HEARTBEAT_INTERVAL = 60  # seconds

_REGISTRY_DIR = Path.home() / ".pi-batch" / "runs"
_own_path: Path | None = None
_heartbeat_stop = threading.Event()


def _registry_dir() -> Path:
    _REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
    return _REGISTRY_DIR


def _redacted_argv(argv: list) -> list:
    """Same redaction as the lock payload: -p/--prompt values are secrets."""
    out: list = []
    skip_next = False
    for arg in argv:
        if skip_next:
            skip_next = False
            out.append("<redacted>")
            continue
        if arg in ("-p", "--prompt"):
            skip_next = True
            out.append(arg)
            continue
        out.append(arg)
    return out


def register_run(mode: str, repo: str, session_name: str = "",
                 status: str = "queued", extra: dict | None = None) -> Path:
    """Publish this process in the global registry; returns the file path.
    Call mark_running() once the lock is held, heartbeat() keeps the
    process visible, unregister() removes it on exit."""
    global _own_path
    meta = {
        "pid": os.getpid(),
        "host": os.uname().nodename,
        "start": time.time(),
        "mode": mode,
        "repo": repo,
        "session": session_name,
        "argv": _redacted_argv(sys.argv),
        "status": status,
        "last_active": time.time(),
    }
    if extra:
        meta.update(extra)
    _own_path = _registry_dir() / f"{os.getpid()}.json"
    _write_atomic(_own_path, meta)
    _heartbeat_stop.clear()
    threading.Thread(target=_heartbeat_loop, daemon=True).start()
    return _own_path


def mark_running() -> None:
    """Transition this run from queued/starting to running (lock held)."""
    _patch_own({"status": "running"})


def mark_waiting_lock() -> None:
    """The process is queued behind a held lock (visible as queued)."""
    _patch_own({"status": "queued"})


def heartbeat() -> None:
    _patch_own({"last_active": time.time()})


def unregister() -> None:
    """Remove this process from the registry (exit path)."""
    global _own_path
    _heartbeat_stop.set()
    if _own_path is not None:
        try:
            _own_path.unlink(missing_ok=True)
        except OSError:
        # best-effort I/O：失败不阻塞主流程（已验证有意）
            pass
        _own_path = None


def _patch_own(meta: dict) -> None:
    if _own_path is None:
        return
    try:
        data = json.loads(_own_path.read_text(encoding="utf-8"))
        data.update(meta)
        _write_atomic(_own_path, data)
    except (OSError, ValueError):
        pass  # registry is best-effort; never fail the run


def _write_atomic(path: Path, meta: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def _heartbeat_loop() -> None:
    while not _heartbeat_stop.wait(HEARTBEAT_INTERVAL):
        heartbeat()


def list_runs() -> list[dict]:
    """All registry entries for live processes on this host, oldest first.
    Entries whose pid is dead are returned with status='dead' so callers
    can show or prune them."""
    runs: list[dict] = []
    directory = _REGISTRY_DIR
    if not directory.is_dir():
        return runs
    for path in sorted(directory.glob("*.json")):
        try:
            meta = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        pid = int(meta.get("pid", 0) or 0)
        meta["status"] = "dead" if not (pid and _alive(pid)) else meta.get("status", "?")
        meta["_file"] = str(path)
        runs.append(meta)
    return runs


def prune_dead() -> int:
    """Remove registry files whose pid is dead; returns count removed."""
    removed = 0
    for run in list_runs():
        if run["status"] == "dead":
            try:
                Path(run["_file"]).unlink(missing_ok=True)
                removed += 1
            except OSError:
            # best-effort I/O：失败不阻塞主流程（已验证有意）
                pass
    return removed


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def format_ps(runs: list[dict]) -> str:
    """Human-readable table: PID / MODE / REPO / SESSION / AGE / IDLE /
    STATUS / ARGV."""
    lines = [f"{'PID':<8} {'MODE':<10} {'REPO':<44} {'SESSION':<24} "
             f"{'AGE':<7} {'IDLE':<6} STATUS  ARGV"]
    now = time.time()
    for r in runs:
        age = now - float(r.get("start", now))
        idle = now - float(r.get("last_active", r.get("start", now)))
        argv = " ".join(r.get("argv", []))
        if len(argv) > 60:
            argv = argv[:57] + "..."
        repo = r.get("repo", "?")
        if len(repo) > 44:
            repo = "..." + repo[-41:]
        lines.append(f"{r.get('pid', 0):<8} {str(r.get('mode', '?')):<10} "
                     f"{repo:<44} {str(r.get('session', ''))[:24]:<24} "
                     f"{_fmt_age(age):<7} {_fmt_age(idle):<6} "
                     f"{str(r.get('status', '?')):<8} {argv}")
    return "\n".join(lines)


def _fmt_age(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m"
    return f"{seconds / 3600:.1f}h"


def ps_main(argv: list) -> None:
    """`pi-batch ps [--json] [--prune]`: list live runs across repos."""
    import argparse
    parser = argparse.ArgumentParser(
        prog="pi-batch.py ps",
        description="List live pi-batch runs (global registry).")
    parser.add_argument("--json", action="store_true", help="emit JSON")
    parser.add_argument("--prune", action="store_true",
                        help="remove dead-pid registry entries first")
    args = parser.parse_args(argv)
    if args.prune:
        prune_dead()
    runs = list_runs()
    if args.json:
        print(json.dumps(runs, ensure_ascii=False, indent=2, default=str))
        return
    if not runs:
        print("No live pi-batch runs.")
        return
    print(format_ps(runs))
