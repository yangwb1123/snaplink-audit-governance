"""Single-instance lock (R4 / T4, 7x24 governance).

A second pi-batch process must not run against the same worktree: two
round loops would double-spend quota and interleave session writes. The
lock is an O_EXCL-published file `.pi-batch.lock` in the process cwd
holding {pid, start, host, argv, token} JSON.

Contract (hardened after the round-4 adversarial reviews):

- **Atomic publication**: the payload is written to a unique temp file
  and linked into place (`os.link`), so no reader can ever observe an
  empty/partial lock file (a non-atomic write + separate O_EXCL create
  let a second instance break a lock that was still being born).
- **Stale = dead PID only**: a lock whose holder PID is alive is refused
  (exit 5) regardless of age. Live locks are NEVER broken on age alone —
  the tool's flagship mode is `--max-rounds 0` 7x24 runs that routinely
  exceed 24h, and breaking a live lock displaces a running instance into
  split-brain. Only a lock whose holder is dead (crash/SIGKILL) is broken.
- **Fail closed on unparseable locks**: an unreadable/empty lock file is
  refused (a partial write under the old format is never "free to take").
- **Ownership-checked release**: `release_lock` unlinks the path only
  when the file still carries this process's token — a foreign (successor)
  lock is left in place.
- **Round re-verification**: `verify_held` lets the round loop confirm
  the lock is still ours between rounds; a lock that vanished (unlinked
  by a concurrent breaker) stops the instance with exit 5 instead of
  continuing in split-brain.
- The file is 0o600 and the `-p`/`--prompt` value is redacted from the
  recorded argv (prompt content is sensitive and the file can survive a
  SIGKILL).

Released in finally; skipped for --dry-run; --no-lock is the documented
escape hatch.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Optional

from .config import log

LOCK_NAME = ".pi-batch.lock"
STALE_AGE_SEC = 24 * 3600
LOCK_EXIT_CODE = 5  # new exit-code contract (docs/RUNNING_247.md)

# token -> lock path, per process: release/verify must only ever touch
# locks this process actually created (a successor's file is never ours).
_OWNED: dict = {}


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def _redacted_argv(argv: list) -> str:
    """sys.argv for the lock payload with prompt values redacted: the lock
    file is world-listed and can survive a SIGKILL, so `-p SECRET` must not
    land in it. Values following -p/--prompt are replaced; other flags are
    kept verbatim."""
    out = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        out.append(arg)
        if arg in ("-p", "--prompt") and i + 1 < len(argv):
            out.append("[redacted]")
            i += 1
        i += 1
    return " ".join(out)


def _payload(cwd: str) -> dict:
    return {
        "pid": os.getpid(),
        "start": time.time(),
        "host": os.uname().nodename,
        "argv": _redacted_argv(sys.argv),
        "token": uuid.uuid4().hex,
    }


def acquire_lock(cwd: str = "", no_lock: bool = False) -> Optional[Path]:
    """Create the lock file; raises SystemExit(5) when another live
    instance holds it (or the lock file is unparseable). Returns the lock
    path (caller must release)."""
    if no_lock:
        return None
    lock_path = Path(cwd or os.getcwd()) / LOCK_NAME
    payload = _payload(cwd)
    # Atomic publication: the payload is fully written to a unique temp
    # file, then hard-linked into place. os.link fails with EEXIST when a
    # lock is already present, so no reader can observe a partial file
    # (a separate O_EXCL create + write left such a window before).
    fd, tmp_name = tempfile.mkstemp(prefix=LOCK_NAME + ".", dir=str(lock_path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        try:
            os.link(tmp_name, str(lock_path))
        except FileExistsError:
            _reject_or_break(lock_path)
            return acquire_lock(cwd, no_lock=False)  # stale lock broken; retry
    finally:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
    os.chmod(str(lock_path), 0o600)
    _OWNED[str(lock_path)] = payload["token"]
    return lock_path


def _read_holder(lock_path: Path) -> dict:
    try:
        return json.loads(lock_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _reject_or_break(lock_path: Path) -> None:
    """Refuse when the holder is alive or the lock is unparseable (fail
    closed: a lock we cannot read is never free to take); otherwise break
    the stale lock (dead PID only — a live holder is NEVER broken on age
    alone, because 7x24 runs routinely outlive the 24h staleness window)."""
    holder = _read_holder(lock_path)
    pid = int(holder.get("pid", 0) or 0)
    if pid and _alive(pid):
        age_h = (time.time() - float(holder.get("start", 0) or 0)) / 3600
        log.error("Another pi-batch instance is running (pid=%s, host=%s, argv=%s, age=%.1fh). "
                  "Refusing to start (exit %d). Use --no-lock to override.",
                  pid, holder.get("host", "?"), holder.get("argv", "?"), age_h, LOCK_EXIT_CODE)
        raise SystemExit(LOCK_EXIT_CODE)
    if not pid:
        # empty/unparseable lock file: could be a torn write or a planted
        # token; fail closed instead of silently taking over.
        log.error("Lock file %s is unreadable (no holder PID); refusing to start (exit %d). "
                  "Remove it manually if it is a leftover.", lock_path, LOCK_EXIT_CODE)
        raise SystemExit(LOCK_EXIT_CODE)
    log.warning("Breaking stale lock %s (pid=%s dead)", lock_path, pid)
    try:
        lock_path.unlink(missing_ok=True)
    except OSError:
        raise SystemExit(LOCK_EXIT_CODE)


def verify_held(lock_path: Optional[Path]) -> bool:
    """True while the lock file at *lock_path* is still this process's own
    (token + PID match). A lock that vanished or was replaced by another
    instance means the single-instance guarantee is gone — the round loop
    must stop instead of running in split-brain."""
    if not lock_path:
        return False
    key = str(lock_path)
    token = _OWNED.get(key)
    if not token:
        return False
    holder = _read_holder(lock_path)
    return (holder.get("token") == token and int(holder.get("pid", 0) or 0) == os.getpid())


def release_lock(lock_path: Optional[Path]) -> None:
    """Remove the lock file only when it is still ours (best-effort, never
    raises): a foreign lock at the same path must survive — unlinking it
    would let a third instance start while the real holder still runs."""
    if not lock_path:
        return
    try:
        if verify_held(lock_path):
            lock_path.unlink(missing_ok=True)
        else:
            log.warning("Not removing %s: lock is not owned by this instance", lock_path)
    except OSError:
        pass
    finally:
        _OWNED.pop(str(lock_path), None)
