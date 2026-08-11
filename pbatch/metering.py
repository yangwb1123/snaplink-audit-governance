"""Cost metering (R6 / T7) and budget cap (R7 / T8).

Metering: after each task, an append-only JSONL event is written to
--events-file (task_finish / task_fail / gate_reject / budget_cap /
stall_stop) with the per-call usage diff read from the pi session file
(spike-validated JSON pointers; a missing/unparseable session degrades to
`usage: null`, never a crash). An optional --webhook URL receives the same
events non-blockingly (5s timeout, failures logged only).

Budget: --budget-max N caps agent invocations per run (exit 3); a stale
session or a webhook outage never breaks the runner. Check + consume are
serialized on a process-wide lock and the daily counter is written
atomically, so parallel workers cannot overshoot the cap or lose daily
counter updates (round-4 findings M2 / DS-2 / DS-4).
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import urllib.request
from datetime import datetime, timezone
from glob import escape as glob_escape
from pathlib import Path
from typing import Optional

from .config import SESSION_LINE_MAX_BYTES, log
from .session import SESSIONS_DIR, workdir_key

# module-level config, set by cli.main() from CLI flags
EVENTS_FILE: str = ""
WEBHOOK_URL: str = ""
INVOCATIONS: int = 0
BUDGET_MAX: int = 0          # 0 = unlimited
DAILY_BUDGET: int = 0        # 0 = unlimited
_DAILY_STATE: str = ""       # path of the daily counter state file
BUDGET_EXIT_CODE = 3

# serializes check-then-act on the run/daily counters across worker threads
_BUDGET_LOCK = threading.Lock()
_USAGE_LOCK = threading.Lock()
# path -> (device, inode, byte offset, cumulative numeric usage)
_USAGE_CACHE: dict[str, tuple[int, int, int, dict]] = {}


def _session_usage(cwd: str, session_name: str) -> Optional[dict]:
    """Sum the usage of every matching session file for *session_name*
    under the workdir and return the summed usage of their assistant
    messages, or None when no usable session exists (degradation, never
    crash). Summing the whole name family (not just the newest file) keeps
    the meter correct after a T10 fork/rotation, which starts a new file
    for the same session name."""
    d = SESSIONS_DIR / workdir_key(cwd)
    if not d.is_dir():
        return None
    total = None
    try:
        escaped = glob_escape(session_name)
        candidates = {path for pattern in (f"*_{escaped}.jsonl",
                                            f"*_{escaped}-*.jsonl")
                      for path in d.glob(pattern)}
        with _USAGE_LOCK:
            for candidate in candidates:
                usage = _usage_for_file(candidate)
                if usage is None:
                    continue
                total = total or {}
                for key, value in usage.items():
                    total[key] = total.get(key, 0) + value
    except OSError:
        return None
    return total


def session_usage(cwd: str, session_name: str) -> Optional[dict]:
    """Public snapshot used around one invocation to calculate a delta."""
    return _session_usage(cwd, session_name)


def _usage_for_file(path: Path) -> Optional[dict]:
    """Incrementally fold complete, bounded JSONL records for one session."""
    if not path.is_file() or path.is_symlink():
        return None
    stat = path.stat()
    cached = _USAGE_CACHE.get(str(path))
    if cached and cached[:2] == (stat.st_dev, stat.st_ino) and stat.st_size >= cached[2]:
        offset, total = cached[2], dict(cached[3])
    else:
        offset, total = 0, {}
    offset = _scan_usage_append(path, offset, total)
    _USAGE_CACHE[str(path)] = (stat.st_dev, stat.st_ino, offset, total)
    return dict(total)


def _scan_usage_append(path: Path, offset: int, total: dict) -> int:
    """Scan appended complete lines; leave an incomplete tail for next time."""
    with path.open("rb") as handle:
        handle.seek(offset)
        while True:
            start = handle.tell()
            raw = handle.readline(SESSION_LINE_MAX_BYTES + 1)
            if not raw:
                return start
            if len(raw) > SESSION_LINE_MAX_BYTES:
                _discard_line_tail(handle, raw)
                offset = handle.tell()
                continue
            if not raw.endswith(b"\n"):
                return start
            _fold_usage_line(raw, total)
            offset = handle.tell()


def _discard_line_tail(handle, raw: bytes) -> None:
    while raw and not raw.endswith(b"\n"):
        raw = handle.readline(SESSION_LINE_MAX_BYTES + 1)


def _fold_usage_line(raw: bytes, total: dict) -> None:
    try:
        message = (json.loads(raw.decode("utf-8", errors="replace")).get("message") or {})
    except (ValueError, AttributeError):
        return
    usage = message.get("usage")
    if not isinstance(usage, dict):
        return
    for key, value in usage.items():
        if isinstance(value, (int, float)):
            total[key] = total.get(key, 0) + value


def record_event(event: str, task: str = "", detail: str = "", cwd: str = "",
                 session_name: str = "", usage_before: Optional[dict] = None) -> None:
    """Append one JSONL event (best-effort, never raises)."""
    if not EVENTS_FILE:
        return
    current = _session_usage(cwd, session_name) if event in ("task_finish", "task_fail") else None
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "event": event,
        "task": task[:120],
        "detail": detail[:500],
        "usage": _usage_delta(current, usage_before),
    }
    line = json.dumps(payload, ensure_ascii=False)
    try:
        p = Path(EVENTS_FILE)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError as e:
        log.warning("events write failed: %s", e)
        return
    if WEBHOOK_URL:
        _post_webhook(line)


def _usage_delta(current: Optional[dict], before: Optional[dict]) -> Optional[dict]:
    if current is None:
        return None
    if before is None:
        return current
    return {key: value - before.get(key, 0) for key, value in current.items()}


def _post_webhook(line: str) -> None:
    """Non-blocking webhook delivery with a 5s deadline; failures are
    logged only so a slow endpoint never blocks the runner."""

    def _send():
        try:
            req = urllib.request.Request(WEBHOOK_URL, data=line.encode("utf-8"),
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=5):
                pass
        except Exception as e:
            log.warning("webhook delivery failed: %s", e)

    threading.Thread(target=_send, daemon=True).start()


def _read_daily_count() -> int:
    """Invocations already spent today (UTC), from the daily state file.
    A missing or corrupt state file reads as 0 (fail-open): the budget is
    a cost control, not a security boundary — documented as such in
    docs/RUNNING_247.md (round-4 finding L8)."""
    if not _DAILY_STATE:
        return 0
    try:
        data = json.loads(Path(_DAILY_STATE).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return 0
    if data.get("date") != datetime.now(timezone.utc).strftime("%Y-%m-%d"):
        return 0
    return int(data.get("count", 0))


def budget_spent_today() -> int:
    """Invocations already spent today (UTC), from the daily state file."""
    return _read_daily_count()


def _save_daily(count: int) -> None:
    """Persist the daily counter atomically (temp + rename): a crash
    mid-write must not corrupt the state file (round-4 finding DS-2)."""
    if not _DAILY_STATE:
        return
    try:
        p = Path(_DAILY_STATE)
        p.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=p.name + ".", suffix=".tmp", dir=str(p.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump({
                    "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    "count": count,
                }, f)
            os.replace(tmp_name, str(p))
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
            # best-effort I/O：失败不阻塞主流程（已验证有意）
                pass
            raise
    except OSError:
    # best-effort I/O：失败不阻塞主流程（已验证有意）
        pass


def budget_allows() -> bool:
    """True when an invocation may start (run budget + daily budget).
    Thread-safe against concurrent budget_consume calls (parallel mode)."""
    with _BUDGET_LOCK:
        if BUDGET_MAX and INVOCATIONS >= BUDGET_MAX:
            return False
        if DAILY_BUDGET and _read_daily_count() >= DAILY_BUDGET:
            return False
        return True


def budget_try_consume() -> bool:
    """Atomically check the caps and count one invocation; False when a
    cap is reached (nothing consumed). Serializing check+consume closes the
    parallel-mode race where N workers all passed budget_allows() before
    any of them consumed (round-4 finding M2/DS-4)."""
    global INVOCATIONS
    with _BUDGET_LOCK:
        if BUDGET_MAX and INVOCATIONS >= BUDGET_MAX:
            return False
        if DAILY_BUDGET and _read_daily_count() >= DAILY_BUDGET:
            return False
        INVOCATIONS += 1
        if DAILY_BUDGET:
            _save_daily(_read_daily_count() + 1)
        return True


def budget_consume() -> None:
    """Count one invocation (run budget and daily budget). The daily
    read-modify-write is serialized and persisted atomically so concurrent
    workers cannot lose updates (round-4 probe: 4 concurrent consumes ->
    persisted count 2)."""
    budget_try_consume()
