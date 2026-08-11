"""Durable JSONL state and fingerprints for repository campaigns."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .config import log
from .memory_io import bounded_lines


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def digest(value) -> str:
    canonical = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def file_digest(path: Path) -> str:
    hasher = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(block)
    except OSError:
        return "missing"
    return hasher.hexdigest()


def tree_digest(path: Path, excludes: tuple[str, ...] = ()) -> str:
    """Hash path names and contents without following directory symlinks."""
    if path.is_file():
        return file_digest(path)
    records = []
    if not path.is_dir():
        return digest(records)
    for item in sorted(path.rglob("*")):
        rel = item.relative_to(path).as_posix()
        if any(part in excludes for part in item.parts):
            continue
        if item.is_symlink():
            records.append((rel, "symlink", os.readlink(item)))
        elif item.is_file():
            records.append((rel, "file", file_digest(item)))
    return digest(records)


def git_snapshot(root: Path, excludes: tuple[str, ...] = ()) -> dict:
    """Return commit and dirty-state fingerprints; non-git dirs still work."""
    head = _git_output(root, ["rev-parse", "HEAD"]) or "not-a-git-repository"
    status = _filtered_status(
        _git_output(root, ["status", "--porcelain=v1", "--untracked-files=all"]), excludes)
    pathspecs = ["."]
    for item in excludes:
        pathspecs.extend([f":(exclude){item.rstrip('/')}", f":(exclude){item.rstrip('/')}/**"])
    diff = _git_output(root, ["diff", "--binary", "HEAD", "--", *pathspecs])
    return {"head": head.strip(), "dirty": digest([status, diff])}


def tool_digest() -> str:
    """Digest of the tool's own code (pbatch package + entry script + config).

    Real-world lesson (aero-vault round 1): the tool repo was fixed
    mid-campaign; the running process kept the old parser and rejected all
    9 directions, but no fingerprint recorded which tool version produced
    them. Including the tool digest makes reuse decisions invalidate when
    the tool itself changes and makes the drift visible in state events.
    """
    pkg = Path(__file__).resolve().parent
    root = pkg.parent
    records: list[tuple[str, str]] = [(pkg.name, tree_digest(pkg, excludes=("__pycache__",)))]
    entry = root / "pi-batch.py"
    if entry.is_file():
        records.append((entry.name, file_digest(entry)))
    for candidate in (root / "pi-batch.yaml", pkg / "pi-batch.yaml"):
        if candidate.is_file():
            records.append((candidate.name, file_digest(candidate)))
            break
    return digest(records)


def tool_head() -> str:
    """Git HEAD of the tool's own repository ('' when not a git checkout)."""
    root = Path(__file__).resolve().parent.parent
    return (_git_output(root, ["rev-parse", "HEAD"]) or "not-a-git-repository").strip()


def _filtered_status(status: str, excludes: tuple[str, ...]) -> str:
    lines = []
    prefixes = tuple(item.rstrip("/") + "/" for item in excludes)
    for line in status.splitlines():
        path = line[3:].split(" -> ")[-1] if len(line) > 3 else ""
        if path in excludes or path.startswith(prefixes):
            continue
        lines.append(line)
    return "\n".join(lines)


def _git_output(root: Path, args: list[str]) -> str:
    try:
        proc = subprocess.run(["git", "-C", str(root), *args], capture_output=True,
                              text=True, timeout=30, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return proc.stdout if proc.returncode == 0 else ""


class StateStore:
    """Append-only campaign events with latest-state and summary helpers."""

    def __init__(self, path: Path, max_line_bytes: int = 256 * 1024):
        self.path = path
        self.max_line_bytes = max(1, max_line_bytes)
        self._latest_cache = None

    def _iter_events(self):
        if not self.path.is_file() or self.path.is_symlink():
            return
        try:
            for number, line in enumerate(bounded_lines(self.path, self.max_line_bytes), 1):
                if line is None:
                    log.warning("CAMPAIGN: ignoring oversized state line %d in %s",
                                number, self.path)
                    continue
                try:
                    event = json.loads(line)
                except ValueError:
                    log.warning("CAMPAIGN: ignoring corrupt state line %d in %s",
                                number, self.path)
                    continue
                if isinstance(event, dict):
                    yield event
        except OSError:
            return

    def events(self) -> list[dict]:
        """Compatibility snapshot; operational lookups stream or use latest cache."""
        return list(self._iter_events())

    def append(self, event: dict) -> dict:
        if self.path.is_symlink():
            raise ValueError(f"refusing symlink state file: {self.path}")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(event)
        payload.setdefault("updated_at", utc_now())
        line = json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n"
        size = len(line.encode("utf-8"))
        if size > self.max_line_bytes:
            raise ValueError(
                f"campaign state event exceeds {self.max_line_bytes} bytes ({size})")
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
        if self._latest_cache is not None:
            self._latest_cache[self._key(payload)] = payload
        return payload

    def latest(self) -> dict[tuple[str, str], dict]:
        if self._latest_cache is not None:
            return dict(self._latest_cache)
        states = {}
        for event in self._iter_events():
            states[self._key(event)] = event
        self._latest_cache = states
        return dict(states)

    @staticmethod
    def _key(event: dict) -> tuple[str, str]:
        return (str(event.get("module", "")), str(event.get("direction_id", "")))

    def reusable(self, module: str, direction_id: str, fingerprint: str,
                 statuses: tuple[str, ...]) -> bool:
        event = self.latest().get((module, direction_id), {})
        return (event.get("fingerprint") == fingerprint and
                event.get("status") in statuses)

    def median_elapsed(self, statuses: tuple[str, ...], fallback: float) -> float:
        values = []
        for event in self._iter_events():
            if event.get("status") not in statuses:
                continue
            try:
                elapsed = float(event.get("elapsed", 0) or 0)
            except (TypeError, ValueError):
                continue
            if elapsed > 0:
                values.append(elapsed)
        values.sort()
        if not values:
            return fallback
        middle = len(values) // 2
        return values[middle] if len(values) % 2 else (values[middle - 1] + values[middle]) / 2


def write_summary(store: StateStore, path: Path, campaign_name: str) -> None:
    """Regenerate the Markdown view atomically from authoritative JSONL."""
    if path.is_symlink():
        raise ValueError(f"refusing symlink summary file: {path}")
    latest = store.latest()
    rows = []
    for (_, direction), event in sorted(latest.items()):
        if direction != "__analysis__":
            rows.append(event)
        elif event.get("status") == "ANALYSIS_FAILED":
            rows.append(dict(event, direction="(module analysis)"))
    counts = {}
    per_campaign: dict[str, dict] = {}
    for event in rows:
        status = str(event.get("status", "UNKNOWN"))
        counts[status] = counts.get(status, 0) + 1
        campaign = str(event.get("campaign", "?"))
        bucket = per_campaign.setdefault(campaign, {})
        bucket[status] = bucket.get(status, 0) + 1
    lines = [f"# Campaign summary: {campaign_name}", "",
             f"Generated: {utc_now()}", "",
             "| Module | Direction | Status | Reason | Evidence |",
             "|---|---|---|---|---|"]
    for event in rows:
        evidence = ", ".join(str(item) for item in event.get("evidence", []))
        lines.append("| %s | %s | %s | %s | %s |" % (
            _cell(event.get("module", "")), _cell(event.get("direction", "")),
            _cell(event.get("status", "")), _cell(event.get("reason", "")), _cell(evidence)))
    lines.extend(["", "## Counts", ""])
    lines.extend(f"- {key}: {counts[key]}" for key in sorted(counts))
    # Per-campaign breakdown: multiple campaigns share one state file
    # (real-world lesson: my round loop and the user's compose queue
    # interleave events in docs/auto/state.jsonl).
    if len(per_campaign) > 1:
        for campaign in sorted(per_campaign):
            bucket = per_campaign[campaign]
            lines.extend(["", f"### {campaign}", ""])
            lines.extend(f"- {key}: {bucket[key]}" for key in sorted(bucket))
    _atomic_text(path, "\n".join(lines) + "\n")


def _cell(value) -> str:
    return str(value or "").replace("|", "\\|").replace("\n", " ")


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(name, path)
    finally:
        try:
            os.unlink(name)
        except OSError:
        # best-effort I/O：失败不阻塞主流程（已验证有意）
            pass
