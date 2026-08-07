"""Small bounded-text primitives used at untrusted prompt boundaries."""

from __future__ import annotations

from pathlib import Path


def read_text_bounded(path: Path, maximum: int, label: str = "file") -> str:
    """Read UTF-8 text without allocating an unbounded input buffer.

    The caller owns the policy for *maximum*.  A positive limit is required so
    a malformed configuration cannot accidentally turn this guard off.  The
    size check is repeated after opening to cover files that grow between
    ``stat`` and ``read``.  ``ValueError`` is deliberately used for policy
    rejection so callers can fail a task/stage without saving partial work.
    """
    if maximum < 1:
        raise ValueError(f"{label} limit must be positive")
    path = Path(path)
    try:
        if path.stat().st_size > maximum:
            raise ValueError(f"{label} exceeds {maximum} bytes: {path}")
        with path.open("rb") as handle:
            raw = handle.read(maximum + 1)
    except ValueError:
        raise
    except OSError as exc:
        raise ValueError(f"unable to read {label} {path}: {exc}") from exc
    if len(raw) > maximum:
        raise ValueError(f"{label} exceeds {maximum} bytes: {path}")
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"{label} is not valid UTF-8: {path}") from exc
