"""Bounded file primitives for the append-only memory index."""

from __future__ import annotations

from pathlib import Path
from typing import Iterator, Optional


def cap_manifest(value: str, maximum: int) -> str:
    """Bound a manifest while retaining its explicit trust-boundary close."""
    raw = value.encode("utf-8")
    if maximum <= 0 or len(raw) <= maximum:
        return value
    closing = b"</pbatch-memory>"
    suffix = b"\n[manifest truncated]\n" + closing
    if maximum < len(suffix):
        return closing.decode() if maximum >= len(closing) else ""
    head = raw[:maximum - len(suffix)].decode("utf-8", errors="ignore")
    return head + suffix.decode()


def bounded_lines(path: Path, maximum: int) -> Iterator[Optional[str]]:
    """Yield decoded lines, using None for an oversized discarded line."""
    chunk = max(1, maximum) + 1
    with path.open("rb") as handle:
        while True:
            raw = handle.readline(chunk)
            if not raw:
                return
            if len(raw) > maximum:
                while raw and not raw.endswith(b"\n"):
                    raw = handle.readline(chunk)
                yield None
                continue
            yield raw.decode("utf-8", errors="replace")


def reverse_lines(path: Path, block_size: int = 8192,
                  maximum: int = 0) -> Iterator[Optional[str]]:
    """Yield newest lines first; None represents an oversized discarded line."""
    with path.open("rb") as handle:
        handle.seek(0, 2)
        position = handle.tell()
        remainder = b""
        discarding = False
        while position > 0:
            size = min(max(1, block_size), position)
            position -= size
            handle.seek(position)
            block = handle.read(size)
            if discarding:
                boundary = block.rfind(b"\n")
                if boundary < 0:
                    continue
                block = block[:boundary + 1]
                discarding = False
            parts = (block + remainder).split(b"\n")
            remainder = parts[0]
            for line in reversed(parts[1:]):
                if line:
                    if maximum > 0 and len(line) > maximum:
                        yield None
                    else:
                        yield line.decode("utf-8", errors="replace")
            if maximum > 0 and len(remainder) > maximum:
                yield None
                remainder = b""
                discarding = True
        if remainder and not discarding:
            yield remainder.decode("utf-8", errors="replace")
