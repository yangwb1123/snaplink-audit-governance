"""Bounded, fenced artifact evidence for downstream agent prompts."""

from __future__ import annotations

import html
from pathlib import Path

from . import config
from .config import log


def combine_outputs(outputs: list, maximum: int = 0) -> str:
    """Join bounded artifacts in declared order under a fair byte budget."""
    values, omitted = _limited_values(outputs)
    paths = _existing_paths(values)
    if not paths:
        return ""
    sizes = [_size(path) for path in paths]
    budget = maximum or config.EVIDENCE_MAX_BYTES
    allowances = sizes if budget <= 0 else _fair_allowances(sizes, budget)
    parts = [_fenced(path, size, allowance)
             for path, size, allowance in zip(paths, sizes, allowances)]
    if omitted:
        parts.append(_omitted_marker(omitted))
    return "\n\n".join(parts)


def source_manifest(outputs: list) -> str:
    """Bound the aggregate `{input_path}` placeholder by the source limit."""
    values, omitted = _limited_values(outputs, warn=False)
    manifest = ", ".join(str(value) for value in values)
    if omitted:
        manifest += f" [and {omitted} more source candidates omitted]"
    return manifest


def _limited_values(outputs: list, warn: bool = True) -> tuple[list, int]:
    limit = max(1, config.EVIDENCE_MAX_SOURCES)
    selected = list(outputs[:limit])
    omitted = max(0, len(outputs) - len(selected))
    if omitted and warn:
        log.warning("Evidence source limit %d reached; omitted %d candidates", limit, omitted)
    return selected, omitted


def _existing_paths(outputs: list) -> list[Path]:
    paths = []
    for value in outputs:
        path = Path(value)
        if not path.is_file() or path.is_symlink():
            log.warning("Output file not found or unsafe: %s", path)
            continue
        paths.append(path)
    return paths


def _size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _omitted_marker(count: int) -> str:
    return (f'<evidence-sources-omitted count="{count}" '
            f'max-sources="{config.EVIDENCE_MAX_SOURCES}" />')


def _fair_allowances(sizes: list[int], budget: int) -> list[int]:
    """Water-fill small sources first, then divide the remainder equally."""
    allowances = [0] * len(sizes)
    pending = set(range(len(sizes)))
    remaining = max(0, budget)
    while pending and remaining:
        share = max(1, remaining // len(pending))
        small = [index for index in pending if sizes[index] <= share]
        if not small:
            quotient, extra = divmod(remaining, len(pending))
            for offset, index in enumerate(sorted(pending)):
                allowances[index] = quotient + (1 if offset < extra else 0)
            break
        for index in small:
            allowances[index] = sizes[index]
            remaining -= sizes[index]
            pending.remove(index)
    return allowances


def _fenced(path: Path, size: int, allowance: int) -> str:
    shown = b""
    try:
        with path.open("rb") as handle:
            shown = handle.read(max(0, allowance))
    except OSError:
        pass  # artifact unreadable: evidence degrades silently
    content = shown.decode("utf-8", errors="ignore")
    if len(shown) < size:
        content += (f"\n[evidence truncated: showing {len(shown)} of {size} bytes; "
                    f"full artifact: {path.resolve()}]")
    source = html.escape(path.stem, quote=True)
    return f'<evidence source="{source}">\n{content}\n</evidence>'
