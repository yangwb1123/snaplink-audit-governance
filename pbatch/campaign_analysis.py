"""Module discovery, analysis tasks, and direction selection."""

from __future__ import annotations

import json
import re
import time
from pathlib import Path

from .campaign_models import CampaignSettings, Direction
from .campaign_state import digest, tool_digest, tree_digest
from .meta import _extract_json_array
from .models import Task, TaskResult
from .runner import run_parallel

_PLANNING_FAILURE_STATUSES = frozenset({
    "FAILED", "VALIDATION_FAILED", "GATE_REJECTED", "GATE_REJECTED_OBSERVED",
    "PIPELINE_FAILED", "ANALYSIS_FAILED", "FAILED_OBSERVED",
})


def discover_modules(root: Path, settings: CampaignSettings, explicit: str = "") -> list[str]:
    """Discover bounded module directories, rejecting paths outside root."""
    if explicit:
        modules = [_safe_module(root, item.strip()) for item in explicit.split(",") if item.strip()]
        return sorted(dict.fromkeys(modules))
    modules = []
    for root_name in settings.roots:
        layer = _inside(root, root / root_name)
        if layer.is_dir():
            modules.extend(_walk_modules(root, layer, settings))
    if not modules and settings.fallback_top_level:
        modules = _fallback_modules(root, settings)
    return sorted(dict.fromkeys(modules))


def _safe_module(root: Path, value: str) -> str:
    candidate = root / value
    try:
        if candidate.is_symlink():
            raise ValueError(f"module is not a real directory: {value}")
        path = _inside(root, candidate)
        if not path.is_dir():
            raise ValueError(f"module is not a real directory: {value}")
    except OSError as exc:
        # robustness: paths longer than the FS limit (or otherwise
        # unstat-able) must fail as invalid modules, not crash the
        # whole campaign (live finding: File name too long)
        raise ValueError(f"module not accessible: {value} ({exc})") from exc
    return path.relative_to(root).as_posix()


def _inside(root: Path, path: Path) -> Path:
    resolved = path.resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError(f"path escapes repository: {path}") from exc
    return resolved


def _walk_modules(root: Path, layer: Path, settings: CampaignSettings) -> list[str]:
    modules = []
    for path in sorted(layer.rglob("*")):
        relative = path.relative_to(root)
        if not path.is_dir() or path.is_symlink() or _excluded(relative, settings.excludes):
            continue
        depth = len(path.relative_to(layer).parts)
        if depth <= settings.max_depth and _contains_files(path):
            modules.append(path.relative_to(root).as_posix())
    if not modules and _contains_files(layer):
        modules.append(layer.relative_to(root).as_posix())
    return modules


def _fallback_modules(root: Path, settings: CampaignSettings) -> list[str]:
    return [path.relative_to(root).as_posix() for path in sorted(root.iterdir())
            if path.is_dir() and not path.is_symlink() and
            not _excluded(path.relative_to(root), settings.excludes) and _contains_files(path)]


# 打包/缓存产物目录：永远不是分析目标（campaign 实战：ai_batch_runner.egg-info
# 被当作模块分析浪费一轮流水线；其方向多是打包/版本噪音）。
_PACKAGE_ARTIFACT_DIRS = ("egg-info", "__pycache__", "dist", "build",
                          "node_modules", ".venv", "venv")


def _excluded(path: Path, excludes: tuple[str, ...]) -> bool:
    lowered = {item.lower() for item in path.parts}
    hidden = any(item.startswith(".") for item in path.parts)
    artifact = any(term in item or item.endswith(term)
                   for item in lowered for term in _PACKAGE_ARTIFACT_DIRS)
    return hidden or artifact or any(term.lower() in lowered
                                     for term in excludes)


def _contains_files(path: Path) -> bool:
    try:
        return any(item.is_file() and not item.name.startswith(".") for item in path.iterdir())
    except OSError:
        return False


def module_slug(module: str) -> str:
    base = re.sub(r"[^A-Za-z0-9]+", "-", module).strip("-").lower() or "module"
    return f"{base[:56]}-{digest(module)[:8]}"


def analysis_prompt(settings: CampaignSettings, module: str) -> str:
    return (settings.analysis_prompt
            .replace("{module}", module)
            .replace("{module_path}", module + "/")
            .replace("{candidate_limit}", str(settings.candidate_limit)))


def build_analysis_task(root: Path, settings: CampaignSettings, module: str,
                        model: str = "", provider: str = "", timeout: int = 0) -> Task:
    output = settings.path(root, settings.output_dir) / "analyses" / f"{module_slug(module)}.json"
    task = Task(prompt=analysis_prompt(settings, module), output=str(output), cwd=str(root))
    task.memory = {"stage": "campaign-analysis", "memory_mode": "execute"}
    task.model = model or task.model
    task.provider = provider or task.provider
    if timeout:
        task.timeout = timeout
    return task


def analysis_fingerprint(root: Path, settings: CampaignSettings, module: str,
                         snapshot: dict, task: Task) -> str:
    return digest({
        "version": 1,
        "module": module,
        "module_tree": tree_digest(root / module, settings.excludes),
        "repository": snapshot,
        "prompt": task.prompt,
        "model": task.model,
        "provider": task.provider,
        "planning_history": _planning_history(root),
        "tool": tool_digest(),
    })


def _planning_history(root: Path) -> list[dict]:
    """Return only recent failure metadata that should invalidate analysis reuse."""
    from .memory import memory_enabled, recent
    if not memory_enabled():
        return []
    try:
        rows = recent(str(root), limit=12)
    except (OSError, ValueError):
        return []
    return [row for row in rows
            if row.get("status") in _PLANNING_FAILURE_STATUSES]


def run_analysis_tasks(tasks: list[Task], jobs: int, retries: int,
                       retry_delay: float = 10, validator=None) -> dict[str, TaskResult]:
    """Run independent analyses in parallel and retry only failed tasks."""
    pending = list(tasks)
    final = {}
    for attempt in range(retries + 1):
        if not pending:
            break
        results = run_parallel(pending, jobs)
        for result in results:
            if result.success and validator and not validator(result):
                result.success = False
                result.reason = "analysis produced no structured directions"
                result.task.output_path().unlink(missing_ok=True)
            final[str(result.task.output_path())] = result
        pending = [result.task for result in results if not result.success]
        if pending and attempt < retries:
            time.sleep(max(0, retry_delay) * (2 ** attempt))
    return final


def parse_directions(module: str, text: str) -> list[Direction]:
    """Parse structured JSON; Markdown headings remain report-only legacy items."""
    raw = _extract_json_array(text)
    if raw:
        try:
            data = json.loads(raw)
        except ValueError:
            data = []
        if isinstance(data, list):
            directions = [Direction.from_mapping(module, item) for item in data if isinstance(item, dict)]
            return [item for item in directions if item.title]
    headings = re.findall(r"^##\s+(.+?)\s*$", text or "", re.MULTILINE)
    return [Direction(module=module, title=title.strip(), raw={"legacy_markdown": True})
            for title in headings if title.strip()]


def evidence_exists(root: Path, direction: Direction) -> bool:
    """True when any evidence path exists under the repo root.

    Real-world lesson (forge-os campaign): the analysis model cited evidence
    WITHOUT the module prefix — `internal/statefs/statefs.go:276` for a
    repository where the file lives at `forge-core/internal/statefs/statefs.go`.
    Every candidate is therefore probed bare first (models that DO include the
    prefix keep working), then with the direction's module prefixed. `_inside`
    rejects anything escaping the root either way.
    """
    module = direction.module
    for evidence in direction.evidence:
        for candidate in _evidence_candidates(evidence):
            if not candidate:
                continue
            for probe in (candidate, f"{module}/{candidate}"):
                try:
                    path = _inside(root, root / probe)
                except ValueError:
                    continue
                try:
                    if path.exists():
                        return True
                except OSError:
                    # untrusted evidence paths may exceed the FS limit
                    # (live finding: File name too long) — skip, never crash
                    continue
    return False


_EVIDENCE_TOKEN = re.compile(r"[A-Za-z0-9_./-]+")
_EVIDENCE_EXT = (".go", ".py", ".js", ".ts", ".tsx", ".md", ".yaml", ".yml",
                 ".json", ".mod", ".sum", ".toml", ".sh", ".sql", ".html", ".css")


def _evidence_candidates(evidence: str) -> list[str]:
    """Path-like candidates from one evidence string.

    Real-world lesson (aero-vault campaign): LLMs mix formats —
    "path/file.go:12 description", "path/file.go returns X on missing"
    (no colon) and prose without any path. Extract path tokens first
    (colon/file:line suffixes stripped), then fall back to the legacy
    whole-string prefix so old behaviour is preserved.
    """
    candidates: list[str] = []
    for token in _EVIDENCE_TOKEN.findall(evidence):
        token = re.sub(r":\d+(?::\d+)?$", "", token)
        token = token.strip("./")
        if not token or token in (".", ".."):
            continue
        if "/" in token or token.endswith(_EVIDENCE_EXT):
            candidates.append(token)
    prefix = evidence.split("#", 1)[0].strip()
    if prefix and prefix not in candidates:
        candidates.append(prefix)
    return candidates


def rejection_reason(root: Path, direction: Direction, minimum_score: float,
                     seen: list[Direction]) -> str:
    """Precise reason a direction was not selected (real-world lesson:
    a conflated "duplicate, low score, missing evidence..." string made
    a 9-direction rejection impossible to diagnose)."""
    if any(_similar_direction(direction, previous) for previous in seen):
        return ("duplicate of a higher-ranked direction "
                "(title overlap >= 0.75 or evidence overlap >= 0.8)")
    if direction.score < minimum_score:
        return f"score {direction.score:.1f} below minimum {minimum_score:g}"
    missing = [name for name, value in (
        ("title", direction.title), ("problem", direction.problem),
        ("evidence", direction.evidence), ("acceptance", direction.acceptance))
        if not value]
    if missing:
        return "missing required field(s): " + ", ".join(missing)
    if not evidence_exists(root, direction):
        shown = " | ".join(direction.evidence[:2])
        return f"no evidence path exists in the repository (evidence: {shown})"
    return "rejected because the selection quota was already filled"


def select_directions(root: Path, directions: list[Direction], maximum: int,
                      minimum_score: float) -> tuple[list[Direction], list[tuple[Direction, str]]]:
    """Dedupe, score, and reject candidates without implementable evidence.
    Rejected items carry their precise reason (see rejection_reason)."""
    ordered = sorted(enumerate(directions), key=lambda item: (-item[1].score, item[0]))
    selected = []
    rejected = []
    seen: list[Direction] = []
    for _, direction in ordered:
        duplicate = any(_similar_direction(direction, previous) for previous in seen)
        valid = (not duplicate and direction.score >= minimum_score and
                 direction.implementable and evidence_exists(root, direction))
        seen.append(direction)
        if valid and len(selected) < maximum:
            selected.append(direction)
        else:
            rejected.append((direction, rejection_reason(root, direction, minimum_score, seen[:-1])))
    return selected, rejected


def _similar_direction(left: Direction, right: Direction) -> bool:
    left_words = set(re.findall(r"[\w\u3400-\u9fff]+", left.title.casefold()))
    right_words = set(re.findall(r"[\w\u3400-\u9fff]+", right.title.casefold()))
    union = left_words | right_words
    title_overlap = len(left_words & right_words) / len(union) if union else 1
    left_evidence = set(left.evidence)
    right_evidence = set(right.evidence)
    evidence_union = left_evidence | right_evidence
    evidence_overlap = (len(left_evidence & right_evidence) / len(evidence_union)
                        if evidence_union else 0)
    return title_overlap >= 0.75 or (title_overlap >= 0.4 and evidence_overlap >= 0.8)
