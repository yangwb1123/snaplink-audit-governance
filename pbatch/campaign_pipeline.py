"""Pipeline materialization and isolated implementation for campaigns."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from .campaign_models import CampaignSettings, Direction, safe_slug
from .campaign_state import digest, file_digest, tree_digest
from . import config
from .config import log, yaml
from .pipeline import load_pipeline, run_pipeline
from .text_io import read_text_bounded


_ACTIVE_PIPELINES: set[subprocess.Popen] = set()
_ACTIVE_LOCK = threading.Lock()


@dataclass
class PipelineOutcome:
    direction: Direction
    status: str
    reason: str
    elapsed: float
    evidence: list[str]
    branch: str = ""
    commit: str = ""


def direction_fingerprint(root: Path, settings: CampaignSettings, direction: Direction,
                          analysis_path: Path, snapshot: dict, model: str, provider: str) -> str:
    template = settings.path(root, settings.pipeline_template)
    return digest({
        "version": 1,
        "direction": direction.raw or direction.title,
        "analysis": file_digest(analysis_path),
        "pipeline": file_digest(template),
        "resources": _pipeline_resources(root, template),
        "runner_config": file_digest(root / "pi-batch.yaml"),
        "campaign": {
            "name": settings.name,
            "maximum": settings.max_directions,
            "minimum_score": settings.minimum_score,
            "requirement_stage": settings.requirement_stage,
        },
        "repository": snapshot,
        "model": model,
        "provider": provider,
    })


def materialize_pipeline(root: Path, settings: CampaignSettings, direction: Direction,
                         analysis_path: Path) -> Path:
    """Create a direction-scoped pipeline without output/decision collisions."""
    template = settings.path(root, settings.pipeline_template)
    if not template.is_file() or template.is_symlink():
        raise ValueError(f"pipeline template is missing or unsafe: {template}")
    if not yaml:
        raise ValueError("campaign pipelines require PyYAML")
    data = yaml.safe_load(read_text_bounded(
        template, config.INPUT_MAX_BYTES, "campaign pipeline template")) or {}
    if not isinstance(data, dict) or not isinstance(data.get("stages"), list):
        raise ValueError(f"invalid pipeline template: {template}")
    run_dir = settings.path(root, settings.output_dir) / "runs" / direction.direction_id
    _scope_pipeline_outputs(data, root, run_dir)
    if not _replace_requirement_prompt(data["stages"], settings, direction, analysis_path):
        raise ValueError(f"requirement stage '{settings.requirement_stage}' not found")
    pipeline_path = run_dir / "pipeline.yaml"
    _atomic_yaml(pipeline_path, data)
    return pipeline_path


def _scope_pipeline_outputs(data: dict, root: Path, run_dir: Path) -> None:
    data["decision_log"] = str(run_dir / "DECISIONS.md")
    data["archive_dir"] = str(run_dir / "archive")
    for stage in data.get("stages", []):
        stage_id = safe_slug(stage.get("name", "stage"), fallback="stage")
        stage_dir = run_dir / "artifacts" / stage_id
        stage["cwd"] = str(root)
        if stage.get("output"):
            stage["output"] = str(stage_dir / _output_name(stage["output"], "output.md"))
        if stage.get("output_dir"):
            stage["output_dir"] = str(stage_dir / "meta")
        for index, task in enumerate(stage.get("tasks", []), 1):
            if task.get("output"):
                name = _output_name(task["output"], f"task-{index}.md")
                task["output"] = str(stage_dir / f"task-{index}-{name}")
        for field in ("role_dir", "role_keywords", "from_dir"):
            if stage.get(field):
                stage[field] = str(_repository_resource(root, stage[field]))
        for task in stage.get("tasks", []):
            if task.get("prompt_template"):
                task["prompt_template"] = str(_repository_resource(root, task["prompt_template"]))


def _output_name(value: str, fallback: str) -> str:
    name = Path(str(value)).name
    return fallback if name in ("", ".", "..") else name


def _repository_resource(root: Path, value: str) -> Path:
    path = Path(value)
    resolved = path.resolve() if path.is_absolute() else (root / path).resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError(f"pipeline resource escapes repository: {value}") from exc
    return resolved


def _pipeline_resources(root: Path, template: Path) -> dict:
    try:
        data = yaml.safe_load(read_text_bounded(
            template, config.INPUT_MAX_BYTES, "campaign pipeline template")) or {}
    except Exception:
        return {}
    resources = {}
    for stage in data.get("stages", []) if isinstance(data, dict) else []:
        for field in ("role_dir", "role_keywords", "from_dir"):
            if not stage.get(field):
                continue
            try:
                path = _repository_resource(root, stage[field])
                resources[f"{stage.get('name', '')}:{field}"] = tree_digest(path)
            except ValueError:
                resources[f"{stage.get('name', '')}:{field}"] = "outside-repository"
    return resources


def _replace_requirement_prompt(stages: list, settings: CampaignSettings,
                                direction: Direction, analysis_path: Path) -> bool:
    for stage in stages:
        if stage.get("name") != settings.requirement_stage:
            continue
        payload = json.dumps(direction.raw or {"title": direction.title}, ensure_ascii=False, indent=2)
        stage["from_prompt"] = (
            f"Produce an evidence-backed requirements specification for module '{direction.module}'.\n"
            f"Selected direction:\n{payload}\n\n"
            f"Source analysis: {analysis_path}\n"
            "Verify every cited file/symbol against the repository. Preserve the supplied acceptance "
            "checks and make them testable. Do not expand scope beyond this direction."
        )
        return True
    return False


def _atomic_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            yaml.safe_dump(data, handle, allow_unicode=True, sort_keys=False)
        os.replace(name, path)
    finally:
        try:
            os.unlink(name)
        except OSError:
            pass


def run_direction(root: Path, settings: CampaignSettings, direction: Direction,
                  analysis_path: Path, model: str = "", provider: str = "",
                  timeout: int = 0, reuse: bool = False) -> PipelineOutcome:
    start = time.monotonic()
    pipeline_path = materialize_pipeline(root, settings, direction, analysis_path)
    pipeline = load_pipeline(str(pipeline_path))
    for stage in pipeline.stages:
        if provider:
            stage.provider = provider
    results, failed = run_pipeline(
        pipeline, model_override=model, reuse=reuse, timeout_override=timeout,
        session_name=f"campaign-{direction.direction_id}", fingerprint_mode=reuse)
    status, reason = classify_pipeline(results, failed, pipeline)
    evidence = _artifact_evidence(pipeline_path, results)
    return PipelineOutcome(direction, status, reason, time.monotonic() - start, evidence)


def _artifact_evidence(pipeline_path: Path, results: list) -> list[str]:
    """Return existing output paths, following successful campaign archives."""
    evidence = [str(pipeline_path)]
    archive_root = pipeline_path.parent / "archive"
    for result in results:
        if not result.success or not result.task.output:
            continue
        output = result.task.output_path()
        if output.exists():
            evidence.append(str(output))
            continue
        archived = sorted(archive_root.glob(f"*/{output.name}"))
        if archived:
            evidence.append(str(archived[-1]))
    return evidence


def classify_pipeline(results: list, failed: list[str], pipeline) -> tuple[str, str]:
    if not failed and all(result.success for result in results):
        return "PASSED", ""
    failed_set = set(failed)
    if any(stage.gate and stage.name in failed_set for stage in pipeline.stages):
        return "GATE_REJECTED", "gate verdict was not PASS"
    if any(result.validation_ok is False for result in results):
        return "VALIDATION_FAILED", "engineering validator rejected an artifact"
    return "PIPELINE_FAILED", ", ".join(failed) or "one or more tasks failed"


def parallel_ready(root: Path) -> tuple[bool, str]:
    """Parallel implementation requires a clean Git worktree at campaign start."""
    try:
        proc = subprocess.run(["git", "-C", str(root), "status", "--porcelain"],
                              capture_output=True, text=True, timeout=30, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, f"could not verify clean Git worktree: {exc}"
    if proc.returncode != 0:
        return False, "parallel pipelines require a Git repository"
    if proc.stdout.strip():
        return False, "parallel pipelines require a clean worktree before analysis"
    return True, ""


def run_directions_isolated(root: Path, settings: CampaignSettings,
                            items: list[tuple[Direction, Path]], jobs: int,
                            agent_args: list[str]) -> list[PipelineOutcome]:
    outcomes = []
    prepared = []
    for direction, analysis in items:
        try:
            worktree, branch = _ensure_worktree(root, settings, direction)
            prepared.append((direction, analysis, worktree, branch))
        except Exception as exc:
            outcomes.append(PipelineOutcome(direction, "PIPELINE_FAILED", str(exc), 0, []))
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        futures = [pool.submit(_safe_worktree_run, settings, direction, analysis,
                               worktree, branch, agent_args)
                   for direction, analysis, worktree, branch in prepared]
        for future in as_completed(futures):
            outcomes.append(future.result())
    return outcomes


def _safe_worktree_run(settings: CampaignSettings, direction: Direction, analysis_path: Path,
                       worktree: Path, branch: str, agent_args: list[str]) -> PipelineOutcome:
    try:
        return _run_in_worktree(settings, direction, analysis_path, worktree, branch, agent_args)
    except Exception as exc:
        return PipelineOutcome(direction, "PIPELINE_FAILED", str(exc), 0, [str(worktree)], branch)


def _run_in_worktree(settings: CampaignSettings, direction: Direction, analysis_path: Path,
                     worktree: Path, branch: str, agent_args: list[str]) -> PipelineOutcome:
    start = time.monotonic()
    local_analysis = worktree / ".pi-batch" / "campaign" / f"{direction.direction_id}-analysis.json"
    local_analysis.parent.mkdir(parents=True, exist_ok=True)
    local_analysis.write_text(read_text_bounded(
        analysis_path, config.OUTPUT_MAX_BYTES, "campaign analysis"), encoding="utf-8")
    pipeline_path = materialize_pipeline(worktree, settings, direction, local_analysis)
    logfile = worktree / "logs" / f"campaign-{direction.direction_id}.log"
    logfile.parent.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, str(worktree / "pi-batch.py"), str(pipeline_path),
           "--reuse", "--reuse-fingerprint", "--log-file", str(logfile), *agent_args]
    # The child RotatingFileHandler is the sole logfile writer. Redirecting
    # its stdout to the same path created duplicate records and unsafe
    # concurrent rotation; isolated model bodies remain in artifacts/sessions.
    proc = subprocess.Popen(cmd, cwd=worktree, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, start_new_session=True)
    with _ACTIVE_LOCK:
        _ACTIVE_PIPELINES.add(proc)
    timed_out = False
    try:
        returncode = proc.wait(timeout=settings.pipeline_timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        _kill_pipeline_group(proc)
        returncode = -1
    finally:
        with _ACTIVE_LOCK:
            _ACTIVE_PIPELINES.discard(proc)
    commit = _git_text(worktree, ["rev-parse", "HEAD"])
    status = "PIPELINE_FAILED" if timed_out else _isolated_status(returncode, logfile)
    reason = (f"isolated pipeline timed out after {settings.pipeline_timeout}s"
              if timed_out else
              ("" if returncode == 0 else f"isolated pipeline exited {returncode}"))
    evidence = [str(worktree), str(pipeline_path), str(logfile)]
    return PipelineOutcome(direction, status, reason, time.monotonic() - start,
                           evidence, branch, commit)


def _isolated_status(returncode: int, logfile: Path) -> str:
    if returncode == 0:
        return "PASSED"
    try:
        tail = logfile.read_text(encoding="utf-8")[-100_000:]
    except OSError:
        tail = ""
    if "GATE REJECTED" in tail:
        return "GATE_REJECTED"
    if "VALIDATION FAILED" in tail:
        return "VALIDATION_FAILED"
    return "PIPELINE_FAILED"


def _ensure_worktree(root: Path, settings: CampaignSettings,
                     direction: Direction) -> tuple[Path, str]:
    campaign_id = safe_slug(settings.name)
    base = settings.path(root, settings.worktree_root) / campaign_id
    worktree = base / direction.direction_id
    branch = f"pbatch-campaign/{campaign_id}/{direction.direction_id}"
    if (worktree / ".git").exists():
        actual = _git_text(worktree, ["branch", "--show-current"])
        if actual != branch:
            raise RuntimeError(f"worktree branch mismatch: expected {branch}, found {actual}")
        return worktree, branch
    worktree.parent.mkdir(parents=True, exist_ok=True)
    exists = bool(_git_text(root, ["show-ref", "--verify", f"refs/heads/{branch}"]))
    args = ["worktree", "add", str(worktree), branch] if exists else [
        "worktree", "add", "-b", branch, str(worktree), "HEAD"]
    try:
        proc = subprocess.run(["git", "-C", str(root), *args], capture_output=True,
                              text=True, timeout=30, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"git worktree add failed: {exc}") from exc
    if proc.returncode != 0:
        raise RuntimeError(f"git worktree add failed: {proc.stderr.strip()}")
    return worktree, branch


def _git_text(root: Path, args: list[str]) -> str:
    try:
        proc = subprocess.run(["git", "-C", str(root), *args], capture_output=True,
                              text=True, timeout=30, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return proc.stdout.strip() if proc.returncode == 0 else ""


def _kill_pipeline_group(proc: subprocess.Popen) -> None:
    """Terminate an isolated pipeline and reap its direct child."""
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (OSError, ProcessLookupError):
        try:
            proc.kill()
        except OSError:
            pass
    try:
        proc.wait(timeout=1)
    except (subprocess.TimeoutExpired, ChildProcessError):
        pass


def kill_isolated_pipelines() -> None:
    """Kill active worktree pipeline process groups during interruption."""
    with _ACTIVE_LOCK:
        processes = list(_ACTIVE_PIPELINES)
    for proc in processes:
        if proc.poll() is not None:
            continue
        _kill_pipeline_group(proc)
