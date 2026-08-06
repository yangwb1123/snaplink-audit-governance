"""Pipeline execution: stage task builders, meta role orchestration,
verdict gates, decision logs, archiving, and task-source loading."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

from . import config
from .config import AGENT_DEFAULT_TIMEOUT, AGENT_DEFAULT_WORKERS, log, yaml
from .models import Pipeline, Stage, Task
from .pipeline_schema import validate_pipeline
from .evidence import combine_outputs, source_manifest
from .meta import run_meta_stage
from .repo_gates import run_stage_repo_validators
from .reuse import fingerprint as reuse_fp
from .reuse import reuse_decision, write_sidecar
from .runner import (_save_validated, print_summary, run_parallel,
                     run_argv, run_serial, run_task, run_validation)
from .text_io import read_text_bounded
from .task_loading import (load_tasks, load_tasks_from_dir, _task_data_error,
                           _task_string_error, _tasks_from_data)
def load_pipeline(path: str) -> Pipeline:
    """Load pipeline definition from YAML file."""
    if not yaml:
        log.error("PyYAML not installed. Install the project: uv sync (or pip install pyyaml)")
        sys.exit(1)
    
    fpath = Path(path)
    if not fpath.exists():
        log.error("Pipeline file not found: %s", path)
        sys.exit(1)
    
    try:
        data = yaml.safe_load(read_text_bounded(
            fpath, config.INPUT_MAX_BYTES, "pipeline file"))
    except Exception as exc:
        log.error("Invalid pipeline YAML: %s", exc)
        sys.exit(1)
    errors = validate_pipeline(data)
    errors.extend(_pipeline_resource_errors(data))
    if errors:
        for error in errors:
            log.error("Invalid pipeline: %s", error)
        sys.exit(1)
    
    # Global settings (applied to all stages that don't set it explicitly)
    global_git_commit = data.get("git_commit", False)

    stages = [_parse_stage_def(s, global_git_commit) for s in data["stages"]]
    
    return Pipeline(stages=stages, decision_log=data.get("decision_log", ""),
                    archive_dir=data.get("archive_dir", ""), name=Path(path).stem)

def _parse_stage_def(s: dict, global_git_commit: bool) -> Stage:
    """Map one YAML stage definition onto a Stage (untrusted keys ignored;
    unknown keys simply do not exist on the dataclass)."""
    return Stage(
        name=s.get("name", ""),
        model=s.get("model", ""),
        provider=s.get("provider", ""),
        from_dir=s.get("from_dir", ""),
        from_outputs=s.get("from_outputs", ""),
        suffix=s.get("suffix", ".md"),
        output_suffix=s.get("output_suffix", ".out.md"),
        mode=s.get("mode", "serial"),
        workers=s.get("workers", AGENT_DEFAULT_WORKERS),
        aggregate=s.get("aggregate", False),
        # Public YAML uses `validate`; `validate_cmd` remains the explicit
        # spelling and wins when both are present.
        validate_cmd=s.get("validate_cmd", s.get("validate")),
        meta=s.get("meta", False),
        meta_prompt=s.get("meta_prompt", ""),
        role_dir=s.get("role_dir", ""),
        role_keywords=s.get("role_keywords", ""),
        output_dir=s.get("output_dir", ""),
        max_iterations=s.get("max_iterations", 3),
        max_roles_per_iteration=s.get("max_roles_per_iteration", 3),
        relevance_enabled=s.get("relevance_enabled", True),
        relevance_min_score=s.get("relevance_min_score", 0),
        gate=s.get("gate", False),
        approval=s.get("approval", False),
        from_prompt=s.get("from_prompt", ""),
        output=s.get("output", ""),
        tasks=s.get("tasks", []),
        commands=s.get("commands", []),
        commands_parallel=s.get("commands_parallel", False),
        command_timeout=s.get("command_timeout", config.COMMAND_TIMEOUT),
        command_output_max_bytes=s.get(
            "command_output_max_bytes", config.COMMAND_OUTPUT_MAX_BYTES),
        cwd=s.get("cwd", ""),
        git_commit=s.get("git_commit", global_git_commit),
        commit_message=s.get("commit_message", ""),
    )


def _pipeline_resource_errors(data: dict) -> list[str]:
    errors = []
    stages = data.get("stages", []) if isinstance(data, dict) else []
    for stage in stages if isinstance(stages, list) else []:
        if not isinstance(stage, dict):
            continue
        tasks = stage.get("tasks", [])
        for index, task in enumerate(tasks if isinstance(tasks, list) else [], 1):
            if not isinstance(task, dict):
                continue
            value = task.get("prompt_template")
            if not value or not isinstance(value, str):
                continue
            path = Path(value)
            if not path.is_file() or path.is_symlink():
                errors.append(
                    f"stage '{stage.get('name', '')}'.tasks[{index}].prompt_template is missing or unsafe: {value}")
            else:
                try:
                    oversized = path.stat().st_size > config.INPUT_MAX_BYTES
                except OSError:
                    oversized = True
                if oversized:
                    errors.append(
                        f"stage '{stage.get('name', '')}'.tasks[{index}].prompt_template exceeds "
                        f"{config.INPUT_MAX_BYTES} bytes: {value}")
    return errors
def _task_prompt(task_def: dict, input_content: str, input_stem: str, input_path: str) -> str:
    """Build the prompt for a task definition: a literal 'prompt' string or
    a 'prompt_template' file, both with {input_content}/{input_stem}/{input_path}
    placeholders. Returns '' when neither is present (caller skips it)."""
    prompt = task_def.get("prompt", "")
    prompt_template_path = Path(task_def.get("prompt_template", ""))
    if task_def.get("prompt_template") and prompt_template_path.exists():
        prompt = read_text_bounded(prompt_template_path, config.INPUT_MAX_BYTES,
                                   "prompt template")
    if not prompt:
        log.error("Task in stage needs 'prompt' or an existing 'prompt_template' file")
        return ""
    return (prompt.replace("{input_content}", input_content)
                 .replace("{input_stem}", input_stem)
                 .replace("{input_path}", input_path))
def _expected_fp(prompt: str, out: str, validate_cmd: str, model: str, provider: str, fp_mode: bool) -> Optional[str]:
    """T9: fingerprint for a reuse candidate when --reuse-fingerprint is on
    (None otherwise); a mismatch regenerates the artifact. model/provider
    are the TASK's EFFECTIVE values (overrides included) — hashing the raw
    stage values instead made the fingerprint never match the sidecar that
    records the executed task's values (round-4 finding M6)."""
    if not fp_mode:
        return None
    return reuse_fp(prompt, out, validate_cmd or "", model, provider)


def _task_from_def(task_def: dict, prompt: str, output_path: str, model_override: str = "",
                   timeout_override: int = 0, stage: Optional[Stage] = None) -> Task:
    """Build a task from a pipeline template definition, applying CLI-level
    model and timeout overrides on top of per-task values."""
    task = Task(
        prompt=prompt,
        output=output_path,
        model=task_def.get("model") or (stage.model if stage else ""),
        provider=task_def.get("provider") or (stage.provider if stage else ""),
        thinking=task_def.get("thinking") or "",
        tools=task_def.get("tools") or "",
        exclude_tools=task_def.get("exclude_tools") or "",
        cwd=task_def.get("cwd", stage.cwd if stage else ""),
        timeout=task_def.get("timeout", AGENT_DEFAULT_TIMEOUT),
        env=dict(task_def.get("env", {})),
        validate=task_def.get("validate"),
    )
    if model_override:
        task.model = model_override
    if timeout_override:
        task.timeout = timeout_override
    return task


def _aggregate_tasks(stage: Stage, prev_outputs: list, model_override: str = "", timeout_override: int = 0, reuse: bool = False,
                     validate_cmd: str = "", reuse_legacy: bool = False,
                     fingerprint_mode: bool = False) -> tuple[list[Task], list[str]]:
    """Combine every upstream artifact into one prompt per task template so
    downstream roles see all evidence (input_stem becomes 'combined') instead
    of fanning each artifact into an independent task.

    Returns (new_tasks, reused_output_paths). With reuse=True, a template
    whose combined output file already exists is skipped and its path is
    returned as reused so downstream stages still see it.
    """
    combined = combine_outputs(prev_outputs)
    if not combined:
        log.warning("No upstream outputs available for aggregate stage '%s'", stage.name)
        return [], []

    tasks = []
    reused = []
    for task_def in stage.tasks:
        prompt = _task_prompt(task_def, combined, "combined", source_manifest(prev_outputs))
        if not prompt:
            continue
        output_path = task_def.get("output", "").replace("{input_stem}", "combined")
        task = _task_from_def(task_def, prompt, output_path, model_override, timeout_override, stage)
        # T9: fingerprint the task's EFFECTIVE inputs (resolved output path
        # + task-level model/provider) so the sidecar written after success
        # matches what the reuse check hashed (round-4 finding M6/P1-3).
        out_resolved = str(task.output_path()) if task.output else ""
        effective_validate = task.validate if task.validate is not None else validate_cmd
        fp = _expected_fp(prompt, out_resolved, effective_validate, task.model,
                          task.provider, fingerprint_mode)
        if reuse and task.output and reuse_decision(
                out_resolved, effective_validate, legacy=reuse_legacy, expected_fp=fp):
            log.info("REUSE: %s (reused+validated)", output_path)
            reused.append(output_path)
            continue
        tasks.append(task)
    return tasks, reused

def _stage_from_prompt_tasks(stage: Stage, reuse: bool, model_override: str, timeout_override: int,
                             validate_cmd: str = "", reuse_legacy: bool = False,
                             fingerprint_mode: bool = False) -> tuple[list[Task], list[str]]:
    """One-sentence starting point: a single task whose output feeds
    downstream from_outputs stages (no input file needed)."""
    if not stage.output:
        log.error("Stage '%s': from_prompt requires output", stage.name)
        return [], []
    task = Task(prompt=stage.from_prompt, output=stage.output, model=stage.model,
                provider=stage.provider, cwd=stage.cwd)
    # from_prompt supports @file references (same semantics as task files):
    # the pipeline can inject an external requirement without YAML editing.
    try:
        task.prompt = task.resolve_prompt("")
    except ValueError as exc:
        log.error("Stage '%s': prompt reference rejected: %s", stage.name, exc)
        return [], []
    if model_override:
        task.model = model_override
    if timeout_override:
        task.timeout = timeout_override
    # T9 (round-4 M6): hash the task's effective inputs so the sidecar
    # written after success matches the reuse-check fingerprint.
    out_resolved = str(task.output_path()) if task.output else ""
    fp = _expected_fp(stage.from_prompt, out_resolved, validate_cmd, task.model, task.provider, fingerprint_mode)
    if reuse and reuse_decision(out_resolved, validate_cmd, legacy=reuse_legacy, expected_fp=fp):
        log.info("REUSE: %s (reused+validated)", stage.output)
        return [], [stage.output]
    log.info("Loaded 1 task from from_prompt for stage '%s'", stage.name)
    return [task], []


def _stage_from_dir_tasks(stage: Stage, reuse: bool, model_override: str, timeout_override: int,
                          validate_cmd: str = "", reuse_legacy: bool = False,
                          fingerprint_mode: bool = False) -> tuple[list[Task], list[str]]:
    """Read .md files from a directory; one task per file, output next to
    the input with the stage's output suffix."""
    dir_path = Path(stage.from_dir)
    if not dir_path.is_dir():
        log.error("Directory not found: %s", stage.from_dir)
        return [], []
    tasks: list[Task] = []
    reused: list[str] = []
    for fpath in sorted(dir_path.glob(f"*{stage.suffix}")):
        if fpath.name.endswith(stage.output_suffix):
            continue
        # Local fix (self-iteration round 2, F1): resolve the output path
        # absolutely here. The task's cwd is the input dir, so a relative
        # output would be double-prefixed by output_path()'s cwd-aware
        # resolution (docs/inputs/docs/inputs/...).
        out_path = (fpath.parent / (fpath.stem + stage.output_suffix)).resolve()
        prompt = read_text_bounded(fpath, config.INPUT_MAX_BYTES,
                                   "stage input")
        task = Task(prompt=prompt, output=str(out_path), cwd=str(fpath.parent))
        task.model = stage.model or task.model
        task.provider = stage.provider or task.provider
        if model_override:
            task.model = model_override
        if timeout_override:
            task.timeout = timeout_override
        fp = _expected_fp(prompt, str(out_path), validate_cmd, task.model, task.provider, fingerprint_mode)
        if reuse and reuse_decision(str(out_path), validate_cmd, str(fpath.parent), reuse_legacy, fp):
            log.info("REUSE: %s (reused+validated)", out_path.name)
            reused.append(str(out_path))
            continue
        tasks.append(task)
    log.info("Loaded %d tasks from %s", len(tasks), stage.from_dir)
    return tasks, reused


def _stage_from_outputs_tasks(stage: Stage, stage_outputs: dict, reuse: bool, model_override: str, timeout_override: int,
                              validate_cmd: str = "", reuse_legacy: bool = False,
                              fingerprint_mode: bool = False) -> tuple[list[Task], list[str]]:
    """Tasks fed by the previous stage's artifacts: one combined task per
    template (aggregate) or one task per artifact per template."""
    names, prev_outputs, missing = _collect_upstream(stage.from_outputs, stage_outputs)
    if missing:
        log.error("Previous stage(s) not found: %s", ", ".join(missing))
        return [], []
    tasks: list[Task] = []
    reused: list[str] = []
    if stage.aggregate:
        # Merge every upstream artifact into one combined prompt per
        # template so downstream roles see all evidence, instead of
        # fanning each artifact into independent (and conflicting) tasks.
        agg_tasks, agg_reused = _aggregate_tasks(stage, prev_outputs, model_override, timeout_override, reuse,
                                                 validate_cmd, reuse_legacy, fingerprint_mode)
        return agg_tasks, agg_reused
    for out_path_str in prev_outputs:
        out_path = Path(out_path_str)
        if not out_path.exists():
            log.warning("Output file not found: %s", out_path)
            continue
        input_content = combine_outputs([str(out_path)])
        input_stem = out_path.stem
        # Create tasks from templates (prompt string or template file,
        # both with {input_content}/{input_stem} placeholders)
        for task_def in stage.tasks:
            prompt = _task_prompt(task_def, input_content, input_stem, str(out_path))
            if not prompt:
                continue
            output_path = task_def.get("output", "").replace("{input_stem}", input_stem)
            task = _task_from_def(task_def, prompt, output_path, model_override, timeout_override, stage)
            # T9 (round-4 M6): fingerprint the task's effective inputs so
            # the sidecar written after success matches this hash.
            out_resolved = str(task.output_path()) if task.output else ""
            effective_validate = task.validate if task.validate is not None else validate_cmd
            fp = _expected_fp(prompt, out_resolved, effective_validate, task.model,
                              task.provider, fingerprint_mode)
            if reuse and task.output and reuse_decision(
                    out_resolved, effective_validate, legacy=reuse_legacy, expected_fp=fp):
                log.info("REUSE: %s (reused+validated)", output_path)
                reused.append(output_path)
                continue
            tasks.append(task)
    log.info("Loaded %d tasks from %d outputs of stage(s) '%s'",
             len(tasks), len(prev_outputs), ", ".join(names))
    return tasks, reused


def _collect_upstream(from_outputs, stage_outputs: dict) -> tuple[list[str], list[str], list[str]]:
    """Resolve one or many upstream stage names into one ordered evidence list."""
    raw = from_outputs if isinstance(from_outputs, (list, tuple)) else [from_outputs]
    names = [str(name).strip() for name in raw if str(name).strip()]
    missing = [name for name in names if name not in stage_outputs]
    outputs = []
    for name in names:
        outputs.extend(stage_outputs.get(name, []))
    return names, outputs, missing


def execute_stage(stage: Stage, stage_outputs: dict[str, list[str]], model_override: str = "", reuse: bool = False, timeout_override: int = 0,
                  session_mode: str = "new", session_name: str = "", validate_cmd: str = "",
                  reuse_legacy: bool = False, fingerprint_mode: bool = False,
                  approval_file: str = "") -> tuple[list[TaskResult], bool]:
    """Execute one stage; task or post-stage command failures make it fail."""
    _log_stage_header(stage, reuse)
    # Per-stage engineering gate: the stage's own validate_cmd wins over the
    # CLI default ("" disables validation for this stage), and per-task
    # validate fields override both inside run_serial/run_parallel.
    stage_validate = stage.validate_cmd if stage.validate_cmd is not None else validate_cmd
    tasks, reused_outputs, early = _build_stage_tasks(stage, stage_outputs, reuse, model_override, timeout_override,
                                                      stage_validate, reuse_legacy, fingerprint_mode, session_name)
    if early is not None:
        results, stage_ok = early
        outputs = stage_outputs.get(stage.name, [])
        return _finalize_stage(stage, results, outputs, stage_ok, approval_file)
    if not tasks:
        results, stage_ok = _empty_stage_result(stage, stage_outputs, reused_outputs)
        outputs = stage_outputs.get(stage.name, reused_outputs)
        return _finalize_stage(stage, results, outputs, stage_ok, approval_file)

    for task in tasks:
        task.memory.setdefault("stage", stage.name)

    # Shared sessions must not run in parallel: interleaved calls would
    # corrupt the conversation order inside one session.
    if session_mode != "new" and stage.mode == "parallel":
        log.error("Stage '%s': --session-mode %s requires serial execution (parallel would interleave one session)",
                  stage.name, session_mode)
        from .memory import record_stage
        record_stage(stage.name, "FAILED", "shared session cannot run in parallel",
                     [], stage.cwd or os.getcwd(), session_name)
        return [], False

    stage_session_id = f"{session_name}-{stage.name}" if session_mode == "per-stage" else session_name

    results, outputs = _execute_stage_tasks(stage, tasks, reused_outputs, stage_validate,
                                            session_mode, session_name, stage_session_id)
    stage_outputs[stage.name] = outputs
    _log_stage_completion(stage, len(outputs), len(tasks))

    # T9 (round-4 finding P1-3): sidecars on the pipeline save path too —
    # previously single-batch only, so --reuse-fingerprint pipelines deleted
    # and regenerated artifacts every run.
    if fingerprint_mode:
        _write_stage_sidecars(results, stage_validate)

    return _finalize_stage(
        stage, results, outputs, all(r.success for r in results), approval_file)


def _finalize_stage(stage: Stage, results: list[TaskResult], outputs: list[str],
                    stage_ok: bool, approval_file: str) -> tuple[list[TaskResult], bool]:
    """Apply stage-wide gates even when every artifact was reused."""
    task_ok = stage_ok
    approved = _apply_approval(stage, task_ok, outputs, approval_file)
    commands_ok = _run_stage_commands(stage)
    repo_ok, repo_failed = run_stage_repo_validators(stage, results, outputs)
    stage_ok = task_ok and approved and commands_ok and repo_ok
    if not stage_ok:
        from .memory import record_stage
        reason = next((str(result.reason).strip() for result in results
                       if not result.success and str(result.reason).strip()), "")
        if not reason:
            reason = "task execution failed" if not task_ok else "stage failed"
        if task_ok and stage.approval and not approved:
            reason += "; approval rejected or missing"
        if not commands_ok:
            reason += "; stage command failed"
        if repo_failed:
            reason += f"; repo validator failed: {repo_failed}"
        record_stage(stage.name, "FAILED", reason[:500], outputs,
                     stage.cwd or os.getcwd())
    _git_commit_stage(stage, outputs)  # advisory: commit issues do not fail the stage
    return results, stage_ok


def _apply_approval(stage: Stage, stage_ok: bool, outputs: list, approval_file: str) -> bool:
    """T12c (D5): a human approval point pauses the pipeline after the
    stage's deliverables exist (env / approve-file / TTY channels)."""
    if not (stage_ok and stage.approval and not _check_approval(stage, outputs, approval_file)):
        return stage_ok
    log.error("APPROVAL: stage '%s' not approved; pipeline halts", stage.name)
    return False


def _write_stage_sidecars(results: list, stage_validate: str) -> None:
    """T9: persist input fingerprints for a stage's successful artifacts so
    a later --reuse --reuse-fingerprint run can skip them. Sidecars are
    written on the pipeline save path (single-batch writes them in
    cli._write_sidecars); the fingerprint hashes the task's effective
    inputs, matching what the reuse check computed (round-4 P1-3/M6)."""
    for r in results:
        if r.success and r.task.output:
            eff = r.task.validate if r.task.validate is not None else stage_validate
            write_sidecar(str(r.task.output_path()), r.task.prompt, eff, r.task.model, r.task.provider)


def _log_stage_completion(stage: Stage, done: int, total_tasks: int) -> None:
    log.info("")
    log.info("Stage '%s' completed: %d/%d tasks succeeded", stage.name, done, total_tasks)


def _check_approval(stage: Stage, outputs: list, approval_file: str) -> bool:
    """T12c (D5): human-in-the-loop gate. Channels, in order:
    PBATCH_APPROVE=1 env -> approved; --approve-file exists -> approved;
    interactive y/n on a real TTY; anything else fails closed."""
    import os as _os
    if _os.environ.get("PBATCH_APPROVE", "").lower() in ("1", "true", "yes"):
        log.info("APPROVAL: stage '%s' approved via PBATCH_APPROVE env", stage.name)
        return True
    if approval_file and Path(approval_file).exists():
        log.info("APPROVAL: stage '%s' approved via %s", stage.name, approval_file)
        return True
    log.info("APPROVAL: stage '%s' deliverables: %s", stage.name, ", ".join(outputs))
    try:
        if not sys.stdin.isatty():
            log.warning("APPROVAL: stage '%s' needs human approval but stdin is not a TTY "
                        "(use --approve-file or PBATCH_APPROVE=1)", stage.name)
            return False
        reply = input(f"Approve stage '{stage.name}' outputs? (y/n): ").strip().lower()
        return reply in ("y", "yes")
    except Exception:
        return False


def _log_pipeline_start(pipeline: Pipeline, reuse: bool) -> None:
    log.info("")
    log.info("=" * 60)
    log.info("PIPELINE START (%d stages)", len(pipeline.stages))
    log.info("Mode: %s", "REUSE existing outputs" if reuse else "FORCE regeneration")
    log.info("=" * 60)


def _empty_stage_result(stage: Stage, stage_outputs: dict, reused_outputs: list) -> tuple[list, bool]:
    """Handle a stage with no tasks to run: fully-reused stages propagate
    their outputs downstream; otherwise warn and pass (nothing failed)."""
    if reused_outputs:
        stage_outputs[stage.name] = reused_outputs
        log.info("Stage '%s' fully reused (%d outputs)", stage.name, len(reused_outputs))
        return [], True
    log.warning("No tasks to execute in stage '%s'", stage.name)
    return [], True


def _log_stage_header(stage: Stage, reuse: bool) -> None:
    """Print the stage banner and whether reuse applies."""
    log.info("")
    log.info("=" * 60)
    log.info("STAGE: %s", stage.name)
    if reuse:
        log.info("(reusing existing outputs if available)")
    log.info("=" * 60)


def _build_stage_tasks(stage: Stage, stage_outputs: dict, reuse: bool, model_override: str, timeout_override: int,
                       validate_cmd: str, reuse_legacy: bool = False,
                       fingerprint_mode: bool = False, session_name: str = "") -> tuple[list, list, Optional[tuple]]:
    """Build the stage's task list from its input source (from_prompt,
    from_dir, or from_outputs). Returns (tasks, reused, early_result);
    early_result is not None when the stage must stop immediately."""
    tasks: list[Task] = []
    reused_outputs: list[str] = []

    # Dynamic role orchestration (meta stage) is handled entirely here.
    if stage.meta:
        return [], [], run_meta_stage(
            stage, stage_outputs, model_override, timeout_override,
            validate_cmd, session_name, reuse, reuse_legacy, fingerprint_mode)

    try:
        if stage.from_prompt:
            tasks, reused_outputs = _stage_from_prompt_tasks(stage, reuse, model_override, timeout_override,
                                                             validate_cmd, reuse_legacy, fingerprint_mode)
            if not tasks and not reused_outputs:
                return [], [], ([], False)
        elif stage.from_dir:
            tasks, reused_outputs = _stage_from_dir_tasks(stage, reuse, model_override, timeout_override,
                                                          validate_cmd, reuse_legacy, fingerprint_mode)
        elif stage.from_outputs:
            tasks, reused_outputs = _stage_from_outputs_tasks(stage, stage_outputs, reuse, model_override, timeout_override,
                                                              validate_cmd, reuse_legacy, fingerprint_mode)
            _, _, missing = _collect_upstream(stage.from_outputs, stage_outputs)
            if missing and not tasks:
                return [], [], ([], False)
        else:
            log.error("Stage '%s' must have either 'from_dir' or 'from_outputs'", stage.name)
            return [], [], ([], False)
    except ValueError as exc:
        # Source/template policy failures (including oversized files and
        # invalid UTF-8) are stage failures, not empty successful stages.
        log.error("Stage '%s' input rejected: %s", stage.name, exc)
        return [], [], ([], False)
    return tasks, reused_outputs, None


def _execute_stage_tasks(stage: Stage, tasks: list, reused_outputs: list, stage_validate: str,
                         session_mode: str, session_name: str, stage_session_id: str) -> tuple[list, list]:
    """Run the stage's tasks (serial or parallel) and collect the output
    paths that downstream stages will consume (reused outputs included)."""
    if stage.mode == "parallel":
        results = run_parallel(tasks, stage.workers, validate_cmd=stage_validate)
    else:
        results = run_serial(tasks, retries=0, session_mode=session_mode, session_id=stage_session_id, session_name=session_name,
                             validate_cmd=stage_validate)
    outputs = list(reused_outputs)
    for r in results:
        # Local fix (self-iteration round 2, P0-1): propagate the RESOLVED
        # path (output_path() honors task.cwd). Raw strings would make
        # git add/combine/archive operate on process-cwd-relative paths.
        if r.success and r.task.output:
            outputs.append(str(r.task.output_path()))
    return results, outputs


def _run_stage_commands(stage: Stage) -> bool:
    """Run the stage's shell commands (post-task hooks); False when any
    command failed, making hooks act as a failure gate for the stage."""
    if not stage.commands:
        return True
    log.info("")
    log.info("Running %d commands for stage '%s'... (parallel=%s)",
             len(stage.commands), stage.name, stage.commands_parallel)
    cmd_cwd = stage.cwd or os.getcwd()

    if stage.commands_parallel:
        with ThreadPoolExecutor(max_workers=len(stage.commands)) as pool:
            futs = {
                pool.submit(_run_single_cmd, cmd, i, len(stage.commands), cmd_cwd,
                            stage.command_timeout,
                            stage.command_output_max_bytes): cmd
                for i, cmd in enumerate(stage.commands, 1)
            }
            cmd_results = [f.result() for f in as_completed(futs)]
        all_cmd_ok = all(cmd_results)
    else:
        all_cmd_ok = True
        for i, cmd in enumerate(stage.commands, 1):
            if not _run_single_cmd(cmd, i, len(stage.commands), cmd_cwd,
                                   stage.command_timeout,
                                   stage.command_output_max_bytes):
                all_cmd_ok = False

    if all_cmd_ok:
        log.info("All %d commands passed for stage '%s'", len(stage.commands), stage.name)
    else:
        log.warning("Stage '%s' FAILED: some post-stage commands failed", stage.name)
    return all_cmd_ok


def _run_single_cmd(cmd: str, index: int, total: int, cmd_cwd: str,
                    timeout: int, output_max_bytes: int) -> bool:
    """Run one post-stage command with a hard process-tree deadline."""
    log.info("CMD [%d/%d]: %s", index, total, cmd)
    result = run_validation(cmd, cmd_cwd, timeout=timeout, cap=output_max_bytes)
    if result.ok:
        log.info("CMD OK (exit=0) [%d/%d]", index, total)
        _log_command_tail(result.stdout, log.info, 10)
        return True
    if result.timed_out:
        log.warning("CMD FAILED (timeout=%ss) [%d/%d]", timeout, index, total)
    else:
        log.warning("CMD FAILED (exit=%d) [%d/%d]", result.exit_code, index, total)
    _log_command_tail(result.stderr, log.warning, 10)
    _log_command_tail(result.stdout, log.info, 5)
    return False


def _log_command_tail(value: str, writer, lines: int) -> None:
    """Log only the already-bounded diagnostic tail for a stage command."""
    for line in (value or "").strip().splitlines()[-lines:]:
        writer("  | %s", line)


def _git_commit_stage(stage: Stage, outputs: list) -> bool:
    """Commit the stage's deliverables into git; failures are advisory
    (a commit problem must not fail the stage itself)."""
    if not stage.git_commit or not outputs:
        return True
    try:
        commit_msg = stage.commit_message or "[pi-batch] Stage: %s - %d tasks completed" % (stage.name, len(outputs))
        git_cwd = stage.cwd or os.getcwd()

        if not run_argv(["git", "rev-parse", "--git-dir"], git_cwd, timeout=10).ok:
            log.warning("Not a git repository, skipping git commit")
            return False
        add = run_argv(["git", "add"] + outputs, git_cwd, timeout=10)
        if not add.ok:
            log.warning("git add failed (exit=%d): %s", add.exit_code,
                        (add.stderr or "").strip()[:500])
            return False
        commit = run_argv(["git", "commit", "-m", commit_msg], git_cwd, timeout=10)
        if not commit.ok:
            log.warning("git commit failed (exit=%d): %s", commit.exit_code,
                        (commit.stderr or "").strip()[:500])
            return False
        log.info("GIT COMMIT: %s (files: %d)", commit_msg, len(outputs))
    except Exception as e:
        log.warning("Git commit failed: %s", e)
        return False
    return True


_GATE_VERDICT_RE = re.compile(r"^\s*\*{0,2}VERDICT\s*:\s*\*{0,2}(PASS|FAIL|REJECT)\b", re.IGNORECASE | re.MULTILINE)


def _archive_outputs(outputs: list, archive_dir: str, label: str) -> list:
    """Move finished deliverables into a timestamped archive subdirectory
    once the stage/pipeline completed (all tasks succeeded, gates passed).
    The worktree stays clean; git history (committed artifacts) retains
    everything, and the decision log points at the original paths. Returns
    the moved destinations for logging."""
    return [destination for _, destination in
            _archive_output_pairs(outputs, archive_dir, label)]


def _archive_output_pairs(outputs: list, archive_dir: str, label: str) -> list[tuple[str, str]]:
    """Move outputs and retain their original-to-archive mapping."""
    if not archive_dir or not outputs:
        return []
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    target = Path(archive_dir) / f"{label}-{stamp}"
    target.mkdir(parents=True, exist_ok=True)
    moved: list[tuple[str, str]] = []
    for o in outputs:
        p = Path(o)
        if p.exists():
            dest = _archive_destination(target, p)
            shutil.move(str(p), str(dest))
            moved.append((str(o), str(dest)))
    if moved:
        log.info("ARCHIVED %d deliverable(s) -> %s", len(moved), target)
    return moved


def _archive_destination(target: Path, source: Path) -> Path:
    """Choose a destination without overwriting an earlier deliverable."""
    direct = target / source.name
    if not direct.exists():
        return direct
    digest = hashlib.sha256(str(source.resolve()).encode("utf-8")).hexdigest()[:8]
    candidate = target / f"{source.stem}-{digest}{source.suffix}"
    index = 2
    while candidate.exists():
        candidate = target / f"{source.stem}-{digest}-{index}{source.suffix}"
        index += 1
    return candidate


def _gate_verdict(output_paths: list) -> Optional[str]:
    """Read a gate stage's deliverables and return its verdict
    (PASS/FAIL/REJECT) or None when no usable VERDICT: line is present.
    T12b: a bare "VERDICT: PASS" with no reason fails closed (a verdict
    must justify itself); the verdict text is untrusted, so any other
    absence is a FAIL too. FAIL/REJECT anywhere wins over an earlier PASS
    (order-independent: a self-contradictory deliverable must not unlock
    downstream stages — round-4 finding L2/DS-6)."""
    first_pass = None
    for p in output_paths:
        path = Path(p)
        if not path.exists():
            continue
        try:
            text = read_text_bounded(path, config.OUTPUT_MAX_BYTES,
                                     "gate artifact")
        except ValueError as exc:
            log.error("GATE: %s; failing closed", exc)
            continue
        for m in _GATE_VERDICT_RE.finditer(text):
            # the reason is the text between the match end and the line end
            line_end = text.find("\n", m.end())
            if line_end == -1:
                line_end = len(text)
            rest = text[m.end():line_end].strip()
            if not rest:
                log.warning("GATE: %s has a bare VERDICT with no reason; failing closed", path)
                continue
            verdict = m.group(1).upper()
            if verdict in ("FAIL", "REJECT"):
                return verdict
            if first_pass is None:
                first_pass = verdict
    return first_pass


def _extract_decisions(text: str, limit: int = 5) -> list[str]:
    """Pull decision points (markdown headings) with their first sentence
    from a deliverable: an index into the full reasoning stored in the
    artifact, so the decision log stays readable while pointing at the
    complete rationale."""
    out = []
    for i, line in enumerate((text or "").splitlines()):
        if re.match(r"^#{2,3}\s+", line):
            heading = line.lstrip("# ").strip()
            nxt = ""
            for n in (text or "").splitlines()[i + 1:i + 4]:
                if n.strip() and not n.lstrip().startswith("#"):
                    nxt = n.strip()[:120]
                    break
            out.append(f"{heading}: {nxt}" if nxt else heading)
            if len(out) >= limit:
                break
    return out


def _append_decision_log(path: str, stage_name: str, results: list, stage_ok: bool, verdict: Optional[str]) -> None:
    """Append one structured decision record per finished stage: the
    stage, its status, each decision point extracted from the deliverables
    (heading + first sentence, the full reasoning lives in the artifacts),
    and the evidence file paths. Appending keeps the whole history; the
    log is a first-class deliverable of the pipeline."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "a", encoding="utf-8") as f:
        f.write(f"\n## {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — stage '{stage_name}' — "
                f"{'PASS' if stage_ok else 'FAIL'}")
        if verdict:
            f.write(f" (gate verdict: {verdict})")
        f.write("\n")
        for r in results:
            decisions = _extract_decisions(r.stdout)
            label = r.task.output or "(stdout)"
            status = "ok" if r.success else f"FAILED: {r.reason}"
            f.write(f"- task {label} [{status}]")
            if decisions:
                f.write(": " + "; ".join(decisions))
            f.write("\n")
        evidence = [r.task.output for r in results if r.task.output]
        if evidence:
            f.write(f"- evidence: {', '.join(evidence)}\n")


def run_pipeline(pipeline: Pipeline, model_override: str = "", dry_run: bool = False, reuse: bool = False, timeout_override: int = 0,
                 session_mode: str = "new", session_name: str = "", validate_cmd: str = "", decision_log: str = "", archive_dir: str = "",
                 reuse_legacy: bool = False, fingerprint_mode: bool = False,
                 approval_file: str = "") -> tuple[list[TaskResult], list[str]]:
    """Execute all stages sequentially; returns (all task results, names of
    stages that failed tasks or commands). Gates halt the pipeline, decision
    logs record every stage, and completed runs archive their deliverables."""
    all_results: list[TaskResult] = []
    failed_stages: list[str] = []
    stage_outputs: dict[str, list[str]] = {}  # stage_name -> [output_file_paths]
    
    _log_pipeline_start(pipeline, reuse)
    
    for stage in pipeline.stages:
        if dry_run:
            _describe_stage(stage, reuse)
            continue

        results, stage_ok = execute_stage(stage, stage_outputs, model_override, reuse, timeout_override,
                                          session_mode, session_name, validate_cmd, reuse_legacy,
                                          fingerprint_mode, approval_file)
        all_results.extend(results)
        if not stage_ok:
            failed_stages.append(stage.name)

        gate_verdict = _handle_gate(stage, stage_outputs, session_name)

        # Structured decision record for this stage (append-only history)
        log_path = decision_log or pipeline.decision_log
        if log_path and results:
            _append_decision_log(log_path, stage.name, results, stage_ok, gate_verdict)

        if gate_verdict in ("FAIL", "REJECT"):
            if stage.name not in failed_stages:
                failed_stages.append(stage.name)
            log.error("Pipeline halted by gate: %s", stage.name)
            break
        if not stage_ok:
            log.error("Pipeline halted by failed stage: %s", stage.name)
            break

    # The pipeline completed (no failed stages, no gate rejection): move the
    # committed deliverables into the archive so the worktree stays clean.
    if not failed_stages and (pipeline.archive_dir or archive_dir):
        _archive_pipeline_results(pipeline, all_results, archive_dir, session_name)
    
    return all_results, failed_stages


def _archive_pipeline_results(pipeline: Pipeline, results: list[TaskResult],
                              archive_dir: str, session_name: str) -> None:
    outputs = [result.task.output for result in results
               if result.success and result.task.output]
    moved = dict(_archive_output_pairs(
        outputs, archive_dir or pipeline.archive_dir, pipeline.name))
    for result in results:
        if result.task.output in moved:
            result.task.output = moved[result.task.output]
    from .memory import record_archive
    cwd = pipeline.stages[0].cwd if pipeline.stages and pipeline.stages[0].cwd else os.getcwd()
    record_archive(list(moved.items()), cwd, session_name)


def _describe_stage(stage: Stage, reuse: bool) -> None:
    """Dry-run: print what the stage would do without executing it."""
    log.info("")
    log.info("STAGE: %s (dry-run)", stage.name)
    if stage.from_dir:
        log.info("  Will read .md files from: %s", stage.from_dir)
        if reuse:
            log.info("  Will skip files with existing outputs")
    elif stage.from_outputs:
        log.info("  Will use outputs from stage: %s", stage.from_outputs)
        log.info("  Task templates: %d", len(stage.tasks))
    if stage.commands:
        log.info("  Commands: %d (parallel=%s)", len(stage.commands), stage.commands_parallel)
        for cmd in stage.commands:
            log.info("    | %s", cmd)
    if stage.git_commit:
        log.info("  Git commit: YES")
    log.info("  Mode: %s", stage.mode)


def _handle_gate(stage: Stage, stage_outputs: dict, session_name: str = "") -> Optional[str]:
    """Evaluate a gate stage's deliverables: VERDICT: PASS/FAIL/REJECT.
    A missing verdict fails closed (no explicit pass, no unlock). Returns
    the verdict or None when the stage is not a gate."""
    if not stage.gate:
        return None
    verdict = _gate_verdict(stage_outputs.get(stage.name, []))
    if verdict is None:
        verdict = "FAIL"
        log.error("GATE stage '%s' produced no VERDICT: line; failing closed", stage.name)
    elif verdict in ("FAIL", "REJECT"):
        log.error("GATE REJECTED at stage '%s' (verdict: %s) — later stages blocked", stage.name, verdict)
    else:
        log.info("GATE PASSED at stage '%s'", stage.name)
    from .memory import record_gate
    record_gate(stage.name, verdict, stage_outputs.get(stage.name, []),
                stage.cwd or os.getcwd(), session_name)
    return verdict


