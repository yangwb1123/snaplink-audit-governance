"""Meta-stage dynamic role orchestration (extracted from pipeline.py when it
hit the 1000-line quality budget — round-4 governance close-out).

The orchestrator analyses the current deliverables and picks the review
roles that still add value: named roles resolve against role_dir templates,
ad-hoc roles carry their own task description. Role tasks run concurrently
in their own agent sessions, their deliverables fold back into the evidence,
and the loop repeats until the orchestrator reports no more roles or
max_iterations is reached. Orchestrator output is untrusted: role names are
sanitized, template lookups stay inside role_dir, and the plan parser is
bracket- and string-aware (a ']' inside a quoted string does not close the
array).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from . import config
from .config import AGENT_DEFAULT_WORKERS, _resolve_validators, log
from .evidence import combine_outputs
from .models import Stage, Task, TaskResult
from .relevance import (constrain_role_plan, format_suggestions,
                        load_role_keywords, score_roles)
from .reuse import fingerprint as reuse_fp
from .reuse import reuse_decision, write_sidecar
from .runner import _save_validated, run_task
from .memory import record_task
from .text_io import read_text_bounded


_DEFAULT_META_PROMPT = """Analyze the deliverables below and decide which expert review roles are still needed to harden them.

Available roles: {roles}

{role_suggestions}

Rules:
- Only choose roles that add real value for this deliverable set.
- Select AT MOST {max_roles} roles.
- Prefer a role name from the list above.
- When no listed role fits, define an ad-hoc role as a JSON object
  {{\"role\": \"<name>\", \"task\": \"<assignment for this reviewer>\"}}.
- Output ONLY a JSON array of role names and/or role objects, e.g.
  ["security_engineer", {{\"role\": \"perf_reviewer\", \"task\": \"Analyze performance bottlenecks\"}}].
- Output [] when the deliverables are complete.

Deliverables:
{input_content}
"""


def _available_roles(role_dir: str) -> list:
    """List role template names (file stems) inside role_dir; empty when the
    dir is missing (meta stages may still run with ad-hoc roles only)."""
    if not role_dir:
        return []
    base = Path(role_dir).resolve()
    if not base.is_dir():
        return []
    return sorted(p.stem for p in base.glob("*.md") if p.stem != "README")


def _load_role_template(role_dir: str, role: str) -> Optional[str]:
    """Load a role template by name, refusing anything outside role_dir (the
    orchestrator output is untrusted input). Confinement is enforced with
    relative_to() — a string-prefix check let a sibling dir whose name
    shares role_dir's prefix escape the boundary (round-4 finding H2) — and
    resolve() refuses symlinks that point outside role_dir."""
    if not role_dir:
        return None
    base = Path(role_dir).resolve()
    if not base.is_dir():
        return None
    target = (base / (role + ".md")).resolve()
    try:
        target.relative_to(base)
    except ValueError:
        log.warning("Unknown role template: %s (must be a .md file inside %s)", role, role_dir)
        return None
    if not target.is_file():
        log.warning("Unknown role template: %s (must be a .md file inside %s)", role, role_dir)
        return None
    try:
        return read_text_bounded(target, config.INPUT_MAX_BYTES, "role template")
    except ValueError as exc:
        log.warning("Role template rejected: %s", exc)
        return None


def _extract_json_array(text: str) -> str:
    """Return the first JSON array literal in *text* (from its '[' to the
    MATCHING ']'), tolerating prose before it. The scan is bracket- and
    string-aware: a ']' inside a quoted string does not close the array —
    the old regex truncated ad-hoc role task text at the first ']'
    (round-4 finding M5/META1)."""
    start = (text or "").find("[")
    if start == -1:
        return ""
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(text)):
        c = text[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            continue
        if c == '"':
            in_str = True
        elif c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
    return ""


def _parse_role_plan(stdout: str) -> list:
    """Parse the orchestrator plan from its JSON output, tolerating prose and
    markdown fences. Each plan item is either a role name (string, resolved
    against role_dir) or an ad-hoc role {"role": ..., "task": ...}. Unparseable
    output -> [] (treat as 'no more roles needed')."""
    arr = _extract_json_array(stdout)
    plan = []
    if arr:
        try:
            data = json.loads(arr)
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, str) and item.strip():
                        plan.append({"role": item.strip(), "task": ""})
                    elif isinstance(item, dict) and str(item.get("role", "")).strip():
                        plan.append({"role": str(item["role"]).strip(),
                                     "task": str(item.get("task", "")).strip()})
                return plan
        except json.JSONDecodeError:
            plan = []
    if not plan:
        log.warning("META orchestrator output did not contain a JSON role plan; treating as complete")
    return []


def _run_meta_stage(stage: Stage, stage_outputs: dict, model_override: str = "", timeout_override: int = 0,
                    validate_cmd: str = "", session_name: str = "", reuse: bool = False,
                    reuse_legacy: bool = False,
                    fingerprint_mode: bool = False,
                    original_prompt: str = "") -> tuple[list[TaskResult], bool]:
    """Dynamic role orchestration: orchestrator picks roles per iteration,
    role outputs fold back into evidence until no more roles or
    max_iterations (self-optimizing role set)."""
    raw_sources = stage.from_outputs if isinstance(stage.from_outputs, (list, tuple)) else [stage.from_outputs]
    source_names = [str(name).strip() for name in raw_sources if str(name).strip()]
    missing = [name for name in source_names if name not in stage_outputs]
    if missing:
        log.error("Previous stage(s) not found: %s", ", ".join(missing))
        return [], False
    if not stage.output_dir:
        log.error("Meta stage '%s' requires output_dir for role deliverables", stage.name)
        return [], False

    evidence_outputs = [p for name in source_names for p in stage_outputs.get(name, [])]
    combined = combine_outputs(evidence_outputs)
    if not combined:
        log.warning("No upstream outputs available for meta stage '%s'", stage.name)
        return [], True

    role_names = _available_roles(stage.role_dir)
    out_dir = Path(stage.output_dir)
    if not out_dir.is_absolute():
        out_dir = Path(stage.cwd or os.getcwd()) / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    all_results: list[TaskResult] = []
    role_outputs: list[str] = []
    for iteration in range(1, stage.max_iterations + 1):
        done, ok, combined = _run_meta_iteration(
            stage, combined, role_names, model_override, timeout_override,
            validate_cmd, iteration, all_results, role_outputs,
            evidence_outputs, session_name, reuse, reuse_legacy,
            fingerprint_mode, original_prompt)
        if done or not ok:
            break

    stage_outputs[stage.name] = role_outputs
    # meta_max_failed_roles: 允许 N 个角色失败而阶段仍成功（审查缺失由
    # 后续 gate 兜底）；默认 0 = 任一角色失败即阶段失败（fail-closed）。
    failed_roles = sum(1 for r in all_results if not r.success)
    stage_ok = failed_roles <= stage.meta_max_failed_roles
    if not stage_ok:
        log.error("META stage '%s': %d role task(s) failed (limit %d)",
                  stage.name, failed_roles, stage.meta_max_failed_roles)
    return all_results, stage_ok

def _run_meta_iteration(stage: Stage, combined: str, role_names: list, model_override: str, timeout_override: int,
                        validate_cmd: str, iteration: int, all_results: list, role_outputs: list,
                        evidence_outputs: list, session_name: str = "", reuse: bool = False,
                        reuse_legacy: bool = False,
                        fingerprint_mode: bool = False,
                        original_prompt: str = "") -> tuple[bool, bool, str]:
    """Run one orchestrator/role round and fold its evidence into context.
    Returns (done, ok, combined); done stops the expansion loop."""
    log.info("META iteration %d/%d for stage '%s' (roles available: %s)",
             iteration, stage.max_iterations, stage.name, ", ".join(role_names) or "(none)")
    meta_task = _build_meta_prompt_task(stage, role_names, combined, model_override, timeout_override,
                                        original_prompt)
    meta_result = run_task(meta_task, session_name=session_name)
    record_task(meta_result)
    if not meta_result.success:
        log.warning("META orchestrator call failed: %s; stopping role expansion", meta_result.reason)
        return True, False, combined
    roles = _select_roles(stage, combined, role_names, meta_result.stdout)
    if not roles:
        log.info("META orchestrator: no more roles needed (iteration %d)", iteration)
        return True, True, combined
    log.info("META orchestrator selected %d role(s)", len(roles))

    role_tasks, _ = _build_role_tasks(stage, roles, combined, model_override, timeout_override)
    failed = _run_role_batch(stage, role_tasks, validate_cmd, session_name, reuse,
                             reuse_legacy, fingerprint_mode, all_results,
                             role_outputs, evidence_outputs)
    combined = combine_outputs(evidence_outputs)
    # meta_role_retries: 失败/超时角色自动重试一次（G2 教训），仍失败才计数。
    if failed and stage.meta_role_retries > 0:
        failed = _retry_failed_roles(stage, all_results, role_outputs, evidence_outputs,
                                     validate_cmd, session_name, reuse, reuse_legacy,
                                     fingerprint_mode)
        combined = combine_outputs(evidence_outputs)
    if failed and stage.meta_max_failed_roles <= 0:
        log.warning("META stage '%s': some role tasks failed in iteration %d", stage.name, iteration)
        return True, False, combined
    if failed:
        log.warning("META stage '%s': %d role task(s) failed in iteration %d (within allowance %d)",
                    stage.name, sum(1 for r in all_results if not r.success), iteration,
                    stage.meta_max_failed_roles)
    return False, True, combined


def _run_role_batch(stage: Stage, role_tasks: list, validate_cmd: str, session_name: str,
                    reuse: bool, reuse_legacy: bool, fingerprint_mode: bool,
                    all_results: list, role_outputs: list, evidence_outputs: list) -> bool:
    """Execute one iteration's role tasks concurrently; returns True when
    any role task failed (caller applies retries/allowance)."""
    failed = False
    with ThreadPoolExecutor(max_workers=max(1, stage.workers or AGENT_DEFAULT_WORKERS)) as pool:
        futures = [pool.submit(
            _run_role_task, item, validate_cmd, session_name, reuse,
            reuse_legacy, fingerprint_mode) for item in role_tasks]
        for fut in as_completed(futures):
            _, result = fut.result()
            all_results.append(result)
            failed = failed or not result.success
            out_path = result.task.output_path()
            if result.success and out_path and out_path.exists():
                role_outputs.append(str(out_path))
                evidence_outputs.append(str(out_path))
    return failed


def _retry_failed_roles(stage: Stage, all_results: list, role_outputs: list, evidence_outputs: list,
                        validate_cmd: str, session_name: str, reuse: bool,
                        reuse_legacy: bool, fingerprint_mode: bool) -> bool:
    """Retry failed role tasks once (timeouts/transient failures absorb a
    single retry). Returns True when failures remain after retrying."""
    still_failed = False
    for result in all_results:
        if result.success:
            continue
        task = result.task
        log.info("META retry role '%s' after %s", task.output_path(), result.reason)
        retry = run_task(task, parallel=True, session_name=session_name)
        if retry.success:
            retry.success = _save_validated(task, retry, validate_cmd)
            if not retry.success:
                retry.reason = "validation failed"
        record_task(retry)
        # 原位替换失败结果：成功则产物入 evidence，失败则保留计数
        if retry.success:
            result.success = True
            result.returncode = 0
            result.reason = "retried"
            result.stdout = retry.stdout
            out_path = task.output_path()
            if out_path and out_path.exists() and str(out_path) not in role_outputs:
                role_outputs.append(str(out_path))
                evidence_outputs.append(str(out_path))
        else:
            still_failed = True
            log.warning("META retry for '%s' failed again: %s", task.output_path(), retry.reason)
    return still_failed


def _run_role_task(item, validate_cmd: str, session_name: str, reuse: bool,
                   reuse_legacy: bool, fingerprint_mode: bool):
    """Execute or freshness-reuse one role selected by the orchestrator."""
    role, task = item
    output = str(task.output_path())
    expected = (reuse_fp(task.prompt, output, validate_cmd, task.model, task.provider)
                if fingerprint_mode else None)
    if reuse and reuse_decision(
            output, validate_cmd, task.workdir(), reuse_legacy, expected):
        result = TaskResult(task=task, success=True, returncode=0, reason="reused")
        record_task(result)
        return role, result
    result = run_task(task, parallel=True, session_name=session_name)
    if result.success:
        result.success = _save_validated(task, result, validate_cmd)
        if not result.success:
            result.reason = "validation failed"
        elif fingerprint_mode:
            write_sidecar(output, task.prompt, validate_cmd, task.model, task.provider)
    record_task(result)
    return role, result


def _select_roles(stage: Stage, combined: str, role_names: list, stdout: str) -> list:
    """Parse an untrusted plan, then enforce relevance, dedupe, and fan-out."""
    roles = _parse_role_plan(stdout)
    keywords = load_role_keywords(stage.role_keywords) if stage.relevance_enabled else {}
    scores = score_roles(combined, role_names, keywords)
    minimum = stage.relevance_min_score if stage.relevance_enabled else 0
    roles, dropped = constrain_role_plan(
        roles, stage.max_roles_per_iteration, scores, minimum)
    if dropped:
        log.warning("META role plan constrained; dropped: %s", ", ".join(dropped))
    return roles


def _build_meta_prompt_task(stage: Stage, role_names: list, combined: str, model_override: str, timeout_override: int,
                            original_prompt: str = "") -> Task:
    """Assemble the orchestrator prompt (custom meta_prompt or the default,
    with {roles}/{role_suggestions}/{max_roles}/{input_content}/{prompt}
    placeholders) as a task. Custom prompts without {role_suggestions}
    receive the block as an appended policy hint."""
    meta_prompt = (stage.meta_prompt or _DEFAULT_META_PROMPT)
    keywords = load_role_keywords(stage.role_keywords) if stage.relevance_enabled else {}
    suggestions = format_suggestions(score_roles(combined, role_names, keywords))
    meta_prompt = meta_prompt.replace("{roles}", ", ".join(role_names) or "(none - define ad-hoc roles)")
    if "{role_suggestions}" in meta_prompt:
        meta_prompt = meta_prompt.replace("{role_suggestions}", suggestions)
    elif suggestions:
        meta_prompt += "\n\n" + suggestions
    meta_prompt = meta_prompt.replace("{max_roles}", str(max(1, stage.max_roles_per_iteration)))
    meta_prompt = meta_prompt.replace("{input_content}", combined)
    meta_prompt = meta_prompt.replace("{prompt}", original_prompt)
    task = Task(prompt=meta_prompt, cwd=stage.cwd)
    task.memory = {"stage": stage.name, "role": "orchestrator"}
    task.model = stage.model or task.model
    task.provider = stage.provider or task.provider
    if model_override:
        task.model = model_override
    if stage.meta_timeout:
        task.timeout = stage.meta_timeout
    elif timeout_override:
        task.timeout = timeout_override
    return task


def _build_role_tasks(stage: Stage, roles: list, combined: str, model_override: str, timeout_override: int) -> tuple[list, bool]:
    """Turn the orchestrator plan into tasks: ad-hoc roles ({"role",
    "task"}) get their task description plus the current context; named
    roles load the role_dir template. Output names are sanitized (the
    orchestrator output is untrusted), and sanitization collisions are
    disambiguated with a short hash so one role cannot silently overwrite
    another's deliverable (round-4 finding DS-7). Returns ([(role, Task)],
    ok); ok is False when a named role had no template."""
    role_tasks = []
    ok = True
    used_names: set = set()
    for item in roles:
        role = item["role"]
        task_desc = item["task"]
        if task_desc:
            prompt = f"{task_desc}\n\nContext (current deliverables):\n{combined}"
        else:
            template = _load_role_template(stage.role_dir, role)
            if template is None:
                ok = False
                continue
            prompt = template.replace("{input_content}", combined).replace("{input_stem}", stage.name)
        safe_name = re.sub(r"[^A-Za-z0-9_-]", "_", role) or "role"
        if safe_name in used_names:
            # security/engineer vs security.engineer both sanitize to
            # security_engineer: disambiguate instead of last-writer-wins
            suffix = hashlib.sha1(role.encode("utf-8", errors="replace")).hexdigest()[:8]
            safe_name = f"{safe_name}-{suffix}"
        used_names.add(safe_name)
        out_path = Path(stage.output_dir) / f"{safe_name}.md"
        task = Task(prompt=prompt, output=str(out_path), cwd=stage.cwd)
        task.memory = {"stage": stage.name, "role": role}
        task.model = stage.model or task.model
        task.provider = stage.provider or task.provider
        if model_override:
            task.model = model_override
        if stage.meta_timeout:
            task.timeout = stage.meta_timeout
        elif timeout_override:
            task.timeout = timeout_override
        role_tasks.append((role, task))
    return role_tasks, ok



def run_meta_stage(stage: Stage, stage_outputs: dict, model_override: str = "", timeout_override: int = 0,
                   validate_cmd: str = "", session_name: str = "", reuse: bool = False,
                   reuse_legacy: bool = False,
                   fingerprint_mode: bool = False,
                   original_prompt: str = "") -> tuple[list, bool]:
    """Public entry: dynamic role orchestration (see module docstring)."""
    return _run_meta_stage(
        stage, stage_outputs, model_override, timeout_override, validate_cmd,
        session_name, reuse, reuse_legacy, fingerprint_mode, original_prompt)
