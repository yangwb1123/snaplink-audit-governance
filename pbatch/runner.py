"""Task execution: run one agent call with a hard deadline, failure
signatures, retries, validation gates, and summaries."""

from __future__ import annotations

import json
import os
import random
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import NamedTuple, Optional

from . import config
from .config import AGENT_BIN, AGENT_DEFAULT_WORKERS, _resolve_validator_specs, _resolve_validators, _session_flags, log
from .models import Task, TaskResult
from . import metering
from . import ratelimit
from .metering import budget_allows, budget_try_consume, record_event
from .memory import (enrich_prompt, memory_enabled, new_session_id,
                     record_task, session_id_from_flags)
from .session import fork_flags, has_compaction, session_size_bytes
from .triage import clear_marker, task_key, write_marker

# registry of live agent subprocesses (parallel mode: a budget cap must be
# able to kill in-flight agents so the executor shutdown is prompt)
_ACTIVE_PROCS: set = set()
_ACTIVE_LOCK = threading.Lock()


def _register_proc(proc: subprocess.Popen) -> None:
    with _ACTIVE_LOCK:
        _ACTIVE_PROCS.add(proc)


def _unregister_proc(proc: subprocess.Popen) -> None:
    with _ACTIVE_LOCK:
        _ACTIVE_PROCS.discard(proc)


def kill_active_procs() -> None:
    """Kill every currently-running agent process group (T8 parallel cap:
    in-flight agents must not keep the runner waiting after the cap)."""
    with _ACTIVE_LOCK:
        procs = list(_ACTIVE_PROCS)
    for proc in procs:
        _kill_group(proc)


def _read_stream(stream, prefix: str, collector: list, cap: int = 0,
                 overflow: Optional[list] = None, emit: bool = False,
                 target=None) -> None:
    """Drain and collect a child stream, optionally displaying it live.
    Collection is capped at *cap* bytes (T12e): a misbehaving agent must
    not OOM the runner before the post-hoc size check; overflow is recorded
    so the caller can reject the result. The stream is still drained (a
    blocked pipe would hang the agent), while collection and live display
    stop at the cap."""
    total = 0
    sink = target or sys.stdout
    try:
        for line in iter(lambda: stream.readline(8192), ""):
            within_cap = True
            if cap > 0:
                total += len(line.encode("utf-8", errors="replace"))
                within_cap = total <= cap
            if within_cap:
                collector.append(line)
                if emit:
                    sink.write(f"{prefix}{line}" if prefix else line)
                    sink.flush()
            elif overflow is not None:
                overflow[0] = True
    except ValueError:
        # stream closed
        pass
    finally:
        stream.close()


def agent_failure_reason(returncode: int, output: str) -> str:
    """Return a short reason when the agent result must be discarded, or ''
    when the output is a usable result. Non-zero exit, empty output, and
    provider/CLI failure signatures (quota, rate limit, auth, billing) all
    reject the result so error replies are never saved as task outputs.
    Signatures are per-agent (agent.agents.<bin>.error_patterns can replace
    the built-in table for claude/codex/... — G line)."""
    if returncode != 0:
        return f"agent exited {returncode}"
    if not output or not output.strip():
        return "agent produced no output"
    for pattern in config.agent_error_patterns():
        if pattern.search(output):
            return f"agent reported provider failure ({pattern.pattern})"
    return ""


def _budget_gate(task: Task, key: str, workdir: str, session_name: str = "") -> None:
    """T8: the budget cap wins over everything — a capped run stops before
    spawning, never after burning quota (exit 3, budget_cap event).
    budget_try_consume is atomic, so parallel workers cannot overshoot the
    cap (round-4 finding M2/DS-4)."""
    if not budget_try_consume():
        log.error("BUDGET: invocation limit reached; stopping (exit 3)")
        record_event("budget_cap", task.prompt, "invocation limit", workdir, session_name)
        clear_marker(key, workdir)
        raise SystemExit(3)



def session_file_path(cwd: str, session_name: str) -> Optional[str]:
    """Active pi session file for a shared/per-stage session (T10)."""
    from .session import session_file
    f = session_file(cwd, session_name)
    return str(f) if f else None


def _prompt_brief(task: Task, cmd: list) -> str:
    """One-line log summary with the prompt truncated (prompt content is
    sensitive and may be shipped by --log-file)."""
    brief_prompt = " ".join(task.prompt.split())[:80]
    if len(brief_prompt) == 80:
        return f"{cmd[0]} -p {brief_prompt}..."
    return f"{cmd[0]} -p {brief_prompt}"


def _kill_group(proc: Optional[subprocess.Popen]) -> None:
    """Kill the whole child process group, tolerating an already-dead
    process, then reap the direct child before returning."""
    if proc is None:
        return
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        try:
            proc.kill()
        except Exception:
            pass  # process already gone
    try:
        proc.wait(timeout=1)
    except (subprocess.TimeoutExpired, ChildProcessError):
        pass


def _spawn_agent(cmd: list, workdir: str, env: dict) -> subprocess.Popen:
    """Spawn the agent in its own process group so the whole child tree
    can be killed on timeout or interrupt."""
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, cwd=workdir, env=env, start_new_session=True)


def _agent_not_found(task: Task) -> TaskResult:
    """The agent binary is missing from PATH: a permanent, non-retryable
    failure (never saved as an artifact)."""
    log.error("'%s' not found in PATH. Is it installed? (configure agent.bin in pi-batch.yaml)", config.AGENT_BIN)
    return TaskResult(task=task, success=False, stderr=f"{config.AGENT_BIN} not found in PATH",
                      reason="agent binary not found")


def _overflow_result(task: Task, proc, stdout_lines: list, stderr_lines: list, start: float) -> TaskResult:
    """T12e: reject a result whose collected stdout exceeded the byte cap.
    The cap is enforced DURING collection (a runaway agent must not OOM the
    runner before the post-hoc size check — round-4 finding L9)."""
    log.error("REJECTED: agent output exceeds %d bytes (T12e cap)", config.OUTPUT_MAX_BYTES)
    return TaskResult(
        task=task, success=False, stdout="".join(stdout_lines), stderr="".join(stderr_lines),
        elapsed=time.monotonic() - start, returncode=proc.returncode,
        reason=f"agent output exceeds {config.OUTPUT_MAX_BYTES} bytes (T12e cap)",
    )


def _prompt_limit_result(task: Task, cmd: list) -> Optional[TaskResult]:
    size = len(cmd[2].encode("utf-8", errors="replace"))
    if config.PROMPT_MAX_BYTES <= 0 or size <= config.PROMPT_MAX_BYTES:
        return None
    reason = f"prompt exceeds {config.PROMPT_MAX_BYTES} bytes ({size} bytes)"
    log.error("REJECTED: %s", reason)
    return TaskResult(task=task, success=False, returncode=-1, reason=reason)


def run_task(task: Task, task_index: int = 0, total: int = 0, parallel: bool = False, session_flags: Optional[list] = None, session_name: str = "") -> TaskResult:
    """Execute one agent task with live-output policy and T7 session metering."""
    cmd, workdir, session_id, effective_name = _memory_invocation(task, session_flags, session_name)
    if prompt_limit := _prompt_limit_result(task, cmd):
        return _attach_session(prompt_limit, session_id, effective_name, workdir)
    start = time.monotonic()
    proc = None
    completed = False
    prefix, env, key, usage_before = _prepare_task_run(
        task, cmd, task_index, total, parallel, workdir, effective_name, session_id)
    try:
        _rate_gate(task, effective_name)
        proc = _spawn_agent(cmd, workdir, env)
        _register_proc(proc)
        # GAP-3 (N1): marker carries the AGENT pid so a killed runner's
        # residue scan can flag a still-running orphan (double billing).
        write_marker(key, workdir, agent_pid=proc.pid)
        streamed = _stream_output_enabled()
        stdout_lines, stderr_lines, overflow = _stream_proc(proc, prefix, start,
                                                            task.timeout, streamed)
        completed = True
        if overflow[0]:
            result = _overflow_result(task, proc, stdout_lines, stderr_lines, start)
            return _finish_task_result(
                result, task, session_id, effective_name, workdir, usage_before)
        result = _result_from_proc(task, proc, stdout_lines, stderr_lines, time.monotonic() - start)
        result.streamed_output = streamed
        return _finish_task_result(
            result, task, session_id, effective_name, workdir, usage_before)
    except subprocess.TimeoutExpired:
        result = _timeout_result(task, proc, start)
        return _finish_task_result(
            result, task, session_id, effective_name, workdir, usage_before)
    except FileNotFoundError:
        result = _agent_not_found(task)
        return _finish_task_result(
            result, task, session_id, effective_name, workdir, usage_before)
    except Exception as e:
        elapsed = time.monotonic() - start
        log.error("ERROR  [%.1fs]  [%s]", elapsed, e)
        result = TaskResult(task=task, success=False, stderr=str(e), elapsed=elapsed, reason=str(e))
        return _finish_task_result(
            result, task, session_id, effective_name, workdir, usage_before)
    finally:
        # On any abnormal exit kill the child group; normal completion is done.
        if proc is not None and not completed:
            _kill_group(proc)
        if proc is not None:
            _unregister_proc(proc)
        clear_marker(key, workdir)


def _prepare_task_run(task: Task, cmd: list, task_index: int, total: int,
                      parallel: bool, workdir: str, session_name: str,
                      session_id: str = "") -> tuple:
    prefix = f"[task-{task_index}] " if (parallel and total > 1) else ""
    log.info(">>  %s  [model=%s]  [timeout=%ss]  [dir=%s]",
             _prompt_brief(task, cmd), task.model or "default", task.timeout, workdir)
    env = {**os.environ, **task.env}
    key = task_key(task)
    write_marker(key, workdir)
    _budget_gate(task, key, workdir, session_name)
    usage_ref = session_id or session_name
    usage = metering.session_usage(workdir, usage_ref) if metering.EVENTS_FILE else None
    return prefix, env, key, usage


def _rate_gate(task: Task, session_name: str = "") -> None:
    """F line: wait for a provider token before spawning the agent. Runs
    AFTER the budget gate so a capped run stops before waiting; blocks
    parallel workers into a queue instead of overloading the provider.
    Best-effort: an acquire timeout (or a provider-missing key) only logs."""
    if ratelimit.RATE_PER_SECOND <= 0 and not ratelimit.PROVIDER_RATES:
        return
    if not ratelimit.acquire(task.provider or "", timeout=300.0):
        log.warning("RATE: token wait timed out for provider '%s' (300s); spawning anyway",
                    task.provider or "default")


def _finish_task_result(result: TaskResult, task: Task, session_id: str,
                        session_name: str, workdir: str,
                        usage_before: Optional[dict]) -> TaskResult:
    _attach_session(result, session_id, session_name, workdir)
    _record_metering(result, task, workdir, session_id or session_name, usage_before)
    return result


def _record_metering(result: TaskResult, task: Task, workdir: str,
                     session_name: str, usage_before: Optional[dict]) -> None:
    record_event("task_finish" if result.success else "task_fail",
                 task.prompt, result.reason, workdir, session_name, usage_before)


def _memory_invocation(task: Task, session_flags: Optional[list],
                       session_name: str) -> tuple[list, str, str, str]:
    workdir = task.workdir()
    flags = session_flags
    name = session_name or task.memory.get("stage", "") or "batch"
    session_id = session_id_from_flags(flags)
    prompt = task.prompt
    if memory_enabled():
        prompt = enrich_prompt(prompt, workdir, str(task.memory.get("memory_mode", "")))
        if flags is None:
            session_id = new_session_id(name, task.prompt, workdir)
            flags = _session_flags("start", session_id, name)
    return replace(task, prompt=prompt).to_cmd(flags), workdir, session_id, name


def _attach_session(result: TaskResult, session_id: str, session_name: str,
                    workdir: str) -> TaskResult:
    result.session_id = session_id
    result.session_name = session_name
    if session_id:
        from .session import session_file
        path = session_file(workdir, session_id)
        result.raw_session = str(path) if path else ""
    return result


def _output_path_is_symlink(task: Task) -> bool:
    """T0.1: probe the UNRESOLVED output path — output_path() resolves
    symlinks away, which would hide a link planted at the output location
    and redirect the write to its target (round-4 finding H1)."""
    probe = Path(task.output)
    if not probe.is_absolute():
        probe = Path(task.workdir()) / probe
    return probe.is_symlink()


def save_result(task: Task, result: TaskResult) -> None:
    """Write a successful task result to its output file, or print to stdout.
    Failed or rejected results are never written to disk: quota or rate-limit
    replies must not become committed artifacts, so the error is logged only.
    """
    out_path = task.output_path()
    if out_path is None:
        if not result.streamed_output:
            sys.stdout.write(result.stdout)
            if result.stderr:
                sys.stderr.write(result.stderr)
        return

    if not result.success:
        log.error("NOT SAVED %s: task failed (exit=%d, %.1fs)", out_path, result.returncode, result.elapsed)
        return
    # T12e: oversized outputs are rejected, not written (runaway agents
    # must not fill the disk).
    if len(result.stdout.encode("utf-8")) > config.OUTPUT_MAX_BYTES:
        log.error("NOT SAVED %s: output exceeds %d bytes (T12e cap)", out_path, config.OUTPUT_MAX_BYTES)
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)
    # T0.1 (full-SDLC pipeline): refuse symlink targets (a symlinked output
    # path could redirect the write elsewhere) and use a unique mkstemp
    # name, mirroring _save_validated's F14 hardening. Note: probe the
    # UNRESOLVED path — output_path() resolves symlinks away.
    if _output_path_is_symlink(task):
        log.error("NOT SAVED %s: output path is a symlink (refusing to follow)", out_path)
        return
    fd, tmp_name = tempfile.mkstemp(prefix=out_path.name + ".", suffix=".tmp", dir=str(out_path.parent))
    os.close(fd)
    tmp = Path(tmp_name)
    tmp.write_text(result.stdout, encoding="utf-8")
    tmp.rename(out_path)
    log.info("WROTE %s  (%d bytes)", out_path, len(result.stdout))


_RETRYABLE_REASON = re.compile(r"rate|429|quota|network|connect|unreachable|timeout", re.IGNORECASE)


def _retry_wait(result: TaskResult, attempt: int, retry_delay: float, backoff: float) -> float:
    """Exponential backoff for a retry attempt; provider/network failures wait
    at least 30s so a rate-limit window can clear. F line: equal jitter
    breaks the thundering herd — parallel workers that failed together must
    not re-sync into the next retry window together (rate-limit failures
    spread within 30..60s, keeping the >=30s floor; other failures get
    +/-50% around the base)."""
    wait = retry_delay * (backoff ** (attempt - 1))
    if _RETRYABLE_REASON.search(result.reason or ""):
        wait = max(wait, 30.0)
        wait += random.uniform(0, min(wait, 30.0))
    else:
        wait *= random.uniform(0.5, 1.5)
    return wait


class ValidationResult(NamedTuple):
    """Outcome of one validator command execution. ok is True only when the
    command finished within the hard deadline and exited 0; exit_code is -1
    for timeout/crash (timed_out distinguishes the two)."""
    ok: bool
    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool


def run_validation(cmd: str, cwd: str, timeout: float = 600,
                   cap: Optional[int] = None) -> ValidationResult:
    """Run one validator command in its own process group with a hard
    deadline and bounded diagnostics. A spawn crash fails closed."""
    if not isinstance(cmd, str):
        raise TypeError("validator command must be a string")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout < 0:
        raise ValueError("validator timeout must be non-negative")
    output_cap = config.VALIDATION_OUTPUT_MAX_BYTES if cap is None else max(1, cap)
    return _run_bounded_process(cmd, cwd, timeout, output_cap, True, "validator")


def run_argv(cmd: list[str], cwd: str, timeout: float = 30,
             cap: Optional[int] = None) -> ValidationResult:
    """Run an argv command with the same bounded process-tree contract."""
    if (not isinstance(cmd, (list, tuple)) or not cmd
            or not all(isinstance(value, str) for value in cmd)):
        raise TypeError("argv command must be a non-empty string list")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout < 0:
        raise ValueError("process timeout must be non-negative")
    output_cap = config.COMMAND_OUTPUT_MAX_BYTES if cap is None else max(1, cap)
    return _run_bounded_process(list(cmd), cwd, timeout, output_cap, False, "process")


def _run_bounded_process(cmd, cwd: str, timeout: float, output_cap: int,
                         shell: bool, label: str) -> ValidationResult:
    try:
        proc = subprocess.Popen(cmd, shell=shell, cwd=cwd,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, start_new_session=True)
    except Exception as e:
        return ValidationResult(ok=False, exit_code=-1, stdout="", stderr=str(e), timed_out=False)
    start = time.monotonic()
    try:
        out, err, out_over, err_over = _drain_process(
            proc, "", start, timeout, output_cap)
    except subprocess.TimeoutExpired:
        _kill_group(proc)
        return ValidationResult(ok=False, exit_code=-1, stdout="", stderr="", timed_out=True)
    stdout, stderr = "".join(out), "".join(err)
    if out_over[0] or err_over[0]:
        reason = f"{label} output exceeds {output_cap} bytes"
        return ValidationResult(False, -1, stdout, stderr + reason, False)
    return ValidationResult(ok=proc.returncode == 0, exit_code=proc.returncode,
                            stdout=stdout, stderr=stderr, timed_out=False)


def _apply_gate_commands(tmp: Path, task: Task, commands: list, result: TaskResult) -> bool:
    """Run every gate command against the temp artifact (AND semantics);
    a failure deletes the artifact, records the outcome on the result
    (T2 feedback fields) and returns False. T12a: a validator may reply
    with a JSON status line -- {"status":"warn"} is allowed with a
    warning, {"status":"fail"} rejects regardless of exit code. H line:
    judge validators (spec.judge) must emit VERDICT: PASS|FAIL - <reason>
    (fail closed on missing/bare verdict). Repo-scoped validators are
    deferred to the stage/batch end (N2) and skipped here."""
    for spec in commands:
        if spec.scope == "repo":
            log.info("VALIDATE: %s deferred (repo scope -> stage end)", spec.cmd)
            continue
        cmd = spec.cmd.replace("{output}", shlex.quote(str(tmp))).replace("{cwd}", shlex.quote(task.workdir()))
        log.info("VALIDATE: %s", cmd)
        v = run_validation(cmd, task.workdir())
        result.validation_ok = v.ok
        result.validation_exit = v.exit_code
        result.validation_stderr = _cap_feedback(v.stderr or "")
        if not _gate_spec_passed(spec, cmd, tmp, result, v):
            return False
    result.validation_ok = True
    result.validation_exit = 0
    return True


def _gate_spec_passed(spec, cmd: str, tmp: Path, result: TaskResult, v) -> bool:
    """Decide whether one gate command passed: exit 0, optional JSON status
    (T12a) and judge VERDICT protocol (fail closed on missing/bare verdict).
    Records failure feedback on the result, deletes the temp artifact and
    returns False on rejection."""
    if not v.ok:
        tmp.unlink(missing_ok=True)
        log.warning("VALIDATION FAILED%s: %s; output NOT saved",
                    " (timeout)" if v.timed_out else f" (exit={v.exit_code})", cmd)
        for line in (v.stdout or "").strip().splitlines()[-10:]:
            log.warning("  | %s", line)
        for line in (v.stderr or "").strip().splitlines()[-10:]:
            log.warning("  | %s", line)
        return False
    if _json_status(v.stdout) == "warn":
        log.warning("VALIDATE WARN: %s (output saved with warning)", cmd)
        return True
    if _json_status(v.stdout) == "fail":
        tmp.unlink(missing_ok=True)
        log.warning("VALIDATION FAILED (JSON status=fail): %s; output NOT saved", cmd)
        return False
    if spec.judge:
        verdict = judge_verdict(v.stdout)
        if verdict in ("FAIL", "REJECT"):
            line = next((ln.strip() for ln in (v.stdout or "").splitlines()
                         if ln.strip().upper().startswith("VERDICT:")), "")
            result.validation_stderr = _cap_feedback(f"judge {verdict}: {line}")
            tmp.unlink(missing_ok=True)
            log.warning("VALIDATION FAILED (judge verdict=%s): %s; output NOT saved", verdict, cmd)
            return False
        if verdict is None:
            result.validation_stderr = _cap_feedback(
                "judge produced no VERDICT: PASS|FAIL - <reason> line")
            tmp.unlink(missing_ok=True)
            log.warning("VALIDATION FAILED (judge: no verdict): %s; output NOT saved", cmd)
            return False
        log.info("VALIDATE JUDGE PASS: %s", cmd)
    return True


def _save_validated(task: Task, result: TaskResult, validate_cmd: str) -> bool:
    """Save a successful result through the engineering gates. The output is
    written to a temp file, every resolved validator command must exit 0
    (AND semantics), then the file is atomically renamed into place; a
    failing gate deletes the temp file and leaves no artifact. {output}
    points at the temp file so gates can inspect the generated content.
    validate_cmd is a comma-separated list of registry names or raw shell
    commands (see _resolve_validators). Returns True when saved."""
    if not result.success:
        return False
    commands = _resolve_validator_specs(validate_cmd)
    if not commands:
        save_result(task, result)
        return True
    out_path = task.output_path()
    if out_path is None:
        # no file target: nothing to validate against, print as usual
        save_result(task, result)
        return True
    # T0.1 (round-4 finding H1): the symlink probe must guard the validator
    # path too — _save_validated is used whenever any validator is
    # configured (the recommended config), and without the probe a planted
    # symlink redirects tmp.rename() to its target (arbitrary file
    # overwrite with agent-controlled bytes).
    if _output_path_is_symlink(task):
        log.error("NOT SAVED %s: output path is a symlink (refusing to follow)", out_path)
        return False
    # Local fix (self-iteration round 2, F14): unique temp name (no fixed
    # .tmp suffix a local observer could pre-create as a symlink).
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=out_path.name + ".", suffix=".tmp", dir=str(out_path.parent))
    os.close(fd)
    tmp = Path(tmp_name)
    tmp.write_text(result.stdout, encoding="utf-8")
    # Local fix (self-iteration round 2, F3): shell-escape {output}/{cwd}
    # substitutions so paths with spaces/quotes cannot inject commands.
    if not _apply_gate_commands(tmp, task, commands, result):
        return False
    tmp.rename(out_path)
    log.info("WROTE %s (validated)", out_path)
    return True


def _json_status(stdout: str) -> str:
    """T12a: extract a validator's JSON status ({"status": "pass|warn|fail"});
    empty when the output carries no JSON status line."""
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            data = json.loads(line)
        except ValueError:
            continue
        status = str(data.get("status", "")).lower()
        if status in ("pass", "warn", "fail"):
            return status
    return ""


_JUDGE_VERDICT_RE = re.compile(r"^\s*\*{0,2}VERDICT\s*:\s*\*{0,2}(PASS|FAIL|REJECT)\b",
                               re.IGNORECASE | re.MULTILINE)


def judge_verdict(text: str) -> Optional[str]:
    """H line (LLM-as-judge protocol): extract a VERDICT: PASS|FAIL|REJECT
    line from a judge validator's stdout. A verdict must carry a reason on
    the same line (bare verdicts fail closed — T12b semantics); any FAIL/
    REJECT wins over an earlier PASS. None = no usable verdict."""
    first = None
    for m in _JUDGE_VERDICT_RE.finditer(text or ""):
        line_end = text.find("\n", m.end())
        if line_end == -1:
            line_end = len(text)
        rest = text[m.end():line_end].strip()
        if not rest:
            continue  # bare verdict without a reason: not a usable judgment
        verdict = m.group(1).upper()
        if verdict in ("FAIL", "REJECT"):
            return verdict
        if first is None:
            first = verdict
    return first


def _cap_feedback(text: str, max_chars: int = 4000, max_lines: int = 40) -> str:
    """Cap validator stderr for retry feedback (decision D4): 4000 chars /
    40 lines, keeping the head of the output (where CLI banners land)."""
    lines = text.splitlines()[:max_lines]
    capped = "\n".join(lines)[:max_chars]
    if len(text) > len(capped):
        capped += "\n... (truncated)"
    return capped


def _retry_task_with_feedback(task: Task, result: TaskResult) -> Task:
    """T2: build the retry task with the previous validator failure appended
    to the prompt so the agent can fix the gate problem directly (the
    feedback is capped per decision D4)."""
    if not result.validation_stderr:
        return task
    feedback = (
        f"\n\n[validator feedback from previous attempt]\n"
        f"exit={result.validation_exit}\n"
        f"{result.validation_stderr}"
    )
    return replace(task, prompt=task.prompt + feedback)


def revalidate_existing(path: Path, validate_cmd: Optional[str], workdir: str = "") -> bool:
    """Re-run the effective validators against an already-written artifact
    (the reuse path: a previously generated output is promoted only while it
    still passes every gate). Missing or empty artifacts fail closed; no
    validators apply -> trivially reusable. Returns True when the artifact
    passes all gates."""
    if not path.is_file():
        return False
    # T0.1 (full-SDLC pipeline): a symlink artifact must never be validated
    # through (the gate could execute against the victim) — fail closed.
    if path.is_symlink():
        log.warning("REVALIDATE: %s is a symlink; failing closed", path)
        return False
    try:
        empty = path.stat().st_size == 0
    except OSError:
        return False  # vanished between is_file and stat: fail closed
    if empty:
        return False
    commands = _resolve_validator_specs(validate_cmd)
    if not commands:
        return True
    wd = workdir or os.getcwd()
    for spec in commands:
        if spec.scope == "repo":
            # N2: full-tree gates run once at stage/batch end, not here
            continue
        if not _revalidate_one(spec, path, wd):
            return False
    log.info("REVALIDATED %s", path)
    return True


def _revalidate_one(spec, path: Path, wd: str) -> bool:
    """Re-run a single validator spec against an existing artifact. T12a:
    the JSON status protocol applies on the reuse path too — an exit-0
    validator reporting {"status":"fail"} must not promote the artifact
    when the fresh-save path would reject it (round-4 finding M3/P2-4).
    H line: judge validators apply the same fail-closed verdict protocol."""
    cmd = spec.cmd.replace("{output}", shlex.quote(str(path))).replace("{cwd}", shlex.quote(wd))
    log.info("REVALIDATE: %s", cmd)
    v = run_validation(cmd, wd)
    if not v.ok:
        log.warning("REVALIDATION FAILED%s: %s; %s NOT reusable",
                    " (timeout)" if v.timed_out else f" (exit={v.exit_code})", cmd, path)
        for line in (v.stdout or "").strip().splitlines()[-10:]:
            log.warning("  | %s", line)
        for line in (v.stderr or "").strip().splitlines()[-10:]:
            log.warning("  | %s", line)
        return False
    status = _json_status(v.stdout)
    if status == "fail":
        log.warning("REVALIDATION FAILED (JSON status=fail): %s; %s NOT reusable", cmd, path)
        return False
    if status == "warn":
        log.warning("REVALIDATE WARN: %s (artifact reusable with warning)", cmd)
    if spec.judge:
        verdict = judge_verdict(v.stdout)
        if verdict in ("FAIL", "REJECT"):
            log.warning("REVALIDATION FAILED (judge verdict=%s): %s; %s NOT reusable",
                        verdict, cmd, path)
            return False
        if verdict is None:
            log.warning("REVALIDATION FAILED (judge: no verdict): %s; %s NOT reusable",
                        cmd, path)
            return False
    return True


def _result_from_proc(task: Task, proc: subprocess.Popen, stdout_lines: list, stderr_lines: list, elapsed: float) -> TaskResult:
    """Assemble the TaskResult from a finished process: failure-signature
    rejection (quota/rate-limit/offline/timeout text) overrides exit code 0."""
    stdout_text = "".join(stdout_lines)
    stderr_text = "".join(stderr_lines)
    reason = agent_failure_reason(proc.returncode, stdout_text + "\n" + stderr_text)
    success = proc.returncode == 0 and not reason
    if reason:
        log.warning("agent output REJECTED: %s", reason)
    if success:
        log.info("OK  done  [%.1fs]  [output=%s]", elapsed, task.output or "(stdout)")
    else:
        log.warning("FAIL  [code=%d]  [%.1fs]", proc.returncode, elapsed)
    return TaskResult(
        task=task, success=success, stdout=stdout_text, stderr=stderr_text,
        elapsed=elapsed, returncode=proc.returncode, reason=reason or "",
    )


def _timeout_result(task: Task, proc: subprocess.Popen, start: float) -> TaskResult:
    """Kill the whole process group (the direct child may have spawned
    helpers that keep pipes open) and return a timed-out result."""
    _kill_group(proc)
    elapsed = time.monotonic() - start
    log.error("TIMEOUT  [%.1fs]  [limit=%ss]", elapsed, task.timeout)
    return TaskResult(
        task=task, success=False, stderr=f"Task timed out after {task.timeout}s",
        elapsed=elapsed, returncode=-1, reason="task timed out",
    )


def _stream_output_enabled() -> bool:
    """Whether agent response bodies should be displayed while running."""
    mode = str(config.STREAM_OUTPUT).lower()
    return mode == "full" or (mode == "auto" and sys.stdout.isatty())


def _stream_proc(proc: subprocess.Popen, prefix: str, start: float,
                 timeout: int, emit: bool = False) -> tuple[list[str], list[str], list]:
    """Drain stdout/stderr concurrently via daemon threads, then wait on a
    hard deadline. Thread joins only drain pipes and must not extend the
    window, so each join/wait gets the remaining budget (they return early
    when the agent exits). Raises TimeoutExpired when the deadline hits.
    Returns (stdout_lines, stderr_lines, overflow): overflow[0] is True
    when stdout collection exceeded the T12e cap (the result must be
    rejected; collection stops but the pipe is still drained)."""
    stdout_lines, stderr_lines, overflow, _ = _drain_process(
        proc, prefix, start, timeout, config.OUTPUT_MAX_BYTES, emit)
    return stdout_lines, stderr_lines, overflow


def _drain_process(proc: subprocess.Popen, prefix: str, start: float,
                   timeout: float, cap: int, emit: bool = False) -> tuple:
    """Drain both pipes with one absolute deadline and bounded collectors."""
    from threading import Thread
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    stdout_overflow: list = [False]
    stderr_overflow: list = [False]
    args = (cap, stdout_overflow, emit, sys.stdout)
    tout = Thread(target=_read_stream, args=(proc.stdout, prefix, stdout_lines, *args), daemon=True)
    err_args = (cap, stderr_overflow, emit, sys.stderr)
    terr = Thread(target=_read_stream, args=(proc.stderr, prefix, stderr_lines, *err_args), daemon=True)
    tout.start()
    terr.start()
    deadline = start + timeout
    tout.join(timeout=max(0, deadline - time.monotonic()))
    terr.join(timeout=max(0, deadline - time.monotonic()))
    proc.wait(timeout=max(0.1, deadline - time.monotonic()))
    return stdout_lines, stderr_lines, stdout_overflow, stderr_overflow


def _rotation_flags(session_id: str, session_name: str, workdir: str = "") -> list:
    """T10: continue flags, or fork flags past the session size watermark
    (compaction entries are warned about). Session lookups use the TASK's
    workdir (sessions live under the task's cwd, not the process cwd —
    round-4 finding P3/L6)."""
    flags = _session_flags("continue", session_id, session_name)
    cwd = workdir or os.getcwd()
    if has_compaction(cwd, session_id):
        log.warning("SESSION: compaction entry in %s (context was shrunk by pi)", session_id)
    sfile = session_file_path(cwd, session_id)
    if sfile and session_size_bytes(cwd, session_id) > config.SESSION_MAX_BYTES:
        log.warning("SESSION ROTATION: %s exceeds %d bytes; forking",
                    sfile, config.SESSION_MAX_BYTES)
        return fork_flags(str(sfile))
    return flags


def _run_with_gate(task: Task, validate_cmd: str, index: int, total: int,
                    session_flags: Optional[list] = None, parallel: bool = False,
                    session_name: str = "") -> TaskResult:
    """Run one task and push its result through the engineering gate;
    a gate rejection marks the task failed with the exit signature."""
    result = run_task(task, task_index=index, total=total, parallel=parallel, session_flags=session_flags,
                      session_name=session_name)
    if result.success:
        result.success = _save_validated(task, result, task.validate if task.validate is not None else validate_cmd)
        if not result.success:
            result.reason = (f"validation failed (exit={result.validation_exit})"
                             if result.validation_exit else "validation failed")
    record_task(result)
    return result


def run_serial(tasks: list[Task], retries: int = 0, retry_delay: float = 10.0, backoff: float = 2.0, min_interval: float = 0.0,
               session_mode: str = "new", session_id: str = "", session_name: str = "", validate_cmd: str = "") -> list[TaskResult]:
    """Execute tasks one by one with real-time output streaming. Failed tasks
    are retried with exponential backoff up to `retries` extra attempts, and
    successful tasks are throttled by `min_interval` seconds so long-running
    24x7 batches do not hammer the provider.

    validate_cmd runs against each agent result before the output file is
    committed (engineering gate); a non-zero exit marks the task failed and
    leaves no artifact, so the retry/round machinery regenerates it.

    With session_mode "shared", every task in this list continues the same
    agent session (the first call starts it with the configured start flags,
    later calls pass the continue flags); "per-stage" behaves the same here
    and is differentiated by the caller via session_id."""
    results = []
    total = len(tasks)
    session_active = False
    for i, task in enumerate(tasks, 1):
        log.info("-- [%d/%d] --", i, total)
        flags = None
        if session_mode != "new":
            if not session_active:
                flags = _session_flags("start", session_id, session_name)
                session_active = True
            else:
                flags = _rotation_flags(session_id, session_name, task.workdir())
        result = _run_with_gate(task, validate_cmd, i, total, flags, session_name=session_name)
        attempt = 0
        retry_task = task
        while not result.success and attempt < retries:
            attempt += 1
            wait = _retry_wait(result, attempt, retry_delay, backoff)
            log.warning("RETRY %d/%d for task [%d/%d] in %.0fs (reason: %s)",
                        attempt, retries, i, total, wait, result.reason or f"exit {result.returncode}")
            time.sleep(wait)
            # T2: a validator rejection feeds its capped stderr back into the
            # retry prompt so the agent can fix the gate problem directly.
            if result.validation_stderr:
                retry_task = _retry_task_with_feedback(task, result)
            # retries stay inside the same session
            retry_flags = _session_flags("continue", session_id, session_name) if session_mode != "new" else None
            result = _run_with_gate(retry_task, validate_cmd, i, total, retry_flags, session_name=session_name)
        results.append(result)
        if result.success and min_interval > 0:
            time.sleep(min_interval)
    return results


def run_parallel(tasks: list[Task], workers: int = 0, validate_cmd: str = "") -> list[TaskResult]:
    workers = workers or config.AGENT_DEFAULT_WORKERS
    """Execute tasks concurrently with a thread pool and real-time output.
    Each result passes the engineering validation gate before its output file
    is committed; a non-zero validation exit leaves no artifact.

    T8: the budget cap is enforced BEFORE submitting (pre-check) and in each
    worker (atomic consume); when the cap trips, in-flight agent groups are
    killed so the runner exits 3 promptly instead of waiting up to the task
    timeout x workers (round-4 finding P1-1/M2/DS-4)."""
    total = len(tasks)
    log.info("PARALLEL x%d  (%d tasks)", workers, total)
    capped = False

    def _run_one(task: Task, index: int) -> Optional[TaskResult]:
        try:
            return _run_with_gate(task, validate_cmd, index, total, parallel=True)
        except SystemExit as e:
            # T8: the budget cap raises SystemExit(3) inside a worker thread;
            # the executor captures it into the future instead of exiting, so
            # translate it into a None marker the main loop understands.
            if e.code == 3:
                return None
            raise

    results: list[TaskResult] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        fut_map = {}
        for i, t in enumerate(tasks, 1):
            if not budget_allows():
                capped = True
                log.error("BUDGET: invocation limit reached; stopping (exit 3)")
                break
            # Pass task_index so parallel output lines are prefixed
            fut_map[pool.submit(_run_one, t, i)] = t
        for fut in as_completed(fut_map):
            result = fut.result()
            if result is None:
                capped = True
                log.error("BUDGET: invocation limit reached; stopping (exit 3)")
            else:
                results.append(result)
                log.info("PROGRESS: %d/%d done", len(results), total)
            if capped:
                # stop in-flight agents NOW so executor shutdown is prompt
                kill_active_procs()

    if capped:
        raise SystemExit(3)
    return results


def print_summary(results: list[TaskResult]) -> None:
    """Print an execution summary table."""
    total = len(results)
    succeeded = sum(1 for r in results if r.success)
    failed = total - succeeded
    total_elapsed = sum(r.elapsed for r in results)
    wall_time = max(r.elapsed for r in results) if results else 0

    print()
    print("=" * 56)
    print("  pi-batch execution report")
    print("=" * 56)
    print("  total:     %d" % total)
    print("  succeeded: %d" % succeeded)
    print("  failed:    %d" % failed)
    print("  CPU time:  %.1fs" % total_elapsed)
    print("  wall time: %.1fs" % wall_time)
    print()
    for r in results:
        icon = "PASS" if r.success else "FAIL"
        brief = r.task.prompt[:60].replace("\n", " ")
        # T2: surface the validator failure signature in the summary line.
        sig = f"  [val exit={r.validation_exit}]" if (not r.success and r.validation_exit) else ""
        print("  %s  [%6.1fs]%s %s..." % (icon, r.elapsed, sig, brief))
    print("=" * 56)
