"""Repo-scoped validator execution (N2).

Full-tree gates (go build ./..., quality.py, uicheck) must not run per
artifact mid-stage — concurrent role agents are still writing files and
would fail the gate spuriously (misreport -> retry -> burned quota).
Deferred here, they run once at stage end and observe the finished tree.
"""

from __future__ import annotations

import os
import shlex
from pathlib import Path

from .config import _resolve_validator_specs, log
from .runner import judge_verdict, run_validation


def run_stage_repo_validators(stage, results: list, outputs: list) -> tuple[bool, str]:
    """N2 (round-5 finding): run repo-scoped validators once at stage end.
    Specs are collected from the stage's validate_cmd plus every task's
    per-task validate field (deduplicated); a failure fails the stage
    (gate semantics). Returns (ok, failing-command-or-empty)."""
    specs = _collect_repo_specs(
        [stage.validate_cmd] + [r.task.validate for r in results if r.task.validate])
    if not specs:
        return True, ""
    wd = stage.cwd or os.getcwd()
    output_dir = str(Path(outputs[0]).parent) if outputs else wd
    for spec in specs:
        cmd = spec.cmd.replace("{output}", shlex.quote(output_dir))
        cmd = cmd.replace("{cwd}", shlex.quote(wd))
        ok, detail = _run_stage_repo_spec(spec, cmd, wd, stage.name)
        if not ok:
            return False, detail
    log.info("REPO VALIDATE PASS (stage '%s'): %d gate(s)", stage.name, len(specs))
    return True, ""


def _collect_repo_specs(raw_specs: list) -> list:
    """Collect and deduplicate repo-scoped validator specs from raw strings."""
    specs: list = []
    seen = set()
    for raw in raw_specs:
        for spec in _resolve_validator_specs(raw or ""):
            if spec.scope == "repo" and spec.cmd not in seen:
                seen.add(spec.cmd)
                specs.append(spec)
    return specs


def _run_stage_repo_spec(spec, cmd: str, wd: str, stage_name: str) -> tuple[bool, str]:
    """Run one repo-scoped validator; (True, "") on success, (False, cmd)
    on failure so the caller can fail the stage with the gate command as
    the detail."""
    log.info("REPO VALIDATE (stage '%s'): %s", stage_name, cmd)
    v = run_validation(cmd, wd)
    if spec.judge and v.ok:
        verdict = judge_verdict(v.stdout)
        if verdict in ("FAIL", "REJECT"):
            log.error("REPO VALIDATE FAILED (judge verdict=%s): %s", verdict, cmd)
            return False, cmd
        if verdict is None:
            log.error("REPO VALIDATE FAILED (judge: no verdict): %s", cmd)
            return False, cmd
    elif not v.ok:
        log.error("REPO VALIDATE FAILED%s: %s",
                  " (timeout)" if v.timed_out else f" (exit={v.exit_code})", cmd)
        for line in (v.stderr or "").strip().splitlines()[-10:]:
            log.error("  | %s", line)
        return False, cmd
    return True, ""
