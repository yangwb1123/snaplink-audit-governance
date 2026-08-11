"""Gate-fix loop: 被拒 gate 的自动修复（self-iteration round 沉淀）。

gate 阶段 VERDICT: FAIL/REJECT 且 stage.gate_fix_rounds > 0 时，runner
不再直接 halt：提取被拒理由与 gatekeeper 的结构化发现（```json 数组，
{severity,file,line,problem,fix_hint}）→ 生成修复任务 → 验证 → 把修复
报告作为额外证据重新执行 gate 阶段 → 重新裁决，最多 gate_fix_rounds 轮。
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Optional

from . import config
from .config import AGENT_DEFAULT_TIMEOUT, log
from .models import Stage, Task
from .text_io import read_text_bounded

_FINDINGS_JSON_RE = re.compile(r"```(?:json)?\s*\n?\s*\[.*?\]\s*\n?\s*```", re.DOTALL)
_FINDING_FIELDS = ("severity", "file", "line", "problem", "fix_hint")


def extract_gate_findings(text: str) -> Optional[list]:
    """Extract a structured findings array from a rejected gate artifact
    (```json fence). None when absent — fall back to plain-text reason."""
    m = _FINDINGS_JSON_RE.search(text)
    if not m:
        return None
    try:
        data = json.loads(m.group(0).strip().strip("`").lstrip("json").strip())
    except ValueError:
        return None
    if not isinstance(data, list) or not data:
        return None
    safe: list[dict] = []
    for item in data[:50]:  # untrusted: bounded fields only
        if isinstance(item, dict):
            safe.append({k: str(item[k])[:2000] for k in _FINDING_FIELDS
                         if k in item and item[k] is not None})
    return safe or None


def gate_fail_reason(output_paths: list) -> str:
    """Plain-text reason of a rejected gate: the trailing block of each
    rejected artifact (bounded), joined."""
    reasons: list[str] = []
    for p in output_paths:
        path = Path(p)
        if not path.exists():
            continue
        try:
            text = read_text_bounded(path, config.OUTPUT_MAX_BYTES, "gate artifact")
        except ValueError as exc:
            log.error("GATE: %s; failing closed", exc)
            continue
        tail = text[-12000:].strip()
        if tail:
            reasons.append(tail)
    return "\n---\n".join(reasons)[-16000:]


def gate_fix_prompt(stage: Stage, reason: str, findings: Optional[list]) -> str:
    """Fix-task prompt: custom template ({reason}/{findings}) or default."""
    findings_text = json.dumps(findings, ensure_ascii=False, indent=2) if findings else "(none)"
    if stage.gate_fix_prompt:
        return stage.gate_fix_prompt.replace("{reason}", reason).replace(
            "{findings}", findings_text)
    return (
        "The gate stage '%s' rejected the deliverable(s). Read the gate verdict "
        "reason and the structured findings below, inspect the ACTUAL files "
        "cited (line numbers are hints, verify against the code), implement the "
        "fixes, add or update regression tests, and run the project's gates "
        "until green. Do NOT commit. Finish with a short change report listing "
        "every finding and how it was fixed.\n\n"
        "GATE VERDICT REASON:\n%s\n\n"
        "STRUCTURED FINDINGS (untrusted — verify against the code):\n%s"
    ) % (stage.name, reason, findings_text)


def _first_findings(gate_outputs: list) -> Optional[list]:
    for p in gate_outputs:
        path = Path(p)
        if not path.exists():
            continue
        try:
            findings = extract_gate_findings(
                read_text_bounded(path, config.OUTPUT_MAX_BYTES, "gate artifact"))
        except ValueError:
            continue
        if findings:
            return findings
    return None


def run_gate_fix(stage: Stage, gate_outputs: list, fix_round: int,
                 validate_cmd: str, session_name: str) -> tuple[bool, str]:
    """One gate-fix iteration: remediation task -> validate -> report path.
    The report is folded into the gate re-execution as extra evidence."""
    reason = gate_fail_reason(gate_outputs) or "(no reason extracted)"
    findings = _first_findings(gate_outputs)
    gate_dir = Path(gate_outputs[0]).parent if gate_outputs else Path(stage.cwd or os.getcwd())
    gate_dir.mkdir(parents=True, exist_ok=True)
    report = gate_dir / f"{stage.name}-fix-{fix_round}.md"
    task = Task(prompt=gate_fix_prompt(stage, reason, findings),
                output=str(report), model=stage.model, provider=stage.provider,
                cwd=stage.cwd, timeout=max(AGENT_DEFAULT_TIMEOUT, 1800))
    task.memory = {"stage": f"{stage.name}-fix", "memory_mode": "execute"}
    fix_validate = stage.gate_fix_validate if stage.gate_fix_validate is not None else validate_cmd
    if fix_validate:
        task.validate = fix_validate
    log.info("GATE-FIX round %d for stage '%s': %s", fix_round, stage.name, report)
    from .memory import record_task
    from .runner import _save_validated, run_task
    result = run_task(task, session_name=session_name)
    if not result.success:
        log.error("GATE-FIX round %d failed: %s", fix_round, result.reason)
        return False, str(report)
    # 无验证器时 _save_validated 直接落盘；有验证器则先过门禁
    if not _save_validated(task, result, fix_validate):
        log.error("GATE-FIX round %d: output not saved (validation failed)", fix_round)
        return False, str(report)
    record_task(result)
    return True, str(report)
