"""Data model for pi-batch: Task, TaskResult, Stage, Pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import re
import os
from pathlib import Path
from typing import Optional

from . import config
from .config import (AGENT_DEFAULT_MODEL, AGENT_DEFAULT_TIMEOUT,
                     AGENT_DEFAULT_WORKERS, COMMAND_OUTPUT_MAX_BYTES,
                     COMMAND_TIMEOUT, log)
from .text_io import read_text_bounded

@dataclass
class Stage:
    """One stage in a pipeline."""
    name: str
    model: str = ""      # T11: model for the stage's orchestrator/role tasks
    provider: str = ""   # T11: provider override for the stage
    from_dir: str = ""
    from_outputs: object = ""  # previous stage name, or a list for a DAG evidence join
    suffix: str = ".md"
    output_suffix: str = ".out.md"
    mode: str = "serial"
    workers: int = AGENT_DEFAULT_WORKERS  # config value captured at import; overrides go through config
    aggregate: bool = False  # from_outputs: merge all upstream outputs into one prompt per template
    validate_cmd: Optional[str] = None  # None = inherit CLI --validate-cmd, "" = disabled, else command
    meta: bool = False  # dynamic role orchestration: agent picks review roles per iteration
    meta_prompt: str = ""  # custom orchestrator prompt ({roles}/{input_content} placeholders)
    role_dir: str = ""  # directory of role templates the orchestrator may choose from
    role_keywords: str = ""  # optional role -> keyword YAML for relevance scoring
    output_dir: str = ""  # where role deliverables are written (required for meta stages)
    max_iterations: int = 3  # orchestrator -> roles -> fold -> re-ask loop limit
    max_roles_per_iteration: int = 3  # runtime-enforced meta fan-out cap
    meta_timeout: int = 0  # dedicated per-role/ orchestrator timeout for meta
                           # stages (long review docs exceed the 900s default;
                           # 0 = inherit CLI/global timeout)
    relevance_enabled: bool = True  # inject keyword-based role suggestions
    relevance_min_score: int = 0  # 0 keeps backward-compatible advisory-only scoring
    meta_max_failed_roles: int = 0  # meta 角色允许失败数（0 = 任一失败即阶段失败）
    meta_role_retries: int = 1  # 角色任务失败/超时自动重试次数
    gate: bool = False  # verdict gate: output must contain VERDICT: PASS/FAIL/REJECT; FAIL/REJECT blocks later stages
    gate_fix_rounds: int = 0  # gate FAIL 后自动修复轮数（0 = 关闭，保持 fail-closed halt）
    gate_fix_prompt: str = ""  # 修复指令模板（{reason}/{findings} 占位符；空 = 默认指令）
    gate_fix_validate: Optional[str] = None  # 修复任务验证器（None = 继承 gate 阶段 validate_cmd）
    approval: bool = False  # T12c: human approval point after the stage (D5)
    or_tasks: bool = False  # P7: 任务模板是 OR 候选——按序执行，第一个通过
                           # （含验证器）者胜出，其余跳过；全部失败则阶段失败
    from_prompt: str = ""  # one-sentence starting prompt instead of from_dir files
    output: str = ""  # output file for the from_prompt task (required with from_prompt)
    tasks: list = field(default_factory=list)
    commands: list = field(default_factory=list)
    commands_parallel: bool = False  # if True, run commands concurrently
    command_timeout: int = COMMAND_TIMEOUT
    command_output_max_bytes: int = COMMAND_OUTPUT_MAX_BYTES
    cwd: str = ""
    git_commit: bool = False
    commit_message: str = ""
    
    def to_dict(self):
        return {
            "name": self.name,
            "from_dir": self.from_dir,
            "from_outputs": self.from_outputs,
            "suffix": self.suffix,
            "output_suffix": self.output_suffix,
            "mode": self.mode,
            "workers": self.workers,
            "aggregate": self.aggregate,
            "validate_cmd": self.validate_cmd,
            "meta": self.meta,
            "meta_prompt": self.meta_prompt,
            "role_dir": self.role_dir,
            "role_keywords": self.role_keywords,
            "output_dir": self.output_dir,
            "max_iterations": self.max_iterations,
            "max_roles_per_iteration": self.max_roles_per_iteration,
            "relevance_enabled": self.relevance_enabled,
            "relevance_min_score": self.relevance_min_score,
            "from_prompt": self.from_prompt,
            "output": self.output,
            "tasks": self.tasks,
            "commands": self.commands,
            "commands_parallel": self.commands_parallel,
            "command_timeout": self.command_timeout,
            "command_output_max_bytes": self.command_output_max_bytes,
            "cwd": self.cwd,
            "git_commit": self.git_commit,
            "commit_message": self.commit_message,
        }


@dataclass
class Pipeline:
    """Multi-stage pipeline definition."""
    stages: list[Stage] = field(default_factory=list)
    decision_log: str = ""  # append structured decisions per finished stage
    archive_dir: str = ""  # move completed deliverables here after a successful run
    name: str = "pipeline"  # label for archive subdirectories
    
    def to_dict(self):
        return {"stages": [s.to_dict() for s in self.stages]}


@dataclass
class Task:
    """A single pi invocation task."""

    prompt: str
    output: str = ""
    model: str = AGENT_DEFAULT_MODEL
    provider: str = ""
    thinking: str = ""
    tools: str = ""
    exclude_tools: str = ""
    cwd: str = ""
    timeout: int = AGENT_DEFAULT_TIMEOUT
    env: dict = field(default_factory=dict)
    validate: Optional[str] = None  # per-task engineering gate; None = inherit, "" = disabled
    memory: dict = field(default_factory=dict)  # stage/role/task metadata for the memory index

    def to_cmd(self, session_flags: Optional[list] = None) -> list[str]:
        cmd = [config.AGENT_BIN, "-p", self.prompt]
        if self.model:
            cmd.extend(["--model", self.model])
        if self.provider:
            cmd.extend(["--provider", self.provider])
        if self.thinking:
            cmd.extend(["--thinking", self.thinking])
        if self.tools:
            cmd.extend(["--tools", self.tools])
        if self.exclude_tools:
            cmd.extend(["--exclude-tools", self.exclude_tools])
        if session_flags:
            cmd.extend(session_flags)
        return cmd

    def workdir(self) -> str:
        return self.cwd or os.getcwd()

    def output_path(self) -> Optional[Path]:
        """Resolve the output path. Relative outputs resolve against the
        task's cwd (where the agent actually runs), not the process cwd.

        Local fix (self-iteration round 1): previously resolved against the
        process cwd, silently writing deliverables to the wrong directory
        whenever task.cwd was set.
        """
        if not self.output:
            return None
        p = Path(self.output)
        if not p.is_absolute():
            p = Path(self.cwd or os.getcwd()) / p
        return p.resolve()

    def resolve_prompt(self, base_dir: str = "") -> str:
        """Resolve @file references in the prompt to file contents.

        Supports:
          @file.md              -> loads file.md content
          @docs/analysis.md     -> loads docs/analysis.md
          prefix text @file.md  -> prepends file content before prefix text

        Returns the resolved prompt string.
        """
        def replace(match: re.Match) -> str:
            fpath = Path(match.group(1))
            if not fpath.is_absolute():
                fpath = Path(base_dir) / fpath
            try:
                if fpath.exists():
                    return read_text_bounded(fpath, config.INPUT_MAX_BYTES,
                                             "prompt reference")
            except ValueError as exc:
                log.warning("prompt reference rejected: %s", exc)
            log.warning("referenced file not found: %s", fpath)
            return match.group(0)

        # Only path-like references with a known extension are resolved so
        # that event versions like vault.file.deleted@1.1 stay literal.
        return re.sub(r"@((?:[\w./-]+/)*[\w.-]+\.(?:md|txt|ya?ml|json))",
                      replace, self.prompt)


@dataclass
class TaskResult:
    task: Task
    success: bool
    stdout: str = ""
    stderr: str = ""
    elapsed: float = 0.0
    returncode: int = -1
    reason: str = ""
    # T2 (full-SDLC pipeline): additive validator-outcome fields, populated
    # when the engineering gate ran against the artifact. Empty/None defaults
    # keep every existing serializer and consumer compatible.
    validation_ok: Optional[bool] = None
    validation_exit: int = 0
    validation_stderr: str = ""
    session_id: str = ""
    # True when stdout/stderr were already shown live. This prevents the
    # stdout-only shortcut from printing a successful answer a second time.
    streamed_output: bool = False
    session_name: str = ""
    raw_session: str = ""
