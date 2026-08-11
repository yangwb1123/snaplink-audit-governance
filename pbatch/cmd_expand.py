"""Validator command placeholder expansion — single entry point.

{ui_scan_dir}/{output}/{cwd}/{tool_root} must expand identically at every
site that executes validator commands: pbatch/runner.py (file-scoped gates,
fresh-save + reuse), pbatch/repo_gates.py (stage-end repo gates) and
pbatch/cli_batch.py (batch-end repo gates). A site that hand-rolls its own
replace chain can leak a literal {ui_scan_dir} into the shell — the script
then scans a nonexistent directory, passes vacuously and the false-green
resurrects (see docs/auto/designs/ui-specs-672fd14a-design.md F1).

Lives here, not in config.py/runner.py: both host files sit at their
filesize gate limits (config 487/500, runner 999/1000) after earlier
directions' uncommitted growth; this module has no budget pressure.
"""

from __future__ import annotations

import os
import shlex
from typing import Optional

from . import config


def ui_scan_dir() -> Optional[str]:
    """项目前端扫描根；未配置/空串/纯空白 → None（展开为 {cwd}/src 默认）。
    经 config._BATCH_CFG 属性查找读取（调用时求值，测试可 monkeypatch）。"""
    v = config._BATCH_CFG.get("ui_scan_dir")
    return v.strip() if isinstance(v, str) and v.strip() else None


def expand_cmd(cmd: str, output, cwd: str, tool_root: str = config.TOOL_ROOT) -> str:
    """验证器命令占位符展开（唯一入口，runner/repo_gates/cli_batch 共用）。

    - {ui_scan_dir}: None → "{cwd}/src"（默认，向后兼容）；相对值 →
      "{cwd}/<值>"（shlex.quote）；绝对路径 → 原样 quote。
    - {output}:      output 为 None 时保留字面量不动（cli_batch 现状语义）。
    - {cwd}/{tool_root}: 与现状一致。
    展开顺序：{ui_scan_dir} 先于 {cwd}（默认展开串自身含 {cwd} 占位符）。
    """
    usd = ui_scan_dir()
    if "{ui_scan_dir}" in cmd:
        if usd is None:
            repl = "{cwd}/src"
        elif os.path.isabs(usd):
            repl = shlex.quote(usd)
        else:
            repl = "{cwd}/" + shlex.quote(usd)
        cmd = cmd.replace("{ui_scan_dir}", repl)
    out = cmd
    if output is not None:
        out = out.replace("{output}", shlex.quote(str(output)))
    out = out.replace("{cwd}", shlex.quote(cwd)).replace("{tool_root}", shlex.quote(tool_root))
    if "{ui_scan_dir}" in out:  # 防御：任何遗留占位符都不该到达 shell
        config.log.error("expand_cmd: unresolved {ui_scan_dir} in %r", cmd)
    return out
