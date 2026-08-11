"""from_dir 阶段任务构建（每文件一任务 / aggregate 合并单任务）。"""
from pathlib import Path
from typing import Optional

from . import config
from .models import Task, Stage
from .pipeline import _expected_fp, reuse_decision
from .text_io import read_text_bounded
from .runner import log


def _stage_from_dir_aggregate(
    stage: Stage, files: list, reuse: bool, model_override: str,
    timeout_override: int, validate_cmd: str, reuse_legacy: bool,
    fingerprint_mode: bool,
) -> tuple[list[Task], list[str]]:
    """P1（使用反馈）：from_dir + aggregate 合并为单任务（有界读取 + 总
    预算 INPUT_MAX_BYTES；超限截断保留完整路径供按需读取）。"""
    dir_path = Path(stage.from_dir)
    parts: list[str] = []
    total = 0
    truncated = 0
    for fpath in files:
        text = read_text_bounded(fpath, config.INPUT_MAX_BYTES, "stage input")
        if total + len(text) > config.INPUT_MAX_BYTES:
            truncated += 1
            continue
        total += len(text)
        parts.append(f"===== {fpath.name} =====\n{text}")
    if not parts:
        log.error("Stage '%s' aggregate input is empty from %s", stage.name, stage.from_dir)
        return [], []
    prompt = "\n\n".join(parts)
    if truncated:
        prompt += f"\n\n[截断提示] {truncated} 个文件超出合并预算，可按需读取其完整路径。"
    out_path = (dir_path / ("aggregate" + stage.output_suffix)).resolve()
    task = Task(prompt=prompt, output=str(out_path), cwd=str(dir_path.resolve()))
    task.model = stage.model or task.model
    task.provider = stage.provider or task.provider
    if model_override:
        task.model = model_override
    if timeout_override:
        task.timeout = timeout_override
    fp = _expected_fp(prompt, str(out_path), validate_cmd, task.model, task.provider, fingerprint_mode)
    if reuse and reuse_decision(str(out_path), validate_cmd, str(dir_path.resolve()), reuse_legacy, fp):
        log.info("REUSE: %s (reused+validated)", out_path.name)
        return [], [str(out_path)]
    log.info("Loaded %d files -> 1 aggregate task from %s", len(files), stage.from_dir)
    return [task], []




def _describe_stage(stage: Stage, reuse: bool) -> None:
    """Dry-run: print what the stage would do without executing it."""
    log.info("")
    log.info("STAGE: %s (dry-run)", stage.name)
    if stage.from_dir:
        log.info("  Will read .md files from: %s", stage.from_dir)
        try:
            files = [
                f for f in Path(stage.from_dir).glob(f"*{stage.suffix}")
                if not f.name.endswith(stage.output_suffix)
            ]
            mode = "merged into 1 aggregate task" if stage.aggregate else "one task per file"
            log.info("  Files: %d (%s)", len(files), mode)
        except OSError:
            log.info("  Files: (directory not readable)")
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

