"""Task source loading: YAML task files, JSON files, plain-text prompts,
and per-file directory inputs (fail closed on malformed sources)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from . import config
from .config import AGENT_DEFAULT_TIMEOUT, log, yaml
from .models import Task
from .text_io import read_text_bounded


def load_tasks(source: str) -> list[Task]:
    """Load tasks from a YAML file, JSON file, or plain text prompt.

    Supports @file.md references in the prompt field.
    """
    path = Path(source)
    if not path.exists():
        return [Task(prompt=source)]

    base_dir = str(path.parent) if path.parent else "."
    try:
        raw = read_text_bounded(path, config.INPUT_MAX_BYTES, "task source")
    except ValueError as exc:
        log.error("Task source rejected: %s", exc)
        sys.exit(1)

    # YAML
    if source.endswith((".yaml", ".yml")) or raw.lstrip().startswith("tasks:"):
        if not yaml:
            # Local fix (self-iteration round 1): a structured task file must
            # not silently degrade into one giant raw-text prompt; fail loud
            # and consistent with load_pipeline's PyYAML gate.
            log.error("Task file '%s' requires PyYAML. Run: pip install pyyaml", source)
            sys.exit(1)
        try:
            data = yaml.safe_load(raw)
        except Exception as e:
            log.error("Task file '%s' is not valid YAML: %s", source, e)
            sys.exit(1)
        return _tasks_from_data(data, base_dir, source)

    # JSON
    try:
        data = json.loads(raw)
        if source.endswith(".json") or isinstance(data, (dict, list)):
            return _tasks_from_data(data, base_dir, source)
    except json.JSONDecodeError as exc:
        if source.endswith(".json"):
            log.error("Task file '%s' is not valid JSON: %s", source, exc)
            sys.exit(1)

    # Plain text prompt
    return [Task(prompt=raw.strip())]


def _tasks_from_data(data, base_dir: str, source: str) -> list[Task]:
    tasks_data = data.get("tasks") if isinstance(data, dict) else data
    if not isinstance(tasks_data, list) or not tasks_data:
        log.error("Task file '%s' must contain a non-empty tasks list", source)
        sys.exit(1)
    tasks = []
    for index, value in enumerate(tasks_data, 1):
        error = _task_data_error(value)
        if error:
            log.error("Task file '%s': tasks[%d]%s", source, index, error)
            sys.exit(1)
        task = Task(**{key: item for key, item in value.items()
                       if key in Task.__dataclass_fields__})
        try:
            task.prompt = task.resolve_prompt(base_dir)
        except ValueError as exc:
            log.error("Task file '%s': referenced prompt rejected: %s", source, exc)
            sys.exit(1)
        tasks.append(task)
    return tasks


def _task_data_error(value) -> str:
    if not isinstance(value, dict):
        return " must be a mapping"
    prompt = value.get("prompt")
    timeout = value.get("timeout", AGENT_DEFAULT_TIMEOUT)
    if not isinstance(prompt, str) or not prompt.strip():
        return ".prompt must be a non-empty string"
    if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout < 1:
        return ".timeout must be an integer >= 1"
    if not isinstance(value.get("env", {}), dict):
        return ".env must be a mapping"
    if value.get("validate") is not None and not isinstance(value.get("validate"), str):
        return ".validate must be a string or null"
    if "memory" in value and not isinstance(value["memory"], dict):
        return ".memory must be a mapping"
    return _task_string_error(value)


def _task_string_error(value: dict) -> str:
    fields = ("output", "model", "provider", "thinking", "tools", "exclude_tools", "cwd")
    nullable = ("model", "provider", "thinking", "tools", "exclude_tools")
    invalid = next((key for key in fields if key in value
                    and not isinstance(value[key], str)
                    and not (key in nullable and value[key] is None)), "")
    return f".{invalid} must be a string" if invalid else ""


def load_tasks_from_dir(directory: str, suffix: str = ".md") -> list[Task]:
    """Create one task per file in a directory.

    Each file's content becomes the prompt, and the output is saved as
    <filename>.out.md in the same directory (or specified output dir).
    """
    tasks = []
    basedir = Path(directory)
    if not basedir.is_dir():
        log.error("not a directory: %s", directory)
        return tasks

    for fpath in sorted(basedir.glob(f"*{suffix}")):
        if fpath.name.endswith(".out.md"):
            continue
        try:
            prompt = read_text_bounded(fpath, config.INPUT_MAX_BYTES,
                                       "directory task input")
        except ValueError as exc:
            log.error("Task source rejected: %s", exc)
            raise
        out_name = fpath.stem + ".out.md"
        # Local fix (self-iteration round 2, F1): absolute output path —
        # cwd is the input dir, so relative outputs would double-prefix.
        tasks.append(Task(
            prompt=prompt,
            output=str((basedir / out_name).resolve()),
            cwd=str(basedir),
        ))
        log.info("loaded task from %s -> %s", fpath.name, out_name)

    return tasks
