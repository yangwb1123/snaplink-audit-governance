"""Fail-closed structural validation for declarative pipeline YAML."""

from __future__ import annotations


_STRING_FIELDS = ("model", "provider", "from_dir", "suffix", "output_suffix",
                  "meta_prompt", "role_dir", "role_keywords", "output_dir",
                  "from_prompt", "output", "cwd", "commit_message")
_BOOL_FIELDS = ("aggregate", "meta", "relevance_enabled", "gate", "approval",
                "commands_parallel", "git_commit")
_TASK_STRING_FIELDS = ("prompt", "prompt_template", "output", "model", "provider",
                       "thinking", "tools", "exclude_tools", "cwd")
_NULLABLE_TASK_STRINGS = ("model", "provider", "thinking", "tools", "exclude_tools")


def validate_pipeline(data) -> list[str]:
    """Return every structural error before a pipeline can start work."""
    if not isinstance(data, dict):
        return ["top level must be a mapping"]
    stages = data.get("stages")
    if not isinstance(stages, list) or not stages:
        return ["stages must be a non-empty list"]
    errors = []
    for key in ("decision_log", "archive_dir"):
        if key in data and not isinstance(data[key], str):
            errors.append(f"{key} must be a string")
    if "git_commit" in data and not isinstance(data["git_commit"], bool):
        errors.append("git_commit must be boolean")
    seen = set()
    for index, stage in enumerate(stages, 1):
        stage_errors, name = _validate_stage(stage, index, seen)
        errors.extend(stage_errors)
        if name:
            seen.add(name)
    return errors


def _validate_stage(stage, index: int, seen: set[str]) -> tuple[list[str], str]:
    label = f"stage[{index}]"
    if not isinstance(stage, dict):
        return [f"{label} must be a mapping"], ""
    errors = []
    name = stage.get("name")
    if not isinstance(name, str) or not name.strip():
        errors.append(f"{label}.name must be a non-empty string")
        name = ""
    else:
        name = name.strip()
    if name and name in seen:
        errors.append(f"{label}.name duplicates earlier stage '{name}'")
    prefix = f"stage '{name}'" if name else label
    errors.extend(_field_errors(stage, prefix))
    errors.extend(_source_errors(stage, prefix, seen))
    errors.extend(_task_errors(stage.get("tasks", []), prefix))
    return errors, name


def _field_errors(stage: dict, prefix: str) -> list[str]:
    errors = [f"{prefix}.{key} must be a string" for key in _STRING_FIELDS
              if key in stage and not isinstance(stage[key], str)]
    errors.extend(f"{prefix}.{key} must be boolean" for key in _BOOL_FIELDS
                  if key in stage and not isinstance(stage[key], bool))
    if stage.get("mode", "serial") not in ("serial", "parallel"):
        errors.append(f"{prefix}.mode must be serial or parallel")
    for key, minimum in (("workers", 1), ("max_iterations", 1),
                         ("max_roles_per_iteration", 1), ("relevance_min_score", 0),
                         ("command_timeout", 1), ("command_output_max_bytes", 1),
                         ("meta_timeout", 1)):
        value = stage.get(key, minimum)
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            errors.append(f"{prefix}.{key} must be an integer >= {minimum}")
    commands = stage.get("commands", [])
    if not isinstance(commands, list) or not all(isinstance(item, str) for item in commands):
        errors.append(f"{prefix}.commands must be a string list")
    validate = stage.get("validate_cmd", stage.get("validate"))
    if validate is not None and not isinstance(validate, str):
        errors.append(f"{prefix}.validate must be a string or null")
    return errors


def _source_errors(stage: dict, prefix: str, seen: set[str]) -> list[str]:
    from_outputs = stage.get("from_outputs", "")
    valid_outputs = (isinstance(from_outputs, str) and (not from_outputs or bool(from_outputs.strip()))) or (
        isinstance(from_outputs, list) and all(
            isinstance(item, str) and bool(item.strip()) for item in from_outputs))
    errors = [] if valid_outputs else [f"{prefix}.from_outputs must be a string or string list"]
    errors.extend(_source_contract(stage, prefix, from_outputs))
    if valid_outputs:
        errors.extend(_upstream_errors(from_outputs, prefix, seen))
    return errors


def _source_contract(stage: dict, prefix: str, from_outputs) -> list[str]:
    errors = []
    if stage.get("meta"):
        if not from_outputs:
            errors.append(f"{prefix} meta stage requires from_outputs")
        if not stage.get("output_dir"):
            errors.append(f"{prefix} meta stage requires output_dir")
        if stage.get("from_prompt") or stage.get("from_dir"):
            errors.append(f"{prefix} meta stage accepts only from_outputs")
    else:
        sources = sum(bool(stage.get(key)) for key in ("from_prompt", "from_dir", "from_outputs"))
        if sources != 1:
            errors.append(f"{prefix} requires exactly one input source")
        if stage.get("from_prompt") and not stage.get("output"):
            errors.append(f"{prefix} from_prompt requires output")
        if from_outputs and not stage.get("tasks"):
            errors.append(f"{prefix} from_outputs requires tasks")
    return errors


def _upstream_errors(from_outputs, prefix: str, seen: set[str]) -> list[str]:
    raw_names = from_outputs if isinstance(from_outputs, list) else [from_outputs]
    names = [name.strip() for name in raw_names if name.strip()]
    return [f"{prefix}.from_outputs references unavailable earlier stage '{name}'"
            for name in names if name not in seen]


def _task_errors(tasks, prefix: str) -> list[str]:
    if not isinstance(tasks, list):
        return [f"{prefix}.tasks must be a list"]
    errors = []
    for index, task in enumerate(tasks, 1):
        label = f"{prefix}.tasks[{index}]"
        if not isinstance(task, dict):
            errors.append(f"{label} must be a mapping")
            continue
        errors.extend(f"{label}.{key} must be a string" for key in _TASK_STRING_FIELDS
                      if key in task and not isinstance(task[key], str)
                      and not (key in _NULLABLE_TASK_STRINGS and task[key] is None))
        timeout = task.get("timeout", 1)
        if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout < 1:
            errors.append(f"{label}.timeout must be an integer >= 1")
        if "env" in task and not isinstance(task["env"], dict):
            errors.append(f"{label}.env must be a mapping")
        if "validate" in task and task["validate"] is not None and not isinstance(task["validate"], str):
            errors.append(f"{label}.validate must be a string or null")
    return errors
