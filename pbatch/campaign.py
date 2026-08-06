"""Repository-level campaign coordinator: discover, analyze, select, implement."""

from __future__ import annotations

import argparse
import logging
import logging.handlers
import math
import signal
import sys
from dataclasses import dataclass
from pathlib import Path

from . import config
from .campaign_analysis import (analysis_fingerprint, build_analysis_task,
                                discover_modules, parse_directions,
                                run_analysis_tasks, select_directions)
from .campaign_models import CampaignSettings, Direction
from .campaign_pipeline import (direction_fingerprint, parallel_ready,
                                run_direction, run_directions_isolated)
from .campaign_state import StateStore, git_snapshot, write_summary
from .config import log, yaml
from .lock import acquire_lock, release_lock
from .runner import kill_active_procs
from .text_io import read_text_bounded


@dataclass
class CampaignContext:
    root: Path
    settings: CampaignSettings
    args: argparse.Namespace
    store: StateStore
    snapshot: dict


def build_campaign_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pi-batch.py campaign",
        description="Discover repository modules and run evidence-backed SDLC campaigns")
    parser.add_argument("--config", default="examples/repository-campaign.yaml")
    parser.add_argument("--modules", default="", help="comma-separated module directories")
    parser.add_argument("--max-directions", type=int)
    parser.add_argument("--top-only", action="store_true")
    parser.add_argument("--skip-passed", action="store_true")
    parser.add_argument("--reuse", action="store_true")
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--jobs", type=int, help="parallel module analyses")
    parser.add_argument("--parallel-pipelines", type=int, help="isolated implementation worktrees")
    parser.add_argument("--pipeline", default="", help="implementation pipeline template override")
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--state-file", default="")
    parser.add_argument("--summary-file", default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--agent-bin", default=config.AGENT_BIN)
    parser.add_argument("--model", default="")
    parser.add_argument("--provider", default="")
    parser.add_argument("--timeout", type=int, default=0)
    parser.add_argument("--retries", type=int, help="analysis retries")
    parser.add_argument("--retry-delay", type=float, default=10)
    parser.add_argument("--log-file", default="logs/full-auto.log")
    parser.add_argument("--stream-output", choices=["auto", "full", "none"],
                        default=config.STREAM_OUTPUT)
    parser.add_argument("--no-lock", action="store_true")
    parser.add_argument("--memory-mode", choices=["auto", "on", "off"], default=config.MEMORY_MODE)
    _add_metering_args(parser)
    return parser


def _add_metering_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--events-file", default="")
    parser.add_argument("--webhook", default="")
    parser.add_argument("--budget-max", type=int, default=0)
    parser.add_argument("--daily-budget", type=int, default=0)
    parser.add_argument("--daily-state", default="")


def load_settings(root: Path, args: argparse.Namespace) -> CampaignSettings:
    data = {}
    config_path = _inside(root, root / args.config) if args.config else None
    if config_path and config_path.exists():
        if not yaml:
            raise ValueError("campaign config requires PyYAML")
        try:
            loaded = yaml.safe_load(read_text_bounded(
                config_path, config.INPUT_MAX_BYTES, "campaign config")) or {}
        except Exception as exc:
            raise ValueError(f"invalid campaign YAML: {config_path}: {exc}") from exc
        if not isinstance(loaded, dict):
            raise ValueError(f"campaign config must be a mapping: {config_path}")
        data = loaded
    elif args.config:
        raise ValueError(f"campaign config not found: {config_path}")
    settings = CampaignSettings.from_mapping(data)
    _apply_overrides(settings, args)
    settings.validate()
    _validate_runtime_args(args)
    _validate_paths(root, settings)
    return settings


def _validate_runtime_args(args: argparse.Namespace) -> None:
    values = {"timeout": args.timeout, "retry-delay": args.retry_delay,
              "budget-max": args.budget_max, "daily-budget": args.daily_budget}
    invalid = [name for name, value in values.items()
               if not math.isfinite(float(value)) or value < 0]
    if not str(args.agent_bin).strip():
        invalid.append("agent-bin")
    if invalid:
        raise ValueError("invalid campaign runtime values: " + ", ".join(invalid))


def _apply_overrides(settings: CampaignSettings, args: argparse.Namespace) -> None:
    if args.max_directions is not None:
        settings.max_directions = args.max_directions
    if args.top_only:
        settings.max_directions = 1
    if args.jobs is not None:
        settings.analysis_jobs = args.jobs
    if args.parallel_pipelines is not None:
        settings.pipeline_concurrency = args.parallel_pipelines
    if args.retries is not None:
        settings.analysis_retries = args.retries
    for attr, value in (("pipeline_template", args.pipeline), ("output_dir", args.output_dir),
                        ("state_file", args.state_file), ("summary_file", args.summary_file)):
        if value:
            setattr(settings, attr, value)


def _validate_paths(root: Path, settings: CampaignSettings) -> None:
    for value in (settings.output_dir, settings.state_file, settings.summary_file,
                  settings.pipeline_template, settings.worktree_root):
        _inside(root, settings.path(root, value))
    template = settings.path(root, settings.pipeline_template)
    if not template.is_file() or template.is_symlink():
        raise ValueError(f"campaign pipeline template is missing or unsafe: {template}")


def _inside(root: Path, path: Path) -> Path:
    resolved = path.resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError(f"campaign path escapes repository: {path}") from exc
    return resolved


def run_campaign(context: CampaignContext) -> int:
    modules = discover_modules(context.root, context.settings, context.args.modules)
    if not modules:
        log.error("CAMPAIGN: no modules found")
        return 1
    if context.args.dry_run:
        _print_plan(context, modules)
        return 0
    directions, failures = _analyze_modules(context, modules)
    selected, selection_failures = _select(context, modules, directions)
    failures += selection_failures
    outcomes = _implement(context, selected)
    failures += sum(1 for outcome in outcomes if outcome.status != "PASSED")
    write_summary(context.store, context.settings.path(context.root, context.settings.summary_file),
                  context.settings.name)
    passed = sum(1 for outcome in outcomes if outcome.status == "PASSED")
    log.info("CAMPAIGN complete: %d passed, %d failed", passed, failures)
    return 1 if failures else 0


def _analyze_modules(context: CampaignContext, modules: list[str]) -> tuple[dict[str, list[Direction]], int]:
    tasks = {}
    fingerprints = {}
    directions = {}
    reuse = _reuse_enabled(context.args)
    for module in modules:
        task = build_analysis_task(context.root, context.settings, module,
                                   context.args.model, context.args.provider, context.args.timeout)
        fp = analysis_fingerprint(context.root, context.settings, module, context.snapshot, task)
        fingerprints[module] = fp
        if reuse and context.store.reusable(module, "__analysis__", fp, ("ANALYZED",)):
            parsed = _parse_analysis_file(module, task)
            if parsed:
                directions[module] = parsed
                continue
        _event(context, module, "__analysis__", "", "ANALYZING", fp, [str(task.output_path())])
        tasks[module] = task
    module_by_output = {str(task.output_path()): module for module, task in tasks.items()}
    def _valid(result):
        module = module_by_output[str(result.task.output_path())]
        return bool(_parse_analysis_file(module, result.task))
    results = run_analysis_tasks(list(tasks.values()), context.settings.analysis_jobs,
                                 context.settings.analysis_retries, context.args.retry_delay, _valid)
    failures = 0
    for module, task in tasks.items():
        result = results.get(str(task.output_path()))
        parsed = _parse_analysis_file(module, task) if result and result.success else []
        if not parsed:
            task.output_path().unlink(missing_ok=True)
            reason = result.reason if result else "analysis produced no structured directions"
            _event(context, module, "__analysis__", "", "ANALYSIS_FAILED",
                   fingerprints[module], [], reason)
            failures += 1
            continue
        directions[module] = parsed
        _event(context, module, "__analysis__", "", "ANALYZED", fingerprints[module],
               [str(task.output_path())], elapsed=result.elapsed)
    return directions, failures


def _parse_analysis_file(module: str, task) -> list[Direction]:
    path = task.output_path()
    try:
        return parse_directions(module, read_text_bounded(
            path, config.OUTPUT_MAX_BYTES, "campaign analysis"))
    except (OSError, ValueError):
        return []


def _select(context: CampaignContext, modules: list[str],
            directions: dict[str, list[Direction]]) -> tuple[list[tuple[Direction, Path, str]], int]:
    selected_items = []
    failures = 0
    for module in modules:
        candidates = directions.get(module, [])
        selected, rejected = select_directions(
            context.root, candidates, context.settings.max_directions, context.settings.minimum_score)
        analysis = _analysis_path(context, module)
        for direction in rejected:
            _direction_event(context, direction, "DIRECTION_REJECTED", "", analysis,
                             "duplicate, low score, missing evidence, or missing acceptance checks")
        if candidates and not selected:
            failures += 1
        for direction in selected:
            fp = direction_fingerprint(context.root, context.settings, direction, analysis,
                                       context.snapshot, context.args.model, context.args.provider)
            if (_reuse_enabled(context.args) and context.store.reusable(
                    module, direction.direction_id, fp, ("PASSED",))):
                continue
            _direction_event(context, direction, "SELECTED", fp, analysis)
            selected_items.append((direction, analysis, fp))
    return selected_items, failures


def _analysis_path(context: CampaignContext, module: str) -> Path:
    task = build_analysis_task(context.root, context.settings, module)
    return task.output_path()


def _implement(context: CampaignContext, items: list[tuple[Direction, Path, str]]):
    if not items:
        return []
    for direction, analysis, fp in items:
        _direction_event(context, direction, "RUNNING", fp, analysis)
    if context.settings.pipeline_concurrency > 1:
        raw = [(direction, analysis) for direction, analysis, _ in items]
        outcomes = run_directions_isolated(context.root, context.settings, raw,
                                           context.settings.pipeline_concurrency,
                                           _agent_args(context.args))
    else:
        outcomes = [_run_direction_safe(context, direction, analysis)
                    for direction, analysis, _ in items]
    fingerprints = {direction.direction_id: fp for direction, _, fp in items}
    for outcome in outcomes:
        _event(context, outcome.direction.module, outcome.direction.direction_id,
               outcome.direction.title, outcome.status, fingerprints[outcome.direction.direction_id],
               outcome.evidence, outcome.reason, outcome.elapsed,
               {"branch": outcome.branch, "commit": outcome.commit})
    return outcomes


def _run_direction_safe(context: CampaignContext, direction: Direction,
                        analysis: Path):
    from .campaign_pipeline import PipelineOutcome
    try:
        return run_direction(context.root, context.settings, direction, analysis,
                             context.args.model, context.args.provider, context.args.timeout,
                             _reuse_enabled(context.args))
    except Exception as exc:
        return PipelineOutcome(direction, "PIPELINE_FAILED", str(exc), 0, [str(analysis)])


def _agent_args(args: argparse.Namespace) -> list[str]:
    values = ["--agent-bin", args.agent_bin, "--memory-mode", args.memory_mode,
              "--stream-output", args.stream_output]
    for flag, value in (("--model", args.model), ("--provider", args.provider),
                        ("--timeout", args.timeout)):
        if value:
            values.extend([flag, str(value)])
    return values


def _event(context: CampaignContext, module: str, direction_id: str, direction: str,
           status: str, fingerprint: str, evidence: list[str], reason: str = "",
           elapsed: float = 0, extra: dict | None = None) -> None:
    payload = {"campaign": context.settings.name, "module": module,
               "direction_id": direction_id, "direction": direction, "status": status,
               "fingerprint": fingerprint, "evidence": evidence, "reason": reason,
               "elapsed": elapsed}
    payload.update(extra or {})
    context.store.append(payload)
    from .memory import record_campaign
    record_campaign(payload, str(context.root))


def _direction_event(context: CampaignContext, direction: Direction, status: str,
                     fingerprint: str, analysis: Path, reason: str = "") -> None:
    _event(context, direction.module, direction.direction_id, direction.title, status,
           fingerprint, [str(analysis)], reason)


def _reuse_enabled(args: argparse.Namespace) -> bool:
    return not args.force and (args.reuse or args.skip_passed or args.retry_failed)


def _print_plan(context: CampaignContext, modules: list[str]) -> None:
    analysis_sec = context.store.median_elapsed(("ANALYZED",), context.settings.analysis_minutes * 60)
    pipeline_sec = context.store.median_elapsed(
        ("PASSED", "GATE_REJECTED", "PIPELINE_FAILED", "VALIDATION_FAILED"),
        context.settings.pipeline_minutes * 60)
    analysis_wall = len(modules) * analysis_sec / context.settings.analysis_jobs
    pipeline_wall = (len(modules) * context.settings.max_directions * pipeline_sec /
                     context.settings.pipeline_concurrency)
    print(f"== Campaign: {context.settings.name} ==")
    print(f"Modules: {len(modules)}; directions/module: {context.settings.max_directions}")
    for module in modules:
        print(f"  - {module}")
    print(f"Estimated analysis: {analysis_wall / 60:.0f} min")
    print(f"Estimated pipelines: {pipeline_wall / 60:.0f} min")
    print(f"Estimated total: {(analysis_wall + pipeline_wall) / 60:.0f} min")
    print("Dry-run made no files or state changes.")


def _wire_runtime(args: argparse.Namespace) -> None:
    from . import metering
    config.AGENT_BIN = args.agent_bin
    config.MEMORY_MODE = args.memory_mode
    config.STREAM_OUTPUT = args.stream_output
    metering.EVENTS_FILE = args.events_file
    metering.WEBHOOK_URL = args.webhook
    metering.BUDGET_MAX = args.budget_max
    metering.DAILY_BUDGET = args.daily_budget
    metering._DAILY_STATE = args.daily_state or str(Path(".pi-batch") / "daily-budget.json")


def main(argv: list[str] | None = None) -> None:
    args = build_campaign_parser().parse_args(argv)
    signal.signal(signal.SIGTERM, _campaign_sigterm)
    root = Path.cwd().resolve()
    try:
        settings = load_settings(root, args)
        if settings.pipeline_concurrency > 1 and not args.dry_run:
            ready, reason = parallel_ready(root)
            if not ready:
                raise ValueError(reason)
    except ValueError as exc:
        log.error("CAMPAIGN config: %s", exc)
        raise SystemExit(2)
    _wire_runtime(args)
    if args.log_file and not args.dry_run:
        Path(args.log_file).parent.mkdir(parents=True, exist_ok=True)
        handler = logging.handlers.RotatingFileHandler(args.log_file, maxBytes=config.LOG_MAX_BYTES,
                                                       backupCount=config.LOG_BACKUP_COUNT, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        log.addHandler(handler)
    store = StateStore(settings.path(root, settings.state_file), settings.state_line_max_bytes)
    excluded = _snapshot_excludes(root, settings, args)
    context = CampaignContext(root, settings, args, store, git_snapshot(root, excluded))
    lock_path = acquire_lock(cwd=str(root), no_lock=args.no_lock or args.dry_run)
    try:
        code = run_campaign(context)
    except KeyboardInterrupt:
        log.warning("CAMPAIGN interrupted; rerun with --reuse --retry-failed")
        code = 130
    finally:
        release_lock(lock_path)
    raise SystemExit(code)


def _campaign_sigterm(signum, frame) -> None:
    from .campaign_pipeline import kill_isolated_pipelines
    log.warning("CAMPAIGN received SIGTERM; stopping active agents and pipelines")
    kill_active_procs()
    kill_isolated_pipelines()
    raise KeyboardInterrupt()


def _snapshot_excludes(root: Path, settings: CampaignSettings,
                       args: argparse.Namespace) -> tuple[str, ...]:
    values = [settings.output_dir, settings.worktree_root]
    if args.log_file:
        log_path = Path(args.log_file)
        values.append(str(log_path if log_path.parent == Path(".") else log_path.parent))
    result = []
    for value in values:
        path = settings.path(root, value)
        try:
            result.append(path.relative_to(root).as_posix())
        except ValueError:
            continue
    return tuple(dict.fromkeys(result))
