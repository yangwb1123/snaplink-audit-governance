"""Repository-level campaign coordinator: discover, analyze, select, implement."""

from __future__ import annotations
from pbatch.pipeline_status import PipelineStatus

import argparse
import logging
import logging.handlers
import math
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from . import config
from .campaign_analysis import (analysis_fingerprint, build_analysis_task,
                                discover_modules, parse_directions,
                                run_analysis_tasks, select_directions)
from .campaign_models import CampaignSettings, Direction
from .campaign_pipeline import (direction_fingerprint, parallel_ready,
                                run_direction, run_directions_isolated)
from .campaign_state import StateStore, _git_output, git_snapshot, tool_digest, tool_head, write_summary
from .config import log, yaml
from .lock import acquire_lock, release_lock
from .registry import mark_running, unregister
from .runner import kill_active_procs, run_argv
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
    parser.add_argument("--preflight-strict", action="store_true",
                        help="abort before direction pipelines when a repo-scoped "
                             "pipeline validator is red at HEAD (default: warn and "
                             "proceed — some baselines are intentionally red)")
    parser.add_argument("--agent-bin", default=config.AGENT_BIN)
    parser.add_argument("--model", default="")
    parser.add_argument("--provider", default="")
    parser.add_argument("--timeout", type=int, default=0)
    parser.add_argument("--retries", type=int, help="analysis retries")
    parser.add_argument("--retry-delay", type=float, default=10)
    parser.add_argument("--log-file", default="logs/full-auto.log")
    parser.add_argument("--stream-output", choices=["auto", "full", "none"],
                        default=config.STREAM_OUTPUT)
    parser.add_argument("--rounds", type=int, default=1,
                        help="repeat the campaign up to N times (0 = infinite; "
                             "fingerprint reuse makes later rounds incremental)")
    parser.add_argument("--round-delay", type=float, default=0, metavar="SECONDS",
                        help="pause between campaign rounds (pacing/rate limit)")
    parser.add_argument("--round-commit", action="store_true",
                        help="git add -A + commit all changes after each round "
                             "(respects .gitignore; advisory, never fails the round)")
    parser.add_argument("--no-lock", action="store_true")
    parser.add_argument("--wait-lock", type=int, default=0, metavar="MINUTES",
                        help="queue behind a held lock (poll 30s up to MINUTES) instead of exit 5")
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
              "budget-max": args.budget_max, "daily-budget": args.daily_budget,
              "round-delay": args.round_delay}
    invalid = [name for name, value in values.items()
               if not math.isfinite(float(value)) or value < 0]
    if args.rounds < 0:
        invalid.append("rounds")
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

# 执行层（pbatch/campaign_exec.py；re-export 保持测试面与旧导入兼容）。
from .campaign_exec import (main, run_campaign,  # noqa: E402
                            run_campaign_loop, _implement, _round_commit,
                            _register_campaign_run)
