"""Command-line entry points: parser, main, round loop, and the
single-batch / pipeline dispatch."""

from __future__ import annotations

import argparse
import json
import hashlib
import logging
import logging.handlers
import math
import os
import re
import shlex
import signal
import sys
import time
from pathlib import Path
from typing import Optional

from . import config
from .config import (AGENT_BIN, AGENT_DEFAULT_WORKERS, COMMIT_PREFIX_DEFAULT,
                     _resolve_validator_specs, log, yaml)
from .models import Pipeline, Task
from .registry import mark_running, unregister
from .reuse import (reuse_decision, reuse_fingerprint, sidecar_path, write_sidecar)
from .lock import LOCK_EXIT_CODE, acquire_lock, release_lock, verify_held
from .triage import (STALL_EXIT_CODE, Circuit, StallDetector, env_signature,
                     kill_orphan, marker_info, pid_alive, residue_markers, task_key)
from .pipeline import (_append_decision_log, _archive_outputs, load_pipeline,
                       load_tasks, load_tasks_from_dir, run_pipeline)
from .runner import (judge_verdict, kill_active_procs, print_summary, run_argv,
                     run_parallel, run_serial, run_validation)
from .text_io import read_text_bounded

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="pi-batch -- batch/pipeline executor (use 'campaign --help' for project campaigns)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    _add_source_args(p)
    _add_runtime_args(p)
    _add_batch_args(p)
    return p


def _add_source_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("source", nargs="?",
                   help="YAML task file / JSON file / plain text prompt")
    p.add_argument("-p", "--prompt", help="inline prompt (single task shortcut)")
    p.add_argument("-o", "--output", help="output file path (single task only)")
    p.add_argument("--from-dir", metavar="DIR",
                   help="load one task per .md file in DIR")
    p.add_argument("--suffix", default=".md",
                   help="file suffix for --from-dir (default: .md)")


def _add_runtime_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--pipeline", metavar="FILE",
                   help="run a multi-stage pipeline from YAML file")
    p.add_argument("--classify", action="store_true",
                   help="run the task type classifier BEFORE execution; frontend UI "
                        "tasks route to the configured UI generation pipeline "
                        "(classifier.frontend_pipeline; explicit --pipeline wins)")
    p.add_argument("--reuse", action="store_true",
                   help="reuse existing .out.md files (skip regeneration)")
    p.add_argument("--reuse-legacy", action="store_true",
                   help="existence-only reuse check (skip validators; escape hatch for T1 freshness revalidation)")
    p.add_argument("--reuse-fingerprint", action="store_true",
                   help="T9: reuse only when the artifact's sidecar input-fingerprint matches (input change -> regenerate)")
    p.add_argument("--force", action="store_true",
                   help="force regeneration, overwrite existing .out.md (default)")
    p.add_argument("--git-commit", action="store_true",
                   help="auto git commit after each stage (overrides pipeline setting)")
    p.add_argument("--no-git-commit", action="store_true",
                   help="disable git commit (overrides pipeline setting)")
    p.add_argument("--commit-prefix", default=COMMIT_PREFIX_DEFAULT,
                   help=f"prefix for auto-generated commit messages (default: {COMMIT_PREFIX_DEFAULT})")
    _add_logging_args(p)
    p.add_argument("--decision-log", default="",
                   help="Append structured per-stage decision records to FILE (overrides pipeline 'decision_log')")
    p.add_argument("--archive-dir", default="",
                   help="Move completed deliverables into DIR/<label>-<timestamp>/ after a fully successful run (keeps the worktree clean; git history retains everything)")
    p.add_argument("--session-mode", choices=["new", "shared", "per-stage"], default="new",
                   help="Session reuse: new = fresh session per call (default), shared = one session for the whole batch/pipeline, per-stage = one session per pipeline stage")
    p.add_argument("--session-max-bytes", type=int, default=config.SESSION_MAX_BYTES,
                   help=f"T10: fork the shared session past this size (default: {config.SESSION_MAX_BYTES})")
    p.add_argument("--session-name", default="",
                   help="Reproducible session base name (default: task source file stem); shared sessions continue across runs")
    _add_memory_arg(p)
    _add_lock_args(p)
    _add_validate_args(p)
    p.add_argument("--kill-orphans", action="store_true",
                   help="GAP-3: kill orphaned agent process groups found in residue markers "
                        "(agents left running by a SIGKILLed runner — double-billing risk)")
    p.add_argument("--approve-file", default="",
                   help="T12c: file whose existence approves approval: true stages (CI-friendly)")
    p.add_argument("--circuit-max", type=int, default=5,
                   help="T5: isolate a task after this many consecutive failures (default: 5)")
    p.add_argument("--stall-rounds", type=int, default=6,
                   help="T5: stop the loop (exit 4) after this many rounds without progress (default: 6)")
    _add_metering_args(p)
    p.add_argument("--dry-run", action="store_true",
                   help="print task list without executing")
    return p


def _add_logging_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "-v", "--verbose", action="count", default=0,
        help="渐进式输出：默认常用（INFO）；-v 详细（DEBUG）")
    p.add_argument("--log-file", default="",
                   help="Append run log to FILE for 24x7 supervision (rotated)")
    p.add_argument("--log-max-bytes", type=int, default=config.LOG_MAX_BYTES,
                   help=f"Rotate the log file after this many bytes (default: {config.LOG_MAX_BYTES})")
    p.add_argument("--log-backups", type=int, default=config.LOG_BACKUP_COUNT,
                   help=f"Keep this many rotated log backups (default: {config.LOG_BACKUP_COUNT})")
    p.add_argument("--stream-output", choices=["auto", "full", "none"],
                   default=config.STREAM_OUTPUT,
                   help="Live model response display: auto only on a TTY, full always, none never")


def _add_memory_arg(p: argparse.ArgumentParser) -> None:
    p.add_argument("--memory-mode", choices=["auto", "on", "off"], default=config.MEMORY_MODE,
                   help="Progressive message memory: auto for configured agents; on/off override")


def _add_lock_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--no-lock", action="store_true",
                   help="skip the single-instance lock (T4 escape hatch)")
    p.add_argument("--wait-lock", type=int, default=0, metavar="MINUTES",
                   help="queue behind a held lock: poll every 30s up to MINUTES "
                        "instead of exiting 5 (shared-machine workflows)")


def _add_validate_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--validate", default="",
                   help="Named validators from pi-batch.yaml validators registry, comma-separated (e.g. 'quick,gofmt'); AND semantics. Unknown names are treated as raw shell commands")
    p.add_argument("--validate-cmd", default="",
                   help="Engineering gate run against every agent result BEFORE its output is saved (e.g. 'go build ./... && go vet ./...', 'python cli.py check'); {output} and {cwd} placeholders are substituted. Non-zero exit rejects the result and leaves no file")


def _add_batch_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--mode", choices=["serial", "parallel"], default="serial",
                   help="execution mode (default: serial)")
    p.add_argument("-w", "--workers", type=int, default=AGENT_DEFAULT_WORKERS,
                   help=f"parallel worker count (default: {AGENT_DEFAULT_WORKERS})")
    p.add_argument("--agent-bin", default=AGENT_BIN,
                   help=f"agent CLI binary to invoke per task (default: {AGENT_BIN}; "
                        "set agent.bin in pi-batch.yaml to change the default)")
    p.add_argument("--model", default="",
                   help="default model override for all tasks")
    p.add_argument("--provider", default="",
                   help="default provider override for all tasks (CLI > task > stage > config)")
    p.add_argument("--timeout", type=int, default=0,
                   help="default timeout override for all tasks (seconds)")
    p.add_argument("--retries", type=int, default=0,
                   help="Retry failed tasks up to N extra attempts with exponential backoff")
    p.add_argument("--retry-delay", type=float, default=10.0,
                   help="Base retry wait in seconds (default: 10; rate-limit/network failures wait at least 30s)")
    p.add_argument("--retry-backoff", type=float, default=2.0,
                   help="Retry backoff multiplier (default: 2)")
    p.add_argument("--min-interval", type=float, default=0.0,
                   help="Minimum seconds between successful tasks (throttle for 24x7 runs)")
    p.add_argument("--max-rounds", type=int, default=1,
                   help="Max execution rounds; 0 = loop forever until every task passes (default: 1)")
    p.add_argument("--round-delay", type=float, default=60.0,
                   help="Seconds to wait between rounds (default: 60)")


def _sigterm(signum, frame):
    """S1 (SRE round-4 finding): SIGTERM (systemd stop, operator kill)
    behaves like Ctrl-C — children are killed, the lock released, exit
    130 — instead of stranding agent process groups and the lock."""
    log.warning("\nSIGTERM received; stopping children and releasing the lock (exit 130)")
    kill_active_procs()
    raise KeyboardInterrupt()


def _apply_verbosity(verbosity: int) -> None:
    """渐进式展示（cognitive-load §3）：默认常用（INFO，含核心），
    -v 显示详细（DEBUG）；--quiet 只在错误时输出。"""
    level = logging.INFO
    if verbosity >= 1:
        level = logging.DEBUG
    logging.getLogger("pi-batch").setLevel(level)


def main() -> None:
    if _dispatch_subcommand():
        return
    # 子命令已处理：subcommand 自身通过 sys.exit 返回退出码
    parser = build_parser()
    args = parser.parse_args()
    _validate_runtime_values(parser, args)
    _apply_verbosity(args.verbose)
    _wire_agent(args)
    if args.session_max_bytes != config.SESSION_MAX_BYTES:
        config.SESSION_MAX_BYTES = args.session_max_bytes

    signal.signal(signal.SIGTERM, _sigterm)

    _wire_metering(args)
    _wire_limits(args)
    _auto_detect_pipeline(args)
    _maybe_classify_route(args)
    handler = _configure_run_log(args)
    _register_run(args)
    try:
        lock_path = acquire_lock(no_lock=args.no_lock or args.dry_run,
                                 wait_minutes=getattr(args, "wait_lock", 0))
        mark_running()
    except SystemExit:
        unregister()
        raise
    try:
        _run_configured(args, lock_path)
    except KeyboardInterrupt:
        log.warning("\nInterrupted by user. Rerun with --reuse to continue later.")
        sys.exit(130)
    except Exception as exc:  # P3（使用反馈，上游 merge 移植）：意外退出记录原因
        _record_fatal_exit(exc, args)
        raise
    finally:
        release_lock(lock_path)
        unregister()
        if handler:
            log.removeHandler(handler)
            handler.close()


def _record_fatal_exit(exc: BaseException, args) -> None:
    """崩溃原因落盘（--reuse 续跑可诊断上次进程为何消失）。"""
    log.error("FATAL: pi-batch crashed (%s: %s)", type(exc).__name__, exc)
    try:
        state = (Path(args.log_file).with_suffix(".exit.json")
                 if getattr(args, "log_file", "") else
                 Path.cwd() / ".pi-batch" / "exit.json")
        state.parent.mkdir(parents=True, exist_ok=True)
        state.write_text(
            json.dumps({"exit": type(exc).__name__,
                        "reason": str(exc)[:500],
                        "time": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                        "argv": sys.argv[:4]}, ensure_ascii=False, indent=1),
            encoding="utf-8")
        log.error("Exit reason written to %s", state)
    except Exception:
        pass


def _register_run(args) -> None:
    """Publish this process in the global run registry (visibility:
    who runs what in which repo). Unregister happens in the main
    finally block and via atexit as a safety net."""
    import atexit
    from . import registry
    mode = "pipeline" if args.pipeline else "batch"
    registry.register_run(mode=mode, repo=os.getcwd(),
                          session_name=args.session_name or "")
    atexit.register(registry.unregister)


def _validate_runtime_values(parser: argparse.ArgumentParser, args) -> None:
    """Reject unsafe numeric values before logs, locks, or agents start."""
    nonnegative = {
        "timeout": args.timeout, "retries": args.retries,
        "retry-delay": args.retry_delay, "min-interval": args.min_interval,
        "max-rounds": args.max_rounds, "round-delay": args.round_delay,
        "log-max-bytes": args.log_max_bytes, "log-backups": args.log_backups,
        "budget-max": args.budget_max, "daily-budget": args.daily_budget,
    }
    positive = {
        "workers": args.workers, "retry-backoff": args.retry_backoff,
        "session-max-bytes": args.session_max_bytes,
        "circuit-max": args.circuit_max, "stall-rounds": args.stall_rounds,
    }
    invalid = [name for name, value in nonnegative.items()
               if not math.isfinite(float(value)) or value < 0]
    invalid += [name for name, value in positive.items()
                if not math.isfinite(float(value)) or value <= 0]
    if not str(args.agent_bin).strip():
        invalid.append("agent-bin")
    if invalid:
        parser.error("invalid runtime value(s): " + ", ".join(f"--{name}" for name in invalid))


def _configure_run_log(args):
    if not args.log_file:
        return None
    Path(args.log_file).parent.mkdir(parents=True, exist_ok=True)
    handler = logging.handlers.RotatingFileHandler(
        args.log_file, maxBytes=args.log_max_bytes, backupCount=args.log_backups,
        encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    log.addHandler(handler)
    return handler


def _run_configured(args, lock_path) -> None:
    pipeline = _setup_pipeline(args)
    timeout_override = args.timeout if "--timeout" in sys.argv else 0
    # T11: CLI --provider wins over stage-level providers (precedence D6:
    # CLI > task > stage > config default).
    if args.provider and pipeline is not None:
        for stage in pipeline.stages:
            stage.provider = args.provider

    session_name = _derive_session_name(args)
    _validate_session_flags(args)

    # Validation gates: named validators (--validate) and the raw command
    # (--validate-cmd) both apply, in that order, with AND semantics.
    cli_validate = ",".join(x for x in (args.validate, args.validate_cmd) if x)
    _run_rounds(args, pipeline, session_name, cli_validate, timeout_override, lock_path)


_SUBCOMMANDS: dict[str, str] = {
    "memory": "memory:main",
    "classify": "classifier:classify_main",
    "rules": "rule_cli:rules_main",
    "assess": "assessor:assess_main",
    "learn": "learn:learn_main",
    "context": "context:context_main",
    "eval": "eval:eval_main",
    "check": "selfcheck:check_main",
    "advance": "advance:advance_main",
    "ps": "registry:ps_main",
    "retro": "retro:retro_main",
    "health": "health:health_main",
    "ui-geometry": "ui_geometry:ui_geometry_main",
    "export-template": "export_template:export_main",
    "campaign": "campaign:main",
    "devices": "fabric_cli:devices_main",
    "profile": "profile:profile_main",
    "race": "racing:race_main",
    "truth": "truth:truth_main",
    "world": "world:world_main",
    "graph": "hypergraph:graph_main",
    "metrics": "metrics:metrics_main",
    "diversity": "diversity:diversity_main",
    "atomize": "atomize:atomize_main",
    "causal": "causality:causality_main",
    "temporal": "temporal:temporal_main",
    "pareto": "pareto:pareto_main",
    "recovery": "recovery:recovery_main",
    "budget": "recovery:budget_main",
    "pareto": "pareto:pareto_main",
    "proposal": "proposal:proposal_main",
    "pinned": "pinned:pinned_main",
    "events": "events:events_main",
    "tools": "tools:tools_main",
    "init": "product_cli:init_main",
    "version": "product_cli:version_main",
    "clean": "product_cli:clean_main",
    "completion": "product_cli:completion_main",
    "doctor": "product_cli:doctor_main",
    "capabilities": "capabilities:capabilities_main",
    "reflect": "reflect:reflect_main",
    "adr": "adr:adr_main",
    "impact": "impact:impact_main",
    "project": "project_memory:project_main",
    "refactor": "refactor:refactor_main",
    "org": "org:org_main",
    "test-report": "test_report:test_report_main",
    "probe": "probe:probe_main",
    "replay": "capsule:replay_main",
    "capsule": "capsule:capsule_main",
    "nversion": "nversion:nversion_main",
    "serve": "fabric_server_cli:serve_main",
    "approve": "fabric_server_cli:approve_main",
    "runners": "fabric_server_cli:runners_main",
    "runner": "fabric_runner:runner_main",
}


def _dispatch_subcommand() -> bool:
    if len(sys.argv) <= 1:
        return False
    target = _SUBCOMMANDS.get(sys.argv[1])
    if target is None:
        return False
    module_name, _, func_name = target.partition(":")
    module = __import__(f"pbatch.{module_name}", fromlist=[func_name])
    subcommand = getattr(module, func_name)
    subcommand(sys.argv[2:])
    return True


def _wire_agent(args) -> None:
    config.AGENT_BIN = args.agent_bin
    config.MEMORY_MODE = args.memory_mode
    config.STREAM_OUTPUT = args.stream_output


def _validate_session_flags(args) -> None:
    """Shared/per-stage sessions require serial execution; per-stage needs
    a pipeline (single-batch has no stages)."""
    if args.session_mode != "new" and args.mode == "parallel" and not args.pipeline:
        log.error("--session-mode %s requires --mode serial (parallel would interleave one session)", args.session_mode)
        sys.exit(1)
    if args.session_mode == "per-stage" and not args.pipeline:
        log.error("--session-mode per-stage requires --pipeline (there are no stages in single-batch mode)")
        sys.exit(1)

# 轮循环/批执行层（拆分后 re-export 保持测试面与旧导入兼容）。
from .cli_batch import (_add_metering_args, _apply_task_overrides,  # noqa: E402
                         _auto_detect_pipeline, _derive_session_name,
                         _drop_isolated, _execute_batch, _filter_reused,
                         _finalize_batch_round, _finish_batch_round,
                         _git_commit_single_batch, _load_batch_tasks,
                         _maybe_classify_route, _print_dry_run,
                         _resolve_frontend_pipeline, _resolve_pipeline,
                         _run_batch_repo_spec, _run_batch_repo_validators,
                         _run_batch_round, _run_pipeline_round,
                         _setup_pipeline, _wire_limits, _wire_metering,
                         _write_sidecars)
from .cli_rounds import (_circuit_all_stopped, _govern_round, _run_rounds,  # noqa: E402
                         _verify_round_lock, _warn_round_residue)
from .cli_residue import _warn_residue_markers  # noqa: E402
