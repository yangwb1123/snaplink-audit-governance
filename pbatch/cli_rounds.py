from __future__ import annotations

"""Round governance & CLI wiring (split from cli_rounds.py).

轮循环、治理裁决、锁与残留检查——被 `pi-batch` 主入口调用。
"""

"""Command-line entry points: parser, main, round loop, and the
single-batch / pipeline dispatch."""


import argparse
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
from .cli_batch import _run_batch_round, _run_pipeline_round  # noqa: E402
from .cli_residue import _warn_residue_markers  # noqa: E402

def _verify_round_lock(lock_path: Optional[Path]) -> None:
    """T4 (round-4 DS-1): re-verify the instance lock between rounds — a
    lock that vanished (unlinked by a concurrent breaker) means the
    single-instance guarantee is gone; continue would be split-brain."""
    if lock_path and not verify_held(lock_path):
        log.error("LOCK LOST: %s is no longer held by this instance (removed by another process?); "
                  "stopping to avoid split-brain (exit %d)", lock_path, LOCK_EXIT_CODE)
        sys.exit(LOCK_EXIT_CODE)


def _warn_round_residue(pipeline: Optional[Pipeline], kill_orphans: bool = False) -> None:
    """T5: warn about residue markers left by a killed run. Markers are
    written under each task's cwd, so cwd-aware tasks' orphans are scanned
    in their own dirs too (round-4 L6). GAP-3/N1: markers now carry the
    agent pid — a still-running orphan is warned about with a kill hint
    (double-billing risk); --kill-orphans kills them."""
    if pipeline is not None:
        dirs = [os.getcwd()] + [s.from_dir for s in pipeline.stages if s.from_dir]
        _warn_residue_markers(dirs, kill_orphans)
    else:
        _warn_residue_markers(kill_orphans=kill_orphans)


def _run_rounds(args, pipeline: Optional[Pipeline], session_name: str, cli_validate: str, timeout_override: int,
                lock_path: Optional[Path] = None) -> None:
    """The 24x7 round loop: rerun only the tasks/stages that failed or were
    rejected each round; --max-rounds 0 loops forever with --round-delay
    rest between rounds so quota and rate-limit windows can clear.

    T5 (7x24 governance): circuit isolation, stall detection, residue
    markers and environment-class halts keep an unattended run from
    burning quota against a broken provider or poisoned task set.
    """
    max_rounds = args.max_rounds
    # F9: stdin is consumed ONCE before the round loop; later rounds reuse
    # the loaded tasks (an exhausted stdin must not kill round 2+).
    stdin_tasks = None
    if pipeline is None and not (args.from_dir or args.prompt or args.source):
        stdin = sys.stdin.read().strip()
        if stdin:
            stdin_tasks = [Task(prompt=stdin)]
    round_no = 0
    circuit = Circuit(max_failures=args.circuit_max)
    stall = StallDetector(k=args.stall_rounds)
    while True:
        round_no += 1
        log.info("")
        log.info("=" * 60)
        log.info("ROUND %d of %s", round_no, "unlimited" if max_rounds == 0 else max_rounds)
        log.info("=" * 60)

        _verify_round_lock(lock_path)
        _warn_round_residue(pipeline, args.kill_orphans)
        round_failed, failed_keys = _execute_round(args, pipeline, stdin_tasks, session_name,
                                                   cli_validate, timeout_override, circuit, round_no)

        if not round_failed:
            log.info("All tasks passed in round %d", round_no)
            return
        if _govern_round(max_rounds, round_no, pipeline, circuit, failed_keys):
            sys.exit(1)
        if _circuit_all_stopped(circuit, failed_keys):
            sys.exit(STALL_EXIT_CODE)
        if stall.update(failed_keys, 0):
            sys.exit(STALL_EXIT_CODE)
        log.warning("Round %d finished with failures; waiting %.0fs before round %d",
                    round_no, args.round_delay, round_no + 1)
        time.sleep(args.round_delay)


def _govern_round(max_rounds: int, round_no: int, pipeline, circuit: Circuit, failed_keys: set) -> bool:
    """T5: round-bound governance — max-rounds exhaustion and pipeline
    circuit feeding; True when the loop must exit."""
    if max_rounds > 0 and round_no >= max_rounds:
        log.error("Max rounds (%d) reached with tasks still failing; rerun with --reuse to continue later", max_rounds)
        return True
    if pipeline is not None:
        # a poison stage must not spin the loop forever (round-4 P0-1)
        for key in failed_keys:
            circuit.register_failure(key, "pipeline round failure")
    return False


def _execute_round(args, pipeline: Optional[Pipeline], stdin_tasks, session_name: str, cli_validate: str,
                   timeout_override: int, circuit: Circuit, round_no: int) -> tuple[bool, set]:
    """Run one round (pipeline or single-batch) and return
    (round_failed, failed_keys) for the T5 governance checks."""
    if pipeline is not None:
        return _run_pipeline_round(args, pipeline, session_name, cli_validate,
                                   timeout_override, reuse_outputs=args.reuse and not args.force)
    return _run_batch_round(args, stdin_tasks, session_name, cli_validate,
                            args.max_rounds, round_no, circuit)


def _circuit_all_stopped(circuit: Circuit, failed_keys: set) -> bool:
    """True when every failing task is circuit-isolated (nothing left to
    run — a poison task set must not spin the loop forever)."""
    if not circuit.isolated:
        return False
    log.warning("CIRCUIT: %d task(s) isolated after repeated failures: %s",
                len(circuit.isolated), ", ".join(sorted(circuit.isolated)))
    if not failed_keys - circuit.isolated:
        log.error("All failing tasks are isolated; stopping (exit %d)", STALL_EXIT_CODE)
        return True
    return False


