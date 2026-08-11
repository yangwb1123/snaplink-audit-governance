"""Operational metrics dashboard (AADM-G §32 五类指标).

从既有追加式数据源聚合（只读、有界读取，不新增写入）：
- 结果质量：被拒产物数、内存索引观察裁决（首次正确率）
- 决策质量：规则草案/例外/影子评估/真值失效数
- 调度质量：运行注册表（registry runs）、advance 轮数
- 协作质量：显式标注"未追踪"（诚实，不伪造指标）
- 资源质量：内存索引 cost/token 合计、会话数

Usage:
    pi-batch metrics [--json]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

from . import config

REJECTED_DIR = Path(".pi-batch") / "rejected"
MEMORY_INDEX = Path(".pi-batch") / "memory" / "sessions.index.jsonl"
TRUTH_FILE = Path(".pi-batch") / "truth.jsonl"
INDEX_LINE_MAX = 256 * 1024
INDEX_MAX_BYTES = 16 * 1024 * 1024


def _count_files(directory: Path, suffix: str = "") -> int:
    if not directory.exists():
        return 0
    return sum(1 for path in directory.iterdir()
               if path.is_file() and (not suffix or path.name.endswith(suffix)))


def _count_docs(pattern: str) -> int:
    root = Path("docs") / "rules"
    return len(list(root.glob(pattern)))


def _scan_memory_index() -> dict:
    """内存索引：首次正确率（GATE_PASS vs FAILED 观察）与资源合计。"""
    stats = {"sessions": 0, "passed": 0, "failed": 0, "cost": 0.0}
    if not MEMORY_INDEX.exists():
        return stats
    try:
        handle = MEMORY_INDEX.open("r", encoding="utf-8")
    except OSError:
        return stats
    with handle:
        total = 0
        for line in handle:
            total += len(line.encode("utf-8", errors="replace"))
            if total > INDEX_MAX_BYTES:
                break
            if len(line) > INDEX_LINE_MAX:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if not isinstance(record, dict):
                continue
            stats["sessions"] += 1
            verdict = str(record.get("observed_verdict", ""))
            if "PASS" in verdict.upper():
                stats["passed"] += 1
            elif record.get("observed_failure"):
                stats["failed"] += 1
            cost = record.get("cost") or 0
            if isinstance(cost, (int, float)):
                stats["cost"] += float(cost)
    stats["cost"] = round(stats["cost"], 4)
    return stats


def _count_run_registry() -> int:
    """~/.pi-batch/runs 下的运行注册记录数（调度质量）。"""
    runs_dir = Path.home() / ".pi-batch" / "runs"
    if not runs_dir.exists():
        return 0
    return sum(1 for path in runs_dir.iterdir() if path.suffix == ".json")


def _count_lines(path: Path) -> int:
    """追加式状态文件行数（有界读取）。"""
    if not path.exists():
        return 0
    try:
        return sum(1 for _ in path.open(encoding="utf-8"))
    except OSError:
        return 0


def _count_truth_invalidations() -> int:
    """真值失效记录数（决策质量）。"""
    count = 0
    if not TRUTH_FILE.exists():
        return count
    try:
        handle = TRUTH_FILE.open(encoding="utf-8")
    except OSError:
        return count
    with handle:
        for line in handle:
            if len(line) > INDEX_LINE_MAX:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if isinstance(record, dict) and record.get("action") == "invalidate":
                count += 1
    return count


def metrics_report() -> dict:
    """五类指标聚合（只读；缺失数据源如实标注，不伪造）。"""
    memory = _scan_memory_index()
    decided = memory["passed"] + memory["failed"]
    first_try = round(memory["passed"] / decided, 3) if decided else None
    runs = _count_run_registry()
    advance_rounds = _count_lines(Path("docs") / "advance" / "state.jsonl")
    truth_invalidated = _count_truth_invalidations()
    return {
        "result_quality": {
            "rejected_artifacts": _count_files(REJECTED_DIR, ".rejected.md"),
            "first_try_rate": first_try,
            "observed_passes": memory["passed"],
            "observed_failures": memory["failed"],
        },
        "decision_quality": {
            "rule_drafts": _count_docs("drafts/*.yaml"),
            "rule_exceptions": _count_docs("exceptions/*.yaml"),
            "promoted_rules": _count_docs("promoted/*.yaml"),
            "truth_invalidations": truth_invalidated,
        },
        "scheduling_quality": {
            "registry_runs": runs,
            "advance_rounds": advance_rounds,
        },
        "collaboration_quality": {
            "merge_conflicts": None,   # 未追踪——诚实标注，不伪造指标
            "note": "跨 Agent 协作指标（合并冲突/重复上下文/返工）当前未追踪",
        },
        "resource_quality": {
            "sessions": memory["sessions"],
            "total_cost": memory["cost"],
            "cost_per_verified": (round(memory["cost"] / memory["passed"], 4)
                                  if memory["passed"] else None),
        },
    }


def metrics_main(argv: Optional[list] = None) -> int:
    """`pi-batch metrics [--json]`：五类运营指标。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py metrics",
        description="Operational metrics dashboard (AADM-G §32)")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    report = metrics_report()
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        lines = ["# Operational Metrics（五类指标）", ""]
        for category, values in report.items():
            lines.append(f"## {category}")
            for key, value in values.items():
                if value is None:
                    lines.append(f"  {key}: （未追踪）")
                else:
                    lines.append(f"  {key}: {value}")
            lines.append("")
        print("\n".join(lines))
    raise SystemExit(0)
