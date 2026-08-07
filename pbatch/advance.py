"""Self-driving iteration engine: `pi-batch advance`.

Scans a project across every spec dimension (UI spacing/colors/styles,
frontend engineering errors/complexity, backend engineering/architecture),
groups the findings into fine-grained per-dimension batches, prioritizes
them (P0 recoverability/security > P1 visual tokens > P2 architecture),
and records the iteration state so progress can resume round after round.

Pipeline per round:
  1. SCAN: run all registered checkers (--json) over the target dir
  2. GROUP: classify every violation into a dimension
  3. PRIORITIZE: P0 -> P1 -> P2 batches, each with samples + suggested fix
  4. RECORD: append to docs/advance/state.jsonl (per-round, per-dimension)
  5. LOOP: rerun until every dimension is clean or --max-rounds reached

Usage:
    pi-batch advance --dir ../snaplink-console          # plan (no changes)
    pi-batch advance --dir ../snaplink-console --json   # machine plan
    pi-batch advance --dir . --max-rounds 3             # round bookkeeping
    pi-batch advance --help

The engine produces the iteration PLAN (what to fix, in what order, per
dimension); the deterministic fixes and agent implementation reuse the
rest of the toolchain (validators, learn, assess, pipelines).
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from .config import log

STATE_DIR = Path("docs/advance")
STATE_FILE = STATE_DIR / "state.jsonl"

# Dimension registry: which checker output maps to which spec dimension.
DIMENSIONS = [
    {"id": "ui_spacing", "checker": "check-ui-spec.py", "mode": "spacing",
     "priority": 1, "label": "魔法间距（非 8pt token）",
     "fix": "吸附到最近 token（scripts/check-ui-spec.py --mode spacing 逐文件核对）"},
    {"id": "ui_color", "checker": "check-ui-spec.py", "mode": "color",
     "priority": 1, "label": "硬编码颜色",
     "fix": "提取为主题 token 类（AppColors / tokens），语义映射替换"},
    {"id": "ui_style", "checker": "check-ui-spec.py", "mode": "style",
     "priority": 1, "label": "inline style",
     "fix": "改用平台样式机制（styled/token/class）"},
    {"id": "fe_errors", "checker": "check-frontend-quality.py", "mode": "error",
     "priority": 0, "label": "前端工程错误（吞异常/不安全 html/测试跳过）",
     "fix": "吞异常加日志；不安全 html 改安全渲染；去掉测试 skip"},
    {"id": "fe_n1", "checker": "check-frontend-quality.py", "mode": "n1",
     "priority": 0, "label": "循环 await（N+1）",
     "fix": "Future.wait/Promise.all 并行化或批量接口"},
    {"id": "fe_complexity", "checker": "check-frontend-quality.py", "mode": "strict",
     "priority": 2, "label": "上帝文件/复杂度/嵌套",
     "fix": "按业务能力拆组件/提 helper（克制：内聚的 API 客户端保留）"},
    {"id": "be_errors", "checker": "check-backend-quality.py", "mode": "error",
     "priority": 0, "label": "后端工程错误（吞异常/危险 DDL/依赖方向）",
     "fix": "加日志/迁移文件/修正依赖方向"},
    {"id": "be_architecture", "checker": "check-backend-quality.py", "mode": "strict",
     "priority": 2, "label": "上帝服务/单实现接口/复杂度",
     "fix": "组合优于继承、去无意义抽象（design-patterns 决策表）"},
]

_PRIORITY_LABEL = {0: "P0", 1: "P1", 2: "P2"}


def _checker_script(name: str) -> str:
    """Locate a checker script (repo scripts/ or tools/ai-dev-gates/)."""
    candidates = [
        Path("scripts") / name,
        Path("tools/ai-dev-gates") / name,
        Path(__file__).resolve().parent.parent / "scripts" / name,
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return str(candidates[0])


def _run_checker(script: str, target: str) -> dict:
    """Run one checker with --json and parse the report (empty on failure)."""
    try:
        result = subprocess.run(
            [sys.executable, script, "--dir", target, "--all" if "ui-spec" in script else "--strict",
             "--json"], capture_output=True, text=True, timeout=300)
    except FileNotFoundError:
        return {"files_scanned": 0, "violations": []}
    try:
        payload = json.loads(result.stdout or "{}")
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {"files_scanned": 0, "violations": []}


def classify_violation(detail: str) -> str:
    """Map one violation detail line to a dimension id."""
    if "swallowed exception" in detail or "unsafe innerHTML" in detail \
            or ".skip/.only" in detail:
        return "fe_errors"
    if "without assertions" in detail:
        return "be_errors"
    if "N+1" in detail or "loop body" in detail:
        return "fe_n1"
    if "nesting depth" in detail or "god-file" in detail \
            or "decision points" in detail or "state hooks" in detail \
            or "event handlers" in detail or "api calls" in detail \
            or "constructor deps" in detail or "public methods" in detail \
            or "single-implementation" in detail or "Base class" in detail:
        return "fe_complexity"
    if "spacing" in detail:
        return "ui_spacing"
    if "color" in detail:
        return "ui_color"
    if "style" in detail:
        return "ui_style"
    if "swallow" in detail or "DDL" in detail or "dependency direction" in detail \
            or "status assignment" in detail:
        return "be_errors"
    return "be_architecture"


def scan_project(target: str) -> dict:
    """Run all registered checkers; returns per-dimension findings."""
    ui_script = _checker_script("check-ui-spec.py")
    fe_script = _checker_script("check-frontend-quality.py")
    be_script = _checker_script("check-backend-quality.py")
    reports = {
        "ui": _run_checker(ui_script, target),
        "fe": _run_checker(fe_script, target),
        "be": _run_checker(be_script, target),
    }
    dimensions = {}
    for dim in DIMENSIONS:
        dimensions[dim["id"]] = {"label": dim["label"], "priority": dim["priority"],
                                 "fix": dim["fix"], "count": 0, "files": set(), "samples": []}
    for report in reports.values():
        for violation in report.get("violations", []):
            detail = str(violation.get("detail", ""))
            dim_id = classify_violation(detail)
            dim = dimensions.get(dim_id)
            if dim is None:
                continue
            dim["count"] += 1
            dim["files"].add(str(violation.get("file", "")))
            if len(dim["samples"]) < 3:
                dim["samples"].append(detail[:90])
    for dim in dimensions.values():
        dim["files"] = sorted(dim["files"])
    return {"target": target, "files_scanned": reports["ui"].get("files_scanned", 0),
            "dimensions": dimensions}


def build_batches(scan: dict) -> list:
    """P0 -> P1 -> P2 batches: one batch per dimension with findings."""
    dims = sorted(scan["dimensions"].values(),
                  key=lambda d: (d["priority"], -d["count"]))
    return [
        {"priority": _PRIORITY_LABEL[d["priority"]], "id": dim_id,
         "count": d["count"], "label": d["label"], "fix": d["fix"],
         "files": d["files"][:8], "samples": d["samples"]}
        for dim_id, d in sorted(scan["dimensions"].items(),
                                key=lambda kv: (kv[1]["priority"], -kv[1]["count"]))
        if d["count"] > 0
    ]


def record_round(scan: dict, round_no: int) -> None:
    """Append one round's per-dimension state (resumable, append-only)."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    state_file = STATE_DIR / "state.jsonl"
    entry = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "round": round_no,
        "target": scan["target"],
        "total": sum(d["count"] for d in scan["dimensions"].values()),
        "dimensions": {dim_id: d["count"] for dim_id, d in scan["dimensions"].items()},
    }
    with state_file.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


def format_plan(batches: list, total: int) -> str:
    """Human-readable iteration plan (per-dimension batches, prioritized)."""
    lines = [f"## 迭代计划（共 {total} 项，按维度分批）"]
    if not batches:
        lines.append("全部维度干净——无需推进。")
        return "\n".join(lines)
    for batch in batches:
        lines.append(f"\n[{batch['priority']}] {batch['label']}（{batch['count']} 项）")
        lines.append(f"  建议: {batch['fix']}")
        for sample in batch["samples"]:
            lines.append(f"  示例: {sample}")
        if batch["files"]:
            lines.append("  文件: " + ", ".join(batch["files"][:5]) +
                         ("…" if len(batch["files"]) > 5 else ""))
    return "\n".join(lines)


def advance_main(argv: list) -> None:
    """`pi-batch advance [--dir PATH] [--json] [--max-rounds N] [--round R]`."""
    import argparse
    parser = argparse.ArgumentParser(
        prog="pi-batch.py advance",
        description="Self-driving iteration engine: scan every spec dimension, "
                    "group into prioritized batches, record round state.")
    parser.add_argument("--dir", default=".", help="target project directory")
    parser.add_argument("--json", action="store_true", help="machine-readable plan")
    parser.add_argument("--max-rounds", type=int, default=0,
                        help="record round bookkeeping (0 = scan only)")
    parser.add_argument("--round", type=int, default=1, help="round number")
    args = parser.parse_args(argv)
    scan = scan_project(args.dir)
    batches = build_batches(scan)
    if args.max_rounds > 0:
        for r in range(args.round, args.round + args.max_rounds):
            record_round(scan, r)
        log.info("advance: recorded %d round(s) in %s", args.max_rounds, STATE_FILE)
    if args.json:
        print(json.dumps({"target": scan["target"],
                          "files_scanned": scan["files_scanned"],
                          "total": sum(d["count"] for d in scan["dimensions"].values()),
                          "batches": batches}, ensure_ascii=False, indent=2))
        return
    print(format_plan(batches, sum(d["count"] for d in scan["dimensions"].values())))
