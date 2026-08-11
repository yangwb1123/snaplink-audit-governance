"""Dry-run 标准输出：`pi-batch probe`（AADM-R §5/§19/§20 落地）。

把"运行前探测"做成确定性标准报告（零 LLM）：
- intent / likelyScope / profile（7 维画像）/ evidence / unknowns /
  conflicts / candidateDirections / recommendedMode(L0-L3) /
  recommendedGraph / estimatedAgents / estimatedPaths /
  estimates(p50/p90/confidence) / confidence
- 估计必须带区间和置信度——拒绝"预计花费 10 分钟"式伪精确。
- --budget-* 时若预算低于最低质量所需 → status: infeasible_under_budget
  + minimum_required + cannot_remove（硬约束不可删除清单，§20）。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

# 最低质量所需的最小成本（无法再删的硬约束；§20 cannot_remove 语义）
CANNOT_REMOVE = ["security_verification", "migration_rollback_test",
                 "contract_test"]
BASE_TOKEN_COST = {"L0": 2000, "L1": 8000, "L2": 30000, "L3": 120000}
BASE_TIME_MIN = {"L0": 2, "L1": 8, "L2": 30, "L3": 120}
CONFLICT_PAIRS = [
    ("尽量少用外部依赖", "使用成熟框架"),
    ("快速上线", "生产级安全"),
    ("零成本", "高可用"),
    ("完全离线", "实时同步"),
]


def _estimate(level: str, multiplier: float) -> dict:
    """带区间与置信度的成本估计：p50 为基数，p90 ≈ 2.5×，低等级更确定。"""
    p50_tokens = BASE_TOKEN_COST[level] * multiplier
    p90_tokens = p50_tokens * 2.5
    confidence = {"L0": 0.9, "L1": 0.75, "L2": 0.6, "L3": 0.5}[level]
    return {
        "tokens": {"p50": int(p50_tokens), "p90": int(p90_tokens),
                   "confidence": confidence},
        "elapsed_minutes": {"p50": int(BASE_TIME_MIN[level] * multiplier),
                            "p90": int(BASE_TIME_MIN[level] * multiplier * 2.5),
                            "confidence": confidence},
    }


def probe_report(text: str, code_dir: str = "") -> dict:
    """确定性 Dry-run 探测：复用 profile/impact/atomize/rules 的信号。"""
    from .atomize import atomize
    from .impact import change_impact
    from .profile import task_profile

    atomized = atomize(text)
    impact = change_impact(text, code_dir)
    profile = task_profile(text)
    level = impact["work_level"]
    directions = (atomized.get("directions") or ["default"]
                  if isinstance(atomized.get("directions"), list)
                  else ["default"])
    actors = impact["required_agents"]
    conflicts = [pair for pair in CONFLICT_PAIRS
                 if pair[0] in text and pair[1] in text]
    unknowns = []
    if not code_dir:
        unknowns.append("code_dir 未提供：无法扫描现有代码证据")
    if not any(signal in text for signal in ("接口", "api", "数据库", "表")):
        unknowns.append("数据/接口契约未提及（scope 不完整）")
    graph = {"L0": "chain", "L1": "chain", "L2": "dag",
             "L3": "and-or-dag"}[level]
    multiplier = max(1.0, len(actors) * 0.5)
    return {
        "intent": atomized.get("summary", text[:60]),
        "likely_scope": {"files": 1 if level == "L0" else 3 if level == "L1"
                         else 8 if level == "L2" else 20,
                         "actors": actors},
        "profile": {k: v for k, v in profile.get("profile", {}).items()
                    if k in ("clarity", "scope", "coupling", "risk",
                             "uncertainty", "reversibility", "novelty")},
        "evidence_found": [],
        "unknowns": unknowns,
        "conflicts": conflicts,
        "candidate_directions": directions,
        "recommended_mode": level,
        "recommended_graph": graph,
        "estimated_agents": len(actors),
        "estimated_paths": max(1, len(directions)),
        "estimates": _estimate(level, multiplier),
        "confidence": {"L0": 0.85, "L1": 0.7, "L2": 0.55, "L3": 0.45}[level],
    }


def _budget_check(report: dict, budget_tokens: int, budget_minutes: int) -> dict:
    """§20：预算低于最低质量所需 → infeasible_under_budget（不伪装完成）。"""
    p50 = report["estimates"]["tokens"]["p50"]
    minimum = {"tokens": p50, "time_min": report["estimates"]
               ["elapsed_minutes"]["p50"]}
    limits = {"tokens": budget_tokens, "time_min": budget_minutes}
    violations = {k: v for k, v in limits.items() if v is not None
                  and minimum[k] > v}
    if not violations:
        return {"status": "feasible", "minimum_required": minimum,
                "current_limit": limits, "cannot_remove": CANNOT_REMOVE}
    return {"status": "infeasible_under_budget",
            "minimum_required": minimum, "current_limit": limits,
            "violations": violations, "cannot_remove": CANNOT_REMOVE}


def probe_main(argv: Optional[list] = None) -> int:
    """`pi-batch probe "任务" [--code DIR] [--budget-tokens N]
    [--budget-minutes N] [--json]`。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py probe",
        description="Dry-run 标准探测：确定性 ProbeReport（区间估计+置信度）")
    parser.add_argument("text", help="任务描述")
    parser.add_argument("--code", default="", help="代码目录（现有证据）")
    parser.add_argument("--budget-tokens", type=int, default=0,
                        help="预算上限（token）；低于最低质量 → infeasible")
    parser.add_argument("--budget-minutes", type=int, default=0,
                        help="预算上限（分钟）")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    report = probe_report(args.text, args.code)
    if args.budget_tokens or args.budget_minutes:
        report["budget"] = _budget_check(
            report, args.budget_tokens or None, args.budget_minutes or None)
        infeasible = report["budget"]["status"] == "infeasible_under_budget"
    else:
        infeasible = False
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"intent: {report['intent']}")
        print(f"mode: {report['recommended_mode']} | "
              f"graph: {report['recommended_graph']} | "
              f"agents: {report['estimated_agents']} | "
              f"paths: {report['estimated_paths']}")
        est = report["estimates"]
        print(f"estimate: {est['elapsed_minutes']['p50']}–"
              f"{est['elapsed_minutes']['p90']} min "
              f"(conf {est['elapsed_minutes']['confidence']}) | "
              f"{est['tokens']['p50']}–{est['tokens']['p90']} tokens "
              f"(conf {est['tokens']['confidence']})")
        for unknown in report["unknowns"]:
            print(f"unknown: {unknown}")
        for conflict in report["conflicts"]:
            print(f"conflict: {conflict}")
        if "budget" in report:
            budget = report["budget"]
            print(f"budget: {budget['status']}"
                  + (f"（violations: {budget['violations']}）"
                     if infeasible else ""))
            if infeasible:
                print("cannot_remove: " + ", ".join(budget["cannot_remove"]))
    raise SystemExit(1 if infeasible else 0)
