"""Pareto optimization & minimal unsatisfied core (AADM §15).

非简单任务不应直接选第一个方案：硬约束过滤 → Pareto 前沿（不被全面压制）
→ Utility 动态选择（权重随任务调整）。无可行方案时不编造结果，返回
Minimal Unsatisfied Core（如"不得修改公共 API"与"必须改变公共 API 结构"
无法同时满足——显式暴露，不静默忽略）。

纯函数，可单测；可接入 campaign/meta 的方案选择。
"""

from __future__ import annotations

import argparse
import itertools
import json
import sys
from typing import Optional


def pareto_frontier(candidates: list, objectives: tuple,
                    minimize: Optional[set] = None) -> list:
    """非支配前沿：不被其他方案在所有目标上全面压制的方案。

    candidates: [{id, scores: {objective: value}}]
    objectives: 参与比较的目标名（元组，保持顺序）
    minimize:   越小越好的目标集合（其余默认越大越好）"""
    minimize = set(minimize or ())
    frontier = []
    for candidate in candidates:
        dominated = False
        for other in candidates:
            if other is candidate:
                continue
            if _dominates(other, candidate, objectives, minimize):
                dominated = True
                break
        if not dominated:
            frontier.append(candidate)
    return frontier


def _dominates(a: dict, b: dict, objectives: tuple, minimize: set) -> bool:
    """a 支配 b：所有目标不差于 b，且至少一个严格更好。"""
    better_or_equal = True
    strictly_better = False
    for objective in objectives:
        av = a["scores"].get(objective, 0.0)
        bv = b["scores"].get(objective, 0.0)
        if objective in minimize:
            av, bv = -av, -bv
        if av > bv:
            strictly_better = True
        elif av < bv:
            better_or_equal = False
            break
    return better_or_equal and strictly_better


def select_by_utility(frontier: list, weights: dict,
                      minimize: Optional[set] = None) -> dict:
    """Utility(p) = Σ wᵢ·scoreᵢ（minimize 目标取负）。"""
    minimize = set(minimize or ())
    best = None
    best_utility = float("-inf")
    for candidate in frontier:
        utility = 0.0
        for objective, weight in weights.items():
            value = candidate["scores"].get(objective, 0.0)
            if objective in minimize:
                value = -value
            utility += weight * value
        if utility > best_utility:
            best, best_utility = candidate, utility
    return {**best, "utility": round(best_utility, 3)}


def select_with_quality_floor(candidates: list, quality: str,
                               floor: float, weights: dict,
                               minimize: Optional[set] = None) -> dict:
    """§19 质量底线：先满足 Quality ≥ Q_min，再在达标者中按 Utility 选优。

    quality: 质量目标名；floor: 最低质量分。无达标者 → 返回
    {status: infeasible_under_floor}（不降质量凑数）。"""
    feasible = [c for c in candidates
                if c["scores"].get(quality, 0.0) >= floor]
    if not feasible:
        return {"status": "infeasible_under_floor",
                "quality_floor": floor,
                "best_quality": max(c["scores"].get(quality, 0.0)
                                    for c in candidates) if candidates else 0}
    selected = select_by_utility(feasible, weights, minimize)
    return {**selected, "status": "feasible", "quality_floor": floor}


def minimal_unsatisfied_core(candidates: list) -> list:
    """最小冲突集合：找不到任何可行方案时，返回**无法被任何方案同时满足**
    的最小约束子集——每个候选都违反其中至少一条。

    例如"不得修改公共 API"与"必须改变公共 API 结构"相互冲突：单独看都
    可满足，合起来无方案可行 → MUC = {A, B}，显式暴露而非静默忽略。
    有界搜索（约束数 ≤ 14 穷举，否则贪心近似）。"""
    constraints = sorted({c for candidate in candidates
                          for c in candidate.get("violated", [])})
    if not constraints or any(not candidate.get("violated")
                              for candidate in candidates):
        return []  # 存在可行方案
    for size in range(1, len(constraints) + 1):
        if size > 14:  # 有界：大集合退化为贪心
            return _greedy_core(candidates, constraints)
        for subset in itertools.combinations(constraints, size):
            if all(set(candidate.get("violated", [])) & set(subset)
                   for candidate in candidates):
                return sorted(subset)
    return constraints


def _greedy_core(candidates: list, constraints: list) -> list:
    """贪心：逐条加入命中候选最多的约束，直到覆盖全部候选（近似 MUC）。"""
    covered = [False] * len(candidates)
    core = []
    remaining = set(constraints)
    while remaining and not all(covered):
        best = None
        best_gain = -1
        for constraint in remaining:
            gain = sum(1 for index, candidate in enumerate(candidates)
                       if not covered[index]
                       and constraint in candidate.get("violated", []))
            if gain > best_gain:
                best, best_gain = constraint, gain
        if best is None or best_gain <= 0:
            break
        remaining.discard(best)
        core.append(best)
        for index, candidate in enumerate(candidates):
            if best in candidate.get("violated", []):
                covered[index] = True
    return sorted(core)


def pareto_main(argv: Optional[list] = None) -> int:
    """`pi-batch pareto --input FILE [--json]`：前沿 + Utility 选择 + MUC。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py pareto",
        description="Pareto frontier + utility selection + MUC (AADM §15)")
    parser.add_argument("--input", required=True,
                        help="JSON: {objectives, minimize, weights, "
                             "candidates: [{id, scores, violated?}]}")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--quality", default="",
                        help="质量底线目标名（§19：Quality ≥ floor 先于资源）")
    parser.add_argument("--quality-floor", type=float, default=0.0,
                        help="最低质量分；无达标者 → infeasible_under_floor")
    args = parser.parse_args(argv)
    try:
        data = json.loads(__import__("pathlib").Path(
            args.input).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        raise SystemExit(2)
    candidates = data.get("candidates", [])
    objectives = tuple(data.get("objectives", []))
    minimize = set(data.get("minimize", []))
    frontier = pareto_frontier(candidates, objectives, minimize)
    weights = data.get("weights", {})
    chosen = select_by_utility(frontier, weights, minimize)
    if args.quality and args.quality_floor > 0:
        chosen = select_with_quality_floor(
            frontier, args.quality, args.quality_floor, weights, minimize)
    core = minimal_unsatisfied_core(candidates)
    report = {"frontier": [c["id"] for c in frontier],
              "chosen": chosen.get("id"), "utility": chosen.get("utility"),
              "quality_floor": args.quality_floor if args.quality else None,
              "minimal_unsatisfied_core": core}
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"frontier: {', '.join(report['frontier'])}")
        print(f"chosen  : {report['chosen']} (utility {report['utility']})")
        if core:
            print("MUC     : 无可行方案——以下约束无法同时满足: "
                  + ", ".join(core))
    raise SystemExit(0)
