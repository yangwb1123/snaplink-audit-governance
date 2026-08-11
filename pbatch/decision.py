"""Lexicographic decision & evidence arbitration (AADM-G §5/§13/§14).

字典序分层决策：低成本不能补偿错误结果，高性能也不能补偿越权——
安全/法律/权限/数据完整性 > 业务正确性与验收 > 用户目标与体验 > 可维护
性/性能 > 时间/Token/成本。

证据仲裁：多数投票不能代替验证。仲裁顺序：
形式化约束 > 确定性工具输出 > 运行测试 > 独立实验 > 独立 Reviewer
> 多数意见 > 执行 Agent 的自我评价。

纯函数，可单测。
"""

from __future__ import annotations

from typing import Optional

# 默认分层顺序（AADM-G §5）。
LAYER_ORDER = ("safety", "correctness", "user_experience",
               "maintainability", "resources")

# 仲裁顺序（AADM-G §14）：数字越小优先级越高。
SOURCE_ORDER = ("formal", "tool", "test", "experiment", "reviewer",
                "majority", "self")


def select_lexicographic(candidates: list, layer_order: tuple = LAYER_ORDER,
                         minimize: Optional[set] = None) -> dict:
    """字典序决策：逐层比较，第一层有差异即定胜负（不分层加总分）。

    candidates: [{id, layers: {layer: score}}]；minimize 层越小越好。"""
    minimize = set(minimize or ())
    best = None
    for candidate in candidates:
        if best is None:
            best = candidate
            continue
        for layer in layer_order:
            a = candidate["layers"].get(layer, 0.0)
            b = best["layers"].get(layer, 0.0)
            if layer in minimize:
                a, b = -a, -b
            if a != b:
                if a > b:
                    best = candidate
                break
    return best


def arbitrate(claims: list, source_order: tuple = SOURCE_ORDER) -> dict:
    """证据仲裁：按来源优先级取最高层裁决；同层内取多数意见。

    claims: [{id, source_type, verdict}]（verdict 任意可比较值）。"""
    if not claims:
        return {"verdict": None, "source": None}
    rank = {source: index for index, source in enumerate(source_order)}
    highest = min(rank.get(claim.get("source_type", "self"), 99)
                  for claim in claims)
    top = [claim for claim in claims
           if rank.get(claim.get("source_type", "self"), 99) == highest]
    votes = {}
    for claim in top:
        verdict = claim.get("verdict")
        votes[verdict] = votes.get(verdict, 0) + 1
    winner = max(votes, key=votes.get)
    return {"verdict": winner, "source": source_order[highest],
            "votes": votes, "tie_breaker_note":
                "同层平票时不升级——确定性证据不足，回到 Reviewer/人工"}


def quality_floor_met(scores: dict, floor: dict) -> tuple:
    """质量底线（AADM-R §19 第一/二层）：硬约束 + 最低质量先于资源优化。"""
    missing = [key for key, minimum in floor.items()
               if (scores.get(key) or 0) < minimum]
    return (not missing, missing)
