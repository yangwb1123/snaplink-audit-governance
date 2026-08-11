"""Staged multi-path racing (AADM-R §12): successive-halving elimination.

多路径不直接全部实现：方案级 Dry-run → 最小 Spike → 代表性切片 → 完整
实现，每轮按当前信号淘汰一半（Successive Halving / Hyperband 思想，
AADM-D §35.1）。两条纪律：

1. 早期信号必须与最终质量正相关——淘汰基于当前阶段分数排序，而不是
   绝对阈值；
2. 信号噪声大时保守淘汰——候选带 confidence，低于阈值的阶段多保留一轮
   （按置信区间淘汰，而不是固定减半）。

纯函数，可单测；未来可接入 meta/campaign 的多方案生成。

Usage:
    pi-batch race --input candidates.json
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# 置信度低于该值时该轮不强制减半（保守淘汰）。
CONFIDENCE_FLOOR = 0.60


@dataclass
class RaceCandidate:
    """一个候选方案（技术路线），scores 记录各阶段得分。"""
    id: str
    description: str = ""
    scores: dict = field(default_factory=dict)
    eliminated: bool = False
    eliminated_at: str = ""


def successive_halving(candidates: list, stage_scores: dict,
                       keep_min: int = 1, confidence: Optional[dict] = None) -> list:
    """按当前阶段分数淘汰：保留前 ceil(N/2)（至少 keep_min）。

    confidence（可选）：{id: 0-1}；若幸存者中最高分候选的置信度低于
    CONFIDENCE_FLOOR，多保留一个（信号不可靠时不减半）。"""
    alive = [c for c in candidates if not c.eliminated]
    if len(alive) <= keep_min:
        return alive
    ranked = sorted(alive, key=lambda c: -stage_scores.get(c.id, 0.0))
    keep = (len(ranked) + 1) // 2
    keep = max(keep, keep_min)
    confidence = confidence or {}
    top_confidence = confidence.get(ranked[0].id, 1.0)
    if top_confidence < CONFIDENCE_FLOOR and keep < len(ranked):
        keep += 1  # 保守淘汰：低置信度时多保留一条路径
    for candidate in ranked[keep:]:
        candidate.eliminated = True
        candidate.eliminated_at = "current_stage"
    return ranked[:keep]


def staged_race(candidates: list, stages: list) -> list:
    """逐轮淘汰。stages: [{"name", "scores": {id: float}, "keep_min": int,
    "confidence": {id: float}}]。返回每轮幸存者历史。"""
    history = []
    for stage in stages:
        scores = stage.get("scores", {})
        survivors = successive_halving(
            candidates, scores,
            keep_min=stage.get("keep_min", 1),
            confidence=stage.get("confidence"))
        history.append({"stage": stage.get("name", "?"),
                        "alive": [c.id for c in survivors]})
    return history


def should_fork(report: dict) -> dict:
    """多路径分叉判据（AADM-G §26）：只在重要不确定性存在时启用。

    需同时满足：重要不确定性或新颖性高、存在真实技术分歧空间、沙箱可
    比较、统一验收、可提前淘汰、比较成本低于错误决策成本。不值得：需求
    明确、已有成熟模式、多路径改同一核心代码。"""
    profile = report.get("profile", {})
    uncertainty = profile.get("uncertainty", 0)
    novelty = profile.get("novelty", 0)
    risk = profile.get("risk", 0)
    reasons, blockers = [], []
    if uncertainty >= 0.5 or novelty >= 0.5:
        reasons.append(f"重要不确定性（U={uncertainty}，N={novelty}）")
    else:
        blockers.append("不确定性低：需求明确，不值得开多路径")
    if novelty < 0.3:
        blockers.append("已有成熟项目模式可循")
    if risk >= 0.8 and profile.get("reversibility", 1) < 0.3:
        blockers.append("高风险且难回滚：多路径竞争加剧风险")
    if blockers and not reasons:
        return {"fork": False, "reasons": [], "blockers": blockers}
    fork = not blockers and len(reasons) >= 1
    return {"fork": fork, "reasons": reasons, "blockers": blockers,
            "max_paths": 3 if uncertainty >= 0.6 else 2}


def load_race_input(path: str) -> tuple:
    """读取竞跑输入 JSON：{candidates: {id: desc}, stages: [...]}。"""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    candidates = [RaceCandidate(id=key, description=str(value))
                  for key, value in (data.get("candidates") or {}).items()]
    return candidates, data.get("stages", [])


def race_main(argv: Optional[list] = None) -> int:
    """`pi-batch race --input FILE [--json]`：分阶段竞跑淘汰。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py race",
        description="Staged multi-path racing (successive halving)")
    parser.add_argument("--input", required=True,
                        help="JSON: {candidates: {id: desc}, stages: "
                             "[{name, scores, keep_min?, confidence?}]}")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        candidates, stages = load_race_input(args.input)
    except (OSError, ValueError) as exc:
        print(f"invalid race input: {exc}", file=sys.stderr)
        raise SystemExit(2)
    if not candidates or not stages:
        print("race needs at least one candidate and one stage",
              file=sys.stderr)
        raise SystemExit(2)
    history = staged_race(candidates, stages)
    survivors = [c.id for c in candidates if not c.eliminated]
    report = {"history": history, "survivors": survivors,
              "eliminated": [c.id for c in candidates if c.eliminated]}
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        lines = ["# Staged Race", ""]
        for entry in history:
            lines.append(f"{entry['stage']:<12} 存活: "
                         + ", ".join(entry["alive"]))
        lines.append("")
        lines.append("胜出: " + ", ".join(survivors))
        lines.append("淘汰: " + ", ".join(report["eliminated"]) or "-")
        print("\n".join(lines))
    raise SystemExit(0)
