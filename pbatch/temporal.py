"""Temporal validity & evidence decay (AADM-G §8).

四种时间（事件/处理/业务有效/入库）+ 证据时效：容易变化的信息置信度
随时间衰减（static 不衰减 / slow_changing 半衰期 7 天 / volatile 半衰期
6 小时）。世界状态、规则、接口文档与外部服务状态都不能被认为永久有效。

Usage:
    pi-batch temporal decay --validity volatile --confidence 0.9 --age-hours 12
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from typing import Optional

FRESHNESS_CLASSES = ("static", "slow_changing", "volatile")
# 半衰期（秒）：volatile 6h / slow_changing 7d / static 不衰减。
HALF_LIVES = {"volatile": 6 * 3600, "slow_changing": 7 * 86400,
              "static": None}


def decayed_confidence(confidence: float, freshness_class: str,
                       age_seconds: float) -> float:
    """指数衰减：C(t) = C0 × 0.5^(t / half_life)。"""
    if confidence < 0 or confidence > 1:
        raise ValueError("confidence must be in [0, 1]")
    if freshness_class not in FRESHNESS_CLASSES:
        raise ValueError(f"freshness_class must be one of {FRESHNESS_CLASSES}")
    half_life = HALF_LIVES[freshness_class]
    if half_life is None or age_seconds <= 0:
        return round(confidence, 3)
    decayed = confidence * (0.5 ** (age_seconds / half_life))
    return round(max(0.0, decayed), 3)


def is_fresh(confidence: float, threshold: float = 0.5) -> bool:
    """衰减后置信度仍高于阈值的证据视为新鲜。"""
    return confidence >= threshold


def temporal_main(argv: Optional[list] = None) -> int:
    """`pi-batch temporal decay --validity X --confidence N --age-hours H`。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py temporal",
        description="Temporal validity & evidence decay (AADM-G §8)")
    parser.add_argument("--validity", default="volatile",
                        choices=FRESHNESS_CLASSES)
    parser.add_argument("--confidence", type=float, default=0.9)
    parser.add_argument("--age-hours", type=float, default=1.0)
    parser.add_argument("--threshold", type=float, default=0.5)
    args = parser.parse_args(argv)
    decayed = decayed_confidence(args.confidence, args.validity,
                                 args.age_hours * 3600)
    print(f"decayed: {args.confidence} → {decayed} "
          f"({args.validity}, {args.age_hours:.1f}h 后)")
    print(f"fresh: {is_fresh(decayed, args.threshold)} "
          f"(threshold {args.threshold})")
    raise SystemExit(0 if is_fresh(decayed, args.threshold) else 1)
