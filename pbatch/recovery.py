"""Recovery & error budget helpers (AADM-G §35.4/§35.5).

- 回滚 vs 前向修复：回滚不总是最优——代码变更通常前向修复 + 回归测试
  更便宜安全；DB migration 需可逆配对。比较 RollbackCost 与
  ForwardFixCost + ResidualRisk 后决定。
- 错误预算：给治理强度设预算（如每 N 个任务允许 1 次未经人工门禁的高
  风险操作）。预算耗尽 → 强制降级/人工门禁；富余 → 允许更激进自动执行。
  防止"永远 L3"与"永远 L0"两个极端。JSONL 账本。
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Optional

BUDGET_FILE = Path(".pi-batch") / "error_budget.jsonl"


def recovery_decision(rollback_cost: float, forward_fix_cost: float,
                      residual_risk: float = 0.0,
                      rollback_ready: bool = True) -> dict:
    """回滚 vs 前向修复决策（§35.4）。

    rollback_ready 且 rollback_cost < 1.5×forward_fix_cost → 回滚；
    否则前向修复 + 回归（代码变更通常如此）。"""
    forward_total = forward_fix_cost * (1 + residual_risk)
    prefer_rollback = (rollback_ready
                       and rollback_cost < forward_total * 1.5)
    return {
        "decision": "rollback" if prefer_rollback else "forward_fix",
        "rollback_cost": rollback_cost,
        "forward_fix_cost": forward_total,
        "residual_risk": residual_risk,
        "rationale": ("回滚更便宜且已就绪" if prefer_rollback
                      else "前向修复 + 回归测试（回滚成本更高或未就绪）"),
    }


def consume_budget(reason: str, cost: float = 1.0,
                   path: Path = BUDGET_FILE) -> dict:
    """记录一次治理预算消耗（追加式账本）。"""
    record = {"reason": " ".join(reason.split())[:200], "cost": float(cost),
              "at": time.strftime("%Y-%m-%dT%H:%M:%S")}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return record


def budget_status(allowance: float, path: Path = BUDGET_FILE) -> dict:
    """错误预算状态：consumed / remaining / exhausted（有界读取）。"""
    consumed = 0.0
    records = []
    if path.exists():
        try:
            handle = path.open("r", encoding="utf-8")
        except OSError:
            handle = None
        if handle is not None:
            with handle:
                total = 0
                for line in handle:
                    total += len(line.encode("utf-8", errors="replace"))
                    if total > 4 * 1024 * 1024:
                        break
                    if len(line) > 256 * 1024:
                        continue
                    try:
                        record = json.loads(line)
                    except ValueError:
                        continue
                    if isinstance(record, dict):
                        consumed += float(record.get("cost", 0))
                        records.append(record)
    remaining = max(0.0, allowance - consumed)
    return {
        "allowance": allowance, "consumed": round(consumed, 3),
        "remaining": round(remaining, 3),
        "exhausted": remaining <= 0,
        "records": records[-10:],
        "verdict": ("budget_exhausted→强制降级/人工门禁" if remaining <= 0
                    else ("budget_low→收紧自动执行" if remaining < allowance * 0.3
                          else "budget_healthy→允许按模式自动执行")),
    }


def recovery_main(argv: Optional[list] = None) -> int:
    """`pi-batch recovery --rollback-cost N --forward-cost N [--residual-risk R]`。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py recovery",
        description="Rollback vs forward-fix decision (AADM-G §35.4)")
    parser.add_argument("--rollback-cost", type=float, required=True)
    parser.add_argument("--forward-cost", type=float, required=True)
    parser.add_argument("--residual-risk", type=float, default=0.0)
    parser.add_argument("--no-rollback-ready", action="store_true")
    args = parser.parse_args(argv)
    decision = recovery_decision(args.rollback_cost, args.forward_cost,
                                 args.residual_risk,
                                 rollback_ready=not args.no_rollback_ready)
    print(f"decision: {decision['decision']} — {decision['rationale']}")
    raise SystemExit(0)


def budget_main(argv: Optional[list] = None) -> int:
    """`pi-batch budget consume|report`：错误预算账本（§35.5）。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py budget",
        description="Governance error budget (AADM-G §35.5)")
    sub = parser.add_subparsers(dest="command", required=True)
    p_consume = sub.add_parser("consume", help="记录一次治理预算消耗")
    p_consume.add_argument("--reason", required=True)
    p_consume.add_argument("--cost", type=float, default=1.0)
    p_report = sub.add_parser("report", help="预算状态")
    p_report.add_argument("--allowance", type=float, default=5.0)
    p_report.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    if args.command == "consume":
        record = consume_budget(args.reason, args.cost)
        print(f"consumed: {record['cost']} — {record['reason']}")
        raise SystemExit(0)
    else:
        report = budget_status(args.allowance)
        if args.json:
            print(json.dumps({k: v for k, v in report.items()
                              if k != "records"}, indent=2))
        else:
            print(f"allowance={report['allowance']} "
                  f"consumed={report['consumed']} "
                  f"remaining={report['remaining']}")
            print(f"verdict: {report['verdict']}")
            if report["exhausted"]:
                print("⚠ 预算耗尽：强制降级模式 / 强制人工门禁")
    raise SystemExit(1 if report["exhausted"] else 0)
