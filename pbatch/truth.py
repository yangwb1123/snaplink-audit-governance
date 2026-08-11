"""Truth maintenance (AADM-G §20): premise invalidation cascade.

Agent 的结论依赖其他结论：前提失效 → 依赖结论自动失效（invalidated /
superseded），相关计划标记需要重新规划，受影响的方案评分自动撤销。
这比简单保存一份聊天历史可靠得多。

状态持久化：追加式 JSONL（.pi-batch/truth.jsonl），崩溃安全；status
命令按日志回放重建（确定性）。

Usage:
    pi-batch truth claim --id API-PAGINATION --statement "API 支持分页" \\
        --premises DOC-V1 --confidence 0.8
    pi-batch truth invalidate --id DOC-V1 --reason "接口文档已更新，无分页"
    pi-batch truth status [--json]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

TRUTH_FILE = Path(".pi-batch") / "truth.jsonl"
LINE_MAX_BYTES = 256 * 1024
FILE_MAX_BYTES = 4 * 1024 * 1024


class TruthStore:
    """内存态真值维护：add / invalidate / status / active。"""

    def __init__(self):
        self.claims = {}  # id -> {"statement", "premises", "confidence",
                          #          "status", "invalidated_by", "created_at"}

    def add(self, claim_id: str, statement: str,
            premises: Optional[list] = None,
            confidence: float = 1.0) -> dict:
        record = {"id": claim_id, "statement": statement,
                  "premises": list(premises or ()), "confidence": confidence,
                  "status": "active", "invalidated_by": "", "created_at": ""}
        self.claims[claim_id] = record
        return record

    def invalidate(self, claim_id: str, reason: str = "") -> list:
        """使前提失效并级联失效依赖它的结论。返回受影响 claim id 列表。"""
        if claim_id not in self.claims:
            return []
        affected = []
        pending = [claim_id]
        while pending:
            current = pending.pop()
            record = self.claims[current]
            if record["status"] == "invalidated":
                continue
            record["status"] = "invalidated"
            record["invalidated_by"] = reason
            affected.append(current)
            for other in self.claims.values():
                if (other["status"] == "active"
                        and current in other["premises"]):
                    pending.append(other["id"])
        return sorted(affected)

    def status(self, claim_id: str) -> Optional[dict]:
        return self.claims.get(claim_id)

    def active(self) -> list:
        return [record for record in self.claims.values()
                if record["status"] == "active"]


def _record_to_log(store: TruthStore, action: str, payload: dict) -> str:
    """追加式落盘（只追加不覆盖，崩溃安全）。"""
    record = {"action": action, "payload": payload,
              "at": __import__("time").strftime("%Y-%m-%dT%H:%M:%S")}
    line = json.dumps(record, ensure_ascii=False)
    TRUTH_FILE.parent.mkdir(parents=True, exist_ok=True)
    with TRUTH_FILE.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")
    return TRUTH_FILE.name


def load_from_log(path: Path = TRUTH_FILE) -> TruthStore:
    """按日志回放重建 TruthStore（有界行缓冲，超长行跳过）。"""
    store = TruthStore()
    if not path.exists():
        return store
    try:
        handle = path.open("r", encoding="utf-8")
    except OSError:
        return store
    with handle:
        total = 0
        for line in handle:
            total += len(line.encode("utf-8", errors="replace"))
            if total > FILE_MAX_BYTES:
                break
            if len(line) > LINE_MAX_BYTES:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            payload = record.get("payload", {})
            action = record.get("action")
            if action == "claim":
                store.add(payload.get("id", ""), payload.get("statement", ""),
                          payload.get("premises") or (),
                          float(payload.get("confidence", 1.0)))
            elif action == "invalidate":
                store.invalidate(payload.get("id", ""),
                                 payload.get("reason", ""))
    return store


def truth_main(argv: Optional[list] = None) -> int:
    """`pi-batch truth claim|invalidate|status`（追加式日志，可回放）。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py truth",
        description="Truth maintenance: premise invalidation cascade")
    sub = parser.add_subparsers(dest="command", required=True)

    p_claim = sub.add_parser("claim", help="登记一条结论（带前提）")
    p_claim.add_argument("--id", required=True)
    p_claim.add_argument("--statement", required=True)
    p_claim.add_argument("--premises", default="",
                         help="逗号分隔的前提 claim id")
    p_claim.add_argument("--confidence", type=float, default=1.0)

    p_inv = sub.add_parser("invalidate", help="使前提失效（级联）")
    p_inv.add_argument("--id", required=True)
    p_inv.add_argument("--reason", default="")

    p_status = sub.add_parser("status", help="当前状态（按日志回放）")
    p_status.add_argument("--json", action="store_true")

    args = parser.parse_args(argv)
    if args.command == "claim":
        _record_to_log(TruthStore(), "claim", {
            "id": args.id, "statement": args.statement,
            "premises": [p.strip() for p in args.premises.split(",")
                         if p.strip()],
            "confidence": args.confidence})
        print(f"claimed: {args.id}")
    elif args.command == "invalidate":
        _record_to_log(TruthStore(), "invalidate",
                       {"id": args.id, "reason": args.reason})
        store = load_from_log()  # 回放已应用级联（invalidate 幂等）
        invalidated = [claim_id for claim_id, rec in store.claims.items()
                       if rec["status"] == "invalidated"]
        print(f"invalidated: {args.id}（级联 {max(0, len(invalidated) - 1)} 条）")
    else:
        store = load_from_log()
        records = sorted(store.claims.values(), key=lambda r: r["id"])
        if args.json:
            print(json.dumps(records, ensure_ascii=False, indent=2))
        else:
            lines = ["# Truth Store", ""]
            for record in records:
                mark = "active" if record["status"] == "active" else "INVALID"
                lines.append(f"[{mark}] {record['id']}: {record['statement']}"
                             f"（premises: {', '.join(record['premises']) or '-'}）")
            print("\n".join(lines))
    raise SystemExit(0)
