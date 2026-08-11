"""Causal hypothesis management (AADM-G §6, 15 条之 7).

很多错误源于"看到症状就套方案"（接口慢→加缓存、数据库压力高→分库分表、
页面卡顿→Web Worker）。正确路径：现象 → 可能原因 → 可观测证据 → 最小
干预实验 → 结果 → 更新因果模型。

Hypothesis 生命周期：unverified → supported / rejected / inconclusive。
持久化：追加式 JSONL（.pi-batch/causality.jsonl），可回放。

Usage:
    pi-batch causal new --symptom "接口慢" --cause "缓存缺失" \\
        --intervention "加缓存" --expected "P95 下降 50%"
    pi-batch causal evidence --id H-1 --supports "命中率 90%"
    pi-batch causal evidence --id H-1 --refutes "P95 无变化"
    pi-batch causal status [--json]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from pathlib import Path
from typing import Optional

CAUSALITY_FILE = Path(".pi-batch") / "causality.jsonl"
LINE_MAX = 256 * 1024
FILE_MAX = 4 * 1024 * 1024

STATUSES = ("unverified", "supported", "rejected", "inconclusive")


def new_hypothesis(symptom: str, proposed_cause: str,
                   intervention: str = "", expected_observation: str = "") -> dict:
    """登记一个因果假设（先测量，后干预）。"""
    return {
        "id": "H-" + uuid.uuid4().hex[:8],
        "symptom": " ".join(symptom.split())[:200],
        "proposed_cause": " ".join(proposed_cause.split())[:200],
        "supporting_evidence": [],
        "contradicting_evidence": [],
        "intervention": intervention,
        "expected_observation": expected_observation,
        "confidence": 0.3,  # 未经证据支持的因果判断只有低置信度
        "status": "unverified",
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def apply_evidence(hypothesis: dict, supports: str = "", refutes: str = "",
                   measurement_note: str = "") -> dict:
    """按证据更新状态与置信度（支持 +0.3，反驳 -0.4，含测量建议）。"""
    if supports:
        hypothesis["supporting_evidence"].append(supports)
        hypothesis["confidence"] = min(1.0,
                                       hypothesis["confidence"] + 0.3)
    if refutes:
        hypothesis["contradicting_evidence"].append(refutes)
        hypothesis["confidence"] = max(0.05,
                                       hypothesis["confidence"] - 0.4)
    if measurement_note:
        hypothesis.setdefault("measurements", []).append(measurement_note)
    if hypothesis["supporting_evidence"] and hypothesis["contradicting_evidence"]:
        hypothesis["status"] = "inconclusive"  # 双向证据：未定论
    elif hypothesis["contradicting_evidence"]:
        hypothesis["status"] = "rejected"
    elif hypothesis["supporting_evidence"] and hypothesis["confidence"] >= 0.6:
        hypothesis["status"] = "supported"
    elif hypothesis["supporting_evidence"]:
        hypothesis["status"] = "inconclusive"
    return hypothesis


def _append(record: dict) -> str:
    CAUSALITY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with CAUSALITY_FILE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return CAUSALITY_FILE.name


def load_hypotheses(path: Path = CAUSALITY_FILE) -> dict:
    """按日志回放重建假设库（有界行缓冲）。"""
    hypotheses = {}
    if not path.exists():
        return hypotheses
    try:
        handle = path.open("r", encoding="utf-8")
    except OSError:
        return hypotheses
    with handle:
        total = 0
        for line in handle:
            total += len(line.encode("utf-8", errors="replace"))
            if total > FILE_MAX:
                break
            if len(line) > LINE_MAX:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            action = record.get("action")
            payload = record.get("payload", {})
            if action == "new":
                hypothesis = new_hypothesis(payload.get("symptom", ""),
                                            payload.get("cause", ""),
                                            payload.get("intervention", ""),
                                            payload.get("expected", ""))
                hypothesis["id"] = payload.get("id", hypothesis["id"])
                hypotheses[hypothesis["id"]] = hypothesis
            elif action == "evidence" and payload.get("id") in hypotheses:
                apply_evidence(hypotheses[payload["id"]],
                               payload.get("supports", ""),
                               payload.get("refutes", ""),
                               payload.get("measurement", ""))
    return hypotheses


def causality_main(argv: Optional[list] = None) -> int:
    """`pi-batch causal new|evidence|status`。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py causal",
        description="Causal hypothesis lifecycle (AADM-G §6)")
    sub = parser.add_subparsers(dest="command", required=True)

    p_new = sub.add_parser("new", help="登记因果假设（先测量后干预）")
    p_new.add_argument("--symptom", required=True)
    p_new.add_argument("--cause", required=True)
    p_new.add_argument("--intervention", default="")
    p_new.add_argument("--expected", default="")

    p_ev = sub.add_parser("evidence", help="记录支持/反驳证据")
    p_ev.add_argument("--id", required=True)
    p_ev.add_argument("--supports", default="")
    p_ev.add_argument("--refutes", default="")
    p_ev.add_argument("--measurement", default="",
                     help="测量结果（如主线程耗时/命中率）")

    p_status = sub.add_parser("status", help="全部假设状态")
    p_status.add_argument("--json", action="store_true")

    args = parser.parse_args(argv)
    if args.command == "new":
        hypothesis = new_hypothesis(args.symptom, args.cause,
                                    args.intervention, args.expected)
        _append({"action": "new", "payload": {
            "id": hypothesis["id"], "symptom": args.symptom,
            "cause": args.cause, "intervention": args.intervention,
            "expected": args.expected}})
        print(f"new: {hypothesis['id']} — {args.cause}")
        print("先测量（主线程/DOM/延迟…）再干预，勿见症状即套方案")
    elif args.command == "evidence":
        _append({"action": "evidence", "payload": {
            "id": args.id, "supports": args.supports,
            "refutes": args.refutes, "measurement": args.measurement}})
        store = load_hypotheses()
        hypothesis = store.get(args.id)
        if hypothesis is None:
            print(f"unknown hypothesis {args.id}", file=sys.stderr)
            raise SystemExit(2)
        print(f"{args.id}: {hypothesis['status']} "
              f"(confidence {hypothesis['confidence']:.2f})")
    else:
        _cmd_status(args)
    raise SystemExit(0)


def _cmd_status(args) -> None:
    """列出全部因果假设（按日志回放）。"""
    store = load_hypotheses()
    records = [store[key] for key in sorted(store)]
    if args.json:
        print(json.dumps(records, ensure_ascii=False, indent=2))
        return
    lines = ["# Causal Hypotheses"]
    for record in records:
        lines.append(f"[{record['status']:>12}] {record['id']}: "
                     f"{record['symptom']} ← {record['proposed_cause']}"
                     f"（conf {record['confidence']:.2f}）")
    print("\n".join(lines))
