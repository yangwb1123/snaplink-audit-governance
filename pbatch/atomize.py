"""Prompt atomization (AADM §2, MUST #1: 先原子化，不能直接转代码).

把需求文本确定性拆成认知原子：目标/角色/操作对象/行为/约束/未知/风险/
验收/假设。每个原子带来源（source=text）、置信度与硬度——Agent 不能把
自己的推断当成用户给出的事实。可选 --graph 把原子组装成超图 JSON。

Usage:
    pi-batch atomize "审批人可以拒绝文件申请，审批完成后必须记录审计" --json
    pi-batch atomize "..." --graph out.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Optional

# 原子类型（AADM §2 子集：确定性可提取的 9 类）。
ATOM_TYPES = ("goal", "actor", "object", "operation", "constraint",
              "unknown", "risk", "acceptance", "assumption")

_SIGNALS = {
    "goal": ("实现", "开发", "做一个", "目标", "完成", "支持", "增加",
             "build", "implement", "create", "support", "add"),
    "actor": ("用户", "角色", "管理员", "审批人", "申请人", "操作员",
              "业务员", "user", "admin", "approver"),
    "object": ("文件", "订单", "审批单", "审计", "通知", "表单", "页面",
               "接口", "数据", "file", "order", "approval"),
    "operation": ("创建", "通过", "拒绝", "撤回", "提交", "审批", "发送",
                  "记录", "查询", "导入", "导出", "重构", "消除", "修复",
                  "拆分", "优化", "重构", "改造", "升级", "重构",
                  "create", "approve", "submit", "send", "record",
                  "refactor", "fix", "optimize", "split"),
    "constraint": ("必须", "不得", "禁止", "不能", "需要", "必须兼容",
                   "must", "must not", "required", "forbidden"),
    "unknown": ("是否", "未知", "待确认", "可能", "暂定", "未定", "不确定",
                "unknown", "tbd", "whether"),
    "risk": ("审批", "支付", "删除", "作废", "迁移", "生产", "权限",
             "审计", "approve", "delete", "payment", "permission"),
    "acceptance": ("验收", "测试", "完成标准", "通过标准", "checklist",
                   "acceptance", "criteria"),
    "assumption": ("默认", "假设", "假定", "暂按", "assume", "assuming"),
}

# 约束词 → 硬度（用户明确说"必须/不得"= hard；"需要"= required）。
_HARDNESS = {"必须": "hard", "不得": "hard", "禁止": "hard", "must": "hard",
             "must not": "hard", "forbidden": "hard",
             "需要": "required", "required": "required"}


def atomize(text: str) -> dict:
    """确定性原子化：按信号词提取命题 + 元数据（来源/置信度/硬度）。"""
    lowered = (text or "").lower()
    atoms = []
    seen = set()
    for atom_type, signals in _SIGNALS.items():
        for signal in signals:
            if signal not in (text or "") and signal not in lowered:
                continue
            proposition = _proposition_for(atom_type, signal, text)
            key = (atom_type, proposition)
            if key in seen:
                continue
            seen.add(key)
            hardness = "advisory"
            if atom_type == "constraint":
                hardness = _HARDNESS.get(signal, "required")
            elif atom_type == "goal":
                hardness = "required"
            atoms.append({
                "type": atom_type, "proposition": proposition,
                "source": "text", "confidence": 0.7,
                "hardness": hardness, "scope": "task",
            })
    return {"atoms": atoms, "count": len(atoms)}


def _proposition_for(atom_type: str, signal: str, text: str) -> str:
    """命题：约束/未知/假设保留信号词上下文；其余按类型固定模板。"""
    if atom_type in ("constraint", "unknown", "assumption"):
        return signal
    if atom_type == "goal":
        return f"目标：{signal}"
    if atom_type == "actor":
        return f"角色：{signal}"
    if atom_type == "object":
        return f"操作对象：{signal}"
    if atom_type == "operation":
        return f"行为：{signal}"
    if atom_type == "risk":
        return f"风险域：{signal}"
    if atom_type == "acceptance":
        return f"验收条件：{signal}"
    return signal


COUNTERFACTUAL_SIGNALS = {
    "multi_tenant": ("多租户", "国际化", "多公司", "组织树", "多组织",
                     "tenant", "saas"),
    "locale": ("国际化", "多语言", "i18n", "locale", "语言"),
    "scale": ("扩展到", "未来", "后续", "规模", "增长", "海量"),
}


def counterfactuals(text: str) -> dict:
    """反事实推理（未来视角）：单公司需求问'未来多公司？'——命中信号则
    建议预留 tenant_id/locale/company_id，避免未来推倒重来。"""
    lowered = (text or "").lower()
    hits = {kind: sorted(set(signal for signal in signals
                             if signal in (text or "") or signal in lowered))
            for kind, signals in COUNTERFACTUAL_SIGNALS.items()}
    suggestions = []
    if hits["multi_tenant"]:
        suggestions.append("预留 tenant_id 进主查询/唯一索引/事件上下文")
    if hits["locale"]:
        suggestions.append("预留 locale/时区字段与 i18n 键隔离")
    if hits["scale"]:
        suggestions.append("设计分页/索引/异步以支撑规模增长")
    return {"signals": hits, "suggestions": suggestions}


def goal_model(text: str) -> dict:
    """目标漂移防护（AADM-G §6）：主目标与护栏指标分离——优化性能不能靠
    减少数据/降低准确性/取消反馈/牺牲可访问性（代理指标劫持）。"""
    atoms = atomize(text)
    goals = [a["proposition"] for a in atoms["atoms"]
             if a["type"] == "goal"]
    guardrails = [clause for clause in ("不减少", "不降低", "不牺牲", "不取消",
                                        "保持", "不得破坏", "不改变")
                  if clause in (text or "")]
    forbidden = [clause for clause in ("不得", "禁止", "不能") if clause in (text or "")]
    return {
        "primary_goal": "；".join(goals) or "（未显式声明）",
        "user_outcome": "（未显式声明）",
        "success_metrics": [],
        "guardrail_metrics": guardrails,
        "forbidden_optimizations": forbidden,
        "negative_requirements": forbidden,
    }


def atoms_to_graph(atoms: list) -> dict:
    """把原子组装成超图 JSON（AADM §5：原子节点 + 语义关系）。"""
    nodes = [{"id": f"{atom['type']}-{index + 1}",
              "type": "atom",
              "payload": {key: atom[key] for key in
                          ("proposition", "source", "confidence",
                           "hardness", "scope")}}
             for index, atom in enumerate(atoms)]
    relations = []
    for index, node in enumerate(nodes):
        payload = node["payload"]
        if payload["hardness"] == "hard":
            relations.append({"id": f"r{index + 1}",
                              "from": [node["id"]],
                              "to": ["constraint-root"],
                              "type": "guards",
                              "condition": "hard 约束"})
    if relations:
        nodes.append({"id": "constraint-root", "type": "rule",
                      "payload": {"proposition": "全部硬约束",
                                  "hardness": "hard"}})
    return {"nodes": nodes, "relations": relations}


def atomize_main(argv: Optional[list] = None) -> int:
    """`pi-batch atomize "<text>" [--json] [--graph FILE]`。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py atomize",
        description="Prompt atomization (AADM §2): 先原子化，不能直接转代码")
    parser.add_argument("text", nargs="*", default=[])
    parser.add_argument("--file", default="")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--graph", default="",
                        help="同时把原子写成超图 JSON 文件")
    args = parser.parse_args(argv)
    if args.file:
        from .text_io import read_text_bounded
        from . import config
        text = read_text_bounded(Path(args.file), config.INPUT_MAX_BYTES,
                                 "atomize source")
    else:
        text = " ".join(args.text)
    if not text.strip():
        parser.error("Provide a requirement (positional or --file)")
    report = atomize(text)
    if args.graph:
        graph = atoms_to_graph(report["atoms"])
        Path(args.graph).write_text(json.dumps(graph, ensure_ascii=False,
                                               indent=2), encoding="utf-8")
        print(f"graph written: {args.graph}")
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        lines = [f"# Atoms ({report['count']})"]
        for atom in report["atoms"]:
            lines.append(f"  [{atom['type']}/{atom['hardness']}] "
                         f"{atom['proposition']}")
        print("\n".join(lines))
    raise SystemExit(0)
