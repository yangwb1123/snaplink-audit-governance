"""Requirement assessment: senior-experience evaluation before rule use.

A task must not load every matching rule just because keywords hit — the
prescription is driven by an expert evaluation of the REQUIREMENT itself:

1. Completeness: which of the 8 requirement dimensions are present
   (goal / user role / main flow / data source / permission / error path /
   acceptance criteria / tech stack) — missing ones are called out with
   suggestions, because rules applied to an under-specified requirement
   are guesses.
2. Scale signal: count real requirement complexity (forms, tables, flows,
   modules) → S / M / L, which caps the effective prescription tier below
   the keyword-matched tier. A login page stays demo-tier even if the
   prompt casually mentions "企业".
3. Prescription: the minimal necessary rule set for THIS requirement —
   plus an explicit "deliberately not selected" list with reasons
   (restraint: do not pile everything on).
4. Optional two-sided check: --llm-json reconciles an independent LLM
   selection (fail closed, REQUIRED rules are veto-protected).

Usage:
    pi-batch assess "企业订单管理：新建订单表单，含支付审批流程"
    pi-batch assess "..." --json
    pi-batch assess "..." --llm-json '{"apply":[],"skip":[]}'
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

from . import config
from .classifier import classify_text
from .config import log, yaml
from .relevance import _keyword_hit
from .rule_matcher import (TIER_ORDER, domain_for, format_manifest,
                           load_registry, match_rules, reconcile)
from .text_io import read_text_bounded
from .product import format_product, product_manifest, productization_level

# Requirement completeness dimensions (bilingual presence keywords).
DIMENSIONS = [
    ("goal", "业务目标",
     ["实现", "开发", "做一个", "目标", "完成", "页面", "功能", "支持",
      "build", "implement", "create", "page", "feature"]),
    ("user_role", "用户角色",
     ["用户", "角色", "管理员", "编辑", "操作员", "业务员", "人员",
      "user", "role", "operator", "admin"]),
    ("main_flow", "主流程",
     ["流程", "步骤", "查询", "新增", "编辑", "提交", "审批", "导入", "导出",
      "flow", "workflow", "create", "approve", "search", "submit"]),
    ("data_source", "数据来源",
     ["接口", "api", "数据库", "数据", "加载", "获取", "后端", "表结构",
      "dataset", "endpoint", "backend"]),
    ("permission", "权限",
     ["权限", "登录", "认证", "授权", "permission", "auth", "login",
      "access control"]),
    ("error_path", "异常路径",
     ["失败", "异常", "错误", "超时", "重试", "冲突", "降级",
      "error", "fail", "timeout", "retry", "conflict"]),
    ("acceptance", "验收标准",
     ["验收", "测试", "通过标准", "完成标准", "acceptance", "criteria",
      "test", "checklist"]),
    ("tech_stack", "技术栈",
     ["tsx", "react", "vue", "flutter", "dart", "typescript", "ts",
      "antd", "技术栈", "框架", "组件库"]),
]

_SCALE_TIERS = {"S": "demo", "M": "standard", "L": "production"}

_SUGGESTIONS = {
    "user_role": "未提及用户角色与权限：按钮可见性/操作范围无法评估",
    "main_flow": "未描述主流程：交互链路与状态机无法设计",
    "data_source": "未提及数据来源：请求/缓存/错误映射无法设计",
    "permission": "未提及权限：审批/删除等敏感操作的可见性无法评估",
    "error_path": "未提及异常路径：失败恢复与重试策略无法设计",
    "acceptance": "未提及验收标准：完成定义缺失，无法判定交付",
    "tech_stack": "未指定技术栈：平台适配（tsx/dart/vue）为假设",
}


def assess_dimensions(text: str) -> list:
    """(name, label, present) for each requirement dimension."""
    lowered = (text or "").lower()
    return [(name, label, any(_keyword_hit(lowered, term) for term in terms))
            for name, label, terms in DIMENSIONS]


def scale_signal(text: str, registry: Optional[dict] = None) -> str:
    """Requirement complexity → S / M / L. Signal keywords and weights come
    from the domain registry (frontend: forms/tables/flows; backend:
    use cases/entities/integrations/distribution)."""
    reg = registry or load_registry(domain=domain_for(text))
    signals = reg.get("signals", {})
    weights = reg.get("signal_weights", {})
    lowered = (text or "").lower()
    score = 0
    for kind, terms in signals.items():
        if any(_keyword_hit(lowered, term) for term in terms):
            score += int(weights.get(kind, 1))
    if score >= 4:
        return "L"
    if score >= 1:
        return "M"
    return "S"


_WORKFLOW_SCALE = {"S": 0, "M": 1, "L": 2}
_WORKFLOW_PRODUCT = {"L0_local_feature": 0, "L1_reusable_module": 1,
                    "L2_platform_capability": 2, "L3_product_feature": 3}


WORKFLOW_SUGGESTIONS = {
    "L0_direct": "单任务直接修改 + 快速验证门禁（无需流水线）",
    "L1_standard": "标准实现流水线：frontend-implementation 或 backend-implementation"
                   "（--classify 自动路由，plan→实现+门禁→对抗审查→gate）",
    "L2_high_risk": "examples/full-sdlc-implement.yaml：对抗审查 + 裁决门 + 验收门"
                     "（权限/支付/迁移/并发）",
    "L3_platform": "examples/full-sdlc-implement.yaml + product_thinker 审查"
                    "（产品推演 + 商业/开源要素）",
}


def workflow_level(text: str, registry: Optional[dict] = None,
                   product_level: Optional[str] = None) -> dict:
    """Risk-graded workflow selection: score = risk×2 + scale + product×3.
    Low-risk small tasks stay on a direct path; high-risk/platform work
    escalates to adversarial review + gates + product thinking."""
    reg = registry or load_registry(domain=domain_for(text))
    matched = match_rules(text, registry=reg)
    risk = 1 if matched["risk"] == "high" else 0
    scale = _WORKFLOW_SCALE.get(scale_signal(text, reg), 0)
    prod = _WORKFLOW_PRODUCT.get(product_level or productization_level(text)[0], 0)
    total = risk * 2 + scale + prod * 3
    if total <= 1:
        level = "L0_direct"
    elif total <= 3:
        level = "L1_standard"
    elif total <= 5:
        level = "L2_high_risk"
    else:
        level = "L3_platform"
    return {"level": level, "score": total,
            "risk": matched["risk"], "scale": matched["tier"],
            "suggestion": WORKFLOW_SUGGESTIONS[level]}


def prescription(text: str, registry: Optional[dict] = None) -> dict:
    """The minimal necessary rule set for THIS requirement.

    Effective tier = min(keyword tier, scale tier) — a login page stays
    demo-tier even when the prompt mentions 企业; a one-off form does not
    pull architecture-budgets just because it says 生产.
    """
    reg = registry or load_registry(domain=domain_for(text))
    matched = match_rules(text, classification=classify_text(text), registry=reg)
    cls = classify_text(text)
    scale = scale_signal(text, reg)
    effective = _SCALE_TIERS.get(scale, "standard")
    rank = TIER_ORDER.get(effective, 1)
    selected, not_selected = [], []
    for item in matched["rules"]:
        if TIER_ORDER.get(item["tier"], 1) <= rank:
            selected.append(item)
        else:
            not_selected.append({
                "id": item["id"], "tier": item["tier"],
                "reason": f"需求评估为 {effective} 档（规模 {scale}），"
                          f"该规则需要 {item['tier']} 档",
            })
    dimensions = assess_dimensions(text)
    present = [name for name, _, ok in dimensions if ok]
    missing = [name for name, _, ok in dimensions if not ok]
    return {
        "requirement": " ".join((text or "").split())[:120],
        "classification": {
            "task_type": cls.task_type,
            "profile": matched["profile"], "platform": cls.platform,
        },
        "product": product_manifest(text),
        "workflow": workflow_level(text, reg, None),
        "scale": scale, "effective_tier": effective,
        "matched_tier": matched["tier"], "risk": matched["risk"],
        "page_types": matched["page_types"],
        "completeness": {"score": len(present), "total": len(DIMENSIONS),
                         "present": present, "missing": missing},
        "prescription": selected,
        "not_selected": not_selected,
        "suggestions": [_SUGGESTIONS[name] for name in missing
                        if name in _SUGGESTIONS],
    }


def format_assessment(assessment: dict) -> str:
    """Human-readable assessment report."""
    lines = ["## 需求评估（资深经验处方）"]
    lines.append(f"需求: {assessment['requirement']}")
    cls = assessment["classification"]
    lines.append(
        f"画像: {cls['task_type'] or 'unknown'} "
        f"[{cls['platform'] or '-'}/{cls['profile'] or 'generic'}] | "
        f"匹配档 {assessment['matched_tier']} → 处方档 "
        f"{assessment['effective_tier']}（规模 {assessment['scale']}）| "
        f"风险 {assessment['risk']}")
    comp = assessment["completeness"]
    missing = ", ".join(comp["missing"]) if comp["missing"] else "无"
    lines.append(f"完整性: {comp['score']}/{comp['total']} — 缺失: {missing}")
    workflow = assessment["workflow"]
    lines.append(f"工作流: {workflow['level']}（分 {workflow['score']}）— "
                 f"{workflow['suggestion']}")
    lines.append(format_product(assessment["product"]))
    selected = assessment["prescription"]
    lines.append(f"处方（最小必要规则集，共 {len(selected)} 条）:")
    for item in selected:
        marker = "必选" if item["required"] else "选用"
        lines.append(f"- [{marker}] {item['id']} ({item['tier']}): "
                     f"{item['description']}")
        for file in item["files"]:
            lines.append(f"    {file}")
    skipped = assessment["not_selected"]
    if skipped:
        lines.append("刻意未选用（克制原则）:")
        for item in skipped:
            lines.append(f"- {item['id']}: {item['reason']}")
    for suggestion in assessment["suggestions"]:
        lines.append(f"建议补充: {suggestion}")
    return "\n".join(lines)


def format_execution_prompt(assessment: dict) -> str:
    """Compile an assessment into a ready-to-use instruction block for the
    implementing agent — the last mile of assessment-driven execution."""
    lines = [
        "## Execution instructions (generated by pi-batch assess)",
        f"REQUIREMENT: {assessment['requirement']}",
        f"CLASSIFICATION: {assessment['classification']['task_type']} "
        f"[{assessment['classification']['platform'] or '-'}/"
        f"{assessment['classification']['profile'] or 'generic'}]",
    ]
    product = assessment["product"]
    lines.append(f"PRODUCTIZATION: {product['level']} — implicit-requirement "
                 f"questions are TO-CONFIRM, never implement silently")
    for scenario in product["scenarios"]:
        lines.append(f"  questions[{scenario['scenario']}]: "
                     + " / ".join(scenario["questions"]))
    for note in product["restraint_notes"]:
        lines.append(f"  restraint: {note}")
    workflow = assessment["workflow"]
    lines.append(f"WORKFLOW: {workflow['level']} — {workflow['suggestion']}")
    lines.append("RULES (load ONLY these files):")
    for item in assessment["prescription"]:
        marker = "REQUIRED" if item["required"] else "optional"
        lines.append(f"  [{marker}] {item['id']} ({item['tier']}): "
                     f"{item['description']}")
        for file in item["files"]:
            lines.append(f"    {file}")
    lines.append("COMPLETION: finish with a completion_report (commands "
                 "executed vs not_executed, no fabricated passes).")
    return "\n".join(lines)


def assess_main(argv: list) -> None:
    """`pi-batch assess "<requirement>" [--json] [--llm-json '...']`."""
    import argparse
    parser = argparse.ArgumentParser(
        prog="pi-batch.py assess",
        description="Evaluate a requirement (completeness, scale, risk) and "
                    "prescribe the minimal necessary ui-spec rules.")
    parser.add_argument("task", nargs="*", default=[], help="requirement text")
    parser.add_argument("--file", default="",
                        help="read the requirement from FILE (markdown/txt)")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    parser.add_argument("--prompt", action="store_true",
                        help="print the compiled execution-instruction block "
                             "(feed it to the implementing agent)")
    parser.add_argument("--llm-json", default="",
                        help='LLM selection JSON {"apply":[],"skip":[]} to reconcile')
    parser.add_argument("--registry", default="", help="rule registry YAML override")
    args = parser.parse_args(argv)
    if args.file:
        from .text_io import read_text_bounded
        text = read_text_bounded(Path(args.file), config.INPUT_MAX_BYTES,
                                 "assess source")
    else:
        text = " ".join(args.task)
    if not text.strip():
        parser.error("Provide a requirement (positional text or --file)")
    registry = load_registry(args.registry) if args.registry else None
    assessment = prescription(text, registry)
    if args.llm_json:
        try:
            selection = json.loads(args.llm_json)
        except Exception as exc:
            log.error("Invalid --llm-json: %s", exc)
            sys.exit(2)
        matched = reconcile(match_rules(text, registry=registry),
                            selection.get("apply", []),
                            selection.get("skip", []), registry)
        assessment["prescription"] = matched["rules"]
        assessment["provenance"] = matched["provenance"]
        assessment["dropped"] = matched["dropped"]
    if args.json:
        print(json.dumps(assessment, ensure_ascii=False, indent=2))
        return
    if args.prompt:
        print(format_execution_prompt(assessment))
        return
    print(format_assessment(assessment))
