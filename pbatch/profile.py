"""多维任务画像：量化模型（AADM §8-10） + 运行时路由（AADM-R §23）。

把需求文本转成结构化数值模型（纯确定性启发式，不调 LLM）：

- profile：12 维任务向量（clarity/scope/coupling/risk/uncertainty/
  reversibility/novelty/testability/evidence/observability/value/
  time_pressure，全部 0~1）
- autonomy：四维自主性向量（information/planning/execution/learning）
- mode：L0-L3 渐进路由（Load 公式 + 硬触发器 + 迟滞阈值）
- explorations：规划探索度 / 行动探索度（AADM §16）
- envelope：裁量包络（AADM §11：allowed/forbidden/maxRisk/requiredProofs）
- assumptions：假设记录 + 处理决策表（AADM §17，源自 product 推演链）

原则：数字是路由信号不是真理（AADM-R §23）；启发式估计是提案，不冒充
已验证；硬触发器直接升级（auth/不可逆 DB/跨项目/资金安全审计/生产删除）。

Usage:
    pi-batch profile "企业订单管理：新建订单表单，含支付审批流程" --json
"""

from __future__ import annotations

import argparse
import json
from typing import Optional

from . import config
from .classifier import classify_text
from .product import product_manifest
from .rule_matcher import domain_for, load_registry, match_rules

MODE_THRESHOLDS = (0.25, 0.50, 0.75)
# 迟滞（AADM-G §17 防止阈值附近振荡）：升级阈值高于降级阈值。
ESCALATE_ABOVE = 0.60
DEESCALATE_BELOW = 0.40

# 硬触发器（AADM-R §23：不受 Load 限制，直接升级）。
HARD_TRIGGERS = [
    (2, ["认证", "授权", "登录鉴权", "oauth", "jwt", "认证授权"]),
    (2, ["数据库迁移", "数据迁移", "migration", "alter table", "ddl"]),
    (2, ["跨项目", "跨系统", "跨仓库", "跨服务", "多项目", "公共契约",
         "公共 api", "公共接口"]),
    (3, ["资金", "财务", "支付", "退款", "对账", "审计", "合规",
         "security", "安全漏洞", "渗透"]),
    (3, ["生产数据", "删除数据", "清空表", "drop table", "delete from"]),
]

def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))

def _estimate_risk(text: str, lowered: str, cls, high_risk: bool) -> float:
    """风险启发式：业务动作词（审批/支付/删除…）才高；仅领域名词（订单/
    采购…）且任务像 UI 调整时不抬高风险（"订单页按钮对齐" ≠ 资金操作）。"""
    action_risk = [k for k in ("审批", "支付", "删除", "作废", "转账",
                               "退款", "提交", "delete", "approve",
                               "transfer", "refund", "pay") if k in lowered]
    domain_only = high_risk and not action_risk
    ui_tweak = cls.task_type == "frontend_ui" and cls.profile in ("", "unknown")
    if action_risk:
        risk = 0.71
    elif domain_only and not ui_tweak:
        risk = 0.40
    else:
        risk = 0.15
    if any(k in lowered for k in ("生产", "上线", "正式环境")):
        risk = max(risk, 0.55)
    return risk

def _estimate_uncertainty(text: str, present: int, total_dims: int) -> float:
    """不确定性启发式：缺失维度为主；具体指令（对齐/修改…）降低权重。"""
    missing_ratio = 1 - present / total_dims
    unknown_hits = any(k in text for k in ("是否", "未知", "待确认", "可能",
                                           "暂定", "未定"))
    concrete = any(k in text for k in ("对齐", "调整", "修改", "统一间距",
                                       "改样式", "增加", "新增"))
    weight = 0.5 if (concrete and not unknown_hits) else 0.8
    return _clamp(missing_ratio * weight + (0.2 if unknown_hits else 0))

def _estimate_qualities(text: str) -> dict:
    """新颖性/可测性/证据/可观测性/价值/时间压力（独立小项）。"""
    lowered = text.lower()
    novelty = 0.60 if any(k in text for k in ("新技术", "首次", "没有先例",
                                              "尚无先例", "探索", "评估方案",
                                              "方案对比", "or", "还是")) else 0.20
    testability = 0.76 if any(k in text for k in ("验收", "测试", "checklist")) \
        else 0.40
    evidence = 0.61 if any(k in text for k in ("已有", "复用", "现有", "沿用")) \
        else 0.30
    observability = 0.70 if any(k in text for k in ("日志", "监控", "可观测",
                                                    "metrics")) else 0.50
    value = {"L0_local_feature": 0.30, "L1_reusable_module": 0.50,
             "L2_platform_capability": 0.70,
             "L3_product_feature": 0.90}.get(
                 product_manifest(text)["level"], 0.5)
    time_pressure = 0.80 if any(k in lowered for k in ("紧急", "今天", "尽快",
                                                       "deadline")) else 0.30
    return {"novelty": round(novelty, 2),
            "testability": round(testability, 2),
            "evidence": round(evidence, 2),
            "observability": round(observability, 2),
            "value": round(value, 2),
            "time_pressure": round(time_pressure, 2)}

def _measure_dimensions(text: str, lowered: str, cls, scale: str,
                        high_risk: bool, present: int,
                        assess_dimensions=None) -> dict:
    """12 维向量的确定性启发式估计（约数，不冒充已验证）。"""
    if assess_dimensions is None:
        from .assessor import assess_dimensions  # 惰性（避免循环）
    total_dims = len(assess_dimensions(text))
    clarity = _clamp(present / total_dims + 0.15)
    scope = {"S": 0.20, "M": 0.50, "L": 0.86}.get(scale, 0.5)
    cross = any(k in text for k in ("跨项目", "跨系统", "跨仓库", "多系统"))
    coupling = 0.84 if cross else (0.5 if scale == "L" else 0.25)
    risk = _estimate_risk(text, lowered, cls, high_risk)
    uncertainty = _estimate_uncertainty(text, present, total_dims)
    irreversible = any(k in text for k in ("删除", "迁移", "生产数据", "清空"))
    reversibility = 0.20 if irreversible else (0.35 if high_risk else 0.85)
    extras = _estimate_qualities(text)
    extras["reversibility"] = round(reversibility, 2)
    return {"clarity": round(clarity, 2), "scope": round(scope, 2),
            "coupling": round(coupling, 2), "risk": round(risk, 2),
            "uncertainty": round(uncertainty, 2), **extras}


def _measure_load(profile: dict) -> float:
    """轻量模式 Load（AADM-R §23）：风险维度与加权维度取最大值。"""
    return max(profile["risk"],
               0.25 * profile["scope"] + 0.20 * profile["uncertainty"]
               + 0.20 * profile["coupling"] + 0.20 * profile["novelty"]
               + 0.15 * (1 - profile["reversibility"]))


def task_profile(text: str, registry: Optional[dict] = None) -> dict:
    """12 维任务向量 + 派生自主性/模式/包络（确定性启发式估计）。"""
    text = text or ""
    lowered = text.lower()
    reg = registry or load_registry(domain=domain_for(text))
    cls = classify_text(text)
    matched = match_rules(text, classification=cls, registry=reg)
    # 惰性导入 assessor（避免 assessor↔profile 模块级循环）
    from .assessor import assess_dimensions, scale_signal, workflow_level
    dimensions = assess_dimensions(text)
    present = sum(1 for _, _, ok in dimensions if ok)
    scale = scale_signal(text, reg)
    high_risk = matched["risk"] == "high"

    profile = _measure_dimensions(text, lowered, cls, scale, high_risk,
                                  present, assess_dimensions)
    load_value = _measure_load(profile)
    mode = _mode_from_load(load_value)
    mode = _apply_hard_triggers(mode, text)
    # AADM-R §21：start_mode 是下界（策略说 L1 起步就不降到 L0）
    from .runtime_policy import RUNTIME_START_MODE
    mode = max(mode, RUNTIME_START_MODE)
    autonomy = _autonomy_vector(profile)
    explorations = _explorations(profile)
    envelope = _discretion_envelope(profile, autonomy, high_risk)
    from .assessor import workflow_level
    workflow = workflow_level(text, reg,
                              product_manifest(text)["level"])
    return _assemble_report(text, mode, load_value, workflow, autonomy,
                            explorations, envelope, profile)


def _assemble_report(text: str, mode: int, load_value: float,
                     workflow: dict, autonomy: dict, explorations: dict,
                     envelope: dict, profile: dict) -> dict:
    """装配最终画像报告（模式/自主性/包络/估计/护栏/策略）。"""
    from .runtime_policy import runtime_policy_view
    return {
        "mode": mode, "load": round(load_value, 2),
        "hard_triggers": _hard_trigger_hits(text),
        "execution_mode": {
            "L0": "chain（1 Agent / 1 方案）",
            "L1": "dag（1-2 Agent / 1-2 方案）",
            "L2": "and-or-dag（2-4 Agent / 2-3 方案，分阶段竞跑）",
            "L3": "dynamic-graph（多专业 Agent + Reviewer + 门禁）",
        }.get(mode, ""),
        "workflow": workflow,
        "autonomy": autonomy,
        "planning_exploration": explorations["planning"],
        "action_exploration": explorations["action"],
        "envelope": envelope,
        "uncertainty_types": uncertainty_types(text),
        "stop_policy": stop_policy({
            "mode": mode, "profile": profile, "envelope": envelope}),
        "approval": approval_level({
            "mode": mode, "profile": profile, "envelope": envelope}),
        "estimates": estimates({
            "mode": mode, "load": load_value, "profile": profile}),
        "goal_model": goal_model(text),
        "counterfactuals": counterfactuals(text),
        "runtime_policy": runtime_policy_view(),
        "nfr_nodes": nfr_requirements({
            "mode": mode, "profile": profile}),
        "hysteresis": {"escalate_above": ESCALATE_ABOVE,
                       "deescalate_below": DEESCALATE_BELOW},
        "profile": profile,
        "_note": "数值为确定性启发式估计（路由信号），不冒充已验证；"
                 "硬触发器优先于 Load 分数",
    }


def _hard_trigger_hits(text: str) -> list:
    lowered = (text or "").lower()
    hits = []
    for level, keywords in HARD_TRIGGERS:
        matched = [k for k in keywords if k in (text or "") or k in lowered]
        if matched:
            hits.append({"min_mode": level, "matched": matched})
    return hits


def _apply_hard_triggers(mode: int, text: str) -> int:
    lowered = (text or "").lower()
    for level, keywords in HARD_TRIGGERS:
        if any(k in (text or "") or k in lowered for k in keywords):
            mode = max(mode, level)
    return mode


def _mode_from_load(load_value: float) -> int:
    if load_value < MODE_THRESHOLDS[0]:
        return 0
    if load_value < MODE_THRESHOLDS[1]:
        return 1
    if load_value < MODE_THRESHOLDS[2]:
        return 2
    return 3


def _autonomy_vector(profile: dict) -> dict:
    """四维自主性（AADM §10）：风险压执行/学习，不确定性抬信息/规划。"""
    risk = profile["risk"]
    uncertainty = profile["uncertainty"]
    reversibility = profile["reversibility"]
    info = _clamp(0.60 + uncertainty * 0.35)
    planning = _clamp(0.60 + uncertainty * 0.25 - risk * 0.15)
    execution = _clamp(1.0 - risk * 0.80 - (1 - reversibility) * 0.40,
                       0.10, 0.95)
    learning = _clamp(0.30 - risk * 0.20, 0.05, 0.40)
    return {"information": round(info, 2), "planning": round(planning, 2),
            "execution": round(execution, 2), "learning": round(learning, 2)}


def _explorations(profile: dict) -> dict:
    """规划探索度与行动探索度分离（AADM §16）。"""
    diversity = 0.50 if profile["novelty"] >= 0.50 else 0.20
    planning = _clamp(0.45 * profile["uncertainty"]
                      + 0.35 * profile["novelty"] + 0.20 * diversity)
    action = planning * (1 - profile["risk"]) * profile["reversibility"] \
        * profile["observability"]
    return {"planning": round(planning, 2), "action": round(action, 2)}


def _discretion_envelope(profile: dict, autonomy: dict,
                         high_risk: bool) -> dict:
    """裁量包络（AADM §11）：包络内自由，包络外禁止。"""
    risk = profile["risk"]
    uncertainty = profile["uncertainty"]
    reversibility = profile["reversibility"]
    scope = profile["scope"]
    forbidden = ["修改公共 API", "修改数据库结构", "跨项目变更"]
    if risk >= 0.5:
        forbidden += ["不可逆操作（删除/迁移生产数据）", "绕过验证门禁"]
    required_proofs = ["test_pass"]
    if risk >= 0.5 or uncertainty >= 0.5:
        required_proofs = ["impact_map", "rollback_plan", "test_pass"]
    if scope >= 0.5:
        required_proofs.append("cross_boundary_check")
    horizon = 3 if (risk + uncertainty) > 1.0 else (8 if reversibility > 0.7 else 5)
    return {
        "allowed_action_types": ["analyze", "modify_in_scope", "add_tests"],
        "forbidden_action_types": forbidden,
        "max_risk": round(1.0 - autonomy["execution"], 2),
        "max_change_surface": round(0.15 if scope >= 0.5 else 0.40, 2),
        "execution_autonomy": autonomy["execution"],
        "planning_autonomy": autonomy["planning"],
        "required_proofs": required_proofs,
        "rollback_required": not reversibility >= 0.7 or risk >= 0.5,
        "human_gate_required": bool(high_risk and not reversibility >= 0.5),
        "max_parallelism": 1 + int((1 - risk) * 2),
        "max_planning_horizon": horizon,
        "stop_conditions": ["acceptance_satisfied", "no_progress",
                            "budget_exhausted"],
    }


# ---------------------------------------------------------------------------
# 假设记录（AADM §17：未知项不都变成"向用户提问"）
# ---------------------------------------------------------------------------

def assumption_records(text: str, manifest: Optional[dict] = None) -> list:
    """把产品推演链的待确认问题升级为带数学风险的假设记录。

    处理决策表：风险很低→用默认值并记录；较低→可回滚方案+验证；
    中等→先查代码/文档/日志；较高→先模拟/实验或请求明确确认；
    极高→阻止不可逆执行。"""
    product = manifest or product_manifest(text)
    records = []
    for scenario in product.get("scenarios", []):
        for question in scenario.get("questions", []):
            statement = f"[{scenario['scenario']}] {question}"
            confidence = 0.40  # 隐含假设，未经确认
            impact = 0.80 if any(k in question for k in (
                "权限", "多级", "状态", "金额", "审计", "并发", "契约")) else 0.50
            reversibility = 0.60
            risk = (1 - confidence) * impact * (1 - reversibility)
            if risk <= 0.10:
                plan, status = "使用合理默认值，并记录", "accepted"
            elif risk <= 0.20:
                plan, status = "使用可回滚方案，同时验证", "accepted"
            elif risk <= 0.35:
                plan, status = "先查看代码/文档/日志/运行测试", "unverified"
            elif risk <= 0.55:
                plan, status = "先模拟/实验或请求明确确认", "unverified"
            else:
                plan, status = "阻止不可逆执行，请求明确确认", "unverified"
            records.append({
                "statement": statement,
                "confidence": confidence,
                "impact_if_wrong": impact,
                "reversibility": reversibility,
                "assumption_risk": round(risk, 2),
                "verification_cost": 1 + int(impact * 3),
                "chosen_default": "见 verification_plan",
                "verification_plan": plan,
                "status": status,
            })
    return records


def execution_horizon(profile: dict) -> int:
    """滚动规划批次大小（AADM-R §18）：
    Horizon = Clamp(H_max × Confidence × Reversibility / (Risk+Uncertainty+ε))。
    风险高/不确定高 → 每步很小；证据足/易回滚 → 批量执行。"""
    confidence = (0.35 * profile["evidence"] + 0.20 * profile["clarity"]
                  + 0.15 * profile["testability"] + 0.30)
    horizon = (8.0 * confidence * profile["reversibility"]
               / (profile["risk"] + profile["uncertainty"] + 0.05))
    return max(1, min(8, int(round(horizon))))


def infeasibility_report(profile: dict, budget_time: Optional[float] = None,
                         budget_tokens: Optional[int] = None,
                         budget_cost: Optional[float] = None) -> dict:
    """预算不足时显式返回 infeasible_under_budget，不伪装完成（AADM-R §20）。
    最低需求按模式/负载/必选证据估算（确定性占位，非伪精确）。"""
    load = profile.get("load", 0.5)
    mode = profile.get("mode", 0)
    base_seconds = {0: 60, 1: 300, 2: 900, 3: 3600}.get(mode, 300)
    minimum = {
        "time": base_seconds * (1 + load),
        "tokens": int(50000 * (1 + load * 3)),
        "cost": round(50000 * (1 + load * 3) / 200000, 2),
    }
    limits = {"time": budget_time, "tokens": budget_tokens,
              "cost": budget_cost}
    violations = [key for key, limit in limits.items()
                  if limit is not None and minimum[key] > limit]
    if not violations:
        return {"status": "feasible", "minimum_required": minimum}
    return {
        "status": "infeasible_under_budget",
        "minimum_required": minimum,
        "current_limit": {key: limits[key] for key in violations},
        "violated": violations,
        "cannot_remove": ["security_verification", "required_proofs",
                           "acceptance_check"],
        "hint": "不能偷偷删除验证后声称完成；提高预算或降低模式（L"
                f"{mode} → 更低）",
    }


from .atomize import counterfactuals, goal_model  # noqa: E402


def estimates(report: dict) -> dict:
    """ProbeReport 估计区间（AADM-R §5）：p50/p90 + 置信度，非伪精确值。"""
    base = {0: 60, 1: 300, 2: 900, 3: 3600}.get(report["mode"], 300)
    risk = report["profile"]["risk"]
    uncertainty = report["profile"]["uncertainty"]
    p50 = base * (1 + report["load"])
    p90 = p50 * (1.5 + risk)
    confidence = round(max(0.3, 1.0 - uncertainty * 0.5), 2)
    return {
        "elapsed_time": {"p50": int(p50), "p90": int(p90),
                         "confidence": confidence},
        "tokens": {"p50": int(50000 * (1 + report["load"])),
                   "p90": int(80000 * (1 + report["load"] * 2)),
                   "confidence": confidence},
    }


def approval_level(report: dict) -> str:
    """人工门禁分级（AADM-G §26，防审批疲劳）：

    低风险可回滚 → 自动执行事后报告；中风险 → 先展示计划与差异；
    高风险可模拟 → 先 Dry-run 再批准真实执行；不可逆/重大 → 双人复核；
    异常偏离 → 自动暂停并升级。"""
    risk = report["profile"]["risk"]
    reversibility = report["profile"]["reversibility"]
    mode = report["mode"]
    envelope = report["envelope"]
    if mode <= 1 and risk < 0.4:
        return "auto_report"
    if risk < 0.5 or reversibility >= 0.7:
        return "preview"
    if envelope.get("rollback_required") and not envelope.get("human_gate_required"):
        return "dry_run_approve"
    if envelope.get("human_gate_required"):
        return "double_review"
    return "preview"


def question_value(decision_change: float, impact: float,
                   interruption_cost: float = 0.3) -> float:
    """提问价值（AADM-G §26）：只有可能显著改变高影响决策时才打断用户。"""
    return round(decision_change * impact - interruption_cost, 2)


from .governance_policy import (nfr_requirements, should_stop,  # noqa: E402
                                     stop_policy, uncertainty_types)


def format_profile(report: dict) -> str:
    """人类可读画像报告。"""
    mode_names = {0: "L0 局部模式", 1: "L1 标准模式",
                  2: "L2 探索模式", 3: "L3 治理模式"}
    lines = ["## 任务画像（量化路由信号）"]
    lines.append(f"模式: {mode_names.get(report['mode'])} "
                 f"(Load={report['load']}) — {report['execution_mode']}")
    for trigger in report["hard_triggers"]:
        lines.append(f"  硬触发器: ≥L{trigger['min_mode']} "
                     f"({', '.join(trigger['matched'])})")
    lines.append(f"工作流: {report['workflow']['level']} "
                 f"（分 {report['workflow']['score']}）")
    profile = report["profile"]
    dims = " ".join(f"{k}={v}" for k, v in profile.items())
    lines.append(f"向量: {dims}")
    autonomy = report["autonomy"]
    lines.append(f"自主性: info={autonomy['information']} "
                 f"plan={autonomy['planning']} exec={autonomy['execution']} "
                 f"learn={autonomy['learning']}")
    lines.append(f"探索: 规划={report['planning_exploration']} "
                 f"行动={report['action_exploration']} "
                 f"(迟滞: 升>{report['hysteresis']['escalate_above']} "
                 f"降<{report['hysteresis']['deescalate_below']})")
    envelope = report["envelope"]
    lines.append("裁量包络:")
    lines.append(f"  允许: {', '.join(envelope['allowed_action_types'])}")
    lines.append(f"  禁止: {', '.join(envelope['forbidden_action_types'])}")
    lines.append(f"  max_risk={envelope['max_risk']} "
                 f"max_change_surface={envelope['max_change_surface']} "
                 f"rollback_required={envelope['rollback_required']} "
                 f"human_gate={envelope['human_gate_required']}")
    lines.append(f"  必须证据: {', '.join(envelope['required_proofs'])}")
    return "\n".join(lines)


def profile_main(argv: Optional[list] = None) -> int:
    """`pi-batch profile "<requirement>" [--json]`：多维画像独立命令。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py profile",
        description="多维任务画像（向量/自主性/L0-L3 路由/裁量包络/假设）")
    parser.add_argument("task", nargs="*", default=[])
    parser.add_argument("--file", default="")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--assumptions", action="store_true",
                        help="包含假设记录（默认随 --json 一起输出）")
    parser.add_argument("--budget-time", type=float, default=None,
                        help="时间预算（秒）：不足则返回 infeasible_under_budget")
    parser.add_argument("--budget-tokens", type=int, default=None,
                        help="Token 预算")
    parser.add_argument("--budget-cost", type=float, default=None,
                        help="成本预算（元）")
    args = parser.parse_args(argv)
    if args.file:
        from .text_io import read_text_bounded
        from . import config
        text = read_text_bounded(__import__("pathlib").Path(args.file),
                                 config.INPUT_MAX_BYTES, "profile source")
    else:
        text = " ".join(args.task)
    if not text.strip():
        parser.error("Provide a requirement (positional text or --file)")
    report = task_profile(text)
    report["horizon"] = execution_horizon(report["profile"])
    report["budget_feasibility"] = infeasibility_report(
        report, budget_time=args.budget_time, budget_tokens=args.budget_tokens,
        budget_cost=args.budget_cost)
    if args.json or args.assumptions:
        report = dict(report)
        report["assumptions"] = assumption_records(text)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(format_profile(report))
    raise SystemExit(0)
