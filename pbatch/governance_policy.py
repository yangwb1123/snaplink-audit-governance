"""Stop policy and uncertainty typing (AADM-G §7/§28).

三类不确定性（认知/随机/对抗）不能用一个数字表示；完成定义
（StopPolicy）要求所有验收标准有证据支持、残余风险在允许范围内、
无进展检测——否则 Agent 永远可以继续优化。
"""

from __future__ import annotations

UNCERTAINTY_SIGNALS = {
    "epistemic": ("是否", "未知", "待确认", "可能", "暂定", "未定",
                  "不确定", "unknown", "tbd"),
    "aleatoric": ("网络", "延迟", "波动", "超时", "随机", "并发",
                  "负载", "抖动", "timeout", "latency", "jitter"),
    "adversarial": ("注入", "攻击", "恶意", "越权", "渗透", "不可信",
                    "注入攻击", "prompt injection", "xss", "csrf"),
}


def uncertainty_types(text: str) -> dict:
    """三类不确定性分类：认知→搜索/探测；随机→容错/冗余；对抗→隔离。"""
    lowered = (text or "").lower()
    return {kind: sorted(set(signal for signal in signals
                             if signal in lowered))
            for kind, signals in UNCERTAINTY_SIGNALS.items()}


def stop_policy(report: dict) -> dict:
    """完成定义：验收覆盖/残余风险/回滚就绪/独立证据/无进展检测。"""
    risk = report["profile"]["risk"]
    envelope = report["envelope"]
    return {
        "acceptance_coverage_required": 1.0,
        "max_critical_open_risks": 0 if risk >= 0.5 else 1,
        "max_residual_risk": round(0.15 if risk < 0.5 else 0.30, 2),
        "require_rollback_ready": bool(envelope.get("rollback_required")),
        "require_independent_evidence": risk >= 0.5,
        "max_no_progress_cycles": 3,
        "resource_cap_mode": "hard" if risk >= 0.5 else "soft",
        "honest_states": ["completed", "partial", "verified_with_residual",
                          "blocked", "infeasible_under_budget",
                          "needs_external_decision"],
    }


def progress_score(acceptance_coverage: float, verified_claims: int,
                   critical_open_risks: int, regression_count: int) -> float:
    """单调进展度量（AADM-G §17）：连续无实质进展时不能重复同一策略。"""
    return round(acceptance_coverage + verified_claims
                 - critical_open_risks - regression_count, 3)


def update_soft_rule_weight(current: float, outcome_score: float,
                            eta: float = 0.3) -> float:
    """软规则权重更新（AADM §21）：w_{t+1} = (1−η)·w_t + η·OutcomeScore_t。

    只有软规则可被学习调整；硬规则升级必须经治理流程（shadow/promote）。"""
    if eta < 0 or eta > 1:
        raise ValueError("eta must be in [0, 1]")
    return round((1 - eta) * current + eta * outcome_score, 4)


def nfr_requirements(report: dict) -> list:
    """非功能需求一等节点（AADM-G §31）：按风险/模式生成必须进入任务图的
    NFR 验证节点——否则它们最后被预算和时间挤掉。"""
    risk = report["profile"]["risk"]
    mode = report["mode"]
    nfr = [{"id": "nfr-performance", "name": "性能验证节点",
            "required": mode >= 1},
           {"id": "nfr-security", "name": "安全验证节点",
            "required": risk >= 0.5},
           {"id": "nfr-observability", "name": "可观察性节点",
            "required": mode >= 1},
           {"id": "nfr-rollback-drill", "name": "回滚演练节点",
            "required": risk >= 0.5 or report["profile"]["reversibility"] < 0.4},
           {"id": "nfr-accessibility", "name": "可访问性节点",
            "required": mode >= 1 and risk < 0.7}]
    return [item for item in nfr if item["required"]]


def should_stop(progress: dict, policy: dict) -> tuple:
    """停止判定：全部硬条件满足才停止；无进展 → 更换策略而非无限重试。"""
    reasons = []
    if (progress.get("acceptance_coverage") or 0) < policy.get(
            "acceptance_coverage_required", 1.0):
        reasons.append("验收覆盖率未达标")
    if (progress.get("critical_open_risks") or 0) > policy.get(
            "max_critical_open_risks", 1):
        reasons.append("关键未处理风险超标")
    if (progress.get("residual_risk") or 1.0) > policy.get(
            "max_residual_risk", 0.3):
        reasons.append("残余风险超允许范围")
    if policy.get("require_rollback_ready") and not progress.get(
            "rollback_ready"):
        reasons.append("回滚方案未就绪")
    if policy.get("require_independent_evidence") and not progress.get(
            "independent_evidence"):
        reasons.append("缺少独立证据")
    if (progress.get("no_progress_cycles") or 0) >= policy.get(
            "max_no_progress_cycles", 3):
        reasons.append("连续无进展，应更换策略而非继续重复")
    return (not reasons, reasons)
