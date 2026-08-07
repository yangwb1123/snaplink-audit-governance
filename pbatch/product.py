"""Productization assessment: how deeply an agent should think about a
requirement as a PRODUCT (product thinking, not code generation).

The thinking depth is decided by the productization level (L0-L3), NOT by
keyword greed: a small internal tool (L0) gets no product push; a platform
or commercial product (L2/L3) triggers implicit-requirement chains, open
source readiness and commercial readiness.

Key restraint rules (from product-specs/product-thinking.md):
- the implicit-requirement chains produce QUESTIONS to confirm, never
  features to implement without confirmation
- low-cost reservations (tenant_id in queries/indexes, org context in
  events, audit fields, API versioning) are design-time defaults;
  high-cost implementations (Billing/Subscription/marketplace) require an
  explicit commercialization signal
- no open-source intent -> no open-source doc suite; L0 -> no product
  structure at all

Used by `pi-batch assess` (pbatch/assessor.py); standalone:
    python -c "from pbatch.product import product_manifest; ..."
"""

from __future__ import annotations

from typing import Optional

from .relevance import _keyword_hit

LEVELS = ("L0_local_feature", "L1_reusable_module", "L2_platform_capability",
          "L3_product_feature")

_PRODUCT_SIGNALS = {
    "L3_product_feature": [
        "产品", "商业化", "开源", "上线", "对外", "发布", "saas",
        "product", "commercial", "open source", "release",
    ],
    "L2_platform_capability": [
        "平台", "多租户", "多端", "开放", "集成", "插件", "生态",
        "platform", "multi-tenant", "webhook", "api key", "marketplace",
    ],
    "L1_reusable_module": [
        "模块", "通用", "复用", "组件库", "sdk", "library", "module",
        "reusable", "组件化",
    ],
    "L0_local_feature": [
        "小工具", "脚本", "内部", "临时", "工具", "utility", "script",
        "internal", "demo",
    ],
}

# Scenario keyword -> implicit-requirement question chains (QUESTIONS to
# confirm with the requester, never features to implement silently).
_SCENARIOS = {
    "登录/认证": (
        ["登录", "认证", "auth", "login", "sso", "oauth"],
        ["权限/RBAC", "令牌刷新/过期", "安全与风控", "审计",
         "SSO/OAuth/OIDC", "多端/设备管理"],
    ),
    "上传/文件": (
        ["上传", "文件", "upload", "file", "附件", "oss", "s3"],
        ["失败重试", "断点续传", "秒传", "大文件/对象存储", "权限/外链",
         "CDN", "生命周期/过期"],
    ),
    "审批流": (
        ["审批", "approval", "approve", "工作流", "workflow"],
        ["审批节点/条件", "模板/版本", "历史/回滚", "代理", "超时", "通知/事件"],
    ),
    "消息/聊天": (
        ["聊天", "消息", "chat", "message", "即时通讯"],
        ["群组/频道", "未读/已读回执", "多端同步", "离线消息",
         "搜索/引用", "撤回/删除", "推送"],
    ),
    "ERP/订单": (
        ["erp", "订单", "采购", "库存", "财务", "order"],
        ["域划分（采购/销售/库存/财务）", "状态机/生命周期",
         "审批/库存/支付/物流/售后"],
    ),
    "搜索": (
        ["搜索", "检索", "search", "lookup"],
        ["精确/前缀/全文/模糊", "排序/相关性", "权限过滤", "索引重建"],
    ),
}

# Product spec files by level (L0 gets none — restraint).
_SPECS_BY_LEVEL = {
    "L0_local_feature": [],
    "L1_reusable_module": ["product-specs/product-thinking.md",
                           "product-specs/completion-evidence.md"],
    "L2_platform_capability": ["product-specs/product-thinking.md",
                               "product-specs/commercial-readiness.md",
                               "product-specs/completion-evidence.md"],
    "L3_product_feature": ["product-specs/product-thinking.md",
                           "product-specs/commercial-readiness.md",
                           "product-specs/open-source-readiness.md",
                           "product-specs/completion-evidence.md"],
}


def productization_level(text: str) -> tuple:
    """(level, evidence): the highest signal level matched; default L0."""
    lowered = (text or "").lower()
    for level in reversed(LEVELS):
        terms = _PRODUCT_SIGNALS.get(level, [])
        matched = tuple(str(t) for t in terms if _keyword_hit(lowered, str(t)))
        if matched:
            return level, matched
    return LEVELS[0], ()


def scenario_questions(text: str) -> list:
    """Implicit-requirement question chains for matched scenarios."""
    lowered = (text or "").lower()
    questions = []
    for label, (terms, chain) in _SCENARIOS.items():
        if any(_keyword_hit(lowered, str(t)) for t in terms):
            questions.append({"scenario": label, "questions": list(chain)})
    return questions


def product_manifest(text: str, level: Optional[str] = None) -> dict:
    """Product dimension of a requirement assessment.

    Returns {level, evidence, scenarios, specs, restraint_notes}."""
    resolved, evidence = productization_level(text) if level is None else (level, ())
    scenarios = scenario_questions(text) if resolved != "L0_local_feature" else []
    notes = []
    if resolved == "L0_local_feature":
        notes.append("L0 小工具需求：禁止产品化结构，只实现需求本身（克制原则）")
    elif resolved in ("L2_platform_capability", "L3_product_feature"):
        notes.append("低成本预留（必须）：tenant_id 进主查询/唯一索引、事件带组织"
                     "上下文、审计字段、API 版本化")
        notes.append("高成本实现（需明确商业信号才做）：Billing/Subscription/"
                     "插件市场禁止提前设计")
    if scenarios:
        notes.append("推演出的隐含需求是待确认问题，不得未经确认直接实现")
    return {
        "level": resolved,
        "evidence": list(evidence),
        "scenarios": scenarios,
        "specs": _SPECS_BY_LEVEL.get(resolved, []),
        "restraint_notes": notes,
    }


def format_product(manifest: dict) -> str:
    """Human-readable product dimension block."""
    lines = [f"产品化: {manifest['level']}"]
    if manifest["evidence"]:
        lines.append("  信号: " + ", ".join(manifest["evidence"]))
    for scenario in manifest["scenarios"]:
        lines.append(f"  推演[{scenario['scenario']}]: " + " / ".join(
            scenario["questions"]))
    if manifest["specs"]:
        lines.append("  规范: " + ", ".join(manifest["specs"]))
    for note in manifest["restraint_notes"]:
        lines.append(f"  克制: {note}")
    return "\n".join(lines)
