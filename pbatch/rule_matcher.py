"""Deterministic rule matching: which ui-spec rules apply to a task.

The spec system is large; a small demo must not load all of it. This
module is the algorithmic half of rule selection (the LLM stays inside
the emitted candidate set). A task is classified along three axes:

- scale:    demo < standard < production (default standard; demos must be
            marked with demo/示例/原型 keywords)
- page_type: form/table/detail/workbench/immersive/auth (matched keywords)
- risk:     high-risk business actions (pay/approve/delete/submit...)

`match_rules` returns a bounded manifest: rule id, tier, matched evidence,
and the spec files to load. Prompts inject ONLY the manifest; agents load
the files on demand (progressive disclosure, like the memory manifest).

Usage:
    pi-batch rules "生成一个支付表单页"            # human manifest
    pi-batch rules "登录页 demo" --json            # machine report
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Optional

from . import config
from .classifier import TaskClassification, classify_text
from .config import log, yaml
from .relevance import _keyword_hit
from .text_io import read_text_bounded

TIER_ORDER = {"demo": 0, "standard": 1, "production": 2}
DEFAULT_TIER = "standard"

# Domain -> registry location (selected by the classified task type).
# "generic" (code maintenance/analysis/docs) loads NO domain specs — a
# refactor task must not be prescribed frontend spacing rules.
DOMAIN_REGISTRIES = {
    "backend": ["backend-specs/rules.yaml"],
    "frontend_ui": ["ui-specs/rules.yaml"],
    "product": ["product-specs/rules.yaml"],
    "generic": [],
}
DEFAULT_DOMAIN = "generic"

_DEFAULT_REGISTRY = {
    "scale": {
        "demo": ["demo", "示例", "演示", "原型", "prototype", "小demo"],
        "production": ["生产", "上线", "正式", "企业", "production", "enterprise"],
    },
    "page_types": {
        "form": ["表单", "新建", "提交页", "form"],
        "table": ["表格", "列表", "分页", "批量", "table"],
        "detail": ["详情", "detail"],
        "workbench": ["工作台", "仪表盘", "看板", "dashboard"],
        "immersive": ["特效", "落地页", "官网", "营销", "动画"],
        "auth": ["登录", "注册", "login", "register"],
    },
    "risk": {"high": ["支付", "审批", "删除", "提交", "作废", "订单", "payment", "approve", "delete"]},
    "signals": {},
    "signal_weights": {},
    "use_case_types": {},
    "rules": {
        "visual-core": {"files": ["ui-specs/spacing.md", "ui-specs/anti-patterns.md"],
                        "min_tier": "demo", "required": True,
                        "level": "invariant",
                        "description": "8pt 间距 token 与反例清单"},
        "component-spec": {"files": ["ui-specs/component-spec.md"], "min_tier": "demo",
                           "description": "组件尺寸/状态/行为规范"},
        "business-profile": {"files_template": "ui-specs/business-profiles/{profile}.md",
                             "min_tier": "demo", "description": "业务风格配置"},
    },
}


def domain_for(text: str, classification: Optional[TaskClassification] = None) -> str:
    """Which spec domain a task belongs to (backend / frontend_ui /
    product / generic — code-maintenance and analysis tasks get NO domain
    specs; L0 local-feature tasks also stay generic (restraint))."""
    cls = classification or classify_text(text)
    if cls.task_type == "backend":
        return "backend"
    if cls.task_type == "frontend_ui":
        return "frontend_ui"
    # Lazy import: pbatch.product imports this module at module level
    # (specs derivation); a module-level import here would be a cycle.
    from .product import productization_level
    if productization_level(text)[0] != "L0_local_feature":
        return "product"
    return DEFAULT_DOMAIN


def _default_registry_paths(domain: str) -> list:
    rels = DOMAIN_REGISTRIES.get(domain, DOMAIN_REGISTRIES[DEFAULT_DOMAIN])
    return ([Path(__file__).resolve().parent.parent / rel for rel in rels]
            + [Path(rel) for rel in rels])


def load_registry(path: str = "", domain: str = "") -> dict:
    """Load the rule registry for a domain (default: frontend). YAML
    sections extend the built-in defaults; a missing backend registry
    degrades to an empty rule set (fail closed, never the frontend rules)."""
    merged = {section: dict(value) for section, value in _DEFAULT_REGISTRY.items()}
    if domain in ("backend", "generic", "product"):
        merged = {"scale": {}, "signals": {}, "signal_weights": {},
                  "page_types": {}, "use_case_types": {},
                  "risk": {}, "rules": {}}
    if not yaml:
        return merged
    candidates = [Path(path)] if path else _default_registry_paths(domain or DEFAULT_DOMAIN)
    for candidate in candidates:
        try:
            data = yaml.safe_load(read_text_bounded(
                candidate, config.INPUT_MAX_BYTES, "rule registry")) or {}
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        for section, value in data.items():
            if section in merged and isinstance(value, dict):
                merged[section].update({str(k): v for k, v in value.items()})
    return merged


def _best_hits(text: str, section: dict, lowered: str = "") -> list:
    """All section entries with their matched keywords (evidence)."""
    hits = []
    for name, terms in section.items():
        matched = tuple(str(t) for t in terms if _keyword_hit(lowered, str(t)))
        if matched:
            hits.append((name, matched))
    return hits


def detect_scale(text: str, registry: dict) -> tuple:
    """(tier, evidence): demo/production keywords win over the default."""
    lowered = (text or "").lower()
    for tier in ("demo", "production"):
        matched = tuple(str(t) for t in registry.get("scale", {}).get(tier, [])
                        if _keyword_hit(lowered, str(t)))
        if matched:
            return tier, matched
    return DEFAULT_TIER, ()


LEVELS = ("invariant", "contract", "policy", "heuristic", "suggestion")


def _rule_item(rule_id: str, rule: dict, tier: str, tier_evidence: tuple,
                page_types: list, high_risk: bool, profile: str) -> dict:
    """规则清单条目：层级 + 激活度（AADM §14 的确定性简化）。"""
    files = _resolve_files(rule, profile)
    level = str(rule.get("level", "policy"))
    if level not in LEVELS:
        level = "policy"
    is_invariant = level == "invariant"
    required = (bool(rule.get("required", False)) or is_invariant
                or (high_risk and bool(rule.get("required_when_risk"))))
    return {
        "id": rule_id, "tier": rule.get("min_tier", "standard"),
        "required": required,
        "level": level,
        # 宪法 1.0 > 必选 0.9 > 选用 0.7
        "activation": 1.0 if is_invariant else (0.9 if required else 0.7),
        "description": rule.get("description", ""),
        "files": files, "evidence": {
            "scale": list(tier_evidence), "page_types": page_types,
            "risk": "high" if high_risk else "low",
            "profile": profile,
        },
    }


def _rule_skip_reason(rule: dict, tier: str, tier_rank: int, page_types: list,
                      high_risk: bool, profile: str) -> str:
    """Why a rule does NOT apply (empty string = it applies).
    Invariant（宪法层）永不跳过；suppress_on 命中 → 显式抑制（AADM §14）。"""
    if rule.get("level") == "invariant":
        return ""
    min_rank = TIER_ORDER.get(rule.get("min_tier", "standard"), 1)
    if tier_rank < min_rank:
        return f"tier below required {rule.get('min_tier')}"
    if rule.get("page_types") and not (set(rule["page_types"]) & set(page_types)):
        return "page type not matched"
    if rule.get("profiles") and profile not in rule.get("profiles", []):
        return f"profile '{profile or 'unknown'}' not in {rule.get('profiles')}"
    if rule.get("required_when_risk") and not high_risk:
        return "not high-risk"
    if rule.get("required"):
        return ""  # 必选规则不受显式抑制（fail closed）
    suppressed_on = rule.get("suppress_on") or []
    matched_signals = {tier} | set(page_types)
    if high_risk:
        matched_signals.add("high")
    if profile:
        matched_signals.add(profile)
    hit = sorted(matched_signals & set(suppressed_on))
    if hit:
        return f"suppressed on {hit[0]}"
    return ""


from pbatch.system_type_methodology import SYSTEM_TYPE_METHODOLOGY


def match_rules(text: str, classification: Optional[TaskClassification] = None,
                registry: Optional[dict] = None) -> dict:
    """Deterministic rule manifest for a task text (domain-aware: the
    registry is picked by the classified task type — backend tasks get the
    backend-specs registry, everything else the ui-specs registry).

    Returns {tier, page_types, risk, rules: [{id, tier, evidence, files,
    description, required}], skipped: [{id, reason}]}.
    """
    cls = classification or classify_text(text)
    if registry is None:
        registry = load_registry(domain=domain_for(text, cls))
    lowered = (text or "").lower()
    tier, tier_evidence = detect_scale(text, registry)
    # Backend registries declare use_case_types instead of page_types.
    case_section = registry.get("use_case_types") or registry.get("page_types", {})
    page_hits = _best_hits(lowered, case_section, lowered)
    risk_hits = _best_hits(lowered, registry.get("risk", {}), lowered)
    page_types = [name for name, _ in page_hits]
    high_risk = bool(risk_hits)
    tier_rank = TIER_ORDER.get(tier, TIER_ORDER[DEFAULT_TIER])

    profile = getattr(cls, "profile", "") or ""

    rules, skipped = [], []
    for rule_id, rule in registry.get("rules", {}).items():
        reason = _rule_skip_reason(rule, tier, tier_rank, page_types,
                                   high_risk, profile)
        if reason:
            skipped.append({"id": rule_id, "reason": reason})
            continue
        rules.append(_rule_item(rule_id, rule, tier, tier_evidence,
                                page_types, high_risk, profile))
    _inject_system_type_methodology(cls, rules)
    rules.sort(key=lambda item: (0 if item["required"] else 1, item["tier"]))
    return {"tier": tier, "page_types": page_types, "risk": "high" if high_risk else "low",
            "profile": profile, "rules": rules, "skipped": skipped,
            "domain": domain_for(text, cls),
            "system_type": getattr(cls, "system_type", "") or "deterministic"}


def _inject_system_type_methodology(cls, rules: list) -> None:
    """哥德尔分类学路由：system_type 命中的方法论规范进入规则清单。"""
    system_type = getattr(cls, "system_type", "") or "deterministic"
    methodology_files = SYSTEM_TYPE_METHODOLOGY.get(system_type, [])
    if not methodology_files:
        return
    rules.append({
        "id": f"system-type-{system_type}",
        "tier": "standard",
        "required": True,
        "description": (
            f"问题系统分类学方法论（{system_type}）：先判定问题属于哪类"
            "系统，再套用该类系统的方法论"
        ),
        "files": methodology_files,
        "evidence": {
            "system_type": system_type,
            "system_evidence": list(getattr(cls, "system_evidence", ())),
        },
    })


def _resolve_files(rule: dict, profile: str) -> list:
    """Rule files; profiles rules resolve the {profile} template."""
    template = rule.get("files_template")
    if template and profile:
        return [template.replace("{profile}", profile)]
    return list(rule.get("files", []))


def _business_terms(text: str, registry: dict, limit: int = 12) -> list:
    """Salient CJK business terms from the task, excluding vocabulary that
    already produced keyword evidence (deterministic compression)."""
    runs = re.findall(r"[\u3400-\u9fff]{2,6}", text or "")
    vocabulary = set()
    for section in ("page_types", "risk", "scale"):
        for terms in registry.get(section, {}).values():
            vocabulary.update(str(t) for t in terms if "\u4e00" <= str(t)[:1] <= "\u9fff")
    seen, terms = set(), []
    for run in runs:
        if run in vocabulary or run in seen:
            continue
        seen.add(run)
        terms.append(run)
        if len(terms) >= limit:
            break
    return terms


def summarize_task(text: str, registry: Optional[dict] = None,
                   max_chars: int = 400) -> str:
    """Deterministic requirement compression: the salient classification
    axes, business terms, and a bounded tail of the original text — the
    input the LLM side of the two-sided rule check reads."""
    reg = registry or load_registry(domain=domain_for(text))
    matched = match_rules(text, registry=reg)
    cls = classify_text(text)
    lines = [
        f"scale={matched['tier']} risk={matched['risk']} "
        f"page_types={','.join(matched['page_types']) or 'none'} "
        f"profile={matched['profile'] or 'generic'} "
        f"platform={cls.platform or 'unknown'} task_type={cls.task_type}",
    ]
    terms = _business_terms(text, reg)
    if terms:
        lines.append("business_terms=" + ",".join(terms))
    cleaned = " ".join((text or "").split())
    if len(cleaned) > max_chars:
        lines.append(f"原文截断({len(cleaned)}> {max_chars}): ..." + cleaned[:max_chars])
    else:
        lines.append("原文: " + cleaned)
    return "\n".join(lines)


def format_llm_prompt(text: str, registry: Optional[dict] = None) -> str:
    """The prompt for the LLM side of the check: compressed requirement +
    the algorithm manifest + a strict JSON output contract."""
    reg = registry or load_registry(domain=domain_for(text))
    matched = match_rules(text, registry=reg)
    return (
        "You are the rule selector. Read the COMPRESSED requirement and the "
        "algorithm manifest below, then independently decide which ui-spec "
        "rules apply to THIS task. You may add or skip rules, but you cannot "
        "skip REQUIRED ones. Respond with ONLY a JSON object:\n"
        '{"apply": ["rule_id", ...], "skip": ["rule_id", ...], '
        '"reason": "one line"}\n\n'
        "--- COMPRESSED REQUIREMENT ---\n" + summarize_task(text, reg) + "\n\n"
        "--- ALGORITHM MANIFEST ---\n" + format_manifest(matched) + "\n\n"
        "Available rule ids: " + ", ".join(sorted(reg.get("rules", {}).keys())))


def reconcile(algorithm: dict, llm_apply: list, llm_skip: list,
              registry: Optional[dict] = None) -> dict:
    """Two-sided check: agreed rules stay, the algorithm vetoes skips of
    REQUIRED/risk-mandated rules (fail closed), and valid LLM additions are
    admitted with provenance. Returns the final manifest."""
    reg = registry or load_registry(
        domain=algorithm.get("domain", "frontend_ui"))
    valid = set(reg.get("rules", {}))
    apply_ids = {str(i) for i in llm_apply} & valid
    skip_ids = {str(i) for i in llm_skip} & valid
    final, provenance, dropped = [], {}, []
    for item in algorithm["rules"]:
        rid = item["id"]
        if rid in skip_ids and not item["required"]:
            provenance[rid] = "llm-skipped"
            dropped.append({"id": rid, "reason": "LLM skipped an optional rule"})
            continue
        if rid in skip_ids:
            provenance[rid] = "algorithm-vetoed-llm-skip"
        elif rid in apply_ids:
            provenance[rid] = "both"
        else:
            provenance[rid] = "algorithm"
        final.append({**item, "provenance": provenance[rid]})
    for rid in sorted(apply_ids - {item["id"] for item in algorithm["rules"]}):
        rule = reg["rules"][rid]
        provenance[rid] = "llm-added"
        final.append({
            "id": rid, "tier": rule.get("min_tier", "standard"),
            "required": False, "description": rule.get("description", ""),
            "files": _resolve_files(rule, algorithm.get("profile", "")),
            "provenance": "llm-added",
        })
    final.sort(key=lambda item: (0 if item["required"] else 1, item["tier"]))
    return {**algorithm, "rules": final, "provenance": provenance,
            "dropped": dropped, "llm_apply": sorted(apply_ids),
            "llm_skip": sorted(skip_ids)}


def format_manifest(matched: dict, limit: int = 8) -> str:
    """Human/markdown manifest for prompt injection (bounded)."""
    lines = [f"## Applicable UI rules (deterministic manifest, tier={matched['tier']}, "
             f"risk={matched['risk']}, profile={matched['profile'] or 'generic'})"]
    # 信任感：system_type 证据透明（AI 建议带依据——认知负担 §7）
    system_type = matched.get("system_type", "")
    if system_type and system_type != "deterministic":
        evidence = matched.get("rules", [])
        src = next((x.get("evidence", {}).get("system_evidence", ())
                    for x in evidence
                    if x.get("id") == f"system-type-{system_type}"), ())
        lines.append(f"System type: {system_type}"
                     + (f" (evidence: {', '.join(src)})" if src else ""))
    lines.append("Apply ONLY these rules; load the listed files, do not load unrelated specs:")
    rules = matched["rules"]
    shown = rules[:limit]
    for item in shown:
        marker = "REQUIRED" if item["required"] else "optional"
        source = ""
        if "provenance" in item and item["provenance"] != "algorithm":
            source = f" [{item['provenance']}]"
        lines.append(f"- [{marker}]{source} {item['id']} ({item['tier']}): {item['description']}")
        for file in item["files"]:
            lines.append(f"    {file}")
    hidden = rules[limit:]
    if hidden:
        lines.append(f"- ... and {len(hidden)} more rule(s) in this manifest: "
                     + ", ".join(item["id"] for item in hidden))
    skipped = matched.get("skipped", [])
    if skipped:
        lines.append("Skipped: " + ", ".join(
            f"{item['id']} ({item['reason']})" for item in skipped))
    return "\n".join(lines)


