"""Change Impact Analyzer — 变更影响分析（高级工程师 vs 初级程序员的区别）.

高级工程师接需求先问"改这个会影响什么"，不是"改哪里"。`pi-batch impact`
在**执行之前**生成影响报告：受影响面（数据库/后端/前端/API/权限）→
风险等级 → 需要的 Agent 角色 → 工作等级（L0-L3 增量路由）→ 变化成本。

**增量式工作流的核心**：不是每个 Prompt 都跑完整流水线——影响面小则只
触发 Coding+Test（L0/L1），影响面大才触发 Domain/Architecture/Security
（L2/L3）。复用已有能力：classify（任务类型）、profile（风险/模式/
假设）、graph extract（模块依赖）、prompts/（角色清单）、adr（决策缓存）。

Usage:
    pi-batch impact --task "增加员工绩效导出" [--code DIR] [--json]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Optional

# 信号词 → 受影响面（与 assessor/reflect 同风格的双语关键词）。
DATABASE_SIGNALS = ("数据库", "表", "字段", "migration", "索引", "存储",
                    "schema", "数据模型")
API_SIGNALS = ("接口", "api", "endpoint", "http", "openapi", "契约")
FRONTEND_SIGNALS = ("页面", "前端", "按钮", "组件", "表单", "列表", "样式",
                    "ui", "页面", "dashboard")
PERMISSION_SIGNALS = ("权限", "rbac", "授权", "角色", "permission")
SECURITY_SIGNALS = ("支付", "资金", "审计", "删除", "生产", "密钥", "敏感",
                    "payment", "audit", "delete")
EXPORT_SIGNALS = ("导出", "大文件", "批量", "报表", "export", "report")

# 受影响面 → 需要的 Agent 角色（prompts/ 角色模板名）。
AGENTS_BY_AREA = {
    "database": "database_architect",
    "backend": "backend_engineer",
    "frontend": "frontend_engineer",
    "api": "api_designer",
    "permission": "security_engineer",
    "security": "security_engineer",
    "performance": "performance_engineer",
}
REVIEW_AGENTS = ("code_architecture_reviewer", "security_engineer",
                 "qa_lead")


def _detect_areas(task: str) -> dict:
    """受影响面检测（数据库/API/前端/权限/安全/性能信号）。"""
    lowered = (task or "").lower()
    areas = {}
    for area, signals in (
            ("database", DATABASE_SIGNALS), ("api", API_SIGNALS),
            ("frontend", FRONTEND_SIGNALS), ("permission",
                                             PERMISSION_SIGNALS)):
        hits = [s for s in signals if s in (task or "") or s in lowered]
        if hits:
            areas[area] = hits
    sensitive = [s for s in SECURITY_SIGNALS
                 if s in (task or "") or s in lowered]
    if sensitive:
        areas["security"] = sensitive
    if any(s in (task or "") or s in lowered for s in EXPORT_SIGNALS):
        areas["performance"] = ["导出/批量信号"]
    return areas


def _change_cost(areas: dict, risk: float) -> str:
    """变化成本：受影响面数 + 风险 + 数据资产（控制变化成本的输入）。"""
    if "security" in areas or ("database" in areas and risk >= 0.5):
        return "critical" if risk >= 0.7 else "high"
    if len(areas) >= 3:
        return "medium"
    return "low"


def _required_agents(areas: dict, mode: int) -> list:
    """需要的 Agent 角色（按受影响面 + 工作等级动态触发）。"""
    agents = sorted({AGENTS_BY_AREA[area]
                     for area in areas if area in AGENTS_BY_AREA})
    if mode >= 2:
        agents += ["architect", "domain_expert"]
    if mode >= 3:
        agents += list(REVIEW_AGENTS)
    return list(dict.fromkeys(agents))


def _flow_for(work_level: str, areas: dict) -> str:
    """增量流程：影响面小则 Coding→Test，大则完整评审。"""
    if work_level in ("L0", "L1") and not areas.get("security"):
        return "Coding → Test"
    if work_level == "L1":
        return "Impact → Backend/Frontend → Test → Review"
    if work_level == "L2":
        return "Domain/Architecture → DB → Coding → Review"
    return "完整流程（需求/产品/架构/数据/编码/全角色审查）"


def change_impact(task: str, code_dir: str = "") -> dict:
    """影响分析：受影响面 → 风险 → 角色 → 工作等级 → 变化成本。"""
    from .profile import task_profile
    areas = _detect_areas(task)
    report = task_profile(task)
    risk = report["profile"]["risk"]
    mode = report["mode"]
    work_level = f"L{mode}"

    affected_modules = []
    if code_dir:
        from .hypergraph import extract_module_graph
        try:
            graph = extract_module_graph(code_dir)
        except OSError:
            graph = None
        if graph is not None:
            affected_modules = sorted(graph.nodes.keys())[:10]

    # 决策缓存：相关 ADR 引用（不重新讨论已定事项）
    from .adr import find_adrs
    related_adrs = [{"number": r["number"], "title": r["title"],
                     "decision": r["decision"][:60], "status": r["status"]}
                    for r in find_adrs(task)][:3]

    return {
        "task": " ".join((task or "").split())[:120],
        "work_level": work_level,
        "risk": risk, "mode": mode,
        "affected_areas": areas,
        "change_cost": _change_cost(areas, risk),
        "required_agents": _required_agents(areas, mode),
        "affected_modules": affected_modules,
        "related_adrs": related_adrs,
        "flow": _flow_for(work_level, areas),
        "_note": "影响面小则增量执行（L0/L1 只触发 Coding+Test）；"
                 "影响面大才触发完整评审——基于项目状态，不是每次全流程",
    }


def impact_main(argv: Optional[list] = None) -> int:
    """`pi-batch impact --task TEXT [--code DIR] [--json]`。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py impact",
        description="Change Impact Analyzer：执行前的影响分析（增量工作流）")
    parser.add_argument("--task", required=True, help="需求文本")
    parser.add_argument("--code", default="",
                        help="代码目录（模块依赖分析用）")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    report = change_impact(args.task, args.code)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        lines = ["# Change Impact Report", ""]
        lines.append(f"任务: {report['task']}")
        lines.append(f"工作等级: {report['work_level']}（增量路由） | "
                     f"风险 {report['risk']} | 变化成本 {report['change_cost']}")
        lines.append(f"受影响面: {report['affected_areas'] or '无'}")
        lines.append(f"需要角色: {', '.join(report['required_agents']) or '-'}")
        if report["related_adrs"]:
            lines.append("相关决策（ADR 缓存）:")
            for adr in report["related_adrs"]:
                lines.append(f"  ADR-{adr['number']:03d} [{adr['status']}] "
                             f"{adr['title']} → {adr['decision']}")
        lines.append(f"流程: {report['flow']}")
        print("\n".join(lines))
    raise SystemExit(0)


if __name__ == "__main__":
    raise SystemExit(impact_main())
