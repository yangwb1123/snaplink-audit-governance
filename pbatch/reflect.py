"""Reflection Engine — 二阶观察层（Meta Cognitive Layer）.

系统已有 check（验证产物）但缺 critique（审视决策链本身）。Reflection
Engine 在 VERIFY 之后对"整个决策链"做二阶分析：目标是否真被解决、需求
是否遗漏、假设是否仍成立、是否过度设计、失败路径是否缺失、是否有未来
债务——并产出可执行的改进动作（learn/truth/eval 闭环）。

**Critic 纪律（防确认偏差）**：只吃证据（需求文本/代码/运行结果/指标），
不看执行者的自我解释。本实现是确定性启发式——天然满足。

**强度分级（防"大炮打蚊子"）**：
- R0 quick：目标对齐 / 需求完整性 / 假设审计（秒级）
- R1 工程：+架构 / 复杂度 / 失败路径 / 安全 / 性能（分钟级）
- R2 高级：+UX / 未来影响 / 反方验证（feature/module/生产）

12 维度复用已有能力：atomize（目标）、assessor（完整性）、profile
（假设）、hypergraph（架构循环）、quality（复杂度）——Reflection 是
"把它们串起来的人"，不是新能力。

Usage:
    pi-batch reflect --task "需求" [--code DIR] [--json]
    pi-batch reflect quick|assumption|architecture|security|full --task ...
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

REFLECTIONS_FILE = Path(".pi-batch") / "reflections.jsonl"

SEVERITY_WEIGHTS = {"critical": 20, "warning": 8, "info": 3}

# 12 维度的检查关键词（与 assessor/atomize 同风格的双语信号）。
FAILURE_SIGNALS = ("失败", "错误", "异常", "超时", "重试", "降级", "冲突",
                   "fail", "error", "timeout", "retry")
SECURITY_SIGNALS = ("权限", "越权", "认证", "加密", "审计", "敏感", "注入",
                    "auth", "permission", "injection")
SECURITY_TRIGGERS = ("权限", "支付", "删除", "审计", "生产", "密钥",
                     "permission", "payment", "delete", "secret")
PERF_SIGNALS = ("性能", "异步", "分页", "缓存", "并发", "批量", "索引",
                "async", "pagination", "cache", "batch")
PERF_TRIGGERS = ("导出", "大文件", "搜索", "同步", "全量", "导入", "报表",
                 "export", "search", "sync", "import", "report")
UX_SIGNALS = ("反馈", "加载", "空态", "错误提示", "loading", "feedback",
              "empty")
FUTURE_SIGNALS = ("临时", "硬编码", "先这样", "快速修复", "暂不", "绕过",
                  "hardcode", "temp", "quick fix")


def _keywords_present(text: str, signals: tuple) -> bool:
    lowered = (text or "").lower()
    return any(signal in (text or "") or signal in lowered
               for signal in signals)


def _goal_alignment_check(task: str, evidence: str) -> list:
    """1. 目标对齐：需求核心词是否在证据/产物中体现（防解决症状非根因）。"""
    from .rule_matcher import _business_terms
    if not evidence:
        return [{"type": "goal_alignment", "severity": "info",
                 "title": "无证据可做目标对齐检查",
                 "evidence": [], "impact": "无法确认最终实现解决用户目标",
                 "action": "提供运行结果/测试/产物后再 reflect"}]
    terms = _business_terms(task, {}, limit=8)
    evidence = evidence or ""
    covered = [term for term in terms
               if term in evidence
               or any(term[i:i + 2] in evidence
                      for i in range(max(0, len(term) - 1)))]
    if not terms:
        return []
    if not covered:
        return [{"type": "goal_alignment", "severity": "warning",
                 "title": f"需求核心词未在证据中体现: {terms}",
                 "evidence": [f"需求词: {', '.join(terms)}"],
                 "impact": "可能解决了症状而非根因（solution optimized "
                           "symptom not cause）",
                 "action": "核对实现是否真正满足用户目标"}]
    return []


def _completeness_check(task: str) -> list:
    """2. 需求完整性：缺失的角色/流程/异常/验收维度。"""
    from .assessor import _SUGGESTIONS, assess_dimensions
    findings = []
    for name, _, present in assess_dimensions(task):
        if not present and name in _SUGGESTIONS:
            findings.append({
                "type": "missed_requirement", "severity": "warning",
                "title": f"需求维度缺失: {name}",
                "evidence": [_SUGGESTIONS[name]],
                "impact": "缺失维度在实现中可能被忽略",
                "action": f"补充 {name} 的设计/验收后再评估",
            })
    return findings


def _assumption_audit(task: str, evidence: str) -> list:
    """3. 假设审计：隐含假设是否被验证（大型 Agent 最大错误来源）。"""
    from .profile import assumption_records
    findings = []
    for record in assumption_records(task):
        risk = record["assumption_risk"]
        statement = record["statement"]
        verified = bool(evidence) and any(
            token in (evidence or "") for token in statement.split()[1:4])
        if verified:
            continue
        findings.append({
            "type": "wrong_assumption", "severity":
                "critical" if risk >= 0.35 else "warning",
            "title": f"假设未验证: {statement[:50]}",
            "evidence": [f"假设风险 {risk}，处理方案: "
                         f"{record['verification_plan']}"],
            "impact": "假设失效会使依赖它的整个计划被撤回（truth 级联）",
            "action": "验证该假设或显式标记 accepted 并记录理由",
        })
    return findings


def _architecture_check(code_dir: str) -> list:
    """4. 架构审查：模块级循环依赖（graph extract）。"""
    if not code_dir:
        return []
    from .hypergraph import extract_module_graph
    try:
        graph = extract_module_graph(code_dir)
    except OSError:
        return []
    cycles = graph.depends_on_cycles()
    if not cycles:
        return []
    return [{"type": "architecture", "severity": "critical",
             "title": f"模块级循环依赖 {len(cycles)} 个",
             "evidence": ["→".join(c) for c in cycles[:3]],
             "impact": "循环依赖增加维护与测试成本（死锁/初始化风险）",
             "action": "用 graph extract 定位并打破循环（惰性导入/拆模块）"}]


def _complexity_check(code_dir: str) -> list:
    """5. 复杂度审查：大函数/高复杂度（quality.py 门禁）。"""
    if not code_dir:
        return []
    try:
        result = subprocess.run(
            [sys.executable, str(Path(__file__).resolve().parent.parent
                                 / "quality.py"), "--strict", code_dir],
            capture_output=True, text=True, timeout=120)
    except (OSError, subprocess.TimeoutExpired):
        return []
    matches = re.findall(r"(.+\.py):\d+ func '(\w+)' (\d+) lines? > (\d+)",
                         result.stdout)
    findings = []
    for path, func, lines, budget in matches[:5]:
        findings.append({
            "type": "over_design", "severity": "warning",
            "title": f"{Path(path).name}.{func} {lines} 行 > {budget} 预算",
            "evidence": [f"{path}:{func}"],
            "impact": "大函数增加维护成本",
            "action": "按职责拆分（本工具 cli.py 967 行拆分是范例）",
        })
    return findings


def _failure_mode_check(task: str) -> list:
    """6. 失败路径缺失。"""
    if _keywords_present(task, FAILURE_SIGNALS):
        return []
    return [{"type": "missing_failure_mode", "severity": "warning",
             "title": "未提及任何失败路径",
             "evidence": ["需求文本无 失败/错误/超时/重试/降级 信号"],
             "impact": "网络断开/磁盘满/重复提交/权限变化等失败未设计",
             "action": "补充异常路径设计（参考 ui-specs/engineering/"
                       "error-recovery.md）"}]


def _security_check(task: str) -> list:
    """7. 安全审查：敏感域无安全设计。"""
    if not _keywords_present(task, SECURITY_TRIGGERS):
        return []
    if _keywords_present(task, SECURITY_SIGNALS):
        return []
    return [{"type": "security_risk", "severity": "critical",
             "title": "涉及权限/支付/审计但无安全设计信号",
             "evidence": ["命中敏感域: 权限/支付/删除/审计/生产"],
             "impact": "越权/注入/敏感数据泄露风险",
             "action": "补充权限矩阵与安全验证（backend-specs/"
                       "production-readiness.md）"}]


def _performance_check(task: str) -> list:
    """8. 性能审查：大数据/导出/搜索场景无性能设计。"""
    if not _keywords_present(task, PERF_TRIGGERS):
        return []
    if _keywords_present(task, PERF_SIGNALS):
        return []
    return [{"type": "performance_issue", "severity": "warning",
             "title": "大数据/导出/搜索场景未提及性能设计",
             "evidence": ["命中: 导出/大文件/搜索/同步/全量"],
             "impact": "N+1 查询/全量同步/大文件内存加载风险",
             "action": "补充异步/分页/缓存/批量设计"}]


def _ux_check(task: str) -> list:
    """9. 用户体验（前端任务）：反馈/加载/空态缺失。"""
    from .classifier import classify_text
    if classify_text(task).task_type != "frontend_ui":
        return []
    if _keywords_present(task, UX_SIGNALS):
        return []
    return [{"type": "ux_gap", "severity": "info",
             "title": "前端任务未提及反馈/加载/空态设计",
             "evidence": ["无 反馈/加载/空态/错误提示 信号"],
             "impact": "操作反馈缺失，认知负担增加",
             "action": "补充 loading/空态/错误恢复（ui-specs/"
                       "engineering/form-table-state.md）"}]


def _future_impact_check(task: str, code_dir: str) -> list:
    """10. 未来影响：临时/硬编码信号 = 未来债务。"""
    findings = []
    if _keywords_present(task, FUTURE_SIGNALS):
        findings.append({
            "type": "future_debt", "severity": "warning",
            "title": "存在临时/硬编码/绕过的信号",
            "evidence": ["命中: 临时/硬编码/先这样/绕过"],
            "impact": "6 个月后成为债务（如硬编码字段阻碍多租户）",
            "action": "为临时方案标注 TODO + 过期时间（learn exception "
                      "模式）"})
    if code_dir:
        hardcoded = 0
        for path in Path(code_dir).rglob("*.py"):
            if ".dart_tool" in path.parts or "test" in path.parts:
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            hardcoded += len(re.findall(r"\b(?:magic|hack|temporary|temp)",
                                        text, re.IGNORECASE))
        if hardcoded > 0:
            findings.append({
                "type": "maintenance_issue", "severity": "info",
                "title": f"代码中 {hardcoded} 处临时/魔法标记",
                "evidence": ["magic/hack/temporary/temp 关键词"],
                "impact": "维护者无法区分临时与正式代码",
                "action": "清理或登记为 learn exception（90 天复查）"})
    return findings


def _adversarial_check(evidence: str) -> list:
    """11. 反方验证：是否只有正面证据。"""
    if not evidence:
        return []
    if _keywords_present(evidence, ("失败", "拒绝", "边界", "负面", "不通过",
                                    "fail", "reject", "boundary")):
        return []
    return [{"type": "adversarial_gap", "severity": "info",
             "title": "证据只有正面结果，无反方验证",
             "evidence": ["未发现 失败/拒绝/边界/负面 信号"],
             "impact": "确认偏差：需要证明方案有问题而非只是通过",
             "action": "补充边界/负面/故障注入验证（nversion + 故障测试）"}]


def _knowledge_extraction(findings: list) -> list:
    """12. 知识提取：从发现生成候选规则（进入 learn 流程）。"""
    if not findings:
        return []
    types = {f["type"] for f in findings if f["severity"] != "info"}
    if not types:
        return []
    rules = []
    if "missed_requirement" in types:
        rules.append({"rule_id": "REFLECT-COMPLETENESS",
                      "title": "需求缺失维度必须在实现前显式确认",
                      "command": "pi-batch learn draft ... --rule-id "
                                 "REFLECT-COMPLETENESS"})
    if "wrong_assumption" in types:
        rules.append({"rule_id": "REFLECT-ASSUMPTION",
                      "title": "高影响假设必须先验证或显式接受",
                      "command": "pi-batch learn draft ... --rule-id "
                                 "REFLECT-ASSUMPTION"})
    if "missing_failure_mode" in types:
        rules.append({"rule_id": "REFLECT-FAILURE-PATH",
                      "title": "涉及外部依赖的任务必须设计失败路径",
                      "command": "pi-batch learn draft ... --rule-id "
                                 "REFLECT-FAILURE-PATH"})
    return rules


CHECKS_R0 = ("goal", "completeness", "assumption")
CHECKS_R1 = CHECKS_R0 + ("architecture", "complexity", "failure", "security",
                         "performance")
CHECKS_FULL = CHECKS_R1 + ("ux", "future", "adversarial")


def reflect_on(task: str, code_dir: str = "", evidence: str = "",
               mode: str = "full") -> dict:
    """对一次任务/决策链做二阶反思（确定性启发式，零 LLM）。"""
    checks = {
        "quick": CHECKS_R0, "assumption": ("assumption",),
        "architecture": ("architecture", "complexity"),
        "security": ("security", "failure"),
        "full": CHECKS_FULL,
    }.get(mode, CHECKS_FULL)
    runners = {
        "goal": lambda: _goal_alignment_check(task, evidence),
        "completeness": lambda: _completeness_check(task),
        "assumption": lambda: _assumption_audit(task, evidence),
        "architecture": lambda: _architecture_check(code_dir),
        "complexity": lambda: _complexity_check(code_dir),
        "failure": lambda: _failure_mode_check(task),
        "security": lambda: _security_check(task),
        "performance": lambda: _performance_check(task),
        "ux": lambda: _ux_check(task),
        "future": lambda: _future_impact_check(task, code_dir),
        "adversarial": lambda: _adversarial_check(evidence),
    }
    findings = []
    for check in checks:
        findings.extend(runners[check]())
    findings.sort(key=lambda f: SEVERITY_WEIGHTS.get(f["severity"], 0),
                  reverse=True)
    score = 100.0
    for finding in findings:
        score -= SEVERITY_WEIGHTS.get(finding["severity"], 0)
    score = round(max(0.0, score), 1)
    return {
        "task": " ".join((task or "").split())[:120],
        "mode": mode, "checked_dimensions": list(checks),
        "findings": findings,
        "reflection_score": score,
        "grade": ("A" if score >= 90 else "B" if score >= 75
                  else "C" if score >= 60 else "D"),
        "knowledge_updates": _knowledge_extraction(findings),
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def _append_reflection(report: dict) -> str:
    """追加式反思账本（truth/learn/eval 闭环的输入）。"""
    REFLECTIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with REFLECTIONS_FILE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(report, ensure_ascii=False) + "\n")
    return str(REFLECTIONS_FILE)


def _render(report: dict) -> str:
    lines = [f"# Reflection Report（mode={report['mode']}）", ""]
    lines.append(f"任务: {report['task']}")
    lines.append(f"反思评分: {report['reflection_score']} "
                 f"(grade {report['grade']}) — 检查维度: "
                 f"{len(report['checked_dimensions'])}")
    lines.append("")
    for finding in report["findings"]:
        lines.append(f"[{finding['severity']:>8}] {finding['title']}")
        for evidence in finding["evidence"]:
            lines.append(f"         证据: {evidence}")
        lines.append(f"         影响: {finding['impact']}")
        lines.append(f"         动作: {finding['action']}")
    if not report["findings"]:
        lines.append("未发现明显问题。")
    if report["knowledge_updates"]:
        lines.append("")
        lines.append("## 知识提取（候选规则 → learn 流程）")
        for update in report["knowledge_updates"]:
            lines.append(f"- {update['title']}")
    lines.append("")
    lines.append("## 闭环动作")
    critical = [f for f in report["findings"]
                if f["severity"] == "critical"]
    if critical:
        lines.append(f"- {len(critical)} 个 critical 发现：先修复再进入下一任务")
    if report["findings"]:
        lines.append("- 发现写入 truth（事实错误）/ causal（因果错误）")
        lines.append("- 重复出现的问题应升级为规则（learn draft → shadow → promote）")
    return "\n".join(lines)


def reflect_main(argv: Optional[list] = None) -> int:
    """`pi-batch reflect [mode] --task TEXT [--code DIR] [--evidence TEXT]
    [--json] [--save]`。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py reflect",
        description="Reflection Engine：对决策链做二阶审视（Critic Layer）")
    parser.add_argument("mode", nargs="?", default="full",
                        choices=("quick", "assumption", "architecture",
                                 "security", "full"),
                        help="强度分级：R0 quick / R1 工程 / R2 高级")
    parser.add_argument("--task", required=True, help="需求/任务文本")
    parser.add_argument("--code", default="",
                        help="代码目录（architecture/complexity 检查用）")
    parser.add_argument("--evidence", default="",
                        help="执行证据（运行结果/测试输出——Critic 只信证据）")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--save", action="store_true",
                        help="追加到 .pi-batch/reflections.jsonl")
    args = parser.parse_args(argv)
    report = reflect_on(args.task, args.code, args.evidence, args.mode)
    if args.save:
        path = _append_reflection(report)
        print(f"saved: {path}")
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(_render(report))
    raise SystemExit(0 if report["reflection_score"] >= 75 else 1)
