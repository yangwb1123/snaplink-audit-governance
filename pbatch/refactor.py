"""God File 重构分析器 — 软件工程规范的可执行化.

发现问题 → 代码度量 → 职责识别 → 模式选择 → 迁移计划 → 测试保护。

**核心判断（20 年经验）**：重构不是拆文件（order1/order2 只是换名字），
而是**拆变化原因**。本分析器用确定性启发式（纯 stdlib + ast）给出：

1. 文件级指标：行数/函数数/imports/依赖（warning/critical 分级）
2. 圈复杂度：每函数（1 + 决策点数，>20 必须拆 / 11-20 重构）
3. 职责污染：方法名关键词聚类到职责域（>1 域 → SRP 违规）
4. 模式信号：elif ≥3 → Strategy；业务层 `Xxx(` 直接构造 → DI；
   重复 try/except → AOP；大方法 → 拆分
5. 拆分建议：按变化原因划分模块结构
6. 迁移计划：测试保护 → 渐进提取（可执行步骤）

Usage:
    pi-batch refactor --file src/order/service.py [--json]
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from pathlib import Path
from typing import Optional

# 文件级指标阈值（warning/critical 两级，对应人类评审红线）。
LINE_WARNING, LINE_CRITICAL = 300, 600
FUNC_WARNING, FUNC_CRITICAL = 20, 40
IMPORT_WARNING = 20
DEP_WARNING = 15
# 圈复杂度分级（用户规范：1-5 简单 / 6-10 可接受 / 11-20 重构 / >20 必须拆）。
CYCLO_OK, CYCLO_ACCEPT, CYCLO_REFACTOR = 5, 10, 20

# 职责域关键词（方法名/注释 → 域；>1 域命中 = SRP 违规）。
RESPONSIBILITY_SIGNALS = {
    "order": ("order", "create_order", "订单", "下单"),
    "pricing": ("price", "pricing", "discount", "价格", "折扣"),
    "notification": ("notify", "notification", "send_email", "send_mail",
                     "通知", "邮件"),
    "audit": ("audit", "log", "审计", "日志"),
    "file": ("file", "upload", "export", "pdf", "文件", "上传", "导出"),
    "payment": ("pay", "payment", "refund", "支付", "退款"),
    "auth": ("auth", "login", "permission", "权限", "登录"),
}


def _cyclomatic(node: ast.AST) -> int:
    """圈复杂度近似：1 + if/while/for/except/and/or/boolop 决策点。"""
    decisions = 0
    for child in ast.walk(node):
        if isinstance(child, (ast.If, ast.While, ast.For, ast.ExceptHandler,
                              ast.Assert)):
            decisions += 1
        elif isinstance(child, ast.BoolOp):
            decisions += len(child.values) - 1
        elif isinstance(child, ast.IfExp):
            decisions += 1
    return 1 + decisions


def _responsibility_of(name: str) -> Optional[str]:
    """方法名 → 职责域（无命中返回 None）。"""
    lowered = name.lower()
    for domain, signals in RESPONSIBILITY_SIGNALS.items():
        if any(signal in lowered for signal in signals):
            return domain
    return None


def _analyze_functions(tree) -> list:
    """全部函数（含类方法）的度量与圈复杂度分级。"""
    functions = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        cyclo = _cyclomatic(node)
        functions.append({
            "name": node.name, "lines": (node.end_lineno or 0)
            - (node.lineno or 0) + 1, "complexity": cyclo,
            "level": ("ok" if cyclo <= CYCLO_OK
                      else "acceptable" if cyclo <= CYCLO_ACCEPT
                      else "refactor" if cyclo <= CYCLO_REFACTOR
                      else "must_split"),
        })
    return functions


def _analyze_classes(tree) -> list:
    """类的行数与方法数（上帝类信号）。"""
    classes = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        methods = [n for n in node.body
                   if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
        classes.append({"name": node.name, "methods": len(methods),
                        "lines": (node.end_lineno or 0)
                        - (node.lineno or 0) + 1})
    return classes


def _detect_responsibilities(tree) -> dict:
    """职责域聚类：方法名关键词 → 域（>1 域 = SRP 违规）。"""
    responsibilities = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        domain = _responsibility_of(node.name)
        if domain:
            responsibilities.setdefault(domain, []).append(node.name)
    return responsibilities


def _detect_signals(tree) -> list:
    """模式信号：分支数 → Strategy；直接构造 → DI；重复 try → AOP。"""
    signals = []
    elif_branches = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and node.orelse:
            if isinstance(node.orelse[0], ast.If):
                elif_branches += 1
    if elif_branches >= 3:
        signals.append({
            "pattern": "Strategy", "severity": "warning",
            "evidence": f"检测到 {elif_branches + 1} 个 if/elif 分支判断业务类型",
            "action": "分支 >3 时提取 Strategy/Factory（封装变化点）"})
    direct_new = sorted({n.func.id for n in ast.walk(tree)
                         if isinstance(n, ast.Call)
                         and isinstance(n.func, ast.Name)
                         and n.func.id[0].isupper()
                         and n.func.id not in (
                             "Path", "ValueError", "TypeError",
                             "RuntimeError", "KeyError", "IndexError",
                             "Exception", "NotImplementedError")})
    if direct_new:
        signals.append({
            "pattern": "DI", "severity": "warning",
            "evidence": f"业务层直接构造: {direct_new[:5]}",
            "action": "构造函数注入替代直接 new（Mysql→Repository 接口）"})
    try_count = sum(1 for n in ast.walk(tree) if isinstance(n, ast.Try))
    if try_count >= 3:
        signals.append({
            "pattern": "AOP", "severity": "warning",
            "evidence": f"{try_count} 处 try/except 横切逻辑",
            "action": "提取装饰器/中间件统一处理事务/日志/错误"})
    return signals


def _build_suggestions(metrics: dict, functions: list,
                       responsibilities: dict) -> list:
    """建议：职责污染/上帝文件/上帝函数/import 膨胀。"""
    suggestions = []
    if len(responsibilities) > 1:
        top = sorted(responsibilities.items(), key=lambda kv: -len(kv[1]))[:3]
        suggestions.append({
            "type": "responsibility_pollution", "severity": "critical",
            "evidence": [f"{domain}: {len(methods)} 个方法"
                         for domain, methods in top],
            "action": "按变化原因拆分为: " + ", ".join(
                domain for domain, _ in top)})
    if metrics["line_level"] == "critical":
        suggestions.append({
            "type": "god_file", "severity": "critical",
            "evidence": [f"{metrics['lines']} 行 > {LINE_CRITICAL} 红线"],
            "action": "按职责提取模块（不是拆文件，是拆变化原因）"})
    for func in functions:
        if func["level"] in ("refactor", "must_split"):
            suggestions.append({
                "type": "god_function",
                "severity": ("critical" if func["level"] == "must_split"
                             else "warning"),
                "evidence": [f"{func['name']} 复杂度 {func['complexity']}"
                             f"（{'必须拆' if func['level'] == 'must_split' else '建议重构'}）"],
                "action": f"拆分 {func['name']}：提取分支/校验/副作用为小函数"})
    if metrics["imports"] > IMPORT_WARNING:
        suggestions.append({
            "type": "import_bloat", "severity": "warning",
            "evidence": [f"{metrics['imports']} 个 import > {IMPORT_WARNING}"],
            "action": "检查是否跨层引用（Presentation→Application→Domain→"
                      "Infrastructure 依赖方向）"})
    return suggestions


def _build_plan(suggestions: list) -> list:
    """迁移计划：测试保护 → 渐进提取 → 知识反向更新。"""
    if not suggestions:
        return []
    return [
        "Step 1 测试保护：为当前行为写特征测试（锁定不变量）",
        "Step 2 提取最大职责模块（按变化原因，非按名称拆分）",
        "Step 3 依赖注入：直接构造改为构造函数注入",
        "Step 4 横切逻辑：try/日志/权限提取为装饰器/中间件",
        "Step 5 迁移：调用方逐个切换 → 删除原代码 → "
        "更新 ADR 与 project memory（知识反向更新）",
    ]


def analyze_file(path: str) -> dict:
    """God File 分析：度量 + 职责 + 模式信号 + 建议（Python AST）。"""
    source_path = Path(path)
    try:
        text = source_path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return {"error": str(exc)}
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return {"error": "不是可解析的 Python 文件（其他语言请人工评估）"}

    lines = len(text.splitlines())
    imports = len([n for n in ast.walk(tree)
                   if isinstance(n, (ast.Import, ast.ImportFrom))])
    functions = _analyze_functions(tree)
    classes = _analyze_classes(tree)
    responsibilities = _detect_responsibilities(tree)
    signals = _detect_signals(tree)
    metrics = {
        "lines": lines,
        "line_level": ("ok" if lines <= LINE_WARNING
                       else "warning" if lines <= LINE_CRITICAL
                       else "critical"),
        "functions": len(functions),
        "function_level": ("ok" if len(functions) <= FUNC_WARNING
                           else "warning" if len(functions) <= FUNC_CRITICAL
                           else "critical"),
        "imports": imports,
        "classes": len(classes),
        "top_complexity": max((f["complexity"] for f in functions),
                              default=0),
    }
    suggestions = _build_suggestions(metrics, functions, responsibilities)
    return {
        "file": str(source_path), "lines": lines,
        "metrics": metrics,
        "functions": sorted(functions, key=lambda f: -f["complexity"])[:10],
        "responsibilities": responsibilities,
        "signals": signals,
        "suggestions": suggestions[:8],
        "migration_plan": _build_plan(suggestions),
        "risk": ("HIGH" if any(s["severity"] == "critical"
                               for s in suggestions) else
                 "MEDIUM" if suggestions else "LOW"),
    }




def refactor_main(argv: Optional[list] = None) -> int:
    """`pi-batch refactor --file PATH [--json]`：God File 重构分析。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py refactor",
        description="God File 重构分析器：度量→职责→模式→迁移计划")
    parser.add_argument("--file", required=True, help="待分析文件")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    report = analyze_file(args.file)
    if "error" in report:
        print(f"refactor: {report['error']}", file=sys.stderr)
        raise SystemExit(2)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        lines = [f"# God File Analysis: {report['file']}", ""]
        m = report["metrics"]
        lines.append(f"行数 {m['lines']} [{m['line_level']}] | "
                     f"函数 {m['functions']} [{m['function_level']}] | "
                     f"imports {m['imports']} | "
                     f"最高圈复杂度 {m['top_complexity']} | "
                     f"风险 {report['risk']}")
        if report["responsibilities"]:
            lines.append("职责分布:")
            for domain, methods in report["responsibilities"].items():
                lines.append(f"  {domain}: {len(methods)} 个方法")
        for signal in report["signals"]:
            lines.append(f"[{signal['pattern']}] {signal['action']}")
        for suggestion in report["suggestions"]:
            lines.append(f"[{suggestion['severity']:>8}] "
                         f"{suggestion['type']}: {suggestion['action']}")
        if report["migration_plan"]:
            lines.append("")
            lines.append("迁移计划:")
            for step in report["migration_plan"]:
                lines.append(f"  {step}")
        print("\n".join(lines))
    raise SystemExit(0 if report["risk"] != "HIGH" else 1)


if __name__ == "__main__":
    raise SystemExit(refactor_main())
