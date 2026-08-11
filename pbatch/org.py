"""Agent 工程组织规范校验器（`pi-batch org`）.

agent-engineering/*.yaml 是 16 个流程节点的"岗位说明书"（9 要素）。
`org check` 强制校验：9 要素齐全、stage 0-16 连续、id 唯一、auto_checks
引用的命令真实存在（规范可验证，不是文档）。

Usage:
    pi-batch org check [--json]
    pi-batch org show [NODE_ID]
    pi-batch org flow
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Optional

from .config import yaml

ORG_DIR = Path("agent-engineering")
REQUIRED_KEYS = ("id", "name", "role", "stage", "parent",
                 "responsibilities", "inputs", "outputs", "check_rules",
                 "forbidden", "best_practices", "failure_cases",
                 "auto_checks", "review_checklist", "role_template")


def load_nodes(org_dir: Optional[Path] = None) -> list:
    """读全部节点规范（无效文件 fail closed）。"""
    org_dir = org_dir or ORG_DIR
    nodes = []
    if not org_dir.exists():
        return nodes
    for path in sorted(org_dir.glob("*.yaml")):
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except (OSError, ValueError):
            continue
        if isinstance(data, dict) and data.get("id"):
            nodes.append(data)
    return nodes


def _known_commands() -> set:
    """已知命令集合：pi-batch 子命令 + scripts/*.py + 常见工具。"""
    from .cli import _SUBCOMMANDS
    commands = set(_SUBCOMMANDS)
    for path in Path("scripts").glob("*.py"):
        commands.add(str(path))
    commands.update({"pi-batch", "pytest", "python", "bash", "go", "make",
                     "git"})
    return commands


def _check_exists(check: str, commands: set) -> bool:
    """auto_check 校验：pi-batch 子命令 / 脚本 / 文件目录引用。"""
    parts = str(check).split()
    if not parts:
        return False
    first = parts[0]
    if first == "pi-batch":
        return len(parts) > 1 and parts[1] in commands
    if first in commands:
        return True
    return Path(first).exists()


def org_check(nodes: Optional[list] = None,
             org_dir: Optional[Path] = None) -> dict:
    """规范完整性：9 要素 / 顺序 / id 唯一 / 命令存在。"""
    nodes = nodes if nodes is not None else load_nodes(org_dir)
    problems = []
    by_id = {}
    for node in nodes:
        node_id = node.get("id", "")
        if node_id in by_id:
            problems.append(f"duplicate id: {node_id}")
        by_id[node_id] = node
        for key in REQUIRED_KEYS:
            if key not in node:
                problems.append(f"{node_id}: 缺少要素 '{key}'")
        template = node.get("role_template", "")
        if template and not Path(template).exists():
            problems.append(f"{node_id}: role_template 不存在: {template}")
    for node in nodes:  # parent 校验须在全部 id 就绪后
        parent = node.get("parent", "")
        if parent and parent not in by_id:
            problems.append(f"{node.get('id')}: parent '{parent}' 不存在")
        commands = _known_commands()
        for check in node.get("auto_checks", []):
            if not _check_exists(str(check), commands):
                problems.append(f"{node_id}: auto_check 命令不存在: {check}")
    stages = sorted(node.get("stage", -1) for node in nodes)
    expected = list(range(len(nodes))) if nodes else []
    if stages != expected:
        problems.append(f"stage 不连续: {stages}（期望 {expected}）")
    return {"valid": not problems, "problems": problems,
            "node_count": len(nodes),
            "stages": stages}


def render_role_prompt(node: dict) -> str:
    """岗位说明书 → 角色注入片段（供 meta 编排/角色 prompt 使用）。"""
    lines = [f"## Role: {node.get('name', '')}（{node.get('role', '')}）", ""]
    lines.append("### 职责")
    for item in node.get("responsibilities", []):
        lines.append(f"- {item}")
    lines.append("")
    lines.append("### 输入")
    lines.append("- " + " / ".join(node.get("inputs", [])))
    lines.append("")
    lines.append("### 输出契约")
    lines.append("- " + " / ".join(node.get("outputs", [])))
    lines.append("")
    lines.append("### 检查规则（必须遵守）")
    for item in node.get("check_rules", []):
        lines.append(f"- {item}")
    lines.append("")
    lines.append("### 禁止事项")
    for item in node.get("forbidden", []):
        lines.append(f"- {item}")
    lines.append("")
    lines.append("### 最佳实践")
    for item in node.get("best_practices", []):
        lines.append(f"- {item}")
    lines.append("")
    lines.append("### Review Checklist（完成前逐项自检）")
    for item in node.get("review_checklist", []):
        lines.append(f"- [ ] {item}")
    lines.append("")
    lines.append("### 自动检测（完成后必须运行）")
    for check in node.get("auto_checks", []):
        lines.append(f"- `{check}`")
    return "\n".join(lines)


def run_gate(node_id: str, task: str = "", code_dir: str = "",
             file_path: str = "", log_path: str = "") -> dict:
    """执行节点 auto_checks 作为流程门禁：占位符注入 → 有界子进程 →
    汇总（缺参命令标记 skipped，不伪装通过）。"""
    import subprocess
    nodes = load_nodes()
    node = next((n for n in nodes if n.get("id") == node_id), None)
    if node is None:
        return {"error": f"unknown node {node_id!r}"}
    results = []
    params = {"{task}": task, "{code}": code_dir, "{file}": file_path,
              "{log}": log_path}
    for check in node.get("auto_checks", []):
        missing = [key for key, value in params.items()
                   if key in str(check) and not value]
        if missing:
            results.append({"check": check, "status": "skipped",
                            "reason": f"缺少参数 {', '.join(missing)}"})
            continue
        results.append(_execute_check(check, params))
    return _summarize(node_id, results)


def _execute_check(check, params: dict) -> dict:
    """执行单个 auto_check（有界子进程；reflect 低分 = attention）。"""
    import subprocess
    cmd = str(check)
    for key, value in params.items():
        cmd = cmd.replace(key, value)
    leftover = re.findall(r"\{[a-z_]+\}", cmd)
    if leftover:
        # 未知占位符未替换 → 配置错误显式暴露（campaign 实战：evolution
        # 节点 {log} 曾静默假通过，方向被 gate 拒绝但 bug 真实）。
        return {"check": check, "status": "failed",
                "reason": f"未替换占位符 {', '.join(leftover)}（配置错误）"}
    # scripts/*.py 无执行位（仓库惯例用解释器调用）——自动加 python 前缀。
    if cmd.startswith("scripts/") and not cmd.startswith(("python", "bash")):
        cmd = f"{sys.executable} {cmd}"
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True,
                                text=True, timeout=120)
        ok = result.returncode == 0
        status = ("passed" if ok else
                  "attention" if "reflect" in str(check) else "failed")
        return {"check": check, "status": status,
                "exit_code": result.returncode,
                "evidence": (result.stdout + result.stderr).strip()[-200:]}
    except subprocess.TimeoutExpired:
        return {"check": check, "status": "failed", "reason": "timeout"}
    except OSError as exc:
        return {"check": check, "status": "failed", "reason": str(exc)}


def _summarize(node_id: str, results: list) -> dict:
    """gate 汇总：failed 阻塞，attention 展示不阻塞。"""
    passed = sum(1 for r in results if r["status"] == "passed")
    failed = sum(1 for r in results if r["status"] == "failed")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    attention = sum(1 for r in results if r["status"] == "attention")
    return {"node": node_id, "passed": passed, "failed": failed,
            "skipped": skipped, "attention": attention,
            "gate_passed": failed == 0, "results": results}
    passed = sum(1 for r in results if r["status"] == "passed")
    failed = sum(1 for r in results if r["status"] == "failed")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    attention = sum(1 for r in results if r["status"] == "attention")
    return {"node": node_id, "passed": passed, "failed": failed,
            "skipped": skipped, "attention": attention,
            "gate_passed": failed == 0, "results": results}


def _cmd_run(args) -> None:
    """流程串联：impact 分流 → 节点 gate 顺序执行 → 汇总（可落盘）。"""
    report = run_flow(args.task, args.code, args.level, args.file,
                      args.artifact, args.log)
    if args.save:
        path = save_flow(report)
        print(f"saved: {path}")
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"flow [{report['run_level']}]（impact 判定 "
              f"{report['impact_level']}，成本 {report['change_cost']}）")
        print(f"角色: {', '.join(report['required_agents']) or '-'}")
        for node in report["nodes"]:
            state = "✓" if node["gate_passed"] else "✗"
            print(f"  {state} {node['node']:<14} "
                  f"{node['passed']}✓ {node['failed']}✗ "
                  f"{node['attention']}! {node['skipped']}·")
        print("flow " + ("passed" if report["flow_passed"] else "FAILED"))
    raise SystemExit(0 if report["flow_passed"] else 1)


def _cmd_prompt(args) -> None:
    """岗位说明书 → 角色注入片段。"""
    node = next((n for n in load_nodes() if n.get("id") == args.node), None)
    if node is None:
        print(f"unknown node {args.node!r}", file=sys.stderr)
        raise SystemExit(2)
    print(render_role_prompt(node))


def _cmd_gate(args) -> None:
    """节点门禁：auto_checks 注入参数后执行并汇总。"""
    report = run_gate(args.node, args.task, args.code, args.file,
                      args.log)
    if "error" in report:
        print(f"org gate: {report['error']}", file=sys.stderr)
        raise SystemExit(2)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"gate [{report['node']}]: "
              f"{report['passed']} 通过 / {report['failed']} 失败 / "
              f"{report['skipped']} 跳过 / {report['attention']} 注意")
        for result in report["results"]:
            mark = {"passed": "✓", "failed": "✗", "skipped": "·",
                    "attention": "!"}[result["status"]]
            print(f"  {mark} {result['check'][:70]}")
            if result["status"] in ("failed", "attention"):
                detail = result.get("evidence") or result.get("reason", "")
                print(f"      {detail[:120]}")
    raise SystemExit(0 if report["gate_passed"] else 1)


def _build_parser():
    """org 子命令参数：check/show/prompt/gate/flow。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py org",
        description="Agent 工程组织规范：17 节点岗位说明书")
    sub = parser.add_subparsers(dest="command", required=True)
    p_check = sub.add_parser("check", help="校验规范完整性")
    p_check.add_argument("--json", action="store_true")
    p_show = sub.add_parser("show", help="查看节点规范")
    p_show.add_argument("node", nargs="?", default="")
    p_prompt = sub.add_parser("prompt", help="岗位说明书 → 角色注入片段")
    p_prompt.add_argument("node")
    p_gate = sub.add_parser("gate", help="执行节点 auto_checks 作为门禁")
    p_gate.add_argument("node")
    p_gate.add_argument("--task", default="", help="需求文本（{task} 注入）")
    p_gate.add_argument("--code", default="", help="代码目录（{code} 注入）")
    p_gate.add_argument("--file", default="", help="文件路径（{file} 注入）")
    p_gate.add_argument("--log", default="", help="日志路径（{log} 注入）")
    p_gate.add_argument("--json", action="store_true")
    p_run = sub.add_parser("run", help="流程串联执行（impact 分流 → 节点 gate）")
    p_run.add_argument("--task", required=True)
    p_run.add_argument("--code", default="")
    p_run.add_argument("--file", default="")
    p_run.add_argument("--log", default="")
    p_run.add_argument("--level", default="",
                       choices=("", "L0", "L1", "L2", "L3"),
                       help="工作等级（缺省由 impact 判定）")
    p_run.add_argument("--json", action="store_true")
    p_run.add_argument("--save", action="store_true",
                       help="落盘流程工件（.pi-batch/flows/）")
    p_run.add_argument("--artifact", default="",
                       help="节点输出工件目录（下游输入，每节点一个 txt）")
    p_lesson = sub.add_parser("lesson", help="失败案例 → learn 沉淀建议")
    p_lesson.add_argument("node")
    p_runs = sub.add_parser("runs", help="历史流程工件清单")
    p_runs.add_argument("--json", action="store_true")
    sub.add_parser("flow", help="全流程节点图")
    return parser


# 工作等级 → 节点集合（增量流程：L0/L1 轻量，L2/L3 全评审）。
LEVEL_NODES = {
    "L0": ["requirement", "developer", "testing"],
    "L1": ["requirement", "developer", "testing", "code_review"],
    "L2": ["requirement", "domain", "architecture", "data", "developer",
           "code_review", "security", "testing"],
    "L3": ["requirement", "product", "domain", "architecture", "data",
           "api", "developer", "code_review", "security", "testing",
           "performance", "release"],
}


def run_flow(task: str, code_dir: str = "", level: str = "",
             file_path: str = "", artifact_dir: str = "",
             log_path: str = "") -> dict:
    """流程串联执行：impact 分流 → 按工作等级顺序跑节点 gate。"""
    from .impact import change_impact
    impact = change_impact(task, code_dir)
    if not level:
        level = impact["work_level"]
    nodes = LEVEL_NODES.get(level, LEVEL_NODES["L1"])
    results = []
    for node_id in nodes:
        gate = run_gate(node_id, task, code_dir, file_path, log_path)
        if "error" in gate:
            continue
        results.append({"node": node_id, "passed": gate["passed"],
                        "failed": gate["failed"], "skipped": gate["skipped"],
                        "attention": gate["attention"],
                        "gate_passed": gate["gate_passed"]})
        if artifact_dir:
            _write_artifact(artifact_dir, node_id, gate)
    return {
        "task": impact["task"], "impact_level": impact["work_level"],
        "change_cost": impact["change_cost"],
        "run_level": level, "required_agents": impact["required_agents"],
        "artifact_dir": artifact_dir or "",
        "nodes": results,
        "flow_passed": all(r["gate_passed"] for r in results),
    }


def _write_artifact(artifact_dir: str, node_id: str, gate: dict) -> None:
    """节点输出工件落盘（下游节点输入；只写证据摘录，不写全量）。"""
    target = Path(artifact_dir)
    target.mkdir(parents=True, exist_ok=True)
    lines = [f"# node: {node_id}", f"passed: {gate['passed']}",
             f"failed: {gate['failed']} | skipped: {gate['skipped']} | "
             f"attention: {gate['attention']}", ""]
    for result in gate["results"]:
        lines.append(f"[{result['status']}] {result['check']}")
        if result.get("evidence"):
            lines.append(f"    {result['evidence'][:150]}")
    (target / f"{node_id}.txt").write_text("\n".join(lines) + "\n",
                                           encoding="utf-8")


FLOWS_DIR = Path(".pi-batch") / "flows"


def save_flow(report: dict) -> str:
    """流程工件落盘（可审计、可续查）：.pi-batch/flows/<ts>-<slug>.json。"""
    import time as _time
    import re as _re
    slug = _re.sub(r"[^a-z0-9]+", "-", (report.get("task") or "flow")
                   .lower())[:40].strip("-") or "flow"
    path = FLOWS_DIR / f"{_time.strftime('%Y%m%d-%H%M%S')}-{slug}.json"
    FLOWS_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    return str(path)


def list_flows() -> list:
    """历史流程工件（新→旧，有界读取）。"""
    records = []
    if not FLOWS_DIR.exists():
        return records
    for path in sorted(FLOWS_DIR.glob("*.json"), reverse=True):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if isinstance(data, dict):
            data["file"] = path.name
            records.append(data)
    return records


def lesson_for(node: dict) -> str:
    """失败案例 → learn draft 沉淀（案例从文档变成可行动知识）。"""
    cases = node.get("failure_cases", [])
    if not cases:
        return f"节点 {node.get('id')} 无失败案例记录"
    lines = [f"# {node.get('name')} — 失败案例与规则沉淀", ""]
    for index, case in enumerate(cases, 1):
        lines.append(f"## 案例 {index}: {case}")
        lines.append("")
        lines.append("### 沉淀为规则草案（learn 流程）")
        lines.append("```bash")
        lines.append("pi-batch learn draft "
                     f"'{case}' --category backend --severity warning "
                     f"--rule-id ORG-"
                     f"{node.get('id', '').upper()[:16]}-{index:02d}")
        lines.append("```")
        lines.append("")
        lines.append("### 复盘（retro/reflection）")
        lines.append("```bash")
        lines.append(f"pi-batch retro --failures-only logs/ --actions")
        lines.append("pi-batch reflect --task " + f"'{case}'")
        lines.append("```")
        lines.append("")
    return "\n".join(lines)


def _cmd_check(args) -> None:
    """规范完整性校验（fail closed）。"""
    report = org_check()
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"agent-org: {report['node_count']} 个节点 | "
              f"stages {report['stages'][0]}-{report['stages'][-1]}")
        for problem in report["problems"]:
            print(f"  ⚠ {problem}")
        print("valid" if report["valid"] else "INVALID")
    raise SystemExit(0 if report["valid"] else 1)


def _cmd_lesson(args) -> None:
    """失败案例 → learn 沉淀建议。"""
    node = next((n for n in load_nodes() if n.get("id") == args.node), None)
    if node is None:
        print(f"unknown node {args.node!r}", file=sys.stderr)
        raise SystemExit(2)
    print(lesson_for(node))


def _cmd_runs(args) -> None:
    """历史流程工件清单（新→旧）。"""
    records = list_flows()
    if args.json:
        print(json.dumps(records, ensure_ascii=False, indent=2))
        return
    print(f"# Flow Records ({len(records)})")
    for record in records:
        print(f"  {record.get('file', ''):<30} "
              f"{record.get('run_level', '?'):<4} "
              f"{'✓' if record.get('flow_passed') else '✗'} "
              f"{record.get('task', '')[:40]}")


def _cmd_flow() -> None:
    """全流程节点图（0-16 顺序）。"""
    nodes = load_nodes()
    for node in sorted(nodes, key=lambda n: n.get("stage", 0)):
        arrow = " → " if node.get("stage", 0) < 16 else ""
        print(f"{node.get('stage', '?'):>2} {node.get('name', '?'):<24}"
              f"{arrow}")


def _cmd_show(args) -> None:
    """查看节点规范（JSON）。"""
    nodes = load_nodes()
    target = args.node
    node = next((n for n in nodes if n.get("id") == target), None)
    if node is None:
        print(f"unknown node {target!r}（可用: "
              + ", ".join(n.get("id", "") for n in nodes) + "）",
              file=sys.stderr)
        raise SystemExit(2)
    print(json.dumps(node, ensure_ascii=False, indent=2))


def org_main(argv: Optional[list] = None) -> int:
    """`pi-batch org check|show|prompt|gate|run|runs|lesson|flow`。"""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "check":
        _cmd_check(args)  # 内部 raise
    elif args.command == "prompt":
        _cmd_prompt(args)
    elif args.command == "gate":
        _cmd_gate(args)  # 内部 raise
    elif args.command == "run":
        _cmd_run(args)  # 内部 raise
    elif args.command == "lesson":
        _cmd_lesson(args)
    elif args.command == "runs":
        _cmd_runs(args)
    elif args.command == "flow":
        _cmd_flow()
    else:
        _cmd_show(args)
    raise SystemExit(0)
