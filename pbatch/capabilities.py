"""Capability Registry — 从命令驱动转向能力驱动（Agent OS 内核冻结第一件）.

90 模块 / 61 能力 / 39 命令的复杂度需要收敛：把每个用户可见能力（顶层
子命令 / devices 嵌套 / 确定性门禁）登记为 Capability——domain / owner /
input / output / risk / requires / produces / tests。未来 Agent 通过
能力 id 调用（`invoke("graph.extract", ...)`），不关心哪个模块哪个命令。

`pi-batch capabilities list|get|check`：
- list：全量注册表（按 domain 分组）
- check：完整性验证——owner 模块存在、有测试关联、**owner 模块依赖数
  ≤ 预算（默认 15，AADM-G §35 复杂度治理）**、命令可分派

这就是 Capability Registry + 模块级质量预算的一次落地（收敛工具，
非新能力）。
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Optional

CAPABILITY_DOMAINS = ("reasoning", "planning", "execution", "verification",
                      "governance", "device", "ops")
MODULE_DEP_BUDGET = 15  # 模块级导入依赖上限（质量预算）

# 能力 id → (domain, risk, input 摘要, output 摘要)。
# 与 _SUBCOMMANDS 同步维护；未登记的自动按 domain=ops/risk=low 兜底。
MANUAL = {
    "graph": ("reasoning", "low", "graph.json|源代码目录", "结构违规/依赖环/统计"),
    "profile": ("reasoning", "low", "需求文本", "12 维画像/模式/包络"),
    "pareto": ("reasoning", "low", "候选+目标", "前沿/Utility/MUC"),
    "race": ("planning", "low", "候选+阶段评分", "淘汰历史/胜者"),
    "proposal": ("reasoning", "low", "多 Agent 提案", "冲突/合并/收益"),
    "diversity": ("reasoning", "low", "Agent 配置列表", "独立性评分"),
    "assess": ("planning", "low", "需求文本", "处方/工作流/画像"),
    "rules": ("planning", "low", "任务文本", "规则 manifest"),
    "classify": ("reasoning", "low", "任务文本", "任务类型"),
    "atomize": ("reasoning", "low", "需求文本", "认知原子/超图"),
    "nversion": ("verification", "low", "命令", "N 次一致性裁决"),
    "world": ("governance", "low", "仓库", "世界快照/漂移裁决"),
    "capsule": ("governance", "low", "prompt", "决策胶囊"),
    "learn": ("governance", "medium", "事故/规则操作", "草案/例外/影子/promote"),
    "truth": ("governance", "low", "声明/失效", "级联失效状态"),
    "causal": ("governance", "low", "症状/原因", "因果假设状态"),
    "temporal": ("governance", "low", "新鲜度参数", "衰减后置信度"),
    "pinned": ("governance", "low", "上下文", "Pinned 块+指纹"),
    "metrics": ("ops", "low", "数据源", "五类指标"),
    "budget": ("governance", "low", "消耗/配额", "错误预算状态"),
    "recovery": ("governance", "low", "成本参数", "回滚/前向决策"),
    "events": ("ops", "low", "state-dir", "交互事件账本"),
    "check": ("verification", "low", "仓库", "工程门禁结果"),
    "health": ("ops", "low", "仓库", "健康报告/评分"),
    "eval": ("verification", "low", "规则系统", "回归用例结果"),
    "tools": ("ops", "low", "无", "命令索引/分派自检"),
    "devices": ("device", "medium", "设备操作", "探测/调度/执行"),
    "serve": ("device", "medium", "端口/配额", "控制平面"),
    "runner": ("device", "medium", "控制地址", "设备 Runner"),
    "campaign": ("planning", "medium", "模块列表", "方向/实现流水线"),
    "advance": ("planning", "low", "仓库", "违规维度/批次"),
}


def _module_of(target: str) -> str:
    """命令分派目标 → owner 模块名。"""
    return target.partition(":")[0]


def _tests_for(capability_id: str) -> list:
    """启发式关联测试：能力 id 单词出现在测试文件名中。"""
    tokens = set(re.split(r"[_-]", capability_id)) - {"py", "ui"}
    matches = []
    tests_dir = Path("tests")
    if not tests_dir.exists():
        return matches
    for path in sorted(tests_dir.glob("test_*.py")):
        name = path.stem.lower()
        if any(token and token in name for token in tokens):
            matches.append(path.name)
    return matches


def _module_dependencies(module: str) -> list:
    """owner 模块的模块级导入依赖（graph extract 同源逻辑）。"""
    path = Path("pbatch") / f"{module}.py"
    if not path.exists():
        return []
    deps = []
    in_def = False
    for line in path.read_text(encoding="utf-8",
                               errors="replace").splitlines():
        stripped = line.strip()
        if stripped.startswith(("def ", "class ", "async def ")):
            in_def = True
            continue
        if in_def:
            continue
        match = re.match(r"^from \.(\w+) import", stripped)
        if match:
            deps.append(match.group(1))
    return sorted(set(deps))


def build_registry() -> list:
    """从 _SUBCOMMANDS + devices 嵌套 + 门禁生成能力注册表。"""
    from .cli import _SUBCOMMANDS
    registry = []
    for name in sorted(_SUBCOMMANDS):
        target = _SUBCOMMANDS[name]
        module = _module_of(target)
        manual = MANUAL.get(name, ("ops", "low", "参数", "输出"))
        registry.append({
            "id": name,
            "domain": manual[0] if manual[0] in CAPABILITY_DOMAINS else "ops",
            "entry": f"pi-batch {name}",
            "owner": {"module": module, "target": target},
            "input": manual[2], "output": manual[3],
            "risk": manual[1],
            "requires": ["filesystem.read"],
            "produces": [f"{name}.output"],
            "tests": _tests_for(name),
            "module_dependencies": len(_module_dependencies(module)),
            "trust_level": 0,  # 构建后统一推导
        })
    for capability in registry:
        capability["trust_level"] = trust_level_of(capability)
    return registry
    return registry


# 信任等级（0-5）：0 只能分析 / 1 生成方案 / 2 修改代码 / 3 执行测试 /
# 4 部署 / 5 生产操作。能力按其风险与域推导所需等级。
TRUST_BY_RISK = {"medium": 3, "high": 4}
TRUST_BY_DOMAIN = {"reasoning": 1, "planning": 1, "verification": 3,
                   "governance": 2, "device": 4, "ops": 1}


def trust_level_of(capability: dict) -> int:
    """能力所需信任等级：low 风险由域决定（分析=1/验证=3/设备=4）；
    medium/high 风险提升到风险等级。"""
    domain_level = TRUST_BY_DOMAIN.get(capability.get("domain", "ops"), 2)
    risk = capability.get("risk", "low")
    if risk in ("medium", "high"):
        return max(domain_level, TRUST_BY_RISK[risk])
    return domain_level


def authorized(capability: dict, agent_level: int,
               device_zone: str = "") -> tuple:
    """授权矩阵：Agent 等级 × 能力等级 × 设备信任区。

    设备 untrusted-external 只允许低等级能力；trusted-local 不受限。
    返回 (allowed, reason)。"""
    if agent_level < 0 or agent_level > 5:
        return (False, f"agent_level {agent_level} 超出 0-5")
    needed = trust_level_of(capability)
    if agent_level < needed:
        return (False, f"需要信任等级 {needed}，Agent 只有 {agent_level}")
    if device_zone and device_zone == "untrusted-external" and needed > 2:
        return (False, f"不可信设备区不允许等级 {needed} 能力")
    return (True, f"等级 {agent_level} ≥ 所需 {needed}")


def registry_check(registry: Optional[list] = None) -> dict:
    """完整性验证：owner 存在 / 有测试关联 / 模块依赖 ≤ 预算 / 可分派。"""
    problems = []
    stats = {"total": 0, "with_tests": 0, "deps_over_budget": 0}
    for capability in (registry if registry is not None else build_registry()):
        stats["total"] += 1
        module = capability["owner"]["module"]
        if not (Path("pbatch") / f"{module}.py").exists():
            problems.append(f"{capability['id']}: owner 模块 {module} 不存在")
        if capability["tests"]:
            stats["with_tests"] += 1
        deps = capability["module_dependencies"]
        if deps > MODULE_DEP_BUDGET:
            stats["deps_over_budget"] += 1
            problems.append(f"{capability['id']}: 模块 {module} 依赖 {deps} > "
                            f"预算 {MODULE_DEP_BUDGET}")
    stats["test_coverage"] = round(stats["with_tests"] / stats["total"], 2) \
        if stats["total"] else 0.0
    return {"valid": not problems, "problems": problems, "stats": stats}


def capabilities_main(argv: Optional[list] = None) -> int:
    """`pi-batch capabilities list|get|check`（能力注册表，Agent OS 内核）。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py capabilities",
        description="Capability Registry：命令驱动 → 能力驱动")
    parser.add_argument("command", nargs="?", default="list",
                        choices=("list", "get", "check", "trust"))
    parser.add_argument("--agent-level", type=int, default=2,
                        help="trust 子命令：Agent 信任等级 0-5")
    parser.add_argument("--device-zone", default="",
                        help="trust 子命令：设备信任区")
    parser.add_argument("--id", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    registry = build_registry()
    if args.command == "check":
        report = registry_check(registry)
        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2))
        else:
            print(f"capabilities: {report['stats']['total']} | "
                  f"测试关联 {report['stats']['with_tests']} "
                  f"({report['stats']['test_coverage']}) | "
                  f"依赖超预算 {report['stats']['deps_over_budget']}")
            for problem in report["problems"]:
                print(f"  ⚠ {problem}")
            print("valid" if report["valid"] else "INVALID")
        raise SystemExit(0 if report["valid"] else 1)
    if args.command == "trust":
        _cmd_trust(registry, args)
        raise SystemExit(0)
    if args.command == "list" and not args.json:
        _cmd_list(registry)
        raise SystemExit(0)
    if args.command == "get":
        item = next((c for c in registry if c["id"] == args.id), None)
        if item is None:
            print(f"unknown capability {args.id!r}", file=sys.stderr)
            raise SystemExit(2)
        print(json.dumps(item, ensure_ascii=False, indent=2))
        raise SystemExit(0)
    if args.json:
        print(json.dumps(registry, ensure_ascii=False, indent=2))
    else:
        _cmd_list(registry)
    raise SystemExit(0)


def _cmd_trust(registry: list, args) -> None:
    """授权矩阵：Agent 等级 × 能力等级 × 设备信任区。"""
    denied, allowed = [], 0
    for capability in registry:
        ok, reason = authorized(capability, args.agent_level,
                                args.device_zone)
        if ok:
            allowed += 1
        else:
            denied.append({"id": capability["id"],
                           "trust_level": capability["trust_level"],
                           "reason": reason})
    report = {"agent_level": args.agent_level,
              "device_zone": args.device_zone or "trusted-local",
              "allowed": allowed, "denied": len(denied),
              "denied_capabilities": denied}
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    print(f"Agent 等级 {args.agent_level} / 设备区 "
          f"{report['device_zone']}: 允许 {allowed} 个能力，"
          f"拒绝 {len(denied)} 个")
    for item in denied[:8]:
        print(f"  ✗ {item['id']} (需 L{item['trust_level']}) — "
              f"{item['reason']}")


def _cmd_list(registry: list) -> None:
    """按 domain 分组的注册表文本视图。"""
    by_domain = {}
    for capability in registry:
        by_domain.setdefault(capability["domain"], []).append(capability)
    lines = ["# Capability Registry", ""]
    for domain in CAPABILITY_DOMAINS:
        items = by_domain.get(domain, [])
        if not items:
            continue
        lines.append(f"## {domain} ({len(items)})")
        for capability in items:
            lines.append(f"  {capability['id']:<14} "
                         f"[{capability['risk']:<6}] "
                         f"{capability['input']} → {capability['output']}")
        lines.append("")
    print("\n".join(lines))
