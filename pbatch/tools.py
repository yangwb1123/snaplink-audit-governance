"""Command index & dispatch integrity check (`pi-batch tools`).

列出全部子命令（含描述）；`--check` 逐一导入分派目标并调用 --help，
发现坏注册立即失败（fail closed）——防止模块拆分/重命名后留下悬空入口。

Usage:
    pi-batch tools [--json]
    pi-batch tools --check          # 分派完整性自检（make ci 用）
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Optional

# 子命令 → 一行描述（与 pbatch/cli.py _SUBCOMMANDS 同步维护）。
DESCRIPTIONS = {
    "memory": "渐进式 message memory（ingest/recent/find/read）",
    "classify": "任务类型判定（frontend/backend 路由）",
    "rules": "规则 manifest（scale×page_type×risk + 双向校验）",
    "assess": "需求评估（完整性/规模/处方/画像）",
    "profile": "多维任务画像（向量/自主性/L0-L3 路由/包络/假设）",
    "learn": "规则生命周期（draft/shadow/exception/promote）",
    "context": "项目文档上下文路由",
    "eval": "规则系统回归套件",
    "check": "工程质量自检（quality + 注册表 schema）",
    "advance": "自推进迭代引擎（逐轮续跑）",
    "ps": "运行注册表（谁在跑什么）",
    "retro": "事故复盘",
    "health": "六层维护可观测性报告",
    "ui-geometry": "UI 几何门禁（裸数字/随机圆角）",
    "export-template": "导出任务模板",
    "campaign": "项目级 Campaign（发现→TopN→实现）",
    "devices": "设备织网（list/probe/placement/run/schedule/...）",
    "serve": "控制平面（Runner 注册/心跳/租约/证据）",
    "runner": "设备 Runner（出站连接，断线重连）",
    "approve": "批准设备 Runner 接入",
    "runners": "已注册设备清单",
    "atomize": "认知原子提取（MUST #1：先原子化）",
    "pareto": "Pareto 前沿 + Utility + MUC",
    "race": "分阶段竞跑（Successive Halving）",
    "proposal": "多 Agent 提案冲突/合并/收益公式",
    "diversity": "多 Agent 独立性评分",
    "truth": "真值维护（前提级联失效）",
    "causal": "因果假设生命周期（先测量后干预）",
    "temporal": "证据衰减（三类新鲜度）",
    "pinned": "Pinned 上下文 + 摘要失效检测",
    "graph": "类型化超图（校验/deps/cycles/summary）",
    "metrics": "五类运营指标",
    "budget": "错误预算账本（耗尽→强制降级）",
    "recovery": "回滚 vs 前向修复决策",
    "capsule": "决策胶囊（版本化重放上下文）",
    "nversion": "N-version 一致性判定",
    "world": "世界状态版本化 + 漂移检查",
    "events": "交互事件账本（不可变追加式）",
    "init": "创建用户级工作目录（~/.config|.cache/.local/share/pbatch）",
    "version": "版本与环境信息",
    "clean": "清空用户级缓存（--all 连配置/密钥）",
    "completion": "生成 shell 补全（bash/zsh）",
    "test-report": "测试分类报告（Unit/Contract/Property/Scenario/Chaos）",
    "probe": "Dry-run 标准探测（区间估计+置信度）",
    "replay": "决策重放：胶囊版本对比 + eval 回归",
}


def list_subcommands() -> list:
    """注册表驱动的命令清单（权威来源 cli._SUBCOMMANDS——completion 用它，
    避免 DESCRIPTIONS 描述表遗漏新命令）。"""
    from .cli import _SUBCOMMANDS
    return sorted(_SUBCOMMANDS)


def tools_main(argv: Optional[list] = None) -> int:
    """`pi-batch tools [--json] [--check]`。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py tools",
        description="Command index & dispatch integrity check")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--check", action="store_true",
                        help="逐一导入分派目标，坏注册即失败（exit 1）")
    args = parser.parse_args(argv)
    from .cli import _SUBCOMMANDS
    report = {}
    for name in sorted(_SUBCOMMANDS):
        report[name] = {"target": _SUBCOMMANDS[name],
                        "description": DESCRIPTIONS.get(name, "")}
    if args.check:
        broken = _check_dispatch(report)
        if args.json:
            print(json.dumps({"valid": not broken, "broken": broken},
                             indent=2))
        else:
            if broken:
                print("DISPATCH BROKEN:")
                for item in broken:
                    print(f"  - {item}")
            else:
                print(f"dispatch ok: {len(report)} subcommands")
        raise SystemExit(1 if broken else 0)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        lines = ["# pi-batch 子命令索引", ""]
        lines.append(f"{'COMMAND':<12} DESCRIPTION")
        for name, info in report.items():
            lines.append(f"{name:<12} {info['description'] or info['target']}")
        print("\n".join(lines))
    raise SystemExit(0)


def _check_dispatch(report: dict) -> list:
    """逐一导入 module:func；再以子进程跑 --help 验证可执行。"""
    broken = []
    for name, info in report.items():
        module_name, _, func_name = info["target"].partition(":")
        try:
            module = __import__(f"pbatch.{module_name}", fromlist=[func_name])
            func = getattr(module, func_name)
            if not callable(func):
                broken.append(f"{name}: {info['target']} 不可调用")
                continue
        except Exception as exc:  # 坏注册 fail closed
            broken.append(f"{name}: {info['target']} 导入失败: {exc}")
            continue
        # 子进程 --help 冒烟（部分命令无 --help 输出也算通过）
        try:
            result = subprocess.run(
                [sys.executable, str(Path(__file__).resolve().parent.parent
                                     / "pi-batch.py"), name, "--help"],
                capture_output=True, text=True, timeout=20)
            if result.returncode not in (0, 2):  # argparse help 退出码 0
                broken.append(f"{name}: --help 退出码 {result.returncode}")
        except (OSError, subprocess.TimeoutExpired) as exc:
            broken.append(f"{name}: --help 冒烟失败: {exc}")
    return broken
