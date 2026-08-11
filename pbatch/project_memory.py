"""Project Memory — 软件系统状态记忆（工程知识循环的存储层）.

Agent 不应每次从零理解项目：`.ai/project-memory/` 保存系统的**状态**
而非文档——facts（已确认事实）/ decisions（ADR 索引）/ architecture
（边界与禁止项）/ constraints（工程宪法引用）。事实与推理分层：facts
= Confirmed（有证据），assumptions 归 profile/truth 管理。

`pi-batch project init|show|verify`：
- init：初始化记忆骨架（存在则不覆盖）
- show：展示当前记忆（facts/decisions/architecture）
- verify：记忆健康检查——facts 文件存在、ADR 无 orphan 引用、
  truth 失效数（记忆过期检测的输入）

代码→知识反向更新：graph extract / advance 的结果可写入 affected 文件
（update 子命令），防止记忆过期。
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Optional

MEMORY_DIR = Path(".ai") / "project-memory"

# 记忆骨架：文件 → 用途。
SKELETON = {
    "facts.yaml": "# Confirmed Facts（已确认事实，非推理）\n"
                  "# 每个事实应可在 truth 或代码中验证\nfacts: []\n",
    "architecture.yaml": "# 系统架构状态（边界/服务/禁止项）\n"
                         "architecture:\n  style: \"\"\n"
                         "  services: []\n  never: []\n",
    "decisions.yaml": "# 决策索引（ADR 库 docs/adr/ 的快捷视图）\n"
                      "decisions: []\n",
    "constraints.yaml": "# 工程宪法与约束（引用 backend-specs/"
                        "architecture-constitution.md）\nconstraints: []\n",
}


def _read_yaml(path: Path) -> dict:
    from .config import yaml
    if not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def init_memory(memory_dir: Optional[Path] = None) -> list:
    memory_dir = memory_dir or MEMORY_DIR
    """初始化记忆骨架（只创建缺失文件，不覆盖已有）。"""
    created = []
    for name, content in SKELETON.items():
        path = memory_dir / name
        if not path.exists():
            memory_dir.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
            created.append(str(path))
    return created


def add_fact(statement: str, source: str = "",
             memory_dir: Optional[Path] = None) -> Path:
    memory_dir = memory_dir or MEMORY_DIR
    """登记一个已确认事实（带来源——事实/推理分层）。"""
    from .config import yaml
    path = memory_dir / "facts.yaml"
    memory_dir.mkdir(parents=True, exist_ok=True)
    data = _read_yaml(path)
    facts = data.get("facts", []) if isinstance(data.get("facts"), list) else []
    facts.append({"statement": " ".join(statement.split())[:200],
                  "source": source, "at": __import__("time").strftime(
                      "%Y-%m-%d")})
    data["facts"] = facts
    path.write_text(yaml.safe_dump(data, allow_unicode=True,
                                   sort_keys=False), encoding="utf-8")
    return path


def index_adrs(memory_dir: Path = MEMORY_DIR,
              adr_dir: Optional[Path] = None) -> Path:
    """重建 decisions.yaml 索引（ADR 库快捷视图，防记忆过期）。"""
    from .adr import ADR_DIR, load_adrs
    from .config import yaml
    adr_dir = adr_dir or ADR_DIR
    decisions = [{"number": r["number"], "title": r["title"],
                  "status": r["status"],
                  "decision": r["decision"][:60]}
                 for r in load_adrs(adr_dir)]
    path = memory_dir / "decisions.yaml"
    memory_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump({"decisions": decisions},
                                   allow_unicode=True, sort_keys=False),
                    encoding="utf-8")
    return path


def memory_report(memory_dir: Optional[Path] = None,
                  adr_dir: Optional[Path] = None) -> dict:
    memory_dir = memory_dir or MEMORY_DIR
    """记忆健康报告：骨架完整性 + ADR 索引 + 事实数。"""
    from .adr import ADR_DIR, load_adrs
    adr_dir = adr_dir or ADR_DIR
    facts = _read_yaml(memory_dir / "facts.yaml").get("facts", [])
    decisions = _read_yaml(memory_dir / "decisions.yaml").get(
        "decisions", [])
    adrs = load_adrs(adr_dir)
    missing = [name for name in SKELETON
               if not (memory_dir / name).exists()]
    return {
        "memory_dir": str(memory_dir),
        "skeleton_complete": not missing,
        "missing_files": missing,
        "facts": len(facts),
        "adr_count": len(adrs),
        "decisions_indexed": len(decisions),
        "adr_index_stale": bool(adrs) and len(decisions) != len(adrs),
        "facts_sample": [f.get("statement", "")[:60] for f in facts[:3]],
    }


def project_main(argv: Optional[list] = None) -> int:
    """`pi-batch project init|show|verify|facts`。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py project",
        description="Project Memory：软件系统状态记忆（工程知识循环）")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init", help="初始化记忆骨架（不覆盖已有）")
    sub.add_parser("show", help="展示当前记忆状态")
    p_facts = sub.add_parser("facts", help="登记一个已确认事实")
    p_facts.add_argument("--statement", required=True)
    p_facts.add_argument("--source", default="")
    p_index = sub.add_parser("index", help="重建 decisions.yaml 索引（ADR 库）")
    p_verify = sub.add_parser("verify", help="记忆健康检查")
    p_verify.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    if args.command == "init":
        created = init_memory()
        print(f"project memory initialized: {len(created)} 个文件"
              + ("" if created else "（已存在）"))
        raise SystemExit(0)
    if args.command == "facts":
        path = add_fact(args.statement, args.source)
        print(f"fact recorded: {path}")
        raise SystemExit(0)
    if args.command == "index":
        path = index_adrs()
        print(f"decisions index rebuilt: {path}")
        raise SystemExit(0)
    report = memory_report()
    if args.command == "verify":
        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2))
        else:
            print(f"project memory: "
                  f"{'完整' if report['skeleton_complete'] else '缺失 ' + str(report['missing_files'])}")
            print(f"facts: {report['facts']} | ADR: {report['adr_count']} | "
                  f"索引: {report['decisions_indexed']}")
            if report["adr_index_stale"]:
                print("⚠ decisions.yaml 索引与 ADR 库不一致——重新索引")
        raise SystemExit(0 if report["skeleton_complete"]
                        and not report["adr_index_stale"] else 1)
    # show
    print("# Project Memory")
    print(f"目录: {report['memory_dir']}")
    print(f"facts: {report['facts']} 条"
          + ("" if not report["facts_sample"]
             else " — " + "; ".join(report["facts_sample"])))
    print(f"ADR: {report['adr_count']} 条决策")
    raise SystemExit(0)
