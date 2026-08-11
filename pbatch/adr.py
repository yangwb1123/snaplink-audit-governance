"""Architecture Decision Records（ADR）— 决策记忆（工程知识循环核心）.

大型系统最重要的不是代码，而是"为什么这么设计"。ADR 库保存结构化
决策（decision/context/alternatives/rejected reasons/status），未来
Agent 查询决策缓存，不再重新讨论已定事项（如"文件存储用 MinIO"——
ADR-003，半年后附件需求直接复用）。

- 存储：docs/adr/ADR-###-slug.yaml（只追加，不修改历史）
- 状态机：proposed → accepted / rejected / superseded
- `pi-batch adr new|list|get|find|status`
- `find`：决策缓存查询——按关键词返回相关 ADR（impact 分析自动引用）

Usage:
    pi-batch adr new --title "文件存储使用 MinIO" --decision "MinIO" \
        --context "大文件/S3 兼容/未来迁移" --alternatives "本地磁盘,OSS" \
        --rejected "本地磁盘:扩容困难" --status accepted
    pi-batch adr find "文件存储"
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Optional

ADR_DIR = Path("docs/adr")
ADR_STATUSES = ("proposed", "accepted", "rejected", "superseded")


def _next_number() -> int:
    """下一个 ADR 编号（现有文件 + 1）。"""
    if not ADR_DIR.exists():
        return 1
    numbers = [int(m.group(1)) for path in ADR_DIR.glob("ADR-*.yaml")
               if (m := re.match(r"ADR-(\d+)-", path.name))]
    return max(numbers, default=0) + 1


def adr_record(title: str, decision: str, context: str = "",
               alternatives: Optional[list] = None,
               rejected: Optional[list] = None,
               status: str = "proposed") -> dict:
    """构造 ADR 记录（决策/背景/备选/否决理由/状态）。"""
    if status not in ADR_STATUSES:
        raise ValueError(f"status must be one of {ADR_STATUSES}")
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:40]
    return {
        "number": _next_number(),
        "slug": slug,
        "title": " ".join(title.split())[:120],
        "decision": " ".join(decision.split())[:300],
        "context": " ".join(context.split())[:300],
        "alternatives": list(alternatives or ()),
        "rejected_reasons": list(rejected or ()),
        "status": status,
        "created_at": time.strftime("%Y-%m-%d"),
        "superseded_by": "",
    }


def write_adr(record: dict, adr_dir: Optional[Path] = None) -> Path:
    adr_dir = adr_dir or ADR_DIR
    """落盘 ADR-###-slug.yaml（只追加）。"""
    adr_dir.mkdir(parents=True, exist_ok=True)
    path = adr_dir / f"ADR-{record['number']:03d}-{record['slug']}.yaml"
    from .config import yaml
    payload = (yaml.safe_dump(record, allow_unicode=True, sort_keys=False)
               if yaml else str(record))
    path.write_text(payload, encoding="utf-8")
    return path


def load_adrs(adr_dir: Optional[Path] = None) -> list:
    adr_dir = adr_dir or ADR_DIR
    """读全部 ADR（按编号排序）。"""
    from .config import yaml
    records = []
    if not adr_dir.exists():
        return records
    for path in sorted(adr_dir.glob("ADR-*.yaml")):
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except (OSError, ValueError):
            continue
        if isinstance(data, dict) and data.get("number"):
            records.append(data)
    return sorted(records, key=lambda r: int(r.get("number", 0)))


# 中文查询停用词（通用动词不参与决策匹配）。
_ADR_STOPWORDS = {"增加", "实现", "功能", "支持", "开发", "需要", "做一个",
                  "使用", "系统", "修改", "添加"}


def _query_tokens(query: str) -> set:
    """查询分词：英文 3+ 词 + 中文 2 字滑窗（去停用词）。"""
    lowered = (query or "").lower()
    tokens = set(re.findall(r"[a-z0-9]{3,}", lowered))
    for run in re.findall(r"[\u4e00-\u9fff]+", lowered):
        if len(run) <= 2:
            tokens.add(run)
        else:
            tokens.update(run[i:i + 2] for i in range(len(run) - 1))
    return {token for token in tokens if token not in _ADR_STOPWORDS}


def find_adrs(query: str, adr_dir: Optional[Path] = None) -> list:
    """决策缓存查询：关键词命中 title/decision/context（不重新讨论已定项）。"""
    adr_dir = adr_dir or ADR_DIR
    tokens = _query_tokens(query)
    if not tokens:
        return []
    hits = []
    for record in load_adrs(adr_dir):
        haystack = " ".join(str(record.get(key, "")) for key in
                            ("title", "decision", "context")).lower()
        if any(token in haystack for token in tokens):
            hits.append(record)
    return hits


def mark_superseded(number: int, by_number: int,
                    adr_dir: Optional[Path] = None) -> None:
    adr_dir = adr_dir or ADR_DIR
    """把旧 ADR 标记 superseded（历史不可修改，只追加状态变更）。"""
    target = None
    for path in adr_dir.glob("ADR-*.yaml"):
        from .config import yaml
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if data.get("number") == number:
            target = path
            break
    if target is None:
        raise ValueError(f"ADR-{number} 不存在")
    from .config import yaml
    data = yaml.safe_load(target.read_text(encoding="utf-8"))
    data["status"] = "superseded"
    data["superseded_by"] = f"ADR-{by_number:03d}"
    target.write_text(yaml.safe_dump(data, allow_unicode=True,
                                     sort_keys=False), encoding="utf-8")


def _cmd_new(args) -> None:
    """记录一个 ADR（只追加）。"""
    record = adr_record(
        args.title, args.decision, args.context,
        [a.strip() for a in args.alternatives.split(",") if a.strip()],
        [r.strip() for r in args.rejected.split(",") if r.strip()],
        args.status)
    path = write_adr(record)
    print(f"ADR-{record['number']:03d} written: {path}")
    print(f"决策: {record['decision']}")


def _cmd_find(args) -> None:
    """决策缓存查询（命中已定事项，不重新讨论）。"""
    hits = find_adrs(args.query)
    if args.json:
        print(json.dumps(hits, ensure_ascii=False, indent=2))
        return
    if not hits:
        print(f"无相关决策记录（{args.query}）——可新增 ADR")
    for record in hits:
        print(f"ADR-{record['number']:03d} [{record['status']}] "
              f"{record['title']} → {record['decision'][:50]}")


def _cmd_list(args) -> None:
    """全部 ADR（编号排序）。"""
    records = load_adrs()
    if args.json:
        print(json.dumps(records, ensure_ascii=False, indent=2))
        return
    for record in records:
        print(f"ADR-{record['number']:03d} [{record['status']}] "
              f"{record['title']} → {record['decision'][:40]}")


def _cmd_get(args) -> None:
    """按编号查看单个 ADR。"""
    records = [r for r in load_adrs() if r["number"] == args.number]
    if not records:
        print(f"ADR-{args.number} 不存在", file=sys.stderr)
        raise SystemExit(2)
    print(json.dumps(records[0], ensure_ascii=False, indent=2))


def adr_main(argv: Optional[list] = None) -> int:
    """`pi-batch adr new|list|get|find|supersede`。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py adr",
        description="Architecture Decision Records：决策记忆库")
    sub = parser.add_subparsers(dest="command", required=True)

    p_new = sub.add_parser("new", help="记录一个架构决策")
    p_new.add_argument("--title", required=True)
    p_new.add_argument("--decision", required=True)
    p_new.add_argument("--context", default="")
    p_new.add_argument("--alternatives", default="",
                       help="逗号分隔的备选方案")
    p_new.add_argument("--rejected", default="",
                       help="逗号分隔的否决理由（方案:理由）")
    p_new.add_argument("--status", default="accepted",
                       choices=ADR_STATUSES)

    p_list = sub.add_parser("list", help="全部 ADR")
    p_list.add_argument("--json", action="store_true")

    p_get = sub.add_parser("get", help="按编号查看")
    p_get.add_argument("number", type=int)

    p_find = sub.add_parser("find", help="决策缓存查询（关键词）")
    p_find.add_argument("query")
    p_find.add_argument("--json", action="store_true")

    p_sup = sub.add_parser("supersede", help="标记被新决策取代")
    p_sup.add_argument("number", type=int)
    p_sup.add_argument("by", type=int)

    args = parser.parse_args(argv)
    if args.command == "new":
        _cmd_new(args)
        raise SystemExit(0)
    if args.command == "list":
        _cmd_list(args)
        raise SystemExit(0)
    if args.command == "get":
        _cmd_get(args)
        raise SystemExit(0)
    if args.command == "find":
        _cmd_find(args)
        raise SystemExit(0)
    mark_superseded(args.number, args.by)
    print(f"ADR-{args.number:03d} → superseded by ADR-{args.by:03d}")
    raise SystemExit(0)
