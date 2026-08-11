"""Interaction event ledger CLI (AADM §4.1): 不可变追加式事件记录.

控制平面把 register/submit/evidence/migrate/halt/revoke 等交互事件追加到
<state>/events.jsonl（只追加不覆盖）。CLI 读取：谁、何时、对什么做了什么
——可回答"是谁做出的决策、基于什么信息、获得了什么验证结果"。

Usage:
    pi-batch events recent [--state-dir DIR] [--limit N] [--json]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

from .fabric_devices import DEVICE_STATE_DIR

EVENT_LINE_MAX = 256 * 1024
EVENT_FILE_MAX = 16 * 1024 * 1024


def read_events(state_dir: str = "", limit: int = 50) -> list:
    """从事件账本读最近 limit 条（有界读取，新→旧）。"""
    path = Path(state_dir or DEVICE_STATE_DIR) / "events.jsonl"
    records = []
    if not path.exists():
        return records
    try:
        handle = path.open("r", encoding="utf-8")
    except OSError:
        return records
    with handle:
        total = 0
        for line in handle:
            total += len(line.encode("utf-8", errors="replace"))
            if total > EVENT_FILE_MAX:
                break
            if len(line) > EVENT_LINE_MAX:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if isinstance(record, dict):
                records.append(record)
    return records[-max(1, limit):][::-1]


def events_main(argv: Optional[list] = None) -> int:
    """`pi-batch events recent [--state-dir DIR] [--limit N] [--json]`。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py events",
        description="Interaction event ledger (AADM §4.1, 不可变追加式)")
    parser.add_argument("command", nargs="?", default="recent",
                        choices=("recent",),
                        help="兼容文档写法：`events recent`（可省略）")
    parser.add_argument("--state-dir", default="")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    records = read_events(args.state_dir, args.limit)
    if args.json:
        print(json.dumps(records, ensure_ascii=False, indent=2))
    else:
        lines = ["# Interaction Events（新→旧）", ""]
        for record in records:
            lines.append(f"{record.get('at', '-'):<20} "
                         f"{record.get('actor', '?'):<24} "
                         f"{record.get('verb', '?'):<10} "
                         f"{record.get('target', '')} "
                         f"{record.get('object_id', '')}")
        print("\n".join(lines))
    raise SystemExit(0)
