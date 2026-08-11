"""Device-Aware Execution Fabric — 控制平面 CLI（serve/approve/runners）。

与 pbatch/fabric_server.py（ControlPlane 核心）分离以控制模块行数。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

from . import config
from .config import log
from .fabric_devices import DEVICE_STATE_DIR
from .fabric_server import ControlPlane, _read_lines_bounded, start_server


def _serve_blocking(args) -> None:
    if args.host not in ("127.0.0.1", "localhost"):
        log.warning("非本机监听：D3a 通道无 TLS，请确认网络环境可信"
                    "（或经反向代理终止 TLS）")
    httpd, port = start_server(args.port, args.state_dir,
                               auto_approve=args.auto_approve, host=args.host,
                               budget_cap=args.budget_cap,
                               tenant_quotas=args.tenant_quotas)
    log.info("control plane listening on %s:%d (approval=%s)",
             args.host, port,
             "auto" if args.auto_approve else "manual")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
    # 优雅停机：serve_forever 收到 Ctrl-C（已验证有意）
        pass
    finally:
        httpd.server_close()


def serve_main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(prog="pi-batch.py serve",
                                     description="设备控制平面（D3）")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--state-dir", default="")
    parser.add_argument("--auto-approve", action="store_true",
                        help="注册即批准（测试/受信环境；默认人工审批）")
    parser.add_argument("--budget-cap", type=float, default=0.0,
                        help="每设备累计预算上限（0=不限，防失控任务烧配额）")
    parser.add_argument("--tenant-quota", default="",
                        help="多租户配额：tenant=并发上限，逗号分隔"
                             "（如 team-a=2,team-b=4）")
    args = parser.parse_args(argv)
    quotas = {}
    for pair in (args.tenant_quota or "").split(","):
        if "=" in pair:
            key, _, value = pair.partition("=")
            try:
                quotas[key.strip()] = int(value)
            except ValueError:
                pass  # 非法配额值（非整数）跳过——配置错误不阻塞启动
    args.tenant_quotas = quotas
    _serve_blocking(args)
    raise SystemExit(0)


def approve_main(argv: Optional[list] = None) -> int:
    """人工审批待接入设备：把注册记录提升为 approved（只追加）。"""
    parser = argparse.ArgumentParser(prog="pi-batch.py approve",
                                     description="批准设备 Runner 接入")
    parser.add_argument("name", help="待批准的设备名（runners 中列出）")
    parser.add_argument("--state-dir", default="")
    args = parser.parse_args(argv)
    state_dir = Path(args.state_dir or DEVICE_STATE_DIR)
    pending = None
    path = state_dir / "runners.jsonl"
    if path.exists():
        for line in _read_lines_bounded(path):
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if (isinstance(record, dict)
                    and record.get("name") == args.name
                    and not pending):
                pending = record
    if pending is None:
        print(f"no registration for {args.name!r}"
              "（先在被接入设备上启动 devices runner）", file=sys.stderr)
        raise SystemExit(2)
    control = ControlPlane(str(state_dir))
    control._approve(pending["device_id"], pending["name"], "cli")
    print(f"approved: {args.name} ({pending['device_id']})")
    raise SystemExit(0)


def runners_main(argv: Optional[list] = None) -> int:
    """列出已注册设备 Runner（读取 runners.jsonl，只读）。"""
    parser = argparse.ArgumentParser(prog="pi-batch.py runners",
                                     description="已注册设备 Runner 清单")
    parser.add_argument("--state-dir", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    state_dir = Path(args.state_dir or DEVICE_STATE_DIR)
    records = []
    path = state_dir / "runners.jsonl"
    if path.exists():
        for line in _read_lines_bounded(path):
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if isinstance(record, dict) and record.get("device_id"):
                records.append(record)
    if args.json:
        print(json.dumps(records, indent=2))
    else:
        lines = ["# Registered Runners", ""]
        lines.append(f"{'DEVICE ID':<24} {'NAME':<20} REGISTERED_AT")
        for record in records:
            lines.append(f"{record['device_id']:<24} {record.get('name', ''):<20} "
                         f"{record.get('registered_at', '-')}")
        print("\n".join(lines))
    raise SystemExit(0)
