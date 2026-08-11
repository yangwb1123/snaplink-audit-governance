"""Federation scheduling commands (D6 最小件).

多租户配额 / 跨区域 / 集群 / Gang 原子提交 / 投机竞跑的 CLI 命令实现
（从 fabric_cmds 拆出以控制行数）。控制平面能力在 fabric_server.py。
"""

from __future__ import annotations

import json
import sys

from . import config
from .fabric_devices import TaskPlacement
from .fabric_rpc import RpcClient
from .fabric_sched import devices_from_status, pick_device


def _fetch_status(client: RpcClient) -> tuple:
    from .fabric_rpc import rpc_post
    return rpc_post(client.base_url, client.secret, "GET", "/status",
                    {"device_id": client.device_id}, timeout=5.0)


def _read_checkpoint(path: str) -> str:
    """读取检查点文件为 base64（有界；失败 fail closed）。"""
    if not path:
        return ""
    try:
        import base64
        from pathlib import Path
        return base64.b64encode(Path(path).read_bytes()).decode("ascii")
    except OSError as exc:
        print(f"checkpoint read failed: {exc}", file=sys.stderr)
        raise SystemExit(2)


def _submit(client: RpcClient, args, blob: str) -> dict:
    return client.submit_task(args.cmd, cwd=args.cwd, timeout=args.timeout,
                              effect_class=args.effect_class,
                              mobility=args.mobility,
                              checkpoint_blob=blob,
                              checkpoint_remote=args.checkpoint_load,
                              estimated_cost=args.estimated_cost)


def pick_in_cluster(args, client: RpcClient, members: list) -> str:
    """集群内选设备：按约束在成员中选可行者（--region 过滤）。"""
    code, view = _fetch_status(client)
    if code != 200:
        print(f"control plane unreachable: {view}", file=sys.stderr)
        raise SystemExit(2)
    devices = [d for d in devices_from_status(view.get("devices", []))
               if d.get("name") in members
               and (not args.region or d.get("region") == args.region)]
    cordoned = {d["name"] for d in devices if d.get("cordoned")}
    placement = TaskPlacement(max_effect_class=args.effect_class)
    chosen = pick_device(placement, devices, cordoned=cordoned,
                         model=args.model or None)
    if chosen is None:
        print(f"集群内无可调度设备（region={args.region or '-'}）",
              file=sys.stderr)
        raise SystemExit(2)
    return chosen


def schedule_gang(args, client: RpcClient) -> None:
    """Gang 提交（D6）：同一命令原子提交到多台设备——任一提交失败则全部
    取消（all-or-nothing），全部成功才进入执行。"""
    devices = [d.strip() for d in (args.devices or "").split(",") if d.strip()]
    if len(devices) < 2:
        print("gang 需要 --devices a,b（至少两台设备）", file=sys.stderr)
        raise SystemExit(2)
    blob = _read_checkpoint(args.checkpoint_save)
    accepted = []
    for device in devices:
        submitted = _submit(client, args, blob)
        task = submitted.get("task")
        if task is None:
            for task_id in accepted:  # 原子性：失败即整体回滚
                client.task_cancel(task_id)
            print(f"gang 提交失败（{submitted.get('error', '?')}）："
                  f"已取消 {len(accepted)} 个已接受任务", file=sys.stderr)
            raise SystemExit(2)
        accepted.append(task["task_id"])
    report = {"gang": accepted, "devices": devices}
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"gang scheduled: {len(accepted)} 台设备原子提交："
              + ", ".join(accepted))
    raise SystemExit(0)


def schedule_speculative(args, client: RpcClient) -> None:
    """投机执行（AADM-D §33）：同一任务派到多台设备，先完成者胜出，
    其余取消。只允许 pure/read_only/sandboxed 副作用；双 Runner 竞争
    由 fencing 拒掉败者证据（at-least-once）。"""
    devices = [d.strip() for d in (args.devices or "").split(",") if d.strip()]
    if len(devices) < 2:
        print("speculative 需要 --devices a,b（至少两台设备）",
              file=sys.stderr)
        raise SystemExit(2)
    blob = _read_checkpoint(args.checkpoint_save)
    task_ids = []
    for _device in devices:
        submitted = _submit(client, args, blob)
        task = submitted.get("task")
        if task is None:
            print(f"submit failed: {submitted.get('error', '?')}",
                  file=sys.stderr)
            raise SystemExit(2)
        task_ids.append(task["task_id"])
    winner, loser = _race_tasks(client, task_ids, timeout=args.timeout)
    report = {"winner": winner, "cancelled": loser, "candidates": len(task_ids)}
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"speculative: {len(task_ids)} 台设备竞跑 → 胜者 {winner}"
              + (f"，取消 {loser}" if loser else ""))
    raise SystemExit(0 if winner else 1)


def _race_tasks(client: RpcClient, task_ids: list,
                timeout: float = 60.0) -> tuple:
    """轮询直到任一任务完成，其余取消（先完成者胜）。"""
    import time as _time
    deadline = _time.monotonic() + max(5.0, timeout)
    pending = list(task_ids)
    while _time.monotonic() < deadline:
        for task_id in list(pending):
            task = client.task_status(task_id).get("task") or {}
            if task.get("status") in ("done", "failed"):
                pending.remove(task_id)
                for other in pending:
                    client.task_cancel(other)
                return task_id, pending
        _time.sleep(0.3)
    return "", task_ids


def resolve_cluster(args) -> list:
    """解析集群成员（device_fabric.clusters）。"""
    members = config.DEVICE_FABRIC_CLUSTERS.get(args.cluster or "")
    if not members:
        print(f"unknown cluster {args.cluster!r}"
              "（配置 device_fabric.clusters）", file=sys.stderr)
        raise SystemExit(2)
    return members
