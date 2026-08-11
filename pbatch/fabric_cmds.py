"""Device-Aware Execution Fabric — `devices` 子命令（执行与调度层）.

run（D2 直接 SSH 受控执行）与 schedule/task/migrate/cordon/drain
（D4 经控制平面 + Runner 路由）的命令实现。解析器由 fabric_cli
统一构建（_add_*_parser），本模块只实现命令逻辑。

安全：schedule/task/migrate/cordon/drain 均以设备密钥（runner 身份）签名
请求控制平面；所有错误 fail closed（exit 2）。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

from . import config
from .fabric import ExecutionSpec, artifact_ref, local_probe
from .fabric_devices import EFFECT_CLASSES, TaskPlacement
from .fabric_exec import (placement_gate_error, probe_required_error,
                          target_for, _transfer_file)
from .fabric_rpc import RpcClient
from .fabric_runner import load_or_create_key
from .fabric_sched import devices_from_status, pick_device
from .fabric_devices import evaluate_placement, device_inventory


def _render_remote_run(evidence) -> str:
    lines = [f"# Remote Run: {evidence.target_id}", ""]
    lines.append(f"attempt   : {evidence.attempt_id}")
    flags = (" [TIMEOUT]" if evidence.timed_out else "")
    flags += " [OUTPUT OVERFLOW]" if evidence.output_overflow else ""
    lines.append(f"result    : {evidence.result} (exit {evidence.exit_code}){flags}")
    lines.append(f"elapsed   : {evidence.elapsed:.1f}s")
    lines.append(f"env digest: {evidence.environment_digest}")
    if evidence.stdout.strip():
        lines.append("")
        lines.append(evidence.stdout.rstrip())
    if evidence.stderr.strip():
        lines.append("")
        lines.append("-- stderr --")
        lines.append(evidence.stderr.rstrip())
    return "\n".join(lines)


def _transfer_pairs(target, pairs: list, download: bool = False) -> list:
    """--upload/--download 的 LOCAL:REMOTE（或反向）解析与执行。
    上传返回 ArtifactRef 列表供证据 input_digest；任何失败 exit 2。"""
    kind = "download" if download else "upload"
    refs = []
    for pair in pairs:
        if download:
            remote, sep, local = pair.partition(":")
        else:
            local, sep, remote = pair.partition(":")
        if not sep or not local or not remote:
            print(f"bad --{kind} {pair!r}（需要 "
                  + ("REMOTE:LOCAL" if download else "LOCAL:REMOTE") + "）",
                  file=sys.stderr)
            raise SystemExit(2)
        if not download and not Path(local).exists():
            print(f"upload source missing: {local}", file=sys.stderr)
            raise SystemExit(2)
        transfer = _transfer_file(target, local, remote, download=download)
        if not transfer["ok"]:
            print(f"{kind} failed: {transfer['detail']}", file=sys.stderr)
            raise SystemExit(2)
        if not download:
            refs.append(artifact_ref(local))
    return refs


def _cmd_run(args) -> None:
    """受控远程执行：结构化 argv（禁任意 Shell）、三重门禁（模式/探测/
    Placement）、副作用等级上限 sandboxed。"""
    if config.DEVICE_FABRIC_MODE not in ("execute", "migrate", "federate"):
        print(f"fabric mode={config.DEVICE_FABRIC_MODE}：run 属于 EXECUTE 模式操作；"
              "先在 pi-batch.yaml 设置 device_fabric.mode: execute",
              file=sys.stderr)
        raise SystemExit(2)
    if not args.cmd:
        print("usage: devices run NAME [opts] -- CMD ARG..."
              "（必须用 -- 分隔命令）", file=sys.stderr)
        raise SystemExit(2)
    target = target_for(args.name, ssh_config_path=args.ssh_config)
    if target is None:
        print(f"unknown device: {args.name}", file=sys.stderr)
        raise SystemExit(2)
    gate = probe_required_error(target.target)
    if gate:
        print(gate, file=sys.stderr)
        raise SystemExit(2)
    gate = placement_gate_error(args.name, args.effect_class)
    if gate:
        print(gate, file=sys.stderr)
        raise SystemExit(2)
    inputs = _transfer_pairs(target.target, args.upload)
    spec = ExecutionSpec(argv=args.cmd, cwd=args.cwd,
                         timeout=args.timeout, label=f"ssh-run:{args.name}",
                         effect_class=args.effect_class, inputs=inputs)
    evidence = target.execute(spec)
    _transfer_pairs(target.target, args.download, download=True)
    if args.json:
        print(json.dumps(evidence.to_dict(), indent=2))
    else:
        print(_render_remote_run(evidence))
    raise SystemExit(0 if evidence.result == "passed" else 1)


# ---------------------------------------------------------------------------
# D4：经控制平面 + Runner 的任务调度命令
# ---------------------------------------------------------------------------

def _cmd_schedule_speculative(args, client, blob: str) -> None:
    """投机执行（AADM-D §33）：同一任务派到多台设备，先完成者胜出，
    其余取消。只允许 pure/read_only/sandboxed 副作用（schedule 的
    choices 已限）；双 Runner 竞争由 fencing 拒掉败者证据（at-least-once）。"""
    devices = [d.strip() for d in (args.devices or "").split(",") if d.strip()]
    if len(devices) < 2:
        print("speculative 需要 --devices a,b（至少两台设备）",
              file=sys.stderr)
        raise SystemExit(2)
    task_ids = []
    for device in devices:
        submitted = client.submit_task(args.cmd, cwd=args.cwd,
                                       timeout=args.timeout,
                                       effect_class=args.effect_class,
                                       mobility=args.mobility,
                                       checkpoint_blob=blob,
                                       checkpoint_remote=args.checkpoint_load,
                                       estimated_cost=args.estimated_cost)
        task = submitted.get("task")
        if task is None:
            print(f"submit to {device} failed: "
                  f"{submitted.get('error', '?')}", file=sys.stderr)
            raise SystemExit(2)
        task_ids.append(task["task_id"])
    winner, loser = _race_tasks(client, task_ids, timeout=args.timeout)
    report = {"winner": winner, "cancelled": loser,
              "candidates": len(task_ids)}
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"speculative: {len(task_ids)} 台设备竞跑 → 胜者 {winner}"
              + (f"，取消 {loser}" if loser else ""))
    raise SystemExit(0 if winner else 1)


def _race_tasks(client, task_ids: list, timeout: float = 60.0) -> tuple:
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


def _control_client(args, acting: str = "") -> RpcClient:
    """以指定设备身份连接控制平面（密钥在 state-dir/keys/<name>.json）。"""
    name = acting or getattr(args, "device", "")
    if not name:
        raise SystemExit("需要设备身份（--device 或 --as-device）")
    key = load_or_create_key(args.state_dir, name)
    base = args.control if args.control.startswith("http") \
        else f"http://{args.control}"
    return RpcClient(base, key["secret"], key["device_id"])


def _task_summary(task: dict) -> str:
    if task is None:
        return "-"
    return (f"{task.get('task_id', '-')} [{task.get('status', '?')}] "
            f"device={task.get('device_id', '-')} "
            f"argv={' '.join(task.get('argv', []))[:60]}")


def _read_checkpoint(path: str) -> str:
    """读取检查点文件为 base64（有界；失败 fail closed）。"""
    if not path:
        return ""
    try:
        import base64
        return base64.b64encode(Path(path).read_bytes()).decode("ascii")
    except OSError as exc:
        print(f"checkpoint read failed: {exc}", file=sys.stderr)
        raise SystemExit(2)


def _auto_pick_device(args, client) -> str:
    """按约束自动选设备（Cordon 感知 + 模型缓存优先）。"""
    code, view = _fetch_status(client)
    if code != 200:
        print(f"control plane unreachable: {view}", file=sys.stderr)
        raise SystemExit(2)
    devices = devices_from_status(view.get("devices", []))
    cordoned = {d["name"] for d in devices if d.get("cordoned")}
    placement = TaskPlacement(
        required_os=args.os, required_architecture=args.arch,
        min_memory_bytes=args.min_memory_mb * 1024 * 1024,
        min_cpu_cores=args.min_cpu,
        max_effect_class=args.effect_class)
    device = pick_device(placement, devices, cordoned=cordoned,
                         model=args.model or None)
    if device is None:
        print("无可调度设备：约束过严或全部 cordoned", file=sys.stderr)
        raise SystemExit(2)
    return device


def _cmd_schedule(args) -> None:
    """把任务经控制平面下发到指定设备（或按约束自动选设备）。"""
    if not args.cmd:
        print("usage: devices schedule [opts] -- CMD ARG..."
              "（必须用 -- 分隔命令）", file=sys.stderr)
        raise SystemExit(2)
    acting = args.device or args.as_device
    if not acting:
        print("schedule 需要 --device（目标+身份）或 --as-device（身份，"
              "配合 --os/--arch 自动选设备）", file=sys.stderr)
        raise SystemExit(2)
    client = _control_client(args, acting)
    from .fabric_fed import (pick_in_cluster, resolve_cluster,
                             schedule_gang, schedule_speculative)
    if args.cluster:
        members = resolve_cluster(args)
        if args.speculative:
            args.devices = ",".join(members)
        else:
            args.device = pick_in_cluster(args, client, members)
    if args.gang:
        return schedule_gang(args, client)
    if args.device:
        device = args.device
    else:
        device = _auto_pick_device(args, client)
    blob = _read_checkpoint(args.checkpoint_save)
    if args.speculative:
        return schedule_speculative(args, client)
    submitted = client.submit_task(args.cmd, cwd=args.cwd,
                                   timeout=args.timeout,
                                   effect_class=args.effect_class,
                                   mobility=args.mobility,
                                   checkpoint_blob=blob,
                                   checkpoint_remote=args.checkpoint_load,
                                   estimated_cost=args.estimated_cost)
    task = submitted.get("task")
    if task is None:
        print(f"submit failed: {submitted.get('error', '?')}", file=sys.stderr)
        raise SystemExit(2)
    if args.json:
        print(json.dumps({"task": task, "device": device}, indent=2))
    else:
        print(f"scheduled: {_task_summary(task)}")
        print("轮询: pi-batch devices task <task_id> --device <name>")
    raise SystemExit(0)


def _fetch_status(client: RpcClient) -> tuple:
    """GET /status（只读视图，不走签名）。"""
    from .fabric_rpc import rpc_post
    return rpc_post(client.base_url, client.secret, "GET", "/status",
                    {"device_id": client.device_id}, timeout=5.0)


def _cmd_task(args) -> None:
    client = _control_client(args)
    result = client.task_status(args.task_id)
    task = result.get("task")
    if task is None:
        print(f"task lookup failed: {result.get('error', '?')}", file=sys.stderr)
        raise SystemExit(2)
    if args.json:
        print(json.dumps(task, indent=2, default=str))
    else:
        lines = [f"# Task {task.get('task_id')}", ""]
        lines.append(f"status    : {task.get('status')}")
        lines.append(f"device    : {task.get('device_id', '-')}")
        lines.append(f"argv      : {' '.join(task.get('argv', []))}")
        lines.append(f"mobility  : {task.get('mobility', '-')}")
        evidence = task.get("evidence")
        if evidence:
            lines.append(f"result    : {evidence.get('result')} "
                         f"(exit {evidence.get('exit_code')})")
            if evidence.get("stdout", "").strip():
                lines.append("")
                lines.append(evidence["stdout"].rstrip())
        print("\n".join(lines))
    raise SystemExit(0)


def _cmd_migrate(args) -> None:
    client = _control_client(args)
    result = client.task_migrate(args.task_id, args.device)
    if not result.get("ok"):
        print(f"migrate failed: {result.get('error', '?')}", file=sys.stderr)
        raise SystemExit(2)
    print(f"migrated: {args.task_id} → {args.device}（新租约已签发，"
          "旧 fencing token 失效）")
    raise SystemExit(0)


def _cmd_revoke(args) -> None:
    """设备退役：吊销密钥 + 任务标 lost（§41.2 生命周期闭环）。"""
    client = _control_client(args)
    result = client.revoke(reason=args.reason)
    if not result.get("ok"):
        print(f"revoke failed: {result.get('error', '?')}", file=sys.stderr)
        raise SystemExit(2)
    print(f"revoked: {args.device}（{result.get('tasks_marked_lost', 0)} 个任务"
          f"标 lost）— {result.get('message', '')}")


def _cmd_halt(args) -> None:
    """Kill Switch：取消全部排队任务 + cordon 所有设备（一键冻结）。"""
    client = _control_client(args)
    result = client.halt()
    if not result.get("ok"):
        print(f"halt failed: {result.get('error', '?')}", file=sys.stderr)
        raise SystemExit(2)
    print(f"halt: 取消 {result.get('cancelled_tasks')} 个排队任务，"
          f"cordon {result.get('cordoned_devices')} 台设备")
    print(f"  {result.get('message', '')}")


def _cmd_cordon(args) -> None:
    client = _control_client(args)
    result = client.cordon(drain=args.drain, undo=args.undo)
    if not result.get("ok"):
        print(f"cordon failed: {result.get('error', '?')}", file=sys.stderr)
        raise SystemExit(2)
    print(f"device {args.device}: cordoned={result.get('cordoned')} "
          f"drain={result.get('drain')}")
    raise SystemExit(0)


# ---------------------------------------------------------------------------
# 解析器构建（供 fabric_cli._build_devices_parser 调用）
# ---------------------------------------------------------------------------

def _add_run_parser(sub) -> None:
    p_run = sub.add_parser("run",
                           help="在指定设备执行结构化命令（SSH 受控执行）")
    p_run.add_argument("name", help="设备名（静态配置或 ~/.ssh/config 别名）")
    p_run.add_argument("--cwd", default="", help="远程工作目录（默认登录目录）")
    p_run.add_argument("--timeout", type=float, default=60.0,
                       help="硬超时秒数（默认 60）")
    p_run.add_argument("--effect-class", default="read_only",
                       choices=("pure", "read_only", "sandboxed"),
                       help="副作用等级（D2 上限 sandboxed）")
    p_run.add_argument("--upload", action="append", default=[],
                       help="LOCAL:REMOTE 上传（scp，可重复）")
    p_run.add_argument("--download", action="append", default=[],
                       help="REMOTE:LOCAL 下载（scp，可重复）")
    p_run.add_argument("--ssh-config", default="",
                       help="自定义 ssh config 路径（默认 ~/.ssh/config）")
    p_run.add_argument("--json", action="store_true")


def _add_common_device_args(parser) -> None:
    parser.add_argument("--control", default="http://127.0.0.1:8765",
                        help="控制平面地址（默认 http://127.0.0.1:8765）")
    parser.add_argument("--state-dir", default="",
                        help="设备密钥目录（默认 .pi-batch/devices）")
    parser.add_argument("--json", action="store_true")


def _add_schedule_parser(sub) -> None:
    p = sub.add_parser("schedule", help="经控制平面 + Runner 下发任务")
    p.add_argument("--device", default="",
                   help="目标设备名（同时作为请求身份）")
    p.add_argument("--as-device", default="",
                   help="请求身份设备名（自动选设备模式）")
    p.add_argument("--os", action="append", default=[])
    p.add_argument("--arch", action="append", default=[])
    p.add_argument("--min-memory-mb", type=int, default=0)
    p.add_argument("--min-cpu", type=int, default=0)
    p.add_argument("--cwd", default="")
    p.add_argument("--timeout", type=float, default=60.0)
    p.add_argument("--effect-class", default="read_only",
                   choices=("pure", "read_only", "sandboxed"))
    p.add_argument("--mobility", default="stateless",
                   choices=("stateless", "restartable", "checkpointable",
                            "pinned"))
    p.add_argument("--checkpoint-save", default="",
                   help="随任务上传检查点文件（断点续传起点）")
    p.add_argument("--checkpoint-load", default="",
                   help="Runner 执行前恢复到的相对路径（checkpointable 任务）")
    p.add_argument("--estimated-cost", type=float, default=0.0,
                   help="任务预估成本（设备预算预留用）")
    p.add_argument("--speculative", action="store_true",
                   help="投机执行：同一任务派多设备，先完成者胜")
    p.add_argument("--devices", default="",
                   help="speculative 的设备列表（逗号分隔，至少两台）")
    p.add_argument("--model", default="",
                   help="模型缓存感知：优先权重已缓存的设备（标签 model.X.cached）")
    p.add_argument("--region", default="", help="区域过滤（跨区域资源池）")
    p.add_argument("--cluster", default="",
                   help="集群目标（device_fabric.clusters 配置的成员组）")
    p.add_argument("--gang", action="store_true",
                   help="Gang 提交：原子提交到多台设备，任一失败全部取消")
    _add_common_device_args(p)


def _add_task_parser(sub) -> None:
    p = sub.add_parser("task", help="查询任务状态")
    p.add_argument("task_id")
    p.add_argument("--device", required=True,
                   help="以哪台设备身份查询（需已注册密钥）")
    _add_common_device_args(p)


def _add_migrate_parser(sub) -> None:
    p = sub.add_parser("migrate", help="迁移任务到其他设备（pinned 除外）")
    p.add_argument("task_id")
    p.add_argument("--device", required=True, help="新目标设备名")
    _add_common_device_args(p)


def _add_revoke_parser(sub) -> None:
    p = sub.add_parser("revoke", help="设备退役：吊销密钥（身份永久失效）")
    p.add_argument("--device", required=True)
    p.add_argument("--reason", default="")
    _add_common_device_args(p)


def _add_halt_parser(sub) -> None:
    p = sub.add_parser("halt", help="Kill Switch：冻结全部新任务并 cordon 所有设备")
    p.add_argument("--device", required=True, help="以哪台设备身份触发")
    _add_common_device_args(p)


def _add_cordon_parser(sub, drain: bool) -> None:
    name = "drain" if drain else "cordon"
    p = sub.add_parser(name, help=("停止接收新任务（drain：并停止新任务认领）"
                                   if drain else "停止接收新任务"))
    p.add_argument("--device", required=True)
    p.add_argument("--undo", action="store_true", help="取消 cordon/drain")
    p.set_defaults(drain=drain)
    _add_common_device_args(p)


DISPATCH = {
    "run": _cmd_run,
    "schedule": _cmd_schedule,
    "task": _cmd_task,
    "migrate": _cmd_migrate,
    "cordon": _cmd_cordon,
    "drain": _cmd_cordon,
    "halt": _cmd_halt,
    "revoke": _cmd_revoke,
}
