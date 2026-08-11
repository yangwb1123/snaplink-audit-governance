"""Device-Aware Execution Fabric — D3: 设备 Runner.

设备侧进程：**主动出站连接**控制平面（NAT/动态 IP 友好，AADM-D §6）——
注册（TOFU + 审批等待）→ 心跳（动态资源 + 运行中尝试）→ 租约 → 证据 →
对账。断线自动重连（指数退避），审批未通过时持续等待。

设备身份：首次运行生成密钥文件（device_id + HMAC secret），
`.pi-batch/devices/keys/<name>.json`（权限 0600）。device_id 由 secret
派生 —— 注册后即密码学身份（AADM-D §7 的 D3 形态；证书化留待 D3b）。

CLI：`pi-batch devices runner --control HOST:PORT --name NAME
[--heartbeat SEC] [--once]`。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import sys
from pathlib import Path
import threading
import time
import uuid
from pathlib import Path
from typing import Optional

from . import config
from .config import log
from .fabric import ExecutionSpec, LocalExecutionTarget, local_probe
from .fabric_devices import DEVICE_STATE_DIR
from .fabric_exec import _ACTIVE_SSH_PROCS, _ACTIVE_SSH_LOCK
from .fabric_rpc import RpcClient

KEY_DIR = "keys"
REGISTER_BACKOFF_MAX = 30.0


def _default_key_dir() -> Path:
    """默认密钥目录：用户级（跨项目复用设备身份）；显式 state_dir
    仍用项目内（测试隔离/多环境）。"""
    from . import user_dirs
    return user_dirs.keys_dir()


def key_path(state_dir: str = "", name: str = "") -> Path:
    base = Path(state_dir or _default_key_dir())
    return base / KEY_DIR / f"{name}.json" if state_dir else base / f"{name}.json"


def load_or_create_key(state_dir: str = "", name: str = "") -> dict:
    """加载或创建设备密钥（device_id + secret，0600）。"""
    path = key_path(state_dir, name)
    if path.exists():
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
            if record.get("device_id") and record.get("secret"):
                return record
        except (OSError, ValueError):
        # best-effort I/O：失败不阻塞主流程（已验证有意）
            pass
    secret = uuid.uuid4().hex + uuid.uuid4().hex
    record = {
        "device_id": "rn-" + hashlib.sha256(secret.encode()).hexdigest()[:16],
        "secret": secret,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record, ensure_ascii=False) + "\n",
                    encoding="utf-8")
    os.chmod(path, 0o600)
    return record


def running_attempts() -> list:
    """当前执行中的远程尝试（来自 fabric_exec 进程登记表）。"""
    with _ACTIVE_SSH_LOCK:
        return sorted(_ACTIVE_SSH_PROCS.keys())


def _register_with_meta(client: RpcClient, name: str,
                         state_dir: str) -> dict:
    """注册（携带租户/区域/证明元数据——D6 联邦最小件）。"""
    meta = {}
    if _META.get("tenant"):
        meta["tenant"] = _META["tenant"]
    if _META.get("region"):
        meta["region"] = _META["region"]
    if _META.get("attestation"):
        meta["attestation"] = _META["attestation"]
    return client.register(name, local_probe(), **meta)


# 运行时元数据（runner_main 从 CLI 参数注入）。
_META: dict = {}


def runner_once(base_url: str, name: str, state_dir: str = "") -> dict:
    """单轮：注册 +（通过后）心跳 + 对账。返回摘要（供 --once 与测试）。"""
    key = load_or_create_key(state_dir, name)
    client = RpcClient(base_url, key["secret"], key["device_id"])
    registered = _register_with_meta(client, name, state_dir)
    if not registered.get("approved"):
        return {"approved": False, "device_id": key["device_id"],
                "message": registered.get("message", "pending")}
    beat = client.heartbeat(local_probe()["dynamic_state"], running_attempts())
    reconciled = client.reconcile(running_attempts())
    return {"approved": True, "device_id": key["device_id"],
            "heartbeat_ok": beat.get("ok", False),
            "reconcile_epoch": reconciled.get("epoch", 0)}


def execute_task(client: RpcClient, task: dict) -> dict:
    """执行一个已认领任务（本地有界执行）并提交证据（带 fencing token）。
    证据被拒（陈旧 token）说明任务已被迁移/重新认领——at-least-once 语义
    下放弃本次结果，不重试。检查点（checkpointable 迁移语义）：执行前下载
    上次状态，执行后回传——断点续传跨设备可用（AADM-D §17/Phase 5）。"""
    _restore_checkpoint(client, task)
    spec = ExecutionSpec(
        argv=task.get("argv", []), cwd=task.get("cwd", "") or ".",
        timeout=float(task.get("timeout", 60) or 60),
        effect_class=str(task.get("effect_class", "read_only")),
        mobility=str(task.get("mobility", "stateless")),
        label=f"task:{task.get('task_id', '')}")
    evidence = LocalExecutionTarget().execute(spec)
    lease = task.get("lease") or {}
    result = client.evidence(
        task.get("task_id", ""), lease.get("lease_id", ""),
        lease.get("fencing_token"), evidence.to_dict(),
        task_id=task.get("task_id", ""))
    if not result.get("accepted"):
        log.warning("任务 %s 证据被拒：%s", task.get("task_id", "?"),
                    result.get("reason", "?"))
    _save_checkpoint(client, task, spec.cwd)
    return result


def _restore_checkpoint(client: RpcClient, task: dict) -> None:
    """执行前：从控制平面取回上次检查点，写入任务 cwd（断点续传）。"""
    remote = task.get("checkpoint_remote") or ""
    if not remote:
        return
    result = client.checkpoint_download(task.get("task_id", ""))
    if not result.get("has_checkpoint"):
        return
    import base64
    import hashlib
    blob = result["blob_b64"]
    expected = result.get("digest", "")
    if expected and hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16] != expected:
        log.warning("检查点校验失败（digest 不符），拒绝恢复——防损坏/篡改")
        return
    path = Path(task.get("cwd", "") or ".") / remote
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(base64.b64decode(blob))
        log.info("检查点恢复 %s（%d 字节）", path, result.get("size", 0))
    except (OSError, ValueError) as exc:
        log.warning("检查点恢复失败：%s", exc)


def _save_checkpoint(client: RpcClient, task: dict, cwd: str) -> None:
    """执行后：把检查点回传控制平面（有界；仅 checkpointable 任务）。"""
    remote = task.get("checkpoint_remote") or ""
    if not remote or task.get("mobility") != "checkpointable":
        return
    import base64
    path = Path(cwd) / remote
    try:
        data = path.read_bytes()
    except OSError:
        return
    blob = base64.b64encode(data).decode("ascii")
    result = client.checkpoint_upload(task.get("task_id", ""), blob)
    if result.get("ok"):
        log.info("检查点回传 %s（%d 字节，digest %s）",
                 path, len(data), result.get("digest", ""))
    else:
        log.warning("检查点回传失败：%s", result.get("error", "?"))


def run_runner_loop(base_url: str, name: str, state_dir: str = "",
                    heartbeat_interval: float = 10.0,
                    stop_event: Optional[threading.Event] = None,
                    drill_skip_heartbeats: int = 0) -> None:
    """常驻循环：注册 → 心跳 → 拉取任务并执行 → 对账；断线退避重连。

    drill_skip_heartbeats > 0：注册后跳过 N 轮心跳（失联演练，AADM-D
    §41.1）——控制平面会因此把该设备标为 stale 并重排队其任务，验证
    断线对账路径真实可用。"""
    key = load_or_create_key(state_dir, name)
    client = RpcClient(base_url, key["secret"], key["device_id"])
    backoff = 2.0
    approved = False
    drill_remaining = max(0, int(drill_skip_heartbeats))
    log.info("runner %s (%s) → %s", name, key["device_id"], base_url)
    while not (stop_event is not None and stop_event.is_set()):
        try:
            if not approved:
                registered = _register_with_meta(client, name, state_dir)
                approved = bool(registered.get("approved"))
                if not approved:
                    log.info("注册待审批（%s），%gs 后重试",
                             registered.get("message", "?"), backoff)
                    _sleep(backoff, stop_event)
                    backoff = min(backoff * 2, REGISTER_BACKOFF_MAX)
                    continue
                backoff = 2.0
                log.info("runner %s 已批准，进入心跳循环", name)
            if drill_remaining > 0:
                drill_remaining -= 1
                log.warning("runner %s 失联演练：跳过心跳 %d 轮",
                            name, drill_remaining + 1)
                _sleep(heartbeat_interval, stop_event)
                continue
            beat = client.heartbeat(local_probe()["dynamic_state"],
                                    running_attempts())
            if not beat.get("ok"):
                log.warning("心跳未确认：%s", beat.get("error", "?"))
            polled = client.poll_tasks()
            for task in polled.get("tasks", []):
                execute_task(client, task)
            client.reconcile(running_attempts())
        except Exception as exc:  # 断线：退避重连（含控制平面重启场景）
            log.warning("控制平面不可达（%s），%gs 后重连", exc, backoff)
            approved = False
            _sleep(backoff, stop_event)
            backoff = min(backoff * 2, REGISTER_BACKOFF_MAX)
            continue
        _sleep(heartbeat_interval, stop_event)


def _sleep(seconds: float, stop_event: Optional[threading.Event]) -> None:
    """可中断睡眠（stop_event 置位立即返回）。"""
    if stop_event is None:
        time.sleep(seconds)
        return
    stop_event.wait(timeout=seconds)


def runner_main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(prog="pi-batch.py runner",
                                     description="设备 Runner（D3，出站连接）")
    parser.add_argument("--control", required=True,
                        help="控制平面地址 host:port")
    parser.add_argument("--name", required=True, help="设备名")
    parser.add_argument("--state-dir", default="")
    parser.add_argument("--heartbeat", type=float, default=10.0,
                        help="心跳间隔秒数（默认 10）")
    parser.add_argument("--once", action="store_true",
                        help="单轮注册+心跳+对账后退出（测试用）")
    parser.add_argument("--drill-skip-heartbeats", type=int, default=0,
                        help="失联演练：注册后跳过 N 轮心跳（验证断线对账）")
    parser.add_argument("--tenant", default="", help="租户标签（多租户配额）")
    parser.add_argument("--region", default="", help="区域标签（跨区域资源池）")
    parser.add_argument("--attestation", default="",
                        help="注册证明（如度量启动哈希；记录用）")
    args = parser.parse_args(argv)
    if not args.control or not args.name:
        parser.error("--control 与 --name 必填")
    base_url = args.control if args.control.startswith("http") \
        else f"http://{args.control}"
    _META.update({"tenant": args.tenant, "region": args.region,
                  "attestation": args.attestation})
    if args.once:
        summary = runner_once(base_url, args.name, args.state_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        raise SystemExit(0 if summary.get("approved") else 2)
    stop_event = threading.Event()

    def _stop(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    run_runner_loop(base_url, args.name, args.state_dir,
                    heartbeat_interval=args.heartbeat,
                    stop_event=stop_event,
                    drill_skip_heartbeats=args.drill_skip_heartbeats)
    log.info("runner %s 已停止", args.name)
    raise SystemExit(0)
