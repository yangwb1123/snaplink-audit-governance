"""Device-Aware Execution Fabric — D2: SSH 受控远程执行。

AADM-D (`docs/AADM_DEVICE.md`) D2 批次：`SshExecutionTarget` 实现
ExecutionTarget 协议，证据与本地同构；结构化 argv（禁任意 Shell）、
副作用等级上限 sandboxed、硬超时 SIGKILL、可取消、scp Artifact 传输、
执行前双门禁（探测证据 + Placement Dry-run）。

安全边界（AADM-D §6/§20/§38）：
- 远程命令每个元素经 shlex.quote 保护（POSIX 引号，防注入）
- 只允许 pure/read_only/sandboxed 副作用；可补偿/不可逆留待 D3
- 执行前必须：设备已探测（能力带有效期）+ Placement Dry-run 通过
- 超时 = 杀掉 ssh 客户端进程组（D2 尽力而为取消；D3 Runner 提供可靠取消）
"""

from __future__ import annotations

import json
import os
import shlex
import threading
import time
import uuid
from typing import Optional

from .fabric import (ExecutionEvidence, ExecutionSpec, FABRIC_VERSION,
                     _digest, _kill_group, _spawn_bounded)
from .fabric_devices import (EFFECT_ORDER, TaskPlacement, _fresh_fingerprint,
                             evaluate_placement, remote_probe, resolve_device)
from .fabric_ssh import _build_ssh_argv, _valid_ssh_host

# D2 受控远程执行的副作用等级上限（AADM-D §9/§20）：远程设备不可信，
# 只允许纯计算/只读/沙箱；可补偿与不可逆留待 D3 Runner + Capability Grant。
REMOTE_MAX_EFFECT = "sandboxed"


def _remote_argv_command(argv: list, cwd: str = "") -> str:
    """把结构化 argv 转成安全的远程 shell 命令：每个元素经 shlex.quote
    保护（POSIX 引号，防注入），可选 cd 前缀。绝不拼接原始用户输入。"""
    if not argv or not all(isinstance(item, str) for item in argv):
        raise ValueError("remote command must be a non-empty string list")
    quoted = " ".join(shlex.quote(item) for item in argv)
    if cwd:
        return f"cd {shlex.quote(cwd)} && exec {quoted}"
    return f"exec {quoted}"


def _scp_argv(target: dict, local_path: str, remote_path: str,
              download: bool = False) -> list:
    """scp 传输 argv：上传 = local → remote；下载 = remote → local。
    注意 scp 端口用大写 -P；用户写入目标串 user@host（D2 Artifact 传输）。"""
    host = target.get("host_name", "")
    if not _valid_ssh_host(host):
        raise ValueError(f"invalid ssh host: {host!r}")
    prefix = f"{target.get('user')}@" if target.get("user") else ""
    remote = f"{prefix}{host}:{remote_path}"
    argv = ["scp", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5"]
    if target.get("config_path"):
        argv += ["-F", str(target["config_path"])]
    if target.get("port"):
        argv += ["-P", str(target["port"])]
    if download:
        argv += [remote, local_path]
    else:
        argv += [local_path, remote]
    return argv


def _transfer_file(target: dict, local_path: str, remote_path: str,
                   download: bool = False, timeout: float = 120.0) -> dict:
    """一次 scp 传输（有界执行）。返回 {ok, detail}，不抛异常。"""
    argv = _scp_argv(target, local_path, remote_path, download)
    spec = ExecutionSpec(argv=argv, cwd=".", timeout=timeout, output_cap=65536,
                         label="scp-transfer", effect_class="sandboxed",
                         mobility="stateless")
    proc, stdout, stderr, timed_out, overflow = _spawn_bounded(
        spec, dict(os.environ))
    ok = (proc is not None and proc.returncode == 0
          and not timed_out and not overflow)
    detail = (stdout + stderr).strip()[-300:] or ("ok" if ok else "transfer failed")
    bytes_sent = 0
    if not download:
        try:
            bytes_sent = os.path.getsize(local_path)
        except OSError:
            bytes_sent = 0
    return {"ok": ok, "detail": detail, "bytes": bytes_sent}


# 执行中的 ssh 进程登记表：cancel(attempt_id) 可杀掉对应进程组。
_ACTIVE_SSH_PROCS = {}
_ACTIVE_SSH_LOCK = threading.Lock()


def _register_ssh_proc(attempt_id: str, proc) -> None:
    with _ACTIVE_SSH_LOCK:
        _ACTIVE_SSH_PROCS[attempt_id] = proc


def _unregister_ssh_proc(attempt_id: str) -> None:
    with _ACTIVE_SSH_LOCK:
        _ACTIVE_SSH_PROCS.pop(attempt_id, None)


def _cancel_remote(attempt_id: str) -> None:
    """取消一个执行中的远程尝试：杀掉 ssh 客户端进程组（连接关闭后远端
    命令随之终止；D2 的尽力而为取消，D3 Runner 提供可靠取消）。"""
    with _ACTIVE_SSH_LOCK:
        proc = _ACTIVE_SSH_PROCS.get(attempt_id)
    if proc is not None:
        _kill_group(proc)


def probe_required_error(target: dict) -> Optional[str]:
    """执行前门禁：远程设备必须有有效期内的探测证据（AADM-D MUST：能力
    必须带版本、证据和有效期）。返回错误文案或 None。"""
    if _fresh_fingerprint(target):
        return None
    return (f"设备 {target.get('name', '')} 无有效探测证据"
            "（先运行 pi-batch devices probe <name>）")


def placement_gate_error(name: str, effect_class: str) -> Optional[str]:
    """执行前门禁：Placement Dry-run 必须通过（AADM-D MUST）。设备不在
    已知设备池同样拒绝。"""
    placement = TaskPlacement(target_required=name,
                              max_effect_class=effect_class)
    found = False
    for item in evaluate_placement(placement):
        if item["device"] == name:
            found = True
            if not item["feasible"]:
                return "Placement Dry-run 未通过: " + "; ".join(item["reasons"])
    if not found:
        return f"设备 {name} 不在已知设备池（先 probe 或加入静态配置）"
    return None


def _execute_remote(target: dict, spec: ExecutionSpec) -> ExecutionEvidence:
    """SSH 受控执行（D2）：结构化 argv（禁任意 Shell）、副作用等级上限
    sandboxed、硬超时 SIGKILL、可取消、输出上限；证据与本地同构。"""
    if (EFFECT_ORDER.get(spec.effect_class, 99)
            > EFFECT_ORDER.get(REMOTE_MAX_EFFECT, 2)):
        raise ValueError(f"effect class {spec.effect_class} 不允许远程执行"
                         f"（D2 上限 {REMOTE_MAX_EFFECT}）")
    start = time.monotonic()
    attempt = f"ssh-{uuid.uuid4().hex[:12]}"
    remote_cmd = _remote_argv_command(spec.argv, spec.cwd)
    run_spec = ExecutionSpec(argv=_build_ssh_argv(target, remote_cmd), cwd=".",
                             timeout=spec.timeout, output_cap=spec.output_cap,
                             label=spec.label, effect_class=spec.effect_class,
                             mobility=spec.mobility, inputs=spec.inputs)

    def _register(proc):
        _register_ssh_proc(attempt, proc)

    proc, stdout, stderr, timed_out, overflow = _spawn_bounded(
        run_spec, dict(os.environ), on_spawn=_register)
    _unregister_ssh_proc(attempt)
    exit_code = -1 if timed_out else (proc.returncode if proc is not None else -1)
    failed = (proc is None) or timed_out or overflow or exit_code != 0
    if timed_out and not stderr:
        stderr = f"timed out after {spec.timeout}s"
    return ExecutionEvidence(
        attempt_id=attempt, target_id=f"ssh:{target.get('name', '')}",
        label=spec.label,
        result="failed" if failed else "passed",
        exit_code=exit_code, timed_out=timed_out, output_overflow=overflow,
        stdout=stdout, stderr=stderr,
        code_digest=spec.code_digest,
        environment_digest=_digest(
            f"{FABRIC_VERSION}|ssh|{_fresh_fingerprint(target)}"),
        input_digest=_digest(
            json.dumps([a.sha256 for a in spec.inputs], sort_keys=True)),
        output_digest=_digest(stdout + stderr),
        runner_version=FABRIC_VERSION,
        elapsed=time.monotonic() - start)


class SshExecutionTarget:
    """SSH 受控执行适配器（D2）：probe/execute/cancel 与本地同构，
    证据格式一致（远程任务与本地任务同一证据格式）。"""

    def __init__(self, target: dict):
        self.target = target
        self.target_id = f"ssh:{target.get('name', '')}"

    def probe(self) -> dict:
        return remote_probe(self.target)

    def execute(self, spec: ExecutionSpec) -> ExecutionEvidence:
        return _execute_remote(self.target, spec)

    def cancel(self, attempt_id: str) -> None:
        _cancel_remote(attempt_id)


def target_for(name: str, ssh_config_path: str = "") -> Optional[SshExecutionTarget]:
    """按名字解析并返回远程执行目标（未知设备返回 None）。"""
    target = resolve_device(name, ssh_config_path=ssh_config_path)
    return SshExecutionTarget(target) if target else None
