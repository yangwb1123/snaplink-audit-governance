"""Device-Aware Execution Fabric — D1: inventory / probe / placement.

AADM-D (`docs/AADM_DEVICE.md`) D1 批次：SSH inventory（~/.ssh/config 只读
解析 + 静态配置）、SSH 远程只读探测（固定命令集、BatchMode、不可达也
返回报告）、探测状态缓存（.pi-batch/devices/state.jsonl，追加式）、
Placement Dry-run（按 OS/架构/内存/能力/副作用等级筛选）。

安全边界（AADM-D §6/§21/§23）：
- 只读：固定命令集，不拼接任意 Shell；不修改设备、不自动安装
- BatchMode=yes：无密码交互，无法登录则 fail closed
- 不读取私钥内容（IdentityFile 只保留引用）
- 远程设备 trust 默认 untrusted-external / max read_only
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from . import config
from .fabric import (CAPABILITY_TTL_HOURS, EFFECT_CLASSES, ExecutionSpec,
                     FABRIC_PROTOCOL, _digest, _spawn_bounded, local_probe)
from .fabric_ssh import _build_ssh_argv

# 固定远程探测命令（AADM-D §6/§38：结构化 TaskSpec，不拼接任意 Shell；
# 这是常量字符串，不插值任何用户输入）。只读，无任何远程修改。
REMOTE_PROBE_CMD = (
    "printf 'os=%s\\n' \"$(uname -s 2>/dev/null || echo unknown)\"; "
    "printf 'os_release=%s\\n' \"$(uname -r 2>/dev/null || echo '')\"; "
    "printf 'architecture=%s\\n' \"$(uname -m 2>/dev/null || echo unknown)\"; "
    "printf 'cpu_cores=%s\\n' \"$(nproc 2>/dev/null || echo 0)\"; "
    "printf 'hostname=%s\\n' \"$(hostname 2>/dev/null || echo unknown)\"; "
    "grep -m1 '^MemTotal:' /proc/meminfo 2>/dev/null || printf 'MemTotal: 0 kB\\n'; "
    "for t in docker python3 node go java git; do "
    "if command -v \"$t\" >/dev/null 2>&1; then printf 'runtime_%s=1\\n' \"$t\"; "
    "else printf 'runtime_%s=0\\n' \"$t\"; fi; done"
)

# SSH 选项与受控执行原语在 pbatch/fabric_ssh.py / fabric_exec.py。

# 远程探测状态缓存（追加式 JSONL，崩溃安全；有界行缓冲）。
DEVICE_STATE_DIR = Path(".pi-batch") / "devices"
DEVICE_STATE_FILE = DEVICE_STATE_DIR / "state.jsonl"
STATE_LINE_MAX_BYTES = 256 * 1024
STATE_FILE_MAX_BYTES = 4 * 1024 * 1024


# ---------------------------------------------------------------------------
# 设备清单（INVENTORY 模式）
# ---------------------------------------------------------------------------

def keymigrate_main(argv: Optional[list] = None) -> int:
    """`devices keymigrate [--from DIR]`：项目内旧密钥 → 用户级。

    产品化迁移（旧版密钥在 .pi-batch/devices/keys/）；迁移后原文件保留
    （不删除——用户确认后手动清理）。"""
    import argparse
    from .fabric_devices import DEVICE_STATE_DIR as _OLD
    from . import user_dirs
    parser = argparse.ArgumentParser(
        prog="pi-batch.py devices keymigrate",
        description="把旧项目内设备密钥迁移到用户级目录")
    parser.add_argument("--from", dest="src", default=str(
        Path(DEVICE_STATE_DIR) / "keys"))
    args = parser.parse_args(argv)
    src = Path(args.src)
    target = user_dirs.keys_dir()
    if not src.is_dir():
        print(f"keymigrate: 无旧密钥目录 {src}（无需迁移）")
        raise SystemExit(0)
    target.mkdir(parents=True, exist_ok=True)
    moved = 0
    for path in sorted(src.glob("*.json")):
        dest = target / path.name
        if dest.exists():
            continue  # 目标已有同名密钥（不覆盖）
        dest.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
        moved += 1
    print(f"keymigrate: {moved} 个密钥已迁入 {target}（原文件保留，确认后手动删）")
    raise SystemExit(0)


def _remote_device_id(target: dict) -> str:
    """远程设备标识（D1）：由 ssh 目标地址派生，静态配置与 ssh alias 解析
    保持一致。注意：地址不是密码学身份（AADM-D §7），D3 登记证书后替换。"""
    raw = f"ssh|{target.get('user', '')}@{target.get('host_name', target.get('name', ''))}"
    return "ssh-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def device_inventory(static: Optional[list] = None,
                     use_cache: bool = True) -> list:
    """INVENTORY 模式设备清单：本机 + 静态配置 + 探测过的 ssh 主机（含
    缓存的最新探测状态）。只读，不探测远程、不扫描网络。"""
    nodes = [local_probe()]
    cache = load_probe_states() if use_cache else {}
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%S")
    for entry in (static if static is not None else config.DEVICE_FABRIC_STATIC):
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name", "unnamed"))
        target = {"name": name, "host_name": str(entry.get("host", name)),
                  "user": str(entry.get("user", "")),
                  "port": str(entry.get("port", "")), "config_path": ""}
        node = {
            "device_id": _remote_device_id(target),
            "kind": str(entry.get("kind", "physical_device")),
            "name": name,
            "labels": entry.get("labels", {}) if isinstance(entry.get("labels"), dict) else {},
            "transports": [{"type": "ssh", "host_alias": name}],
            "static_capabilities": {},
            "runtimes": {},
            "dynamic_state": {"status": "unknown", "last_seen_at": ""},
            "trust": {"level": 0, "zone": "untrusted-external",
                      "max_effect_class": "read_only"},
            "protocol_version": "",
            "capability_fingerprint": "",
            "capability_valid_until": "",
        }
        cached = cache.get(node["device_id"])
        if cached and isinstance(cached.get("report"), dict):
            report = cached["report"]
            if report.get("capability_valid_until", "") >= now_iso:
                node.update(report)  # 用最新探测结果覆盖占位
        nodes.append(node)
    # 探测过的 ssh-config 主机（非静态条目）也进清单；过期标记为 stale。
    seen = {node["device_id"] for node in nodes}
    for device_id, record in cache.items():
        if device_id in seen or not isinstance(record.get("report"), dict):
            continue
        report = record["report"]
        if report.get("capability_valid_until", "") < now_iso:
            report = dict(report)
            report["dynamic_state"] = dict(report.get("dynamic_state", {}))
            report["dynamic_state"]["status"] = "stale"
        nodes.append(report)
        seen.add(device_id)
    _merge_registered_runners(nodes, seen)
    return nodes


def _merge_registered_runners(nodes: list, seen: set) -> None:
    """Runner 注册表（runners.jsonl）中的设备也进清单（status=registered，
    能力待探测）。设备织网 D3 与 D1 探测清单的集成视图。"""
    path = DEVICE_STATE_DIR / "runners.jsonl"
    if not path.exists():
        return
    try:
        handle = path.open("r", encoding="utf-8")
    except OSError:
        return
    with handle:
        total = 0
        for line in handle:
            total += len(line.encode("utf-8", errors="replace"))
            if total > STATE_FILE_MAX_BYTES:
                break
            if len(line) > STATE_LINE_MAX_BYTES:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            device_id = record.get("device_id") if isinstance(record, dict) else ""
            if not device_id or device_id in seen:
                continue
            nodes.append({
                "device_id": device_id,
                "name": record.get("name", device_id),
                "kind": "physical_device",
                "labels": {"execution.fabric": "true"},
                "transports": [{"type": "runner"}],
                "static_capabilities": {},
                "runtimes": {},
                "dynamic_state": {"status": "registered", "last_seen_at": ""},
                "trust": {"level": 0, "zone": "untrusted-external",
                          "max_effect_class": "read_only"},
                "protocol_version": "",
                "capability_fingerprint": "",
                "capability_valid_until": "",
            })
            seen.add(device_id)


# ---------------------------------------------------------------------------
# SSH inventory 与远程只读探测（D1）
# ---------------------------------------------------------------------------

def parse_ssh_config(config_path: str = "") -> list:
    """只读解析 ~/.ssh/config：Host 别名 + HostName/User/Port/ProxyJump/
    IdentityFile 引用。通配符模式跳过（不可直接探测）；不读取私钥内容。"""
    path = Path(config_path or os.path.expanduser("~/.ssh/config"))
    if not path.exists():
        return []
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    hosts = []
    current = None
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        key, value = parts[0].lower(), parts[1]
        if key == "host":
            for alias in value.split():
                if "*" in alias or "?" in alias:
                    continue
                current = {"alias": alias, "host_name": alias}
                hosts.append(current)
        elif current is not None and key in ("hostname", "user", "port",
                                             "proxyjump", "identityfile"):
            current["host_name" if key == "hostname" else key] = value
    return hosts


def resolve_device(name: str, static: Optional[list] = None,
                   ssh_config_path: str = "") -> Optional[dict]:
    """按名字解析执行目标：静态配置 > ssh config 别名。返回 ssh 目标信息
    或 None（未知设备）。"""
    if not name:
        return None
    for entry in (static if static is not None else config.DEVICE_FABRIC_STATIC):
        if not isinstance(entry, dict):
            continue
        if str(entry.get("name", "")) == name:
            return {"name": name, "host_name": str(entry.get("host", name)),
                    "user": str(entry.get("user", "")),
                    "port": str(entry.get("port", "")),
                    "config_path": ssh_config_path}
    for host in parse_ssh_config(ssh_config_path):
        if host["alias"] == name:
            return {"name": name, "host_name": host.get("host_name", name),
                    "user": host.get("user", ""), "port": host.get("port", ""),
                    "config_path": ssh_config_path}
    return None


def _valid_ssh_host(name: str) -> bool:
    """SSH 目标名校验：仅字母数字._-，且不以 - 开头（防选项注入）。"""
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", name or ""))


def _ssh_probe_argv(target: dict) -> list:
    """只读探测 ssh argv：BatchMode（无密码交互）、连接超时、固定命令集。"""
    return _build_ssh_argv(target, REMOTE_PROBE_CMD)


def _parse_remote_probe(stdout: str) -> dict:
    """解析固定命令集的 key=value 输出（容错：未知行忽略）。"""
    fields = {"os": "unknown", "os_release": "", "architecture": "unknown",
              "cpu_cores": 0, "hostname": "unknown",
              "total_memory_bytes": 0, "runtimes": {}}
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("MemTotal:"):
            try:
                fields["total_memory_bytes"] = int(line.split()[1]) * 1024
            except (IndexError, ValueError):
                pass  # 解析容错：坏行跳过（有界读取不崩溃）
            continue
        key, sep, value = line.partition("=")
        if not sep:
            continue
        key, value = key.strip(), value.strip()
        if key == "os":
            fields["os"] = value or "unknown"
        elif key == "os_release":
            fields["os_release"] = value
        elif key == "architecture":
            fields["architecture"] = value or "unknown"
        elif key == "cpu_cores":
            try:
                fields["cpu_cores"] = int(value)
            except ValueError:
                pass  # 非整数核心数跳过（容错解析）
        elif key == "hostname":
            fields["hostname"] = value or "unknown"
        elif key.startswith("runtime_"):
            fields["runtimes"][key[len("runtime_"):]] = value == "1"
    return fields


def remote_probe(target: dict, timeout: float = 20.0) -> dict:
    """SSH 只读探测（P0+P1）：固定命令集、BatchMode、有界输出。不可达也
    返回报告（status=unreachable，含错误摘录），不抛异常（故障显式）。"""
    spec = ExecutionSpec(argv=_ssh_probe_argv(target), cwd=".",
                         timeout=timeout, output_cap=65536,
                         label=f"ssh-probe:{target.get('name', '')}",
                         effect_class="read_only", mobility="stateless")
    proc, stdout, stderr, timed_out, overflow = _spawn_bounded(
        spec, dict(os.environ))
    fields = _parse_remote_probe(stdout)
    reachable = (proc is not None and proc.returncode == 0
                 and not timed_out and not overflow)
    static_caps = {key: value for key, value in fields.items()
                   if key != "runtimes"}
    now = time.time()
    fingerprint = ""
    valid_until = ""
    if reachable:
        fingerprint = _digest(json.dumps(fields, sort_keys=True))[:16]
        valid_until = time.strftime(
            "%Y-%m-%dT%H:%M:%S",
            time.localtime(now + CAPABILITY_TTL_HOURS * 3600))
    return {
        "device_id": _remote_device_id(target),
        "kind": "physical_device",
        "name": target.get("name", target.get("host_name", "")),
        "labels": {"execution.fabric": "true",
                   "trust.zone": "untrusted-external"},
        "transports": [{"type": "ssh", "host_alias": target.get("name", "")}],
        "static_capabilities": static_caps if reachable else {},
        "runtimes": fields.get("runtimes", {}) if reachable else {},
        "dynamic_state": {
            "status": "online" if reachable else "unreachable",
            "load": 0.0,
            "available_memory_bytes": 0,
            "running_tasks": 0,
            "last_seen_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        },
        "trust": {"level": 0, "zone": "untrusted-external",
                  "max_effect_class": "read_only"},
        "protocol_version": FABRIC_PROTOCOL,
        "capability_fingerprint": fingerprint,
        "capability_valid_until": valid_until,
        "probe_error": stderr.strip()[-300:] if not reachable else "",
    }


# ---------------------------------------------------------------------------
# 探测状态缓存（追加式 JSONL，崩溃安全）
# ---------------------------------------------------------------------------

def save_probe_state(report: dict) -> str:
    """追加式记录一次远程探测结果（只追加不覆盖，崩溃安全）。"""
    DEVICE_STATE_DIR.mkdir(parents=True, exist_ok=True)
    record = {
        "device_id": report["device_id"],
        "name": report.get("name", ""),
        "probed_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "report": report,
    }
    with DEVICE_STATE_FILE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return str(DEVICE_STATE_FILE)


def load_probe_states() -> dict:
    """读回每个设备最近一次探测记录；有限行缓冲，超长行跳过。"""
    states = {}
    try:
        handle = DEVICE_STATE_FILE.open("r", encoding="utf-8")
    except OSError:
        return states
    with handle:
        total = 0
        for line in handle:
            total += len(line.encode("utf-8", errors="replace"))
            if total > STATE_FILE_MAX_BYTES:
                break
            if len(line) > STATE_LINE_MAX_BYTES:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if isinstance(record, dict) and record.get("device_id"):
                states[record["device_id"]] = record
    return states


def _fresh_fingerprint(target: dict) -> str:
    """缓存里最新的能力指纹；无缓存或已过期返回空串（AADM-D §8 有效期）。"""
    record = load_probe_states().get(_remote_device_id(target), {})
    report = record.get("report", {}) if isinstance(record, dict) else {}
    valid = report.get("capability_valid_until", "")
    if valid and valid >= time.strftime("%Y-%m-%dT%H:%M:%S"):
        return str(report.get("capability_fingerprint", ""))
    return ""


# ---------------------------------------------------------------------------
# Placement Dry-run（AADM-D §11：先判断在哪里做）
# ---------------------------------------------------------------------------

EFFECT_ORDER = {name: index for index, name in enumerate(EFFECT_CLASSES)}


@dataclass
class TaskPlacement:
    """一次任务的设备放置约束（AADM-D §10 的 D1 子集）。"""
    required_os: list = field(default_factory=list)
    required_architecture: list = field(default_factory=list)
    min_memory_bytes: int = 0
    min_cpu_cores: int = 0
    required_capabilities: list = field(default_factory=list)
    max_effect_class: str = "read_only"
    target_required: str = ""


def _resource_reasons(placement: TaskPlacement, caps: dict) -> list:
    """OS/架构/内存/核心硬过滤原因。"""
    reasons = []
    required_os = [item.lower() for item in placement.required_os]
    if required_os and caps.get("os", "").lower() not in required_os:
        reasons.append(f"OS {caps.get('os', '-')} 不在 {placement.required_os}")
    required_arch = [item.lower() for item in placement.required_architecture]
    if required_arch and caps.get("architecture", "").lower() not in required_arch:
        reasons.append(f"架构 {caps.get('architecture', '-')} "
                       f"不在 {placement.required_architecture}")
    memory = caps.get("total_memory_bytes") or 0
    if placement.min_memory_bytes and memory < placement.min_memory_bytes:
        reasons.append(f"内存 {memory // (1024 ** 2)}MiB < "
                       f"{placement.min_memory_bytes // (1024 ** 2)}MiB")
    cores = caps.get("cpu_cores") or 0
    if placement.min_cpu_cores and cores < placement.min_cpu_cores:
        reasons.append(f"核心 {cores} < {placement.min_cpu_cores}")
    return reasons


def _capability_reasons(placement: TaskPlacement, caps: dict, runtimes: dict,
                        labels: dict, trust: dict) -> list:
    """能力与副作用等级硬过滤原因（placement 判据第一层硬约束）。"""
    reasons = _resource_reasons(placement, caps)
    for cap in placement.required_capabilities:
        if not (labels.get(cap) or runtimes.get(cap)):
            reasons.append(f"缺少能力 {cap}")
    device_effect = trust.get("max_effect_class", "irreversible")
    if (EFFECT_ORDER.get(device_effect, 5)
            < EFFECT_ORDER.get(placement.max_effect_class, 5)):
        reasons.append(f"副作用等级 {placement.max_effect_class} "
                       f"超出设备允许 {device_effect}")
    return reasons


def _check_placement(placement: TaskPlacement, device: dict) -> tuple:
    """单设备可行性检查：返回 (feasible, reasons, warnings)。"""
    reasons = []
    caps = device.get("static_capabilities", {}) or {}
    runtimes = device.get("runtimes", {}) or {}
    labels = device.get("labels", {}) or {}
    state = device.get("dynamic_state", {}) or {}
    trust = device.get("trust", {}) or {}
    name = device.get("name", device.get("device_id", ""))
    if placement.target_required and name != placement.target_required:
        reasons.append(f"不是目标设备 {placement.target_required}")
    reasons += _capability_reasons(placement, caps, runtimes, labels, trust)
    warnings = []
    if state.get("status") != "online":
        warnings.append(f"设备状态 {state.get('status', 'unknown')}")
    valid_until = device.get("capability_valid_until", "")
    if valid_until and valid_until < time.strftime("%Y-%m-%dT%H:%M:%S"):
        warnings.append("能力信息已过期")
    return (not reasons, reasons, warnings)


def evaluate_placement(placement: TaskPlacement,
                       devices: Optional[list] = None) -> list:
    """对全部已知设备做放置 Dry-run：返回候选清单（AADM-D §11 形状）。"""
    out = []
    for device in (devices if devices is not None else device_inventory()):
        feasible, reasons, warnings = _check_placement(placement, device)
        out.append({
            "device": device.get("name", device.get("device_id", "")),
            "device_id": device.get("device_id", ""),
            "feasible": feasible,
            "reasons": reasons,
            "warnings": warnings,
        })
    return out
