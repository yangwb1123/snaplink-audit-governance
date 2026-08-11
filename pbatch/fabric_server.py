"""Device-Aware Execution Fabric — D3: 控制平面（Control Plane）.

接收设备 Runner 的出站连接（NAT/动态 IP 友好，AADM-D §6）：注册（TOFU +
人工审批）、心跳、任务租约（Lease + Epoch + Fencing Token）、证据提交
（陈旧 fencing token 拒绝 = 防 split-brain）、对账（reconcile）。

状态持久化（追加式 JSONL，崩溃安全）：
- <state>/runners.jsonl   设备注册记录（device_id/name/secret）
- <state>/approvals.jsonl 人工审批记录（只追加，不删除）

CLI：`pi-batch devices serve [--port N] [--auto-approve]`、
`pi-batch devices approve NAME`、`pi-batch devices runners`。
默认只监听 127.0.0.1（D3a 无 TLS；跨机部署需反向代理终止 TLS）。
"""

from __future__ import annotations

import json
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Optional

from . import config
from .config import log
from .fabric_devices import DEVICE_STATE_DIR
from . import fabric_server_util
from .fabric_rpc import BODY_MAX_BYTES, verify_request
from .fabric_server_util import CHECKPOINT_MAX_B64, sha256

# 心跳超时判定（秒）：超过该间隔未收到心跳的设备视为 stale。
HEARTBEAT_STALE_SECONDS = 90

_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{3,63}$")

def _valid_name(name: str) -> bool:
    return bool(_ID_RE.fullmatch(name or ""))

class ControlPlane(fabric_server_util.CheckpointOpsMixin):
    """设备注册表 + 租约 + 证据账本的权威状态（进程内 + 追加式持久化）。"""

    def __init__(self, state_dir: str = "", auto_approve: bool = False,
                 budget_cap: float = 0.0,
                 tenant_quotas: Optional[dict] = None,
                 device_quotas: Optional[dict] = None):
        self.state_dir = Path(state_dir or DEVICE_STATE_DIR)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.auto_approve = auto_approve
        self.secrets: dict = {}          # device_id -> secret（验签）
        self.devices: dict = {}          # device_id -> {name, last_seen, state}
        self.epochs: dict = {}           # device_id -> 当前 lease epoch
        self.leases: dict = {}           # lease_id -> lease（按租约索引）
        self.current_lease: dict = {}    # device_id -> lease_id（通用租约最新）
        self.task_leases: dict = {}      # task_id -> lease_id（任务租约最新）
        self.tasks: dict = {}            # task_id -> 任务记录
        self.cordoned: dict = {}         # device_id -> {drain, since}
        self.revoked: set = set()        # 退役设备（密钥吊销，永久拒绝）
        self.events: list = []           # 内存事件（有界；追加式落盘）
        self.device_budget_cap = float(budget_cap or 0)   # 0 = 不限
        self.device_budget_spent: dict = {}  # device_id -> 累计消耗
        self.tenant_quotas: dict = {str(k): int(v) for k, v in
                                    (tenant_quotas or {}).items()}
        self.device_quotas: dict = {str(k): int(v) for k, v in
                                    (device_quotas or {}).items()}
        self.device_running: dict = {}
        self.tenant_running: dict = {}       # tenant -> 进行中任务数
        self.checkpoints: dict = {}      # task_id -> {blob_b64, digest, size}
        self.replay: dict = {}           # nonce 防重放缓存（有界）
        self._load_runners()

    # -- 持久化 ------------------------------------------------------------

    def _load_runners(self) -> None:
        """启动时读回已注册设备（secret 恢复，验签可用；吊销的不恢复）。"""
        self.revoked = set(self._revocation_ids())
        path = self.state_dir / "runners.jsonl"
        if not path.exists():
            return
        for line in _read_lines_bounded(path):
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if (isinstance(record, dict) and record.get("device_id")
                    and record["device_id"] not in self.revoked):
                self.secrets[record["device_id"]] = str(record.get("secret", ""))
                self.devices.setdefault(record["device_id"], {
                    "name": str(record.get("name", "")),
                    "last_seen": 0, "dynamic_state": {}, "heartbeats": 0,
                })

    def _persist_registration(self, device_id: str, name: str,
                              secret: str) -> None:
        record = {"device_id": device_id, "name": name, "secret": secret,
                  "registered_at": time.strftime("%Y-%m-%dT%H:%M:%S")}
        with (self.state_dir / "runners.jsonl").open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _approval_ids(self) -> set:
        """读回审批记录（每次注册实时读，CLI 审批无需重启服务）。"""
        approved = set()
        path = self.state_dir / "approvals.jsonl"
        if not path.exists():
            return approved
        for line in _read_lines_bounded(path):
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if isinstance(record, dict) and record.get("device_id"):
                approved.add(record["device_id"])
        return approved

    def _approve(self, device_id: str, name: str, by: str) -> None:
        record = {"device_id": device_id, "name": name,
                  "approved_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                  "approved_by": by}
        with (self.state_dir / "approvals.jsonl").open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    # -- 请求处理 ----------------------------------------------------------

    def secret_for(self, device_id: str) -> Optional[str]:
        return self.secrets.get(device_id)

    def handle_register(self, payload: dict) -> dict:
        """TOFU 注册：设备自生成 secret 首次声称身份；是否放行由审批决定。"""
        device_id = str(payload.get("device_id", ""))
        name = str(payload.get("name", ""))
        secret = str(payload.get("secret", ""))
        if not _valid_name(device_id) or not _valid_name(name) or not secret:
            return {"approved": False, "error": "missing or invalid fields"}
        if device_id in self.revoked:
            return {"approved": False, "error": "device revoked"}
        known = self.secrets.get(device_id)
        if known is not None and known != secret:
            return {"approved": False, "error": "secret mismatch"}
        if known is None:
            self.secrets[device_id] = secret
            self.devices.setdefault(device_id, {
                "name": name, "last_seen": 0, "dynamic_state": {},
                "heartbeats": 0,
            })["name"] = name
            self._persist_registration(device_id, name, secret)
        probe = payload.get("probe")
        if isinstance(probe, dict):
            self.devices[device_id]["probe"] = probe  # 能力证据（调度用）
        for meta_key in ("tenant", "region", "attestation"):
            value = payload.get(meta_key)
            if value:
                self.devices[device_id][meta_key] = str(value)[:128]
        if self.auto_approve:
            self._approve(device_id, name, "auto")
        if device_id in self._approval_ids():
            self._record_event(device_id, "register", "device")
            return {"approved": True, "message": "approved"}
        return {"approved": False, "message": "pending approval",
                "hint": "运行 pi-batch devices approve <name>"}

    def handle_heartbeat(self, payload: dict) -> dict:
        device_id = str(payload.get("device_id", ""))
        state = payload.get("dynamic_state", {})
        if not isinstance(state, dict):
            state = {}
        device = self.devices.setdefault(device_id, {"name": "", "last_seen": 0,
                                                     "dynamic_state": {},
                                                     "heartbeats": 0})
        device["last_seen"] = time.time()
        device["heartbeats"] += 1
        device["dynamic_state"] = state
        device["running_attempts"] = payload.get("running_attempts", [])
        return {"ok": True, "server_time": time.time(),
                "heartbeats": device["heartbeats"]}

    def handle_lease(self, payload: dict) -> dict:
        """通用租约（CLI/诊断用）：设备级最新租约。"""
        device_id = str(payload.get("device_id", ""))
        if device_id not in self._approval_ids():
            return {"lease": None, "error": "device not approved"}
        resources = payload.get("resources", {})
        if not isinstance(resources, dict):
            resources = {}
        lease = self._issue_lease(device_id, resources,
                                  int(payload.get("ttl_seconds", 300) or 300))
        self.leases[lease["lease_id"]] = lease
        self.current_lease[device_id] = lease["lease_id"]
        return {"lease": lease}

    def handle_evidence(self, payload: dict) -> dict:
        """证据提交门禁：仅持有最新 fencing token 的租约可提交；任务证据
        按 task_id 幂等（重复提交接受但不重复计数，AADM-D §18）。"""
        device_id = str(payload.get("device_id", ""))
        lease_id = str(payload.get("lease_id", ""))
        token = payload.get("fencing_token")
        task_id = str(payload.get("task_id", ""))
        task = self.tasks.get(task_id) if task_id else None
        if task is not None and task.get("status") in ("done", "failed"):
            return {"accepted": True, "duplicate": True, "task_id": task_id}
        if task_id:
            expected = self.task_leases.get(task_id)
            if expected is None:
                return {"accepted": False, "reason": "unknown task"}
            if lease_id != expected:
                return {"accepted": False, "reason": "stale fencing token"}
        else:
            expected = self.current_lease.get(device_id)
            if expected is None:
                return {"accepted": False, "reason": "no lease"}
            if lease_id != expected:
                return {"accepted": False, "reason": "stale fencing token"}
        lease = self.leases.get(lease_id)
        if lease is None or lease.get("device_id") != device_id:
            return {"accepted": False, "reason": "no lease"}
        if token != lease["fencing_token"]:
            return {"accepted": False, "reason": "stale fencing token"}
        if lease["expires_at"] < time.time():
            return {"accepted": False, "reason": "lease expired"}
        if task is not None:
            evidence = payload.get("evidence", {}) or {}
            task["status"] = ("done" if evidence.get("result") == "passed"
                               else "failed")
            task["evidence"] = evidence
            task["finished_at"] = time.time()
            self._tenant_quota_dec(device_id)
            self._device_quota_dec(device_id)
            self._record_event(device_id, "evidence", "task", task_id)
            return {"accepted": True, "task_id": task_id}
        self.devices.setdefault(device_id, {}).setdefault(
            "evidence_count", 0)
        self.devices[device_id]["evidence_count"] += 1
        return {"accepted": True, "attempt_id": payload.get("attempt_id", "")}

    def handle_reconcile(self, payload: dict) -> dict:
        """对账：服务端视图（当前租约/epoch）返回给 Runner 比对。"""
        device_id = str(payload.get("device_id", ""))
        lease_id = self.current_lease.get(device_id)
        lease = self.leases.get(lease_id) if lease_id else None
        return {"device_id": device_id,
                "epoch": self.epochs.get(device_id, 0),
                "lease": lease,
                "server_time": time.time()}

    # -- D4: 任务注册表 ------------------------------------------------------

    def _reschedule_stale(self) -> None:
        """心跳超时/从未心跳的设备：stateless/restartable 任务重新排队
        （供其他设备认领），pinned/checkpointable 标记 lost。"""
        now = time.time()
        for device_id, device in list(self.devices.items()):
            last = device.get("last_seen", 0)
            if last and (now - last) <= HEARTBEAT_STALE_SECONDS:
                continue
            for task in self.tasks.values():
                if (task.get("device_id") == device_id
                        and task.get("status") in ("queued", "leased",
                                                    "running")):
                    if task.get("mobility") in ("stateless", "restartable"):
                        task["status"] = "queued"
                        task["device_id"] = None
                        task["rescheduled_at"] = time.time()
                    else:
                        task["status"] = "lost"
                        task["lost_at"] = time.time()

    def _store_checkpoint(self, task_id: str, payload: dict) -> str:
        """检查点存取（有界）；返回错误串（空 = 成功）。"""
        checkpoint = payload.get("checkpoint_blob", "")
        if not (isinstance(checkpoint, str) and checkpoint):
            return ""
        if len(checkpoint) > CHECKPOINT_MAX_B64:
            return "checkpoint too large"
        self.checkpoints[task_id] = {
            "blob_b64": checkpoint, "size": len(checkpoint),
            "digest": sha256(checkpoint), "updated_at": time.time(),
        }
        return ""

    def _task_view(self, task: dict) -> dict:
        view = {key: task.get(key) for key in (
            "task_id", "device_id", "argv", "cwd", "timeout",
            "effect_class", "mobility", "checkpoint_remote", "status",
            "created_at", "finished_at", "evidence", "rescheduled_at",
            "lost_at")}
        lease = self.leases.get(task.get("lease_id", ""))
        view["lease"] = lease
        return view

    def handle_task_submit(self, payload: dict) -> dict:
        """提交任务到指定设备（须已审批且未 cordon）。"""
        device_id = str(payload.get("device_id", ""))
        if device_id not in self._approval_ids():
            return {"task": None, "error": "device not approved"}
        if device_id in self.cordoned:
            return {"task": None, "error": "device cordoned/draining"}
        argv = payload.get("argv")
        if (not isinstance(argv, list) or not argv
                or not all(isinstance(item, str) for item in argv)):
            return {"task": None, "error": "argv must be a string list"}
        quota_error = self._tenant_quota_check(device_id)
        if quota_error:
            return {"task": None, "error": quota_error}
        quota_error = self._device_quota_check(device_id)
        if quota_error:
            return {"task": None, "error": quota_error}
        cost = float(payload.get("estimated_cost", 0) or 0)
        cost = max(0.0, min(cost, 1000.0))
        spent = self.device_budget_spent.get(device_id, 0.0)
        if self.device_budget_cap > 0 and spent + cost > self.device_budget_cap:
            return {"task": None,
                    "error": f"device budget exhausted (spent {spent:.1f} + "
                             f"{cost:.1f} > cap {self.device_budget_cap:.1f})"}
        task_id = "t-" + uuid.uuid4().hex[:12]
        lease = self._issue_lease(device_id, {},
                                  int(payload.get("ttl_seconds", 300) or 300))
        self.leases[lease["lease_id"]] = lease
        self.task_leases[task_id] = lease["lease_id"]
        checkpoint_error = self._store_checkpoint(task_id, payload)
        if checkpoint_error:
            return {"task": None, "error": checkpoint_error}
        self.device_budget_spent[device_id] = spent + cost  # 提交即预留
        self._tenant_quota_inc(device_id)
        self._device_quota_inc(device_id)
        self._record_event(device_id, "submit", "task", task_id,
                           attempt=lease["lease_id"])
        self.tasks[task_id] = {
            "task_id": task_id, "device_id": device_id, "argv": argv,
            "estimated_cost": cost,
            "cwd": str(payload.get("cwd", "")),
            "timeout": float(payload.get("timeout", 60) or 60),
            "effect_class": str(payload.get("effect_class", "read_only")),
            "mobility": str(payload.get("mobility", "stateless")),
            "checkpoint_remote": str(payload.get("checkpoint_remote", "")),
            "status": "queued", "lease_id": lease["lease_id"],
            "created_at": time.time(), "evidence": None,
        }
        return {"task": self._task_view(self.tasks[task_id])}

    def handle_task_poll(self, payload: dict) -> dict:
        """Runner 拉取任务：本设备排队任务 + 无主排队任务（认领即发新
        租约；双 Runner 竞争时由 fencing 拒掉败者证据——at-least-once）。"""
        device_id = str(payload.get("device_id", ""))
        self._reschedule_stale()
        granted = []
        for task in self.tasks.values():
            if task.get("status") != "queued":
                continue
            if task.get("device_id") not in (None, device_id):
                continue
            if task.get("device_id") is None:
                lease = self._issue_lease(device_id, {}, 300)
                self.leases[lease["lease_id"]] = lease
                self.task_leases[task["task_id"]] = lease["lease_id"]
                task["lease_id"] = lease["lease_id"]
                task["device_id"] = device_id
                task["claimed_at"] = time.time()
            task["status"] = "leased"
            granted.append(self._task_view(task))
        cordon = self.cordoned.get(device_id, {})
        return {"tasks": granted, "server_time": time.time(),
                "cordoned": device_id in self.cordoned,
                "draining": bool(cordon.get("drain"))}

    def handle_task_status(self, payload: dict) -> dict:
        task_id = str(payload.get("task_id", ""))
        task = self.tasks.get(task_id)
        if task is None:
            return {"task": None, "error": "unknown task"}
        return {"task": self._task_view(task)}

    def handle_task_cancel(self, payload: dict) -> dict:
        """取消排队/已认领任务（运行中的任务 D4 不打断，完成后自然结束）。"""
        task_id = str(payload.get("task_id", ""))
        task = self.tasks.get(task_id)
        if task is None:
            return {"ok": False, "error": "unknown task"}
        if task.get("status") not in ("queued", "leased"):
            return {"ok": False, "error": f"task is {task.get('status')}"}
        task["status"] = "cancelled"
        task["cancelled_at"] = time.time()
        return {"ok": True, "task_id": task_id}

    def handle_task_migrate(self, payload: dict) -> dict:
        """任务迁移：重新排队到新设备并签发新租约（旧 fencing token
        立即失效——被迁移任务的陈旧执行结果无法提交）。"""
        task_id = str(payload.get("task_id", ""))
        new_device = str(payload.get("target_device", ""))
        task = self.tasks.get(task_id)
        if task is None:
            return {"ok": False, "error": "unknown task"}
        if task.get("mobility") == "pinned":
            return {"ok": False, "error": "pinned task cannot migrate"}
        if task.get("status") in ("done", "failed", "cancelled"):
            return {"ok": False, "error": f"task is {task.get('status')}"}
        if new_device not in self._approval_ids():
            return {"ok": False, "error": "target device not approved"}
        if new_device in self.cordoned:
            return {"ok": False, "error": "target device cordoned"}
        lease = self._issue_lease(new_device, {}, 300)
        self.leases[lease["lease_id"]] = lease
        self.task_leases[task_id] = lease["lease_id"]
        task.update({"device_id": new_device, "lease_id": lease["lease_id"],
                     "status": "queued", "migrated_at": time.time()})
        self._record_event(str(payload.get("device_id", "")), "migrate",
                           "task", task_id)
        return {"ok": True, "task_id": task_id, "lease": lease}

    def handle_halt(self, payload: dict) -> dict:
        """Kill Switch（AADM-G §1 可信内核）：取消全部排队/已认领任务，
        并 cordon 所有设备——异常时一键冻结，防止失控扩散。"""
        cancelled = 0
        for task in self.tasks.values():
            if task.get("status") in ("queued", "leased"):
                task["status"] = "cancelled"
                task["cancelled_at"] = time.time()
                task["cancelled_by"] = "halt"
                cancelled += 1
        now = time.time()
        for device_id in list(self.secrets):
            self.cordoned[device_id] = {"drain": True, "since": now,
                                        "reason": "halt"}
        self._record_event(str(payload.get("device_id", "")), "halt",
                           "all_devices")
        return {"ok": True, "cancelled_tasks": cancelled,
                "cordoned_devices": len(self.cordoned),
                "message": "Kill Switch 已触发：全部新任务冻结，运行中任务"
                           "完成后不再认领"}

    def handle_cordon(self, payload: dict) -> dict:
        """Cordon/Drain（§31）：不再接收新任务；drain 同时停止新任务认领。"""
        device_id = str(payload.get("device_id", ""))
        if device_id not in self.secrets:
            return {"ok": False, "error": "unknown device"}
        if payload.get("undo"):
            self.cordoned.pop(device_id, None)
        else:
            self.cordoned[device_id] = {"drain": bool(payload.get("drain")),
                                        "since": time.time()}
        return {"ok": True, "cordoned": device_id in self.cordoned,
                "drain": device_id in self.cordoned
                and self.cordoned[device_id].get("drain")}

    def status_view(self) -> list:
        """设备在线视图（供 GET /status；心跳超时标 stale）。"""
        now = time.time()
        view = []
        for device_id, device in sorted(self.devices.items()):
            last = device.get("last_seen", 0)
            online = last > 0 and (now - last) <= HEARTBEAT_STALE_SECONDS
            probe = device.get("probe", {}) or {}
            item = {
                "device_id": device_id, "name": device.get("name", ""),
                "online": online, "heartbeats": device.get("heartbeats", 0),
                "evidence_count": device.get("evidence_count", 0),
                "last_seen": last,
                "cordoned": device_id in self.cordoned,
                "draining": bool((self.cordoned.get(device_id) or {}).get("drain")),
                "static_capabilities": probe.get("static_capabilities", {}),
                "runtimes": probe.get("runtimes", {}),
                "capability_valid_until": probe.get("capability_valid_until", ""),
                "tenant": device.get("tenant", ""),
                "region": device.get("region", ""),
                "attestation": device.get("attestation", ""),
            }
            view.append(item)
        return view

def _read_lines_bounded(path: Path, max_bytes: int = 4 * 1024 * 1024) -> list:
    """有界行缓冲读取（实现见 pbatch/fabric_server_util.py；re-export）。"""
    from .fabric_server_util import read_lines_bounded
    return read_lines_bounded(path, max_bytes)

def start_server(port: int = 0, state_dir: str = "", auto_approve: bool = False,
                 host: str = "127.0.0.1", budget_cap: float = 0.0,
                 tenant_quotas: Optional[dict] = None,
                 device_quotas: Optional[dict] = None):
    """启动控制平面（实现见 pbatch/fabric_server_http.py；re-export 兼容）。"""
    from .fabric_server_http import start_server as _start
    return _start(port, state_dir, auto_approve, host, budget_cap,
                  tenant_quotas=tenant_quotas, device_quotas=device_quotas)
