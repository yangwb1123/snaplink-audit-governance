"""Control plane utilities (split from fabric_server.py for line budget)."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from pathlib import Path

# 检查点上限（base64 256KB ≈ 192KB 原始状态；断点续传的小状态文件）。
CHECKPOINT_MAX_B64 = 256 * 1024


def read_lines_bounded(path: Path, max_bytes: int = 4 * 1024 * 1024) -> list:
    """有界行缓冲读取（超长行跳过，总量超限截断）。"""
    lines = []
    try:
        handle = path.open("r", encoding="utf-8")
    except OSError:
        return lines
    with handle:
        total = 0
        for line in handle:
            total += len(line.encode("utf-8", errors="replace"))
            if total > max_bytes:
                break
            if len(line) > 256 * 1024:
                continue
            lines.append(line)
    return lines


def sha256(text: str) -> str:
    """短摘要（16 字符，检查点/指纹用）。"""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


class CheckpointOpsMixin:
    """检查点与设备吊销处理器（从 ControlPlane 拆出以控制行数）。

    依赖 self.checkpoints / self.state_dir / self.secrets / self.tasks /
    self.cordoned / self.revoked / self.revoked.add。
    """

    def _tenant_of(self, device_id: str) -> str:
        device = self.devices.get(device_id, {})
        return str(device.get("tenant", "default"))

    def _tenant_quota_check(self, device_id: str) -> str:
        """多租户配额（D6）：进行中任务数超配额 → 拒绝新提交。"""
        tenant = self._tenant_of(device_id)
        quota = self.tenant_quotas.get(tenant)
        if quota is None:
            return ""
        running = self.tenant_running.get(tenant, 0)
        if running >= quota:
            return (f"tenant quota exhausted ({tenant}: {running}/{quota} "
                    "进行中)")
        return ""

    def _tenant_quota_inc(self, device_id: str) -> None:
        tenant = self._tenant_of(device_id)
        if tenant in self.tenant_quotas:
            self.tenant_running[tenant] = self.tenant_running.get(tenant, 0) + 1

    def _tenant_quota_dec(self, device_id: str) -> None:
        tenant = self._tenant_of(device_id)
        if tenant in self.tenant_quotas:
            self.tenant_running[tenant] = max(
                0, self.tenant_running.get(tenant, 0) - 1)


    def _device_quota_key(self, device_id: str) -> str:
        """配额键：优先 device_id（rn-xxx），其次注册名（node-xxx）。"""
        if device_id in self.device_quotas:
            return device_id
        name = self.devices.get(device_id, {}).get("name", "")
        return name if name in self.device_quotas else ""

    def _device_quota_check(self, device_id: str) -> str:
        """per-device 并发上限（D6+ 差值表：准入控制 per-device）。"""
        key = self._device_quota_key(device_id)
        if not key:
            return ""
        quota = self.device_quotas[key]
        running = self.device_running.get(key, 0)
        if running >= quota:
            return (f"device quota exhausted ({key}: "
                    f"{running}/{quota} 进行中)")
        return ""

    def _device_quota_inc(self, device_id: str) -> None:
        key = self._device_quota_key(device_id)
        if key:
            self.device_running[key] = self.device_running.get(key, 0) + 1

    def _device_quota_dec(self, device_id: str) -> None:
        key = self._device_quota_key(device_id)
        if key:
            self.device_running[key] = max(0, self.device_running.get(key, 0) - 1)

    def _record_event(self, actor: str, verb: str, target: str = "",
                      object_id: str = "", attempt: str = "") -> None:
        """追加式交互事件（AADM §4.1）：不可变、可回答谁/何时/何事。

        attempt 绑定租约编号（审计 device/attempt 维度）。"""
        record = {"at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                  "actor": actor, "verb": verb, "target": target,
                  "object_id": object_id, "source": "control_plane"}
        if attempt:
            record["attempt"] = attempt
        self.events.append(record)
        if len(self.events) > 2000:
            self.events = self.events[-1000:]  # 有界内存
        try:
            with (self.state_dir / "events.jsonl").open(
                    "a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        except OSError:
        # best-effort I/O：失败不阻塞主流程（已验证有意）
            pass

    def _issue_lease(self, device_id: str, resources: dict,
                     ttl_seconds: int = 300) -> dict:
        """发放租约：epoch 单调递增，fencing_token = epoch。"""
        ttl = max(30, min(int(ttl_seconds or 300), 86400))
        epoch = self.epochs.get(device_id, 0) + 1
        self.epochs[device_id] = epoch
        now = time.time()
        return {
            "lease_id": uuid.uuid4().hex[:16],
            "device_id": device_id,
            "epoch": epoch,
            "fencing_token": epoch,
            "resources": resources,
            "expires_at": now + ttl,
            # D3 最小 Capability Grant：随租约签发，短期、可撤销（换租约即失效）
            "grant": {"grant_id": uuid.uuid4().hex[:16],
                      "actions": ["execute"],
                      "max_effect_class": "sandboxed",
                      "expires_at": now + ttl},
        }

    def handle_desired_reconcile(self, payload: dict) -> dict:
        """Desired-State Controller：观察实际任务数 → 对比期望 → 补/撤
        直到 actual == desired（Kubernetes 核心模式的设备织网版）。"""
        services = payload.get("services", [])
        results = []
        for service in services if isinstance(services, list) else []:
            name = str(service.get("name", ""))
            replicas = max(0, int(service.get("replicas", 1) or 1))
            if not name:
                continue
            marker = f"__desired__:{name}"
            running = [task for task in self.tasks.values()
                       if task.get("status") in ("queued", "leased",
                                                 "running")
                       and any(marker in item
                               for item in task.get("argv", []))]
            diff = replicas - len(running)
            actions = []
            actual = len(running)
            if diff > 0:
                for _ in range(diff):
                    argv = [marker] + list(service.get("cmd", []))
                    task = self.handle_task_submit({
                        "device_id": payload.get("device_id", ""),
                        "argv": argv,
                        "effect_class": str(service.get(
                            "effect_class", "read_only")),
                        "timeout": 600,
                    }).get("task")
                    if task:
                        actions.append(f"scale_up: {task['task_id']}")
                        actual += 1
                    else:
                        actions.append("scale_up_failed: quota/budget")
                        break
            elif diff < 0:
                for task in running[:abs(diff)]:
                    self.handle_task_cancel({"task_id": task["task_id"]})
                    actions.append(f"scale_down: {task['task_id']}")
                actual = replicas
            results.append({"service": name, "desired": replicas,
                            "actual": actual, "diff": diff,
                            "converged": actual == replicas,
                            "actions": actions})
        self._record_event(str(payload.get("device_id", "")),
                           "desired_reconcile", "services",
                           str(len(results)))
        return {"results": results, "converged": all(
            r["converged"] for r in results)}

    def handle_checkpoint_download(self, payload: dict) -> dict:
        """Runner 取任务检查点（断点续传：迁移后从上次状态继续）。"""
        task_id = str(payload.get("task_id", ""))
        checkpoint = self.checkpoints.get(task_id)
        if checkpoint is None:
            return {"has_checkpoint": False}
        return {"has_checkpoint": True,
                "blob_b64": checkpoint["blob_b64"],
                "digest": checkpoint["digest"],
                "size": checkpoint["size"]}

    def handle_checkpoint_upload(self, payload: dict) -> dict:
        """Runner 执行后回传检查点（有界、幂等覆盖）。"""
        task_id = str(payload.get("task_id", ""))
        blob = payload.get("blob_b64", "")
        if not isinstance(blob, str) or not blob:
            return {"ok": False, "error": "missing blob"}
        if len(blob) > CHECKPOINT_MAX_B64:
            return {"ok": False, "error": "checkpoint too large"}
        self.checkpoints[task_id] = {
            "blob_b64": blob, "size": len(blob),
            "digest": sha256(blob), "updated_at": time.time(),
        }
        return {"ok": True, "digest": self.checkpoints[task_id]["digest"]}

    def _revocation_ids(self) -> list:
        """吊销记录（只追加；退役设备身份永久失效）。"""
        revoked = []
        path = self.state_dir / "revocations.jsonl"
        if not path.exists():
            return revoked
        for line in read_lines_bounded(path):
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if isinstance(record, dict) and record.get("device_id"):
                revoked.append(record["device_id"])
        return revoked

    def handle_revoke(self, payload: dict) -> dict:
        """设备退役（§41.2）：吊销密钥、取消其任务、cordons 清理。"""
        device_id = str(payload.get("device_id", ""))
        if device_id not in self.secrets:
            return {"ok": False, "error": "unknown device"}
        record = {"device_id": device_id, "revoked_at": time.time(),
                  "reason": str(payload.get("reason", ""))[:200]}
        with (self.state_dir / "revocations.jsonl").open(
                "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        self.revoked.add(device_id)
        self.secrets.pop(device_id, None)
        self.cordoned.pop(device_id, None)
        cancelled = 0
        for task in self.tasks.values():
            if task.get("device_id") == device_id and task.get("status") in (
                    "queued", "leased", "running"):
                task["status"] = "lost"
                task["lost_at"] = time.time()
                cancelled += 1
        self._record_event(device_id, "revoke", "device")
        return {"ok": True, "device_id": device_id,
                "tasks_marked_lost": cancelled,
                "message": "密钥已吊销：身份永久失效，需重新注册+审批"}
