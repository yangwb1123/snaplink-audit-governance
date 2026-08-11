"""Device-Aware Execution Fabric — D3: control-plane RPC 通道.

设备 Runner 与控制平面的请求/响应通道：注册、心跳、租约、证据、对账。
纯 stdlib（http.client + hmac），签名请求 = HMAC-SHA256 over
`method|path|ts|nonce` + payload 序列化字节；服务端校验时间窗（±300s）
与 nonce 防重放。

安全说明（AADM-D §20/§21）：
- /register 是 TOFU（trust-on-first-use）：设备自生成 secret 并首次
  声称身份；此后所有请求必须携带正确签名（fail closed）
- 通道加密（TLS/mTLS）不在 D3a 范围：默认绑定 127.0.0.1，部署到局域网
  时需在反向代理层终止 TLS，或等待 D3b 证书体系
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from typing import Optional
from urllib import error as urlerror
from urllib import request as urlrequest

# 请求时间窗（秒）与 nonce 防重放缓存上限（有界，超限清空）。
SIGN_TIMESTAMP_WINDOW = 300
NONCE_CACHE_MAX = 10000
BODY_MAX_BYTES = 1024 * 1024


def _sign(secret: str, method: str, path: str, body: bytes,
          ts: int, nonce: str) -> str:
    """签名输入 = `method|path|ts|nonce` 字节 + payload 原始字节。"""
    message = f"{method}|{path}|{ts}|{nonce}".encode("utf-8") + body
    return hmac.new(secret.encode("utf-8"), message,
                    hashlib.sha256).hexdigest()


def serialize_payload(payload: dict) -> bytes:
    """payload 的确定性序列化（客户端签名 / 服务端验签共用）。"""
    return json.dumps(payload, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":")).encode("utf-8")


def build_request(secret: str, method: str, path: str,
                  payload: dict) -> dict:
    """构造一个签名请求体：{ts, nonce, sig, payload}。"""
    ts = int(time.time())
    nonce = uuid.uuid4().hex
    body = serialize_payload(payload)
    return {"ts": ts, "nonce": nonce,
            "sig": _sign(secret, method, path, body, ts, nonce),
            "payload": payload}


def verify_request(secret: str, method: str, path: str, request: dict,
                   replay_cache: Optional[dict] = None,
                   now: Optional[int] = None) -> Optional[str]:
    """校验签名请求。返回 None = 通过；否则返回拒绝原因（fail closed）。"""
    ts = request.get("ts")
    nonce = request.get("nonce")
    sig = request.get("sig")
    payload = request.get("payload")
    if (isinstance(ts, bool) or not isinstance(ts, int)
            or not isinstance(nonce, str) or not isinstance(sig, str)
            or not isinstance(payload, dict)):
        return "malformed request"
    current = int(time.time()) if now is None else now
    if abs(current - ts) > SIGN_TIMESTAMP_WINDOW:
        return "stale timestamp"
    if replay_cache is not None:
        key = f"{nonce}"
        if key in replay_cache:
            return "replay"
        if len(replay_cache) >= NONCE_CACHE_MAX:
            replay_cache.clear()  # 有界：超限清空（等价于冷启动）
        replay_cache[key] = ts
    expected = _sign(secret, method, path, serialize_payload(payload),
                     ts, nonce)
    if not hmac.compare_digest(expected, sig):
        return "bad signature"
    return None


def rpc_post(url: str, secret: str, method: str, path: str,
             payload: dict, timeout: float = 5.0) -> tuple:
    """发一次签名请求。返回 (status_code, response_dict)；网络错误返回
    (0, {"error": ...})，不抛异常（故障显式）。"""
    request = build_request(secret, method, path, payload)
    data = json.dumps(request).encode("utf-8")
    req = urlrequest.Request(url + path, data=data,
                             headers={"Content-Type": "application/json"},
                             method=method)
    try:
        with urlrequest.urlopen(req, timeout=timeout) as resp:
            body = resp.read(BODY_MAX_BYTES + 1)
            if len(body) > BODY_MAX_BYTES:
                return (0, {"error": "response too large"})
            return (resp.status, json.loads(body or b"{}"))
    except urlerror.HTTPError as exc:
        try:
            detail = json.loads(exc.read(BODY_MAX_BYTES) or b"{}")
        except (ValueError, OSError):
            detail = {"error": f"http {exc.code}"}
        return (exc.code, detail)
    except (urlerror.URLError, OSError, ValueError) as exc:
        return (0, {"error": str(exc)})


class RpcClient:
    """设备 Runner 侧客户端：注册 / 心跳 / 租约 / 证据 / 对账。"""

    def __init__(self, base_url: str, secret: str, device_id: str):
        self.base_url = base_url.rstrip("/")
        self.secret = secret
        self.device_id = device_id

    def _post(self, path: str, payload: dict, timeout: float = 5.0) -> tuple:
        return rpc_post(self.base_url, self.secret, "POST", path,
                        payload, timeout=timeout)

    def register(self, name: str, probe: Optional[dict] = None,
                 tenant: str = "", region: str = "",
                 attestation: str = "") -> dict:
        """TOFU 注册；probe 为能力快照，tenant/region/attestation 为联邦
        元数据（D6 最小件：多租户配额/跨区域/注册证明）。"""
        code, data = self._post("/register", {
            "device_id": self.device_id, "name": name,
            "secret": self.secret, "probe": probe,
            "tenant": tenant, "region": region, "attestation": attestation,
        })
        return data if code == 200 else {"approved": False, "error": data}

    def heartbeat(self, dynamic_state: dict, running_attempts: list) -> dict:
        code, data = self._post("/heartbeat", {
            "device_id": self.device_id, "dynamic_state": dynamic_state,
            "running_attempts": running_attempts,
        })
        return data if code == 200 else {"ok": False, "error": data}

    def lease(self, resources: dict, ttl_seconds: int = 300) -> dict:
        code, data = self._post("/lease", {
            "device_id": self.device_id, "resources": resources,
            "ttl_seconds": ttl_seconds,
        })
        return data if code == 200 else {"lease": None, "error": data}

    def evidence(self, attempt_id: str, lease_id: str,
                 fencing_token: int, evidence: dict,
                 task_id: str = "") -> dict:
        code, data = self._post("/evidence", {
            "device_id": self.device_id, "attempt_id": attempt_id,
            "lease_id": lease_id, "fencing_token": fencing_token,
            "evidence": evidence, "task_id": task_id,
        })
        return data if code == 200 else {"accepted": False, "error": data}

    def reconcile(self, running_attempts: list) -> dict:
        code, data = self._post("/reconcile", {
            "device_id": self.device_id, "running_attempts": running_attempts,
        })
        return data if code == 200 else {"error": data}

    # -- D4: 任务调度与设备管理 --------------------------------------------

    def submit_task(self, argv: list, cwd: str = "", timeout: float = 60.0,
                    effect_class: str = "read_only",
                    mobility: str = "stateless",
                    ttl_seconds: int = 300,
                    checkpoint_blob: str = "",
                    checkpoint_remote: str = "",
                    estimated_cost: float = 0.0) -> dict:
        code, data = self._post("/task/submit", {
            "device_id": self.device_id, "argv": argv, "cwd": cwd,
            "timeout": timeout, "effect_class": effect_class,
            "mobility": mobility, "ttl_seconds": ttl_seconds,
            "checkpoint_blob": checkpoint_blob,
            "checkpoint_remote": checkpoint_remote,
            "estimated_cost": estimated_cost,
        })
        return data if code == 200 else {"task": None, "error": data}

    def desired_reconcile(self, services: list) -> dict:
        """Desired-State Controller：提交期望状态，服务端观察并纠偏。"""
        code, data = self._post("/desired/reconcile", {
            "device_id": self.device_id, "services": services,
        })
        return data if code == 200 else {"results": [], "error": data}

    def revoke(self, reason: str = "") -> dict:
        """设备退役：吊销本设备密钥（永久失效，需重新注册+审批）。"""
        code, data = self._post("/revoke", {
            "device_id": self.device_id, "reason": reason,
        })
        return data if code == 200 else {"ok": False, "error": data}

    def checkpoint_download(self, task_id: str) -> dict:
        code, data = self._post("/checkpoint/download", {
            "device_id": self.device_id, "task_id": task_id,
        })
        return data if code == 200 else {"has_checkpoint": False,
                                         "error": data}

    def checkpoint_upload(self, task_id: str, blob_b64: str) -> dict:
        code, data = self._post("/checkpoint/upload", {
            "device_id": self.device_id, "task_id": task_id,
            "blob_b64": blob_b64,
        })
        return data if code == 200 else {"ok": False, "error": data}

    def poll_tasks(self) -> dict:
        code, data = self._post("/task/poll", {"device_id": self.device_id})
        return data if code == 200 else {"tasks": [], "error": data}

    def task_status(self, task_id: str) -> dict:
        code, data = self._post("/task/status", {
            "device_id": self.device_id, "task_id": task_id,
        })
        return data if code == 200 else {"task": None, "error": data}

    def task_cancel(self, task_id: str) -> dict:
        code, data = self._post("/task/cancel", {
            "device_id": self.device_id, "task_id": task_id,
        })
        return data if code == 200 else {"ok": False, "error": data}

    def task_migrate(self, task_id: str, new_device: str) -> dict:
        code, data = self._post("/task/migrate", {
            "device_id": self.device_id, "task_id": task_id,
            "target_device": new_device,
        })
        return data if code == 200 else {"ok": False, "error": data}

    def halt(self) -> dict:
        """Kill Switch：取消全部排队任务 + 全设备 cordon（异常冻结）。"""
        code, data = self._post("/halt", {"device_id": self.device_id})
        return data if code == 200 else {"ok": False, "error": data}

    def cordon(self, drain: bool = False, undo: bool = False) -> dict:
        code, data = self._post("/cordon", {
            "device_id": self.device_id, "drain": drain, "undo": undo,
        })
        return data if code == 200 else {"ok": False, "error": data}
