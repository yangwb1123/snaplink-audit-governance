"""Control plane HTTP layer (split from fabric_server.py for line budget).

JSON RPC 处理器（除 /register 的 TOFU 首注册外全部验签）与服务器启动。
ControlPlane 状态与业务在 pbatch/fabric_server.py。
"""

from __future__ import annotations

import http.server
import json
from typing import Optional

from .fabric_rpc import BODY_MAX_BYTES, verify_request


class _Handler(http.server.BaseHTTPRequestHandler):
    """JSON RPC 处理器：验签（除 /register 的 TOFU 首注册）→ 路由。"""

    server: "http.server.ThreadingHTTPServer"

    def log_message(self, fmt, *args):  # 安静：运营日志由调用方负责
        pass

    def _reply(self, code: int, data: dict) -> None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):  # noqa: N802（http.server 协议方法名）
        try:
            length = int(self.headers.get("Content-Length", 0))
        except (TypeError, ValueError):
            self._reply(400, {"error": "bad content length"})
            return
        if length <= 0 or length > BODY_MAX_BYTES:
            self._reply(413, {"error": "body too large"})
            return
        raw = self.rfile.read(length)
        try:
            request = json.loads(raw)
        except ValueError:
            self._reply(400, {"error": "bad json"})
            return
        control = self.server.control
        payload = request.get("payload", {}) if isinstance(request, dict) else {}
        device_id = str(payload.get("device_id", ""))
        secret = control.secret_for(device_id)
        if self.path != "/register":
            if secret is None:
                self._reply(404, {"error": "unknown device"})
                return
            reason = verify_request(secret, "POST", self.path, request,
                                    replay_cache=control.replay)
            if reason is not None:
                self._reply(401, {"error": f"auth failed: {reason}"})
                return
        handler = {
            "/register": control.handle_register,
            "/heartbeat": control.handle_heartbeat,
            "/lease": control.handle_lease,
            "/evidence": control.handle_evidence,
            "/reconcile": control.handle_reconcile,
            "/task/submit": control.handle_task_submit,
            "/task/poll": control.handle_task_poll,
            "/task/status": control.handle_task_status,
            "/task/cancel": control.handle_task_cancel,
            "/task/migrate": control.handle_task_migrate,
            "/cordon": control.handle_cordon,
            "/halt": control.handle_halt,
            "/desired/reconcile": control.handle_desired_reconcile,
            "/revoke": control.handle_revoke,
            "/checkpoint/download": control.handle_checkpoint_download,
            "/checkpoint/upload": control.handle_checkpoint_upload,
        }.get(self.path)
        if handler is None:
            self._reply(404, {"error": "unknown path"})
            return
        self._reply(200, handler(payload))

    def do_GET(self):  # noqa: N802
        if self.path == "/status":
            self._reply(200, {"devices": self.server.control.status_view()})
        else:
            self._reply(404, {"error": "unknown path"})


def start_server(port: int = 0, state_dir: str = "",
                 auto_approve: bool = False, host: str = "127.0.0.1",
                 budget_cap: float = 0.0,
                 tenant_quotas: Optional[dict] = None,
                 device_quotas: Optional[dict] = None):
    """启动控制平面（测试/嵌入用）。返回 (httpd, actual_port)。"""
    from .fabric_server import ControlPlane, DEVICE_STATE_DIR
    control = ControlPlane(state_dir or str(DEVICE_STATE_DIR),
                           auto_approve=auto_approve, budget_cap=budget_cap,
                           tenant_quotas=tenant_quotas,
                           device_quotas=device_quotas)
    httpd = http.server.ThreadingHTTPServer((host, port), _Handler)
    httpd.control = control
    return httpd, httpd.server_address[1]
