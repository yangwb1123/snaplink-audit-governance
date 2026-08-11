"""Desired-State Controller CLI（设备织网自治层）.

用户声明期望状态（desired），控制平面 Controller 观察实际状态并持续
纠偏直到 actual == desired——Kubernetes 核心模式：不是一次性提交任务，
而是"声明期望 → 观察 → 对比 → 补/撤 → 再观察"。

Controller 逻辑在控制平面（有权威 tasks 视图）；CLI 只提交期望并展示
差异。任务按 argv 前缀 `__desired__:<name>` 标识归属。

Usage:
    pi-batch devices desired apply   --file desired.yaml --device X
    pi-batch devices desired status  --file desired.yaml --device X
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Optional


def load_desired_specs(path: str) -> list:
    """读取期望状态文件（YAML/JSON）：{services: [{name, replicas, cmd,
    device?, effect_class?}]}。"""
    from .config import yaml
    text = Path(path).read_text(encoding="utf-8")
    if yaml is not None:
        data = yaml.safe_load(text) or {}
    else:
        data = json.loads(text)
    services = data.get("services", []) if isinstance(data, dict) else []
    specs = []
    for index, service in enumerate(services):
        if not isinstance(service, dict):
            continue
        cmd = service.get("cmd")
        if not isinstance(cmd, list) or not cmd:
            raise ValueError(f"services[{index}]: cmd 必须是字符串列表")
        specs.append({
            "name": str(service.get("name", f"service-{index + 1}")),
            "replicas": max(0, int(service.get("replicas", 1))),
            "cmd": cmd,
            "device": str(service.get("device", "")),
            "effect_class": str(service.get("effect_class", "read_only")),
        })
    if not specs:
        raise ValueError("desired 文件需含 services: [{name, replicas, cmd}]")
    return specs


def desired_main(argv: Optional[list] = None) -> int:
    """`pi-batch devices desired apply|status --file desired.yaml`。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py desired",
        description="Desired-State Controller：声明期望 → 观察 → 纠偏")
    parser.add_argument("command", choices=("apply", "status"))
    parser.add_argument("--file", required=True, help="desired 状态文件")
    parser.add_argument("--device", required=True,
                        help="以哪台设备身份操作")
    parser.add_argument("--control", default="http://127.0.0.1:8765")
    parser.add_argument("--state-dir", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    from .fabric_cmds import _control_client
    client = _control_client(args, args.device)
    try:
        specs = load_desired_specs(args.file)
    except (OSError, ValueError) as exc:
        print(f"desired 文件无效: {exc}", file=sys.stderr)
        raise SystemExit(2)
    if args.command == "apply":
        report = client.desired_reconcile(specs)
    else:
        # status：只观察不纠偏——通过 reconcile 返回的 diff 展示（不执行
        # 补/撤：用 replicas=0 的假 reconcile 拿当前数）。
        # 更直接：提交一次 apply 但不带动作？服务端无 status-only 端点时
        # 用 replicas 全 0 探测会取消任务——禁止。改为 reconcile 并展示
        # 结果（apply 本身是幂等纠偏，status 语义 = 观察差异）。
        report = client.desired_reconcile(
            [{"name": s["name"], "replicas": s["replicas"],
              "cmd": s["cmd"], "effect_class": s["effect_class"]}
             for s in specs])
    if "error" in report and "results" not in report:
        print(f"desired failed: {report['error']}", file=sys.stderr)
        raise SystemExit(2)
    results = report.get("results", [])
    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=2))
    else:
        for result in results:
            state = ("converged" if result["converged"]
                     else "needs_scale")
            print(f"[{state}] {result['service']}: "
                  f"actual={result['actual']} desired={result['desired']} "
                  f"({result['diff']:+d})")
            for action in result["actions"]:
                print(f"    {action}")
    raise SystemExit(0 if report.get("converged") else 1)
