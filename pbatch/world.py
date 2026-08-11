"""World state plane (AADM-G §2): versioned snapshot + drift detection.

任务图表达"准备做什么"，世界状态表达"现在真实是什么"——两者必须分开。
快照：git HEAD + 工作区变更数 + 服务/协议版本 + 事实 + 单调版本号。
计划绑定一个世界版本；漂移超过阈值 → 不能机械执行旧计划，必须重新
验证前提（AADM-D §16 外部化状态的本地平面）。

纯 stdlib（git 经子进程只读调用）；失败 fail closed（非 git 仓库 →
head=unknown，漂移恒真）。

Usage:
    pi-batch world bind --out snapshot.json        # 绑定当前世界版本
    pi-batch world drift --bound snapshot.json     # 检查漂移
    pi-batch world drift --bound snapshot.json --threshold 3
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

FABRIC_MODULES = ("pbatch/fabric.py", "pbatch/fabric_exec.py",
                  "pbatch/fabric_server.py", "pbatch/fabric_runner.py")


def _git(cwd: str, args: list) -> str:
    """只读 git 调用；非仓库或失败 → 空串（fail closed）。"""
    try:
        result = subprocess.run(["git", *args], cwd=cwd, capture_output=True,
                                text=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return result.stdout.strip() if result.returncode == 0 else ""


def capture_world_snapshot(cwd: str = "") -> dict:
    """当前世界快照：仓库版本 + 工作区变更 + fabric 协议版本 + 事实。"""
    root = cwd or "."
    head = _git(root, ["rev-parse", "HEAD"])
    dirty = _git(root, ["status", "--porcelain"])
    dirty_count = len([line for line in dirty.splitlines() if line.strip()])
    facts = {
        "repository_head": head or "unknown",
        "dirty_files": dirty_count,
        "fabric_protocol": _module_versions(root),
        "as_of": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    fingerprint = hashlib.sha256(
        json.dumps(facts, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return {"version": fingerprint, **facts}


def _module_versions(root: str) -> str:
    """fabric 模块文件 mtime 摘要（版本变化 → 能力语义变化）。"""
    digest = hashlib.sha256()
    for rel in FABRIC_MODULES:
        path = Path(root) / rel
        try:
            digest.update(rel.encode())
            digest.update(str(path.stat().st_mtime_ns).encode())
        except OSError:
            digest.update(b"missing")
    return digest.hexdigest()[:12]


def plan_bind(snapshot: dict) -> dict:
    """把计划绑定到一个世界版本（执行前记录；version 保留原键供漂移对比）。"""
    return {"version": snapshot.get("version", ""),
            "plan_bound_world_version": snapshot.get("version", ""),
            "bound_at": snapshot.get("as_of", ""),
            "bound_repository_head": snapshot.get("repository_head", ""),
            "repository_head": snapshot.get("repository_head", ""),
            "dirty_files": snapshot.get("dirty_files", 0)}


def drift_between(bound: dict, current: dict,
                  threshold: int = 0) -> dict:
    """漂移检查：版本/HEAD/工作区变更数对比。超过阈值 → 需重新验证前提。"""
    head_changed = (bound.get("repository_head")
                    != current.get("repository_head"))
    dirty_delta = (int(current.get("dirty_files", 0))
                   - int(bound.get("dirty_files", 0)))
    version_changed = (bound.get("version") != current.get("version"))
    exceeds = version_changed and (
        head_changed or abs(dirty_delta) > max(0, threshold))
    return {
        "version_changed": version_changed,
        "head_changed": head_changed,
        "dirty_delta": dirty_delta,
        "threshold": threshold,
        "exceeds_threshold": exceeds,
        "verdict": "replan_required" if exceeds else "proceed",
    }


def world_main(argv: Optional[list] = None) -> int:
    """`pi-batch world bind|drift`：世界状态版本化 + 漂移检查。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py world",
        description="World state plane: versioned snapshot + drift")
    sub = parser.add_subparsers(dest="command", required=True)

    p_bind = sub.add_parser("bind", help="绑定当前世界版本到文件")
    p_bind.add_argument("--out", required=True)
    p_bind.add_argument("--cwd", default="")

    p_drift = sub.add_parser("drift", help="检查当前与绑定版本的漂移")
    p_drift.add_argument("--bound", required=True, help="bind 输出的文件")
    p_drift.add_argument("--threshold", type=int, default=0)
    p_drift.add_argument("--cwd", default="")
    p_drift.add_argument("--json", action="store_true")

    args = parser.parse_args(argv)
    if args.command == "bind":
        snapshot = capture_world_snapshot(args.cwd)
        record = {"snapshot": snapshot, "plan": plan_bind(snapshot)}
        Path(args.out).write_text(json.dumps(record, indent=2),
                                  encoding="utf-8")
        print(f"bound world version {snapshot['version']} → {args.out}")
    else:
        try:
            bound = json.loads(Path(args.bound).read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            print(f"cannot read bound snapshot: {exc}", file=sys.stderr)
            raise SystemExit(2)
        current = capture_world_snapshot(args.cwd)
        drift = drift_between(bound.get("snapshot", {}), current,
                              threshold=args.threshold)
        if args.json:
            print(json.dumps({"bound": bound.get("snapshot", {}),
                              "current": current, "drift": drift}, indent=2))
        else:
            verdict = drift["verdict"]
            print(f"world drift: {verdict}"
                  f"（head_changed={drift['head_changed']} "
                  f"dirty_delta={drift['dirty_delta']} "
                  f"threshold={args.threshold}）")
            if verdict == "replan_required":
                print("⚠ 世界状态已变化：不能机械执行旧计划，必须重新验证前提")
        raise SystemExit(1 if drift["exceeds_threshold"] else 0)
    raise SystemExit(0)
