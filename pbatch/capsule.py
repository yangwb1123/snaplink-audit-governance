"""Decision capsule (AADM-G §22): versioned, replayable decision context.

要能重放一次 Agent 决策，至少需要：prompt 哈希、世界版本、模型/规则集/
工具版本、上下文快照、证据与资源。封装为 DecisionCapsule——用于事故
分析、结果复现、规则回归、模型升级评估、责任追踪。

`pi-batch capsule --prompt "..." --out FILE` 抓取当前上下文；字段全部
确定性可比较（重放 = 同胶囊 + 同版本 → 同一（统计）行为）。

注意：LLM 非确定性意味着重放是统计性的（AADM-G §35.7），胶囊保证
"版本可复现"而非"逐 token 复现"。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
import uuid
from pathlib import Path
from typing import Optional

from . import config
from .fabric import FABRIC_VERSION
from .world import _module_versions, capture_world_snapshot

RULE_REGISTRIES = ("ui-specs/rules.yaml", "backend-specs/rules.yaml",
                   "pbatch/task_keywords.yaml", "pbatch/role_keywords.yaml")


def _file_digest(path: Path) -> str:
    try:
        data = path.read_bytes()
    except OSError:
        return "missing"
    return hashlib.sha256(data).hexdigest()[:12]


def capture_context_capsule(prompt: str, cwd: str = "",
                            rule_digests: Optional[dict] = None) -> dict:
    """当前上下文胶囊：所有参与决策的版本可见且可比较。"""
    root = Path(cwd or ".")
    world = capture_world_snapshot(cwd)
    rules = rule_digests or {
        rel: _file_digest(root / rel) for rel in RULE_REGISTRIES
    }
    return {
        "capsule_id": "cap-" + uuid.uuid4().hex[:12],
        "prompt_hash": hashlib.sha256(
            (prompt or "").encode("utf-8")).hexdigest()[:16],
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "world_version": world["version"],
        "repository_head": world["repository_head"],
        "model": config.AGENT_DEFAULT_MODEL or "default",
        "rule_set_version": hashlib.sha256(
            json.dumps(rules, sort_keys=True).encode()).hexdigest()[:12],
        "rule_digests": rules,
        "tool_versions": {
            "fabric": FABRIC_VERSION,
            "python": __import__("sys").version.split()[0],
            "fabric_modules_digest": _module_versions(cwd),
        },
        "context_snapshot_id": world["version"],
        "status": "captured",
    }


def _run_eval() -> tuple:
    """跑 eval 回归（有界子进程）：返回 (ok, 输出尾部)。"""
    import subprocess
    try:
        result = subprocess.run(
            [sys.executable, str(Path(__file__).resolve().parent.parent
                                 / "pi-batch.py"), "eval"],
            capture_output=True, text=True, timeout=300)
        return (result.returncode == 0,
                (result.stdout + result.stderr).strip()[-300:])
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, str(exc)


def replay_main(argv: Optional[list] = None) -> int:
    """`pi-batch replay --capsule FILE [--json]`：决策重放对比。

    读取决策胶囊（历史规则/工具版本）→ 对比当前版本 → 跑 eval 回归 →
    输出"版本差异 + 回归结果"（模型升级/规则变更评估的输入）。"""
    import subprocess
    parser = argparse.ArgumentParser(
        prog="pi-batch.py replay",
        description="Decision replay: capsule 版本对比 + eval 回归")
    parser.add_argument("--capsule", required=True, help="历史胶囊 JSON")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        capsule = json.loads(Path(args.capsule).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"capsule 无效: {exc}", file=sys.stderr)
        raise SystemExit(2)
    current = capture_context_capsule(capsule.get("prompt_hash", ""))
    differences = capsule_diff(capsule, current)
    eval_ok, eval_tail = _run_eval()
    report = {
        "capsule": capsule.get("capsule_id", "?"),
        "captured_rule_set": capsule.get("rule_set_version", "?"),
        "current_rule_set": current["rule_set_version"],
        "rule_set_changed": capsule.get("rule_set_version") != current["rule_set_version"],
        "model_changed": capsule.get("model") != current["model"],
        "differences": differences,
        "eval_passed": eval_ok,
        "eval_tail": eval_tail,
        "_note": "重放是统计性的（LLM 非确定性）：版本可复现 ≠ 逐 token 复现",
    }
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        raise SystemExit(0 if report["eval_passed"] else 1)
    else:
        print(f"capsule: {report['capsule']}")
        print(f"规则集: {report['captured_rule_set']} → "
              f"{report['current_rule_set']}"
              + ("（已变更）" if report["rule_set_changed"] else ""))
        print(f"模型: {capsule.get('model')} → {current['model']}"
              + ("（已变更）" if report["model_changed"] else ""))
        for diff in report["differences"]:
            print(f"  ⚠ {diff}")
        print(f"eval 回归: {'PASS' if report['eval_passed'] else 'FAIL'}")
    raise SystemExit(0 if report["eval_passed"] else 1)


def capsule_main(argv: Optional[list] = None) -> int:
    """`pi-batch capsule --prompt TEXT --out FILE [--json]`。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py capsule",
        description="Capture a replayable decision capsule (AADM-G §22)")
    parser.add_argument("--prompt", default="", help="任务 prompt 文本")
    parser.add_argument("--out", default="", help="写出 JSON 文件")
    parser.add_argument("--cwd", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    capsule = capture_context_capsule(args.prompt, args.cwd)
    if args.out:
        Path(args.out).write_text(json.dumps(capsule, ensure_ascii=False,
                                             indent=2), encoding="utf-8")
        print(f"capsule written: {capsule['capsule_id']} → {args.out}")
        raise SystemExit(0)
    print(json.dumps(capsule, ensure_ascii=False, indent=2))
    raise SystemExit(0)


def capsule_diff(a: dict, b: dict) -> list:
    """两个胶囊的可复现性差异（重放前提是否一致）。"""
    differences = []
    for key in ("prompt_hash", "world_version", "repository_head",
                "rule_set_version", "model"):
        if a.get(key) != b.get(key):
            differences.append(f"{key}: {a.get(key)} != {b.get(key)}")
    return differences
