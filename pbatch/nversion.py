"""N-version execution (AADM-G §35.7): non-determinism control.

同一命令运行 N 次，比较退出码与输出摘要：一致 → 结果可信；发散 → 显式
报告分歧（关键节点不得把单次输出当证据）。这是对 LLM/环境非确定性的
N-version 对冲（确定性命令自然收敛，随机命令显式暴露）。

Usage:
    pi-batch nversion --n 3 -- python3 -c "print('hello')"
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Optional

from .fabric import ExecutionSpec, LocalExecutionTarget


def run_n_versions(argv: list, n: int = 3, cwd: str = "",
                   timeout: float = 60.0) -> dict:
    """N 次有界执行 + 一致性判定。返回 {runs, consensus, identical_runs}。"""
    target = LocalExecutionTarget()
    runs = []
    for index in range(max(1, n)):
        spec = ExecutionSpec(argv=argv, cwd=cwd or ".", timeout=timeout,
                             label=f"nversion-{index + 1}",
                             effect_class="pure")
        evidence = target.execute(spec)
        runs.append({
            "run": index + 1,
            "result": evidence.result,
            "exit_code": evidence.exit_code,
            "output_digest": evidence.output_digest,
            "elapsed": round(evidence.elapsed, 3),
        })
    signatures = {(r["result"], r["exit_code"], r["output_digest"])
                  for r in runs}
    consensus = "identical" if len(signatures) <= 1 else "divergent"
    return {"runs": runs, "consensus": consensus,
            "identical_runs": len(runs) - len(signatures) + 1}


def nversion_main(argv: Optional[list] = None) -> int:
    """`pi-batch nversion --n 3 [--json] -- CMD ARG...`。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py nversion",
        description="N-version execution with consensus check")
    parser.add_argument("--n", type=int, default=3)
    parser.add_argument("--cwd", default="")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("cmd", nargs="*", default=[])
    args = parser.parse_args(argv)
    if args.n < 1 or args.n > 10:
        parser.error("--n must be in [1, 10]")
    if not args.cmd:
        parser.error("usage: nversion [opts] -- CMD ARG...（必须用 -- 分隔）")
    report = run_n_versions(args.cmd, n=args.n, cwd=args.cwd,
                            timeout=args.timeout)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        lines = [f"# N-version ({len(report['runs'])} runs)"]
        for run in report["runs"]:
            lines.append(f"  run {run['run']}: {run['result']} "
                         f"(exit {run['exit_code']}) {run['output_digest'][:12]}")
        lines.append(f"consensus: {report['consensus']} "
                     f"({report['identical_runs']}/{len(report['runs'])} 一致)")
        print("\n".join(lines))
    raise SystemExit(0 if report["consensus"] == "identical" else 1)
