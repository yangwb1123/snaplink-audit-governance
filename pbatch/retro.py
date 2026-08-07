#!/usr/bin/env python3
"""运行回顾分析器（run retro）：从 pi-batch 运行日志中提取自我迭代信号。

扫描一个或多个日志文件（或目录），统计真实运行中暴露的模式：
  - 失败签名 / 验证失败 / 重试分布（哪些门禁反复拦截）
  - 任务耗时分布（识别慢任务/超时风险）
  - GATE 裁决结果（PASS/REJECTED/FAILED/无 VERDICT）
  - 锁等待与排队事件、环境熔断、circuit 隔离
  - 产物落盘/复用/归档事件

用法:
  python scripts/run-retro.py [--json] LOG_FILE_OR_DIR...
  python scripts/run-retro.py logs/                          # 主仓库全部
  python scripts/run-retro.py --json logs/ ~/aero-vault/logs/ # 跨仓库聚合

输出: 计数摘要 + 按模式分组的证据行（前 N 条），供人工提炼教训。
纯标准库，零依赖。
"""

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

# (name, regex) —— 提取的日志事件类别
PATTERNS = [
    ("validation_failed", re.compile(r"VALIDATION FAILED", re.I)),
    ("validation_warn", re.compile(r"VALIDATE WARN", re.I)),
    ("retry", re.compile(r"RETRY \d+/\d+", re.I)),
    ("agent_rejected", re.compile(r"agent output REJECTED", re.I)),
    ("task_failed", re.compile(r"FAIL\s+\[", re.I)),
    ("task_ok", re.compile(r"OK\s+done\s+\[(\d+(?:\.\d+)?)s\]", re.I)),
    ("gate_passed", re.compile(r"GATE PASSED", re.I)),
    ("gate_rejected", re.compile(r"GATE REJECTED", re.I)),
    ("gate_no_verdict", re.compile(r"produced no VERDICT", re.I)),
    ("stage_failed", re.compile(r"Stage '\S+' completed: (\d+)/(\d+) tasks succeeded", re.I)),
    ("stage_halted", re.compile(r"Pipeline halted", re.I)),
    ("lock_wait", re.compile(r"LOCK: waiting for holder", re.I)),
    ("lock_refused", re.compile(r"Another pi-batch instance is running", re.I)),
    ("lock_stale", re.compile(r"Breaking stale lock", re.I)),
    ("environment_stall", re.compile(r"ENVIRONMENT:", re.I)),
    ("circuit_isolate", re.compile(r"circuit|isolated task", re.I)),
    ("reuse", re.compile(r"REUSED|REUSE:", re.I)),
    ("revalidate_fail", re.compile(r"REVALIDATION FAILED", re.I)),
    ("archive", re.compile(r"ARCHIVED", re.I)),
    ("timeout", re.compile(r"TIMEOUT\s+\[", re.I)),
    ("refusal", re.compile(r"REFUSAL-CHECK", re.I)),
    ("decision_gate", re.compile(r"DECISION-GATE: (PASS|FAIL)", re.I)),
    ("manifest_gate", re.compile(r"MANIFEST-GATE: (PASS|FAIL)", re.I)),
]

DURATION_RE = re.compile(r"OK\s+done\s+\[(\d+(?:\.\d+)?)s\]")
FAIL_REASON_RE = re.compile(r"FAIL\s+\[[^\]]*\]\s*\[val exit=(\d+)\]|FAIL\s+\[code=(\d+)\]")


def scan_file(path: Path, counts: Counter, evidence: dict, durations: list, max_evidence: int = 6):
    """Scan one log file, updating counters/evidence/durations in place."""
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return
    matched = 0
    for lineno, line in enumerate(lines):
        for name, pattern in PATTERNS:
            m = pattern.search(line)
            if not m:
                continue
            if name == "stage_failed":
                if int(m.group(1)) >= int(m.group(2)):
                    break  # stage fully succeeded; not a failure
            if name == "timeout" and len(evidence[name]) < max_evidence:
                # context: the 3 lines around a hard timeout locate the hotspot
                ctx = lines[max(0, lineno - 3):lineno + 4]
                evidence[name].append(f"{path.name}:{line.strip()[:160]} | context: "
                                      + " || ".join(c.strip()[:100] for c in ctx[1:]))
                matched += 1
                break
            counts[name] += 1
            if len(evidence[name]) < max_evidence and matched < 120:
                evidence[name].append(f"{path.name}:{line.strip()[:200]}")
                matched += 1
            break  # one category per line
        m = DURATION_RE.search(line)
        if m:
            durations.append(float(m.group(1)))


def scan_target(target: str) -> tuple:
    counts: Counter = Counter()
    evidence: dict = defaultdict(list)
    durations: list = []
    path = Path(target)
    files = sorted(path.rglob("*.log")) if path.is_dir() else [path]
    for f in files:
        scan_file(f, counts, evidence, durations)
    return counts, evidence, durations


def fmt_durations(durations: list) -> dict:
    if not durations:
        return {"count": 0}
    durations.sort()
    n = len(durations)
    return {
        "count": n,
        "p50": durations[n // 2],
        "p90": durations[int(n * 0.9)],
        "max": durations[-1],
        "total_minutes": round(sum(durations) / 60, 1),
    }


FAILURE_CATEGORIES = {"agent_rejected", "task_failed", "validation_failed",
                       "stage_failed", "stage_halted", "gate_rejected",
                       "gate_no_verdict", "timeout", "lock_refused",
                       "circuit_isolate", "refusal", "revalidate_fail",
                       "environment_stall"}


def _filter_failures(counts: Counter, evidence: dict, durations: list):
    """Keep only failure categories (--failures-only mode)."""
    for name in list(counts):
        if name not in FAILURE_CATEGORIES:
            del counts[name]
    for name in list(evidence):
        if name not in FAILURE_CATEGORIES:
            del evidence[name]
    return counts, evidence, []


def retro_main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pi-batch.py retro", description=__doc__)
    parser.add_argument("targets", nargs="+", help="log files or directories")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--top-evidence", type=int, default=6,
                        help="max evidence lines per category (default: 6)")
    parser.add_argument("--failures-only", action="store_true",
                        help="only failure categories (feed to pi-batch learn)")
    args = parser.parse_args(argv)

    all_counts: Counter = Counter()
    all_evidence: dict = defaultdict(list)
    all_durations: list = []
    for target in args.targets:
        counts, evidence, durations = scan_target(target)
        all_counts.update(counts)
        for k, v in evidence.items():
            all_evidence[k].extend(v)
        all_durations.extend(durations)

    if args.failures_only:
        all_counts, all_evidence, all_durations = _filter_failures(
            all_counts, all_evidence, all_durations)
    report = {
        "files": len([p for t in args.targets for p in
                      (Path(t).rglob("*.log") if Path(t).is_dir() else [Path(t)])]),
        "counts": dict(all_counts),
        "durations": fmt_durations(all_durations),
        "evidence": {k: v[:args.top_evidence] for k, v in all_evidence.items()},
    }
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        return 0
    print(f"== run retro: {report['files']} file(s) ==")
    for name, count in sorted(all_counts.items(), key=lambda kv: -kv[1]):
        if count:
            print(f"  {name:<18} {count}")
    print(f"  {'task_durations':<18} {report['durations']}")
    print("\n== evidence (first lines per category) ==")
    for name, lines in sorted(all_evidence.items()):
        if not lines:
            continue
        print(f"\n-- {name} ({all_counts[name]}) --")
        for line in lines[:args.top_evidence]:
            print(f"   {line}")
    return 0


if __name__ == "__main__":
    sys.exit(retro_main())
