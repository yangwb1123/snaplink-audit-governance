"""`pi-batch health` — 六层维护可观测性报告（运营智能落地）。

对项目做一次"系统健康体检"（纯 stdlib、只读、无副作用）：

- 测试健康：测试数（pytest 收集计数，不执行）
- 门禁健康：检查器自扫描（designintelligence/knowledge/backend-quality）
- 规范资产：ui-specs/backend-specs 文件数、规则数
- 知识维护：模块文档覆盖（knowledge 检查器）
- 技术债：quality 违规数
- 维护记录：docs/rules/drafts 草案数、docs/advance/state.jsonl 轮数

输出按信息优先级：核心（FAIL/0 违规）→ 概览 → 详细。
`--json` 机器可读。
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SKIP = ".git,.dart_tool,test,build,vendor,checks"


def _count_tests() -> int:
    """Count test functions without running them (stdlib scan)."""
    total = 0
    for path in (ROOT / "tests").rglob("test_*.py"):
        if ".dart_tool" in path.parts:
            continue
        try:
            source = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        total += len(re.findall(r"^def test_|^    def test_", source, re.M))
    return total


def _checker_violations(name: str) -> int:
    script = ROOT / "scripts" / f"{name}.py"
    if not script.exists():
        return -1
    cmd = [sys.executable, str(script), "--dir", str(ROOT),
           "--json", "--skip", SKIP]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode not in (0, 1):
        return -1
    try:
        # stdout = exactly one JSON document. A trailing summary line now
        # means a checker stdout regression -> ValueError -> -1 sentinel
        # (fail-closed: healthy becomes False instead of a silent misread).
        return json.loads(result.stdout).get("total", -1)
    except ValueError:  # JSONDecodeError is a ValueError subclass
        return -1


def _quality_violations() -> int:
    result = subprocess.run(
        [sys.executable, str(ROOT / "quality.py"), str(ROOT)],
        capture_output=True, text=True, timeout=120,
    )
    match = re.search(r"(\d+) violation", result.stdout)
    return int(match.group(1)) if match else 0


def _spec_counts() -> dict:
    ui = len(list((ROOT / "ui-specs").rglob("*.md")))
    be = len(list((ROOT / "backend-specs").rglob("*.md")))
    drafts = len(list((ROOT / "docs" / "rules" / "drafts").glob("*.yaml")))
    rounds = 0
    state = ROOT / "docs" / "advance" / "state.jsonl"
    if state.exists():
        try:
            rounds = sum(1 for _ in state.open(encoding="utf-8"))
        except OSError:
            rounds = 0
    return {"ui_specs": ui, "backend_specs": be, "rule_drafts": drafts,
            "advance_rounds": rounds}


def _governance_counts(root: Path = ROOT) -> dict:
    """设备织网与治理状态计数（只读，缺失为 0）。"""
    from pbatch.fabric_devices import DEVICE_STATE_DIR
    counts = {"runners": 0, "events": 0, "drafts": 0,
              "exceptions": 0, "promoted": 0, "truth_invalidations": 0}
    state_dir = root / ".pi-batch" / "devices"
    runners = state_dir / "runners.jsonl"
    if runners.exists():
        try:
            counts["runners"] = sum(1 for _ in runners.open(encoding="utf-8"))
        except OSError:
        # best-effort I/O：失败不阻塞主流程（已验证有意）
            pass
    events = state_dir / "events.jsonl"
    if events.exists():
        try:
            counts["events"] = sum(1 for _ in events.open(encoding="utf-8"))
        except OSError:
        # best-effort I/O：失败不阻塞主流程（已验证有意）
            pass
    counts["drafts"] = len(list((root / "docs" / "rules" / "drafts").glob("*.yaml")))
    counts["exceptions"] = len(list((root / "docs" / "rules" / "exceptions").glob("*.yaml")))
    counts["promoted"] = len(list((root / "docs" / "rules" / "promoted").glob("*.yaml")))
    truth = root / ".pi-batch" / "truth.jsonl"
    if truth.exists():
        try:
            for line in truth.open(encoding="utf-8"):
                if '"invalidate"' in line:
                    counts["truth_invalidations"] += 1
        except OSError:
        # best-effort I/O：失败不阻塞主流程（已验证有意）
            pass
    return counts


def health_report(root: Path = ROOT) -> dict:
    """Gather health signals. Read-only; slow checkers capped by timeout."""
    tests = _count_tests()
    checkers = {
        "designintelligence": _checker_violations("check-design-intelligence"),
        "knowledge": _checker_violations("check-knowledge-freshness"),
        "backendquality": _checker_violations("check-backend-quality"),
    }
    quality = _quality_violations()
    specs = _spec_counts()
    governance = _governance_counts(root)
    return {
        "governance": governance,
        "score": _health_score(checkers, quality, tests),
        "tests": tests,
        "checkers": checkers,
        "quality_violations": quality,
        "specs": specs,
        "healthy": (
            quality == 0
            and all(v == 0 for v in checkers.values())
            and tests > 0
        ),
    }


def _health_score(checkers: dict, quality: int, tests: int) -> dict:
    """Agent OS Health Score（0-100）：代码质量 + 门禁 + 架构 + 治理预算。"""
    score = 100.0
    breakdown = {}
    # 代码质量：0 违规满分 25
    quality_score = max(0, 25 - quality * 5)
    score -= (25 - quality_score)
    breakdown["code_quality"] = quality_score
    # 门禁：全部 0 违规满分 25
    gate_score = 25.0
    for name, count in checkers.items():
        gate_score -= max(0, count * 8)
    gate_score = max(0, gate_score)
    score -= (25 - gate_score)
    breakdown["gates"] = round(gate_score, 1)
    # 架构：模块级循环（graph extract）
    arch_score = 20.0
    try:
        from .hypergraph import extract_module_graph
        graph = extract_module_graph(str(Path(__file__).resolve().parent))
        cycles = len(graph.depends_on_cycles())
        arch_score -= cycles * 5
    except Exception:
        cycles = -1
    arch_score = max(0, arch_score)
    score -= (20 - arch_score)
    breakdown["architecture_cycles"] = cycles
    breakdown["architecture"] = arch_score
    # 治理预算：模块依赖超限（capabilities check）
    gov_score = 15.0
    try:
        from .capabilities import MODULE_DEP_BUDGET, registry_check
        report = registry_check()
        over = report["stats"]["deps_over_budget"]
        gov_score -= over * 3
    except Exception:
        over = -1
    gov_score = max(0, gov_score)
    score -= (15 - gov_score)
    breakdown["budget_overruns"] = over
    breakdown["governance_budget"] = gov_score
    # 测试规模：≥ 800 满分 15
    test_score = min(15, tests / 800 * 15)
    score -= (15 - test_score)
    breakdown["tests_collected"] = tests
    breakdown["test_scale"] = round(test_score, 1)
    return {"score": round(max(0, score), 1), "breakdown": breakdown,
            "grade": ("A" if score >= 90 else "B" if score >= 75
                      else "C" if score >= 60 else "D")}


def _render(report: dict) -> str:
    """信息优先级输出：核心结论 → 概览 → 详细（3 秒理解）。"""
    lines = ["# System Health", ""]
    status = "HEALTHY" if report["healthy"] else "ATTENTION NEEDED"
    score = report.get("score", {})
    lines.append(f"## {status} — Health Score {score.get('score', '?')}"
                 f" (grade {score.get('grade', '?')})")
    if score:
        lines.append("  组件: " + ", ".join(
            f"{key}={value}" for key, value in score.get("breakdown", {}).items()))
    lines.append("")
    lines.append(f"- 测试: {report['tests']}")
    lines.append(f"- 技术债(quality): {report['quality_violations']}")
    for name, count in report["checkers"].items():
        label = "未知" if count < 0 else f"{count} 违规"
        lines.append(f"- 门禁 {name}: {label}")
    lines.append("")
    lines.append("## 治理与设备织网")
    gov = report.get("governance", {})
    lines.append(f"- Runner 设备: {gov.get('runners', 0)} | "
                 f"交互事件: {gov.get('events', 0)}")
    lines.append(f"- 规则草案/例外/promoted: {gov.get('drafts', 0)}/"
                 f"{gov.get('exceptions', 0)}/{gov.get('promoted', 0)} | "
                 f"真值失效: {gov.get('truth_invalidations', 0)}")
    lines.append("## 规范资产")
    lines.append(f"- ui-specs: {report['specs']['ui_specs']} 篇")
    lines.append(f"- backend-specs: {report['specs']['backend_specs']} 篇")
    lines.append(f"- 规则草案: {report['specs']['rule_drafts']}")
    lines.append(f"- advance 轮数: {report['specs']['advance_rounds']}")
    return "\n".join(lines)


def health_main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pi-batch.py health",
                                     description=__doc__)
    parser.add_argument("--json", action="store_true",
                        help="machine-readable report")
    args = parser.parse_args(argv)
    report = health_report()
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(_render(report))
    return 0 if report["healthy"] else 1


if __name__ == "__main__":
    raise SystemExit(health_main())
