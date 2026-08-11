"""One-command self-check for the tool itself: `pi-batch check`.

Runs the local engineering gates that keep the rule system healthy:
- quality: code-organization scanner (functions/lines/complexity/dupes)
- registry: schema integrity of the three domain rule registries (rules --check)
- eval: the rule-system regression suite (evals/, 25 cases)

Exit 0 only when everything passes (fail closed); useful after installing
or editing rules/keywords/registry to confirm the environment is sound.
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path


def _run_step(name: str, command: list) -> bool:
    """Run one gate; print its tail; True when it passed."""
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=600)
    except FileNotFoundError:
        print(f"CHECK [{name}] command not found: {command[0]}", file=sys.stderr)
        return False
    tail = (result.stdout + result.stderr).strip().splitlines()[-3:]
    for line in tail:
        print(f"CHECK [{name}] {line}")
    return result.returncode == 0


def _run_refusal_step(validators=None, script=None) -> bool:
    """`refusal` gate: registry entry + canonical script + 3 smoke runs.

    Fail closed: any missing/broken piece returns False — never a no-op
    print (A4.3). Parameters exist for unit tests; defaults read the live
    config so the step is always wired to reality.
    """
    from pbatch import config as pbconfig
    validators = pbconfig.VALIDATORS if validators is None else validators
    if script is None:
        script = Path(__file__).resolve().parents[1] / "scripts" \
            / "check-no-refusal.py"
    else:
        script = Path(script)
    entry = validators.get("refusalcheck")
    if not entry:
        print("CHECK [refusal] FAIL: 'refusalcheck' missing from pi-batch.yaml "
              "validator registry", file=sys.stderr)
        return False
    print(f"CHECK [refusal] registry entry: {entry}")
    if not script.is_file():
        print(f"CHECK [refusal] FAIL: script not found: {script}",
              file=sys.stderr)
        return False
    print(f"CHECK [refusal] script: {script}")
    smoke = [
        ("clean", "这是一份正常的阶段产物，内容完整。", 0),
        ("marker", "无法继续：上游输入缺失", 1),
        ("missing", None, 1),
    ]
    with tempfile.TemporaryDirectory() as td:
        for label, body, want in smoke:
            path = Path(td) / f"{label}.md"
            if body is not None:
                path.write_text(body, encoding="utf-8")
            try:
                r = subprocess.run([sys.executable, str(script), str(path)],
                                   capture_output=True, text=True, timeout=30)
            except (OSError, subprocess.TimeoutExpired) as exc:
                print(f"CHECK [refusal] FAIL: {label} smoke run: {exc}",
                      file=sys.stderr)
                return False
            ok = r.returncode == want
            print(f"CHECK [refusal] {label}: exit {r.returncode} (want {want})"
                  + (" OK" if ok else " FAIL"))
            if not ok:
                return False
    print("CHECK [refusal] OK")
    return True


def _default_steps(args) -> list:
    """默认门禁集：代码质量 / 规则注册表 / 规范完整性 / 能力注册表 /
    架构循环 / 命令分派 / eval 回归。"""
    steps = [
        ("quality", [sys.executable, str(Path("quality.py")), "--strict", "."]),
        ("registry", [sys.executable, "-m", "pbatch", "rules", "--check"]),
        ("org", [sys.executable, "-m", "pbatch", "org", "check"]),
        ("capabilities", [sys.executable, "-m", "pbatch", "capabilities",
                          "check"]),
        ("graph", [sys.executable, "-m", "pbatch", "graph", "extract",
                   "--input", "pbatch"]),
        ("tools", [sys.executable, "-m", "pbatch", "tools", "--check"]),
        ("knowledge", [sys.executable, str(Path("scripts")
                                           / "check-knowledge-freshness.py"),
                       "--dir", "."]),
    ]
    steps.append(("refusal", _run_refusal_step))
    if not args.skip_eval:
        cmd = [sys.executable, "-m", "pbatch", "eval"]
        if args.quick_eval:
            cmd.append("--quick")
        steps.append(("eval", cmd))
    return steps


def check_main(argv: list) -> None:
    """`pi-batch check [--skip-eval] [--steps a,b]`：全面自检。"""
    import argparse
    parser = argparse.ArgumentParser(
        prog="pi-batch.py check",
        description="One-command self-check: quality + registry + org + "
                    "capabilities + graph + tools + eval.")
    parser.add_argument("--skip-eval", action="store_true",
                        help="skip the eval suite (slowest step)")
    parser.add_argument("--quick-eval", action="store_true",
                        help="run only core eval cases (rules + classifier)")
    parser.add_argument("--steps", default="",
                        help="仅运行指定步骤（逗号分隔）")
    args = parser.parse_args(argv)
    steps = _default_steps(args)
    if args.steps:
        wanted = {name.strip() for name in args.steps.split(",") if name.strip()}
        known = {item[0] for item in _default_steps(args)} | {"eval"}
        unknown = wanted - known
        if unknown:
            print(f"CHECK: unknown step(s): {', '.join(sorted(unknown))}",
                  file=sys.stderr)
            sys.exit(2)
        steps = [item for item in steps if item[0] in wanted]
    failed = []
    for name, step in steps:
        ok = step() if callable(step) else _run_step(name, step)
        if not ok:
            failed.append(name)
    if failed:
        print(f"CHECK: failed: {', '.join(failed)}", file=sys.stderr)
        sys.exit(1)
    print(f"CHECK: all {len(steps)} gates passed")
