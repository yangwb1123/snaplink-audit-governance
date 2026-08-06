"""Evaluation Engineering: a regression suite for the rule system itself.

`pi-batch eval` runs the evals/*.yaml cases against the live classifier /
rule matcher / assessor and fails when a keyword or registry change broke
behavior — the "Evals 缺失，规范调整后无法知道能力是提升还是退化" gap.

Eval case schema:

    name: classifier
    cases:
      - id: c1
        input: "用 Flutter 实现 ERP 排程列表页"
        assert:
          task_type: frontend_ui     # classify_text
          platform: dart
          tier: standard             # match_rules (from scale keywords)
          has_rule: [visual-core]    # rule ids present
          lacks_rule: [async-data]   # rule ids absent
          product_level: L0_local_feature   # product_manifest
          workflow_level: L1_standard       # assessor workflow_level

Assertion keys are dispatched to the matching analyzer; list values mean
"all of these must hold". Exit code 1 on any failure (CI-friendly).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from . import config
from .assessor import workflow_level
from .classifier import classify_text
from .config import log, yaml
from .product import product_manifest
from .rule_matcher import match_rules
from .text_io import read_text_bounded

EVAL_DIR = Path("evals")


def load_eval_files() -> list:
    """All evals/*.yaml suites; malformed files fail loudly (fail closed)."""
    suites = []
    if not EVAL_DIR.is_dir():
        log.error("No evals/ directory — create evals/*.yaml case files")
        sys.exit(2)
    for path in sorted(EVAL_DIR.glob("*.yaml")):
        data = yaml.safe_load(read_text_bounded(
            path, config.INPUT_MAX_BYTES, "eval file")) or {}
        if not isinstance(data, dict) or not isinstance(data.get("cases"), list):
            log.error("Invalid eval suite %s: needs 'cases' list", path)
            sys.exit(2)
        suites.append({"path": path, "name": str(data.get("name", path.stem)),
                       "cases": data["cases"]})
    return suites


def _assert_value(key: str, input_text: str) -> object:
    """Dispatch one assertion key to the live analyzer."""
    if key in ("task_type", "platform", "profile", "confident"):
        cls = classify_text(input_text)
        return {"task_type": cls.task_type, "platform": cls.platform,
                "profile": cls.profile, "confident": cls.confident}[key]
    if key in ("tier", "risk", "page_types"):
        matched = match_rules(input_text)
        return {"tier": matched["tier"], "risk": matched["risk"],
                "page_types": matched["page_types"]}[key]
    if key in ("has_rule", "lacks_rule"):
        return {item["id"] for item in match_rules(input_text)["rules"]}
    if key == "product_level":
        return product_manifest(input_text)["level"]
    if key == "workflow_level":
        return workflow_level(input_text)["level"]
    if key == "scale":
        from .assessor import scale_signal
        return scale_signal(input_text)
    raise ValueError(f"unknown eval assertion key: {key}")


def run_suite(suite: dict, only_id: str = "") -> list:
    """[(case_id, ok, detail)] for one suite."""
    results = []
    for case in suite["cases"]:
        case_id = str(case.get("id", "?"))
        if only_id and case_id != only_id:
            continue
        asserts = case.get("assert", {})
        failures = []
        for key, expected in asserts.items():
            try:
                actual = _assert_value(key, str(case.get("input", "")))
            except ValueError as exc:
                failures.append(f"{key}: {exc}")
                continue
            expected_list = expected if isinstance(expected, list) else [expected]
            if key in ("has_rule",):
                missing = [e for e in expected_list if e not in actual]
                if missing:
                    failures.append(f"{key} missing {missing} (got {sorted(actual)})")
            elif key == "lacks_rule":
                present = [e for e in expected_list if e in actual]
                if present:
                    failures.append(f"{key} present {present} (got {sorted(actual)})")
            elif actual not in expected_list:
                failures.append(f"{key}: expected {expected_list}, got {actual!r}")
        results.append((case_id, not failures, "; ".join(failures) if failures else ""))
    return results


def eval_main(argv: list) -> None:
    """`pi-batch eval [--filter CASE_ID] [--json]` — run the rule-system
    regression suite."""
    import argparse
    parser = argparse.ArgumentParser(
        prog="pi-batch.py eval",
        description="Run the rule-system regression suite (evals/*.yaml).")
    parser.add_argument("--filter", default="", help="run only this case id")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    args = parser.parse_args(argv)
    total, failed = 0, []
    for suite in load_eval_files():
        for case_id, ok, detail in run_suite(suite, args.filter):
            total += 1
            status = "PASS" if ok else "FAIL"
            log.info("EVAL %s/%s: %s", suite["name"], case_id, status)
            if not ok:
                failed.append(f"{suite['name']}/{case_id}: {detail}")
                log.error("EVAL %s/%s FAIL: %s", suite["name"], case_id, detail)
    if args.json:
        print(json.dumps({"total": total, "failed": len(failed),
                          "failures": failed}, ensure_ascii=False, indent=2))
    if failed:
        log.error("EVAL: %d/%d failed", len(failed), total)
        sys.exit(1)
    log.info("EVAL: all %d passed", total)
