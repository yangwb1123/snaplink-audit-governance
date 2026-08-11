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
          prescription_has_rule: [product-thinking]  # assess prescription rule ids present
          prescription_lacks_rule: [billing]         # assess prescription rule ids absent

Assertion keys are dispatched to the matching analyzer; list values mean
"all of these must hold".

A case asserts via `assert:`; when `assert` is absent (or `{}`), the
case's `expect:` block becomes the assertion source and only its
*executable* keys are run (EXECUTABLE_EXPECT_KEYS — system_type, tier,
includes, excludes). Display-only expect keys (task_type, domain,
matched_tier, effective_tier) are consumed solely by _suite_domains for
coverage display and never executed. When `assert` is present, `expect`
stays inert (precedence). A case with neither `assert` nor any executable
expect key raises VacuousCaseError (fail closed — it can never be a real
regression case).

Exit codes: 0 = all pass; 1 = assertion failures (CI-friendly); 2 =
schema/vacuous-case error OR --filter matched zero cases (consistent with
load_eval_files). Exit 2 never emits a JSON payload (F-7): a filter typo
is a usage error, not a run result — the message naming the filter goes to
stderr (logging), stdout stays clean.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from . import config
from .assessor import prescription, workflow_level
from .classifier import classify_text
from .config import log, yaml
from .product import product_manifest
from .rule_matcher import match_rules
from .text_io import read_text_bounded

EVAL_DIR = Path("evals")


# Expect keys that are executable as assertions. Display-only expect keys
# (task_type, domain, matched_tier, effective_tier) are consumed solely by
# _suite_domains and never executed.
EXECUTABLE_EXPECT_KEYS = ("system_type", "tier", "includes", "excludes")


class VacuousCaseError(ValueError):
    """A case has neither assert nor any executable expect key (fail closed)."""


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
        cases = data["cases"]
        for idx, case in enumerate(cases):
            if not isinstance(case, dict):
                log.error("Invalid eval suite %s: case %d is not a mapping",
                          path, idx)
                sys.exit(2)
            case_id = str(case.get("id", "")).strip()
            if not case_id:
                log.error("Invalid eval suite %s: case %d missing non-empty "
                          "'id' (cases must be --filter targetable)", path, idx)
                sys.exit(2)
            for key in ("assert", "expect"):
                value = case.get(key)
                if value is not None and not isinstance(value, dict):
                    log.error("Invalid eval suite %s: case %s '%s' must be "
                              "a mapping, got %s", path, case_id, key,
                              type(value).__name__)
                    sys.exit(2)
        suites.append({"path": path, "name": str(data.get("name", path.stem)),
                       "cases": cases})
    return suites


def _assert_value(key: str, input_text: str) -> object:
    """Dispatch one assertion key to the live analyzer."""
    if key in ("task_type", "platform", "profile", "confident", "system_type"):
        cls = classify_text(input_text)
        return {"task_type": cls.task_type, "platform": cls.platform,
                "profile": cls.profile, "confident": cls.confident,
                "system_type": cls.system_type}[key]
    if key in ("tier", "risk", "page_types"):
        matched = match_rules(input_text)
        return {"tier": matched["tier"], "risk": matched["risk"],
                "page_types": matched["page_types"]}[key]
    if key in ("has_rule", "lacks_rule", "includes", "excludes"):
        # includes/excludes share has_rule/lacks_rule semantics: the rule-id
        # set from the live match_rules manifest.
        return {item["id"] for item in match_rules(input_text)["rules"]}
    if key in ("prescription_has_rule", "prescription_lacks_rule"):
        # Prescription-level keys evaluate the assess prescription (effective
        # tier = min(keyword tier, scale tier)), not the raw matcher manifest.
        return {item["id"] for item in prescription(input_text)["prescription"]}
    if key == "product_level":
        return product_manifest(input_text)["level"]
    if key == "workflow_level":
        return workflow_level(input_text)["level"]
    if key == "scale":
        from .assessor import scale_signal
        return scale_signal(input_text)
    raise ValueError(f"unknown eval assertion key: {key}")


def _effective_asserts(suite: dict, case: dict, case_id: str) -> dict:
    """The assertion source for a case: assert wins; otherwise the
    executable expect keys (R1 precedence). A case with neither is
    vacuous — fail closed with a typed error naming suite/case (R3)."""
    asserts = case.get("assert", {})
    if asserts:
        return asserts
    expect = case.get("expect", {})
    expect = expect if isinstance(expect, dict) else {}
    asserts = {k: v for k, v in expect.items()
               if k in EXECUTABLE_EXPECT_KEYS}
    if not asserts:
        raise VacuousCaseError(
            f"{suite['name']}/{case_id}: no assert and no executable "
            f"expect keys {list(EXECUTABLE_EXPECT_KEYS)}")
    return asserts


def _list_assertion_failure(key: str, expected_list: list, actual: set) -> str:
    """Rule-id set semantics (has_rule/includes = all present;
    lacks_rule/excludes = none present); '' when satisfied."""
    if key in ("has_rule", "includes", "prescription_has_rule"):
        missing = [e for e in expected_list if e not in actual]
        if missing:
            return f"{key} missing {missing} (got {sorted(actual)})"
    elif key in ("lacks_rule", "excludes", "prescription_lacks_rule"):
        present = [e for e in expected_list if e in actual]
        if present:
            return f"{key} present {present} (got {sorted(actual)})"
    return ""


def run_suite(suite: dict, only_id: str = "") -> list:
    """[(case_id, ok, detail)] for one suite."""
    results = []
    for case in suite["cases"]:
        case_id = str(case.get("id", "?"))
        if only_id and case_id != only_id:
            continue
        failures = []
        for key, expected in _effective_asserts(suite, case, case_id).items():
            try:
                actual = _assert_value(key, str(case.get("input", "")))
            except ValueError as exc:
                failures.append(f"{key}: {exc}")
                continue
            expected_list = expected if isinstance(expected, list) else [expected]
            if key in ("has_rule", "includes", "lacks_rule", "excludes",
                       "prescription_has_rule", "prescription_lacks_rule"):
                failure = _list_assertion_failure(key, expected_list, actual)
                if failure:
                    failures.append(failure)
            elif actual not in expected_list:
                failures.append(f"{key}: expected {expected_list}, got {actual!r}")
        results.append((case_id, not failures, "; ".join(failures) if failures else ""))
    return results


def _count_cases(suite: dict) -> int:
    """Case count for a suite."""
    cases = suite.get("cases")
    if isinstance(cases, list):
        return len(cases)
    return 0


def _suite_domains(suite: dict) -> list:
    """Domains exercised by a suite (from case expect fields)."""
    domains = []
    cases = suite.get("cases")
    if not isinstance(cases, list):
        return []
    for case in cases:
        if not isinstance(case, dict):
            continue
        expect = case.get("expect", {})
        for key in ("task_type", "system_type", "domain", "tier",
                   "matched_tier", "effective_tier"):
            value = expect.get(key) if isinstance(expect, dict) else None
            if value and str(value) not in domains:
                domains.append(str(value))
    return domains[:5]


def _print_coverage(total: int, failed: list, json_out: bool) -> None:
    """Per-suite case counts + domain coverage (data-expression)."""
    coverage = []
    for suite in load_eval_files():
        coverage.append({"suite": suite.get("name", "?"),
                         "cases": _count_cases(suite),
                         "domains": _suite_domains(suite)})
    if json_out:
        print(json.dumps({"total": total, "failed": len(failed),
                          "failures": failed, "coverage": coverage},
                         ensure_ascii=False, indent=2))
    else:
        print("## Eval 域覆盖")
        for item in coverage:
            print(f"- {item['suite']}: {item['cases']} 用例"
                  f" (域: {', '.join(item['domains']) or 'n/a'})")
        print(f"总计 {total} 用例 / {len(coverage)} 套件")


def eval_main(argv: list) -> None:
    """`pi-batch eval [--filter CASE_ID] [--json]` — run the rule-system
    regression suite."""
    import argparse
    parser = argparse.ArgumentParser(
        prog="pi-batch.py eval",
        description="Run the rule-system regression suite (evals/*.yaml).")
    parser.add_argument("--filter", default="", help="run only this case id")
    parser.add_argument(
        "--quick", action="store_true",
        help="run only core regression cases (rules + classifier domains)")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    parser.add_argument(
        "--coverage", action="store_true",
        help="print per-suite case counts and domain coverage")
    args = parser.parse_args(argv)
    total, failed = 0, []
    for suite in load_eval_files():
        if args.quick and suite.get("name") not in {"rules", "classifier"}:
            continue
        try:
            results = run_suite(suite, args.filter)
        except VacuousCaseError as exc:
            log.error("EVAL schema error: %s", exc)
            sys.exit(2)
        for case_id, ok, detail in results:
            total += 1
            status = "PASS" if ok else "FAIL"
            log.info("EVAL %s/%s: %s", suite["name"], case_id, status)
            if not ok:
                failed.append(f"{suite['name']}/{case_id}: {detail}")
                log.error("EVAL %s/%s FAIL: %s", suite["name"], case_id, detail)
    if args.coverage:
        _print_coverage(total, failed, args.json)
        return
    if total == 0:
        # Fail closed: zero executed cases = usage error, not "all 0 passed".
        if args.filter:
            log.error("EVAL: --filter %r matched no eval cases (exact case id "
                      "required; see evals/*.yaml)", args.filter)
        else:
            log.error("EVAL: executed 0 eval cases (no evals/*.yaml loaded?)")
        sys.exit(2)
    if args.json:
        print(json.dumps({"total": total, "failed": len(failed),
                          "failures": failed}, ensure_ascii=False, indent=2))
    if failed:
        log.error("EVAL: %d/%d failed", len(failed), total)
        sys.exit(1)
    log.info("EVAL: all %d passed", total)
