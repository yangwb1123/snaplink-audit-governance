"""Rule registry schema check + `pi-batch rules` CLI (split for line budget).

与 pbatch/rule_matcher.py（匹配算法）分离；本模块只负责注册表校验与
命令行界面。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

from . import config
from .config import log
from .rule_matcher import (LEVELS, TIER_ORDER, _default_registry_paths,
                           format_llm_prompt, format_manifest, load_registry,
                           match_rules, reconcile, summarize_task)

CHECK_ROOT = Path(__file__).resolve().parent.parent


def _reconcile_cli(text: str, llm_json: str, registry: dict,
                   want_json: bool) -> None:
    """双向校验：LLM 独立选择 vs 算法 manifest（必选规则不可跳过）。"""
    try:
        selection = json.loads(llm_json)
    except Exception as exc:
        log.error("Invalid --llm-json: %s", exc)
        sys.exit(2)
    matched = reconcile(match_rules(text, registry=registry),
                        selection.get("apply", []),
                        selection.get("skip", []), registry)
    if want_json:
        print(json.dumps(matched, ensure_ascii=False, indent=2))
        return
    print(format_manifest(matched))
    dropped = matched.get("dropped", [])
    if dropped:
        print("Dropped (LLM skipped optional): " +
              ", ".join(item["id"] for item in dropped))


def _check_suppressions(rule_id: str, rule: dict, violations: list) -> None:
    """suppress_on 引用必须是合法信号（档位或 high/low）。"""
    for signal in rule.get("suppress_on", []) or []:
        if str(signal) not in TIER_ORDER and str(signal) not in ("high", "low"):
            violations.append(f"rule '{rule_id}': suppress_on '{signal}' "
                              "must be a tier or high/low")


def _check_self_reference(rule_id: str, rule: dict, files: list,
                          template: str, registry_paths: set,
                          violations: list) -> None:
    """A rule whose files/files_template resolves to the registry file
    itself is a violation (fail closed; resolve-normalized comparison)."""
    for file in files:
        if str(Path(str(file)).resolve()) in registry_paths:
            violations.append(
                f"rule '{rule_id}': files entry '{file}' resolves to the "
                "registry file itself (self-referential rule)")
    if template:
        for profile in rule.get("profiles", []):
            target = str(template).replace("{profile}", str(profile))
            if str(Path(target).resolve()) in registry_paths:
                violations.append(
                    f"rule '{rule_id}': files_template resolves to the "
                    "registry file itself (self-referential rule)")


def _check_rule(rule_id: str, rule, violations: list,
                registry_paths: Optional[set] = None) -> None:
    """One rule's schema: required fields, tier, referenced files.
    registry_paths: resolved paths of the registry file(s) the rules were
    loaded from; when given, a rule whose files/files_template resolves to
    any of them is a violation (self-referential rule, fail closed)."""
    if not isinstance(rule, dict):
        violations.append(f"rule '{rule_id}': must be a mapping")
        return
    for key in ("description", "min_tier"):
        if not str(rule.get(key, "")).strip():
            violations.append(f"rule '{rule_id}': missing '{key}'")
    if rule.get("min_tier") not in TIER_ORDER:
        violations.append(f"rule '{rule_id}': invalid min_tier "
                          f"{rule.get('min_tier')!r}")
    if rule.get("level") is not None and rule.get("level") not in LEVELS:
        violations.append(f"rule '{rule_id}': invalid level "
                          f"{rule.get('level')!r} (must be one of {LEVELS})")
    _check_suppressions(rule_id, rule, violations)
    files = rule.get("files", [])
    template = rule.get("files_template")
    if not files and not template:
        violations.append(f"rule '{rule_id}': missing 'files' or "
                          f"'files_template'")
    for file in files:
        if not Path(str(file)).exists():
            violations.append(f"rule '{rule_id}': missing file {file}")
    if registry_paths:
        _check_self_reference(rule_id, rule, files, template,
                              registry_paths, violations)


def check_registry(registry: dict,
                   registry_paths: Optional[list] = None) -> list:
    """All registry rules: schema + file references; [] = clean.
    registry_paths: resolved paths of the registry file(s) the rules were
    loaded from; when given, a rule whose files/files_template resolves to
    any of them is a violation (self-referential rule, fail closed)."""
    resolved = None
    if registry_paths:
        resolved = {str(Path(p).resolve()) for p in registry_paths}
    violations = []
    for rule_id, rule in (registry.get("rules") or {}).items():
        _check_rule(rule_id, rule, violations, resolved)
    return violations


def _check_registries_cli() -> None:
    """`rules --check`: validate the three domain registries (fail
    closed). Passes the actual registry paths so a rule whose files
    points at the registry file itself is rejected (self-reference, fail
    closed). A domain whose registry file is missing/empty is refused
    (exit 1) — an empty rules section must never report OK (vacuous
    pass)."""
    domains = ("frontend_ui", "backend", "product")
    for domain in domains:
        registry = load_registry(domain=domain)
        if not registry.get("rules"):
            log.error("[%s] registry empty or missing file; refusing OK",
                      domain)
            sys.exit(1)
        paths = sorted({str(p.resolve())
                        for p in _default_registry_paths(domain)})
        violations = check_registry(registry, paths)
        if violations:
            log.error("[%s] registry invalid:", domain)
            for violation in violations:
                log.error("  %s", violation)
            sys.exit(1)
        log.info("[%s] registry: OK", domain)


def rules_main(argv: list) -> None:
    """`pi-batch rules "<task>" [--json|--summary|--llm-json|--check]`."""
    import argparse
    parser = argparse.ArgumentParser(
        prog="pi-batch.py rules",
        description="Deterministic rule manifest for a task "
                    "(scale x page_type x risk; LLM stays inside the set)")
    parser.add_argument("task", nargs="*", default=[], help="task text")
    parser.add_argument("--file", default="")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--summary", action="store_true",
                        help="压缩需求（供独立 LLM 选择，双向校验用）")
    parser.add_argument("--llm-prompt", default="",
                        help="把压缩需求 + 清单写入 FILE（LLM 检查 prompt）")
    parser.add_argument("--llm-json", default="",
                        help='LLM 选择 JSON {"apply":[],"skip":[]} 双向校验')
    parser.add_argument("--check", action="store_true",
                        help="注册表 schema 校验（三域：frontend/backend/product）")
    parser.add_argument("--registry", default="", help="registry YAML override")
    args = parser.parse_args(argv)
    if args.check:
        _check_registries_cli()
        return
    if args.file:
        from .text_io import read_text_bounded
        text = read_text_bounded(Path(args.file), config.INPUT_MAX_BYTES,
                                 "rules source")
    else:
        text = " ".join(args.task)
    if not text.strip():
        parser.error("Provide a task description (positional or --file)")
    registry = load_registry(args.registry) if args.registry else None
    if args.llm_json:
        _reconcile_cli(text, args.llm_json, registry, args.json)
        return
    if args.summary:
        print(summarize_task(text, registry))
        return
    if args.llm_prompt:
        Path(args.llm_prompt).write_text(
            format_llm_prompt(text, registry), encoding="utf-8")
        log.info("LLM check prompt written to %s", args.llm_prompt)
        return
    matched = match_rules(text, registry=registry)
    if args.json:
        print(json.dumps(matched, ensure_ascii=False, indent=2))
    else:
        print(format_manifest(matched))
