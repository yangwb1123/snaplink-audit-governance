"""Evolution Engineering: turn an incident (RCA) into a rule draft.

The self-evolving loop: production bug / review finding → root-cause
analysis → rule draft (unified Rule Schema) → human approval → merged into
the domain rules.yaml registry → the matcher/gates/roles consume it on the
next task. Drafts are NEVER auto-promoted — an accidental one-off must not
pollute the registry (restraint: human approval gate).

Usage:
    pi-batch learn "订单在并发下创建了两笔重复记录" \
      --category backend --severity error --rule-id DB-IDEMPOTENCY-002
Writes docs/rules/drafts/<rule-id>-<date>.yaml and prints the draft.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

from .config import log

DRAFT_DIR = Path("docs/rules/drafts")
EXCEPTION_DIR = Path("docs/rules/exceptions")
CATEGORIES = ("backend", "frontend", "product", "database", "security",
              "testing", "devops")
SEVERITIES = ("error", "warning", "advice")


def exception_record(rule_id: str, reason: str, expected_benefit: float,
                     added_risk: float, compensating: list, scope: list,
                     expires_at: str = "") -> dict:
    """软规则例外记录（AADM §22）：例外必须可解释、可审计、可回收。

    - reason: 为什么不拆/为什么违反默认规则
    - expected_benefit: 预期收益（质量/性能/成本，1-5）
    - added_risk: 增加的风险（1-5）
    - compensating_controls: 通过哪些测试/门禁补偿
    - scope: 例外只适用于哪些路径/函数（回收边界）
    - expires_at: 过期时间（默认 90 天后，强制复查）
    """
    return {
        "rule_id": rule_id,
        "reason": " ".join(reason.split())[:300],
        "expected_benefit": float(expected_benefit),
        "added_risk": float(added_risk),
        "compensating_controls": list(compensating),
        "required_evidence": list(compensating),  # 例外必须带补偿证据
        "scope": list(scope),
        "expires_at": expires_at or f"{date.today().isoformat()} +90d 复查",
        "approved_by": "pending",
    }


def write_exception(record: dict) -> Path:
    """把例外记录落盘（docs/rules/exceptions/，只追加新文件）。"""
    EXCEPTION_DIR.mkdir(parents=True, exist_ok=True)
    path = EXCEPTION_DIR / f"{record['rule_id']}-{date.today():%Y%m%d}.yaml"
    if path.exists():
        path = EXCEPTION_DIR / f"{record['rule_id']}-{date.today():%Y%m%d}-{id(path) % 1000:03d}.yaml"
    from .config import yaml
    payload = (yaml.safe_dump(record, allow_unicode=True, sort_keys=False)
               if yaml else str(record))
    path.write_text(payload, encoding="utf-8")
    return path


def shadow_evaluate(rule_id: str, log_path: str, draft_dir: Path = DRAFT_DIR,
                    trigger_terms: Optional[list] = None) -> dict:
    """影子模式（AADM §21）：用历史运行日志评估草案规则的命中率。

    一次成功不能证明策略正确——影子阶段验证规则在真实数据上的命中率
    后再决定是否升级（观察 → 候选经验 → 影子规则 → 软策略 → 门禁）。
    """
    draft = _load_draft(rule_id, draft_dir)
    terms = trigger_terms or [str(t) for t in
                              draft.get("trigger", {}).get("operations", [])]
    terms = [t for t in terms if t.strip()]
    if not terms:
        return {"error": "draft has no trigger operations to shadow"}
    path = Path(log_path)
    if not path.exists():
        return {"error": f"log not found: {path}"}
    hits = misses = 0
    sample_lines = 0
    for line in path.open(encoding="utf-8", errors="replace"):
        if not line.strip():
            continue
        sample_lines += 1
        if any(term in line for term in terms):
            hits += 1
        else:
            misses += 1
    total = hits + misses
    hit_rate = round(hits / total, 3) if total else 0.0
    verdict = "promote_candidate" if hit_rate >= 0.05 else "insufficient_signal"
    if hit_rate >= 0.30:
        verdict = "promote_to_soft_policy"
    shadow = {
        "rule_id": rule_id,
        "evaluated_at": date.today().isoformat(),
        "log_path": str(path),
        "sample_lines": sample_lines,
        "hits": hits,
        "misses": misses,
        "hit_rate": hit_rate,
        "verdict": verdict,
        "note": "影子命中率是升级候选经验→软策略的输入；硬规则升级仍须治理流程",
    }
    return shadow


PROMOTED_DIR = Path("docs/rules/promoted")


def promote_draft(rule_id: str, registry: str = "frontend",
                  files: Optional[list] = None, min_tier: str = "standard",
                  level: str = "policy", required: bool = False,
                  draft_dir: Path = DRAFT_DIR,
                  registry_path: str = "") -> tuple:
    """把草案并入规则注册表（规则生命周期最后一步：人工批准后落地）。

    前置：草案已填 RCA（requirements/forbidden/verification 至少一项）、
    files 全部存在、rule_id 未占用。注册表按文本追加（保留既有注释），
    草案移入 docs/rules/promoted/ 存档。"""
    draft = _load_draft(rule_id, draft_dir)
    if not draft:
        raise ValueError(f"draft not found: {rule_id}")
    if not (draft.get("requirements") or draft.get("forbidden")
            or draft.get("verification", {}).get("tests")):
        raise ValueError("draft 未填 RCA（requirements/forbidden/verification）"
                         "——先完成根因分析再提升")
    if not files:
        raise ValueError("--files 必填（规则引用的规范文件，须存在）")
    missing = [f for f in files if not Path(f).exists()]
    if missing:
        raise ValueError(f"rule files missing: {missing}")
    if not registry_path:
        registry_path = {"frontend": "ui-specs/rules.yaml",
                         "backend": "backend-specs/rules.yaml"}.get(registry, "")
    if not registry_path or not Path(registry_path).exists():
        raise ValueError(f"unknown registry {registry!r}")
    text = Path(registry_path).read_text(encoding="utf-8")
    if f"\n  {rule_id}:\n" in text or f"  {rule_id}:\n" in text:
        raise ValueError(f"rule '{rule_id}' already exists in {registry_path}")
    block = (
        f"  {rule_id}:\n"
        f"    description: {draft.get('title', rule_id)}\n"
        f"    files: {files}\n"
        f"    min_tier: {min_tier}\n"
        f"    level: {level}\n"
        f"    required: {str(required).lower()}\n"
        f"    # promoted from draft {rule_id}（{draft.get('severity', '')}，"
        f"影子/人工批准后并入）\n")
    if not text.endswith("\n"):
        text += "\n"
    text += block
    Path(registry_path).write_text(text, encoding="utf-8")
    PROMOTED_DIR.mkdir(parents=True, exist_ok=True)
    moved = _move_draft(draft_dir, rule_id)
    return registry_path, moved


def _move_draft(draft_dir: Path, rule_id: str) -> str:
    """草案移入 promoted 存档（只移动最新一份）。"""
    matches = sorted(draft_dir.glob(f"{rule_id}-*.yaml"))
    if not matches:
        return ""
    destination = PROMOTED_DIR / matches[-1].name
    destination.write_text(matches[-1].read_text(encoding="utf-8"),
                           encoding="utf-8")
    matches[-1].unlink()
    return str(destination)


def _load_draft(rule_id: str, draft_dir: Path = DRAFT_DIR) -> dict:
    from .config import yaml
    matches = sorted(draft_dir.glob(f"{rule_id}-*.yaml"))
    if not matches:
        return {}
    try:
        data = yaml.safe_load(matches[-1].read_text(encoding="utf-8")) or {}
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def rule_schema_template() -> dict:
    """The unified Rule Schema every draft (and registry rule) follows."""
    return {
        "id": "DRAFT-000",
        "title": "",
        "category": "backend",
        "severity": "error",
        "scope": {"languages": [], "paths": ["src/**"]},
        "trigger": {"operations": []},
        "rationale": "",
        "requirements": [],
        "forbidden": [],
        "exceptions": [{"condition": ""}],
        "verification": {"static": [], "tests": []},
        "examples": {"good": [], "bad": []},
        "owner": "",
        "version": "0.1.0",
        "status": "draft",
    }


def make_draft(failure: str, category: str, severity: str,
               rule_id: str, owner: str = "") -> tuple:
    """Create a draft file; returns (path, draft_dict)."""
    draft = rule_schema_template()
    draft["id"] = rule_id or f"DRAFT-{date.today():%Y%m%d}"
    draft["title"] = " ".join(failure.split())[:80]
    draft["category"] = category
    draft["severity"] = severity
    draft["rationale"] = ("Observed failure: " + " ".join(failure.split()) +
                          ". Root-cause analysis is required before this "
                          "draft can be promoted (edit requirements/forbidden/"
                          "verification with the actual RCA).")
    draft["owner"] = owner
    DRAFT_DIR.mkdir(parents=True, exist_ok=True)
    path = DRAFT_DIR / f"{draft['id']}-{date.today():%Y%m%d}.yaml"
    if path.exists():
        path = DRAFT_DIR / f"{draft['id']}-{date.today():%Y%m%d}-{id(path) % 1000:03d}.yaml"
    from .config import yaml
    payload = yaml.safe_dump(draft, allow_unicode=True, sort_keys=False) if yaml else str(draft)
    path.write_text(payload, encoding="utf-8")
    return path, draft


def _build_learn_parser():
    """learn 子命令参数：draft / exception / shadow / promote。"""
    import argparse
    parser = argparse.ArgumentParser(
        prog="pi-batch.py learn",
        description="Evolution Engineering: failure -> draft -> approval -> "
                    "registry; soft-rule exceptions; shadow-mode evaluation.")
    sub = parser.add_subparsers(dest="command")

    p_draft = sub.add_parser("draft", help="把事故/RCA 沉淀为规则草案（默认）")
    p_draft.add_argument("failure", nargs="*", default=[])
    p_draft.add_argument("--category", default="backend", choices=CATEGORIES)
    p_draft.add_argument("--severity", default="error", choices=SEVERITIES)
    p_draft.add_argument("--rule-id", default="")
    p_draft.add_argument("--owner", default="")
    p_draft.add_argument("--from-memory", default="")

    p_exc = sub.add_parser("exception",
                           help="记录软规则例外（可解释/可审计/可回收）")
    p_exc.add_argument("--rule-id", required=True)
    p_exc.add_argument("--reason", required=True)
    p_exc.add_argument("--benefit", type=float, default=2.0)
    p_exc.add_argument("--risk", type=float, default=1.0)
    p_exc.add_argument("--compensating", default="",
                       help="逗号分隔的补偿验证（测试/门禁）")
    p_exc.add_argument("--scope", default="",
                       help="逗号分隔的适用路径（例外回收边界）")
    p_exc.add_argument("--expires", default="")

    p_promote = sub.add_parser(
        "promote", help="把已批准草案并入规则注册表（规则生命周期闭环）")
    p_promote.add_argument("--rule-id", required=True)
    p_promote.add_argument("--registry", default="frontend",
                           choices=("frontend", "backend"))
    p_promote.add_argument("--files", default="",
                           help="逗号分隔的规范文件（须存在）")
    p_promote.add_argument("--min-tier", default="standard",
                           choices=("demo", "standard", "production"))
    p_promote.add_argument("--level", default="policy",
                           choices=("invariant", "contract", "policy",
                                    "heuristic", "suggestion"))
    p_promote.add_argument("--required", action="store_true")

    p_shadow = sub.add_parser("shadow", help="影子模式：日志上评估草案命中率")
    p_shadow.add_argument("--rule-id", required=True)
    p_shadow.add_argument("--log", required=True, help="历史运行日志路径")
    p_shadow.add_argument("--draft-dir", default="")
    return parser


def learn_main(argv: list) -> None:
    """`pi-batch learn draft|exception|shadow|promote`。"""
    if not argv or argv[0] not in ("draft", "exception", "shadow", "promote"):
        # 兼容旧用法：`pi-batch learn "<failure>" [--category X] ...`
        argv = ["draft"] + list(argv or [])
    args = _build_learn_parser().parse_args(argv)
    _DISPATCH = {"exception": _cmd_exception, "shadow": _cmd_shadow,
                 "promote": _cmd_promote, "draft": _cmd_draft}
    _DISPATCH.get(args.command, _cmd_draft)(args)


def _cmd_exception(args) -> None:
    """软规则例外落盘：可解释、可审计、可回收（默认 90 天复查）。"""
    record = exception_record(
        args.rule_id, args.reason, args.benefit, args.risk,
        [c.strip() for c in args.compensating.split(",") if c.strip()],
        [s.strip() for s in args.scope.split(",") if s.strip()],
        args.expires)
    path = write_exception(record)
    log.info("Rule exception written to %s（默认 90 天复查；"
             "例外不豁免 invariant/必选规则）", path)
    print(path)


def _cmd_shadow(args) -> None:
    """影子模式：历史日志上评估草案规则命中率并给裁决。"""
    shadow = shadow_evaluate(args.rule_id, args.log,
                             args.draft_dir or DRAFT_DIR)
    if "error" in shadow:
        log.error("%s", shadow["error"])
        raise SystemExit(1)
    from .config import yaml as _yaml
    print(_yaml.safe_dump(shadow, allow_unicode=True, sort_keys=False)
          if _yaml else str(shadow))
    log.info("影子命中率 %s%%（%d/%d 行），裁决: %s",
             shadow["hit_rate"] * 100, shadow["hits"],
             shadow["sample_lines"], shadow["verdict"])


def _cmd_promote(args) -> None:
    """把已批准草案并入注册表；失败（未填 RCA/文件缺失/重名）fail closed。"""
    try:
        registry_path, moved = promote_draft(
            args.rule_id, args.registry,
            [f.strip() for f in args.files.split(",") if f.strip()],
            args.min_tier, args.level, args.required)
    except ValueError as exc:
        log.error("promote failed: %s", exc)
        raise SystemExit(2)
    log.info("rule %s promoted → %s（草案存档 %s）",
             args.rule_id, registry_path, moved)
    log.info("运行 pi-batch rules --check 验证注册表 schema")
    print(registry_path)


def _cmd_draft(args) -> None:
    """把事故/RCA 写成规则草案（人工审批后才可并入注册表）。"""
    failure = " ".join(args.failure or [])
    if not failure and not args.from_memory:
        from .config import log as _log
        _log.error("Provide a failure description or --from-memory TERM")
        raise SystemExit(2)
    if args.from_memory:
        from .memory import find as memory_find
        hits = memory_find(args.from_memory)
        if not hits:
            log.error("No memory records match %r", args.from_memory)
            raise SystemExit(1)
        top = hits[0]
        failure = (f"{args.from_memory} | {top.get('domain') or top.get('stage') or ''} "
                   f"{str(top.get('error') or top.get('status') or top)[:200]}")
        log.info("Using memory hit %r as the observed failure", top.get("session_id", ""))
    path, _ = make_draft(failure, args.category,
                         args.severity, args.rule_id, args.owner)
    log.info("Rule draft written to %s", path)
    log.info("Review the draft, fill requirements/forbidden/verification "
             "with the RCA, then merge it into the domain rules.yaml "
             "(ui-specs/rules.yaml or backend-specs/rules.yaml).")
    print(path)
