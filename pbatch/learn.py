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
CATEGORIES = ("backend", "frontend", "product", "database", "security",
              "testing", "devops")
SEVERITIES = ("error", "warning", "advice")


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


def learn_main(argv: list) -> None:
    """`pi-batch learn "<failure>" --category X --severity Y [--rule-id Z]`."""
    import argparse
    parser = argparse.ArgumentParser(
        prog="pi-batch.py learn",
        description="Turn an incident/RCA into a rule draft (Evolution "
                    "Engineering: failure -> draft -> human approval -> registry).")
    parser.add_argument("failure", nargs="*", default=[],
                        help="observed failure description (or use --from-memory)")
    parser.add_argument("--category", default="backend", choices=CATEGORIES,
                        help="rule category")
    parser.add_argument("--severity", default="error", choices=SEVERITIES,
                        help="rule severity")
    parser.add_argument("--rule-id", default="", help="rule id (default DRAFT-<date>)")
    parser.add_argument("--owner", default="", help="rule owner")
    parser.add_argument("--from-memory", default="",
                        help="use the top memory search hit as the failure "
                             "description (Evolution loop: incident -> draft)")
    args = parser.parse_args(argv)
    if not args.failure and not args.from_memory:
        parser.error("Provide a failure description or --from-memory TERM")
    if args.from_memory:
        from .memory import find as memory_find
        hits = memory_find(args.from_memory)
        if not hits:
            log.error("No memory records match %r", args.from_memory)
            sys.exit(1)
        top = hits[0]
        failure = (f"{args.from_memory} | {top.get('domain') or top.get('stage') or ''} "
                   f"{str(top.get('error') or top.get('status') or top)[:200]}")
        log.info("Using memory hit %r as the observed failure", top.get("session_id", ""))
    else:
        failure = " ".join(args.failure)
    path, draft = make_draft(failure, args.category,
                             args.severity, args.rule_id, args.owner)
    log.info("Rule draft written to %s", path)
    log.info("Review the draft, fill requirements/forbidden/verification "
             "with the RCA, then merge it into the domain rules.yaml "
             "(ui-specs/rules.yaml or backend-specs/rules.yaml).")
    print(path)
