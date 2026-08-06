"""Context routing (Context Engineering): load the project knowledge a
task actually needs, instead of dumping the whole repository into the
prompt.

The rules manifest (pi-batch rules/assess) selects WHICH SPECS apply; the
context router selects WHICH PROJECT DOCUMENTS to load — ADRs, domain
glossaries, API contracts, security baselines — matched by file path
patterns and task keywords from docs/agent-context/context-map.yaml:

    routes:
      - when:
          paths: ["src/order/**", "modules/order/**"]
        load: [docs/adr/order-state-machine.md, docs/contracts/order-api.yaml]
      - when:
          task_contains: ["权限", "认证", "登录", "auth"]
        load: [docs/security/authentication.md, docs/security/authorization.md]

Usage:
    pi-batch context "<task>"                         # task-keyword routing
    pi-batch context "<task>" --paths src/order/x.ts   # + path routing
    pi-batch context --paths modules/order/**          # path-only routing
    pi-batch context "<task>" --json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

from . import config
from .config import log, yaml
from .relevance import _keyword_hit
from .text_io import read_text_bounded

DEFAULT_MAP = Path("docs/agent-context/context-map.yaml")


def load_context_map(path: str = "") -> list:
    """Routes from context-map.yaml; absent/malformed -> [] (advisory)."""
    candidates = [Path(path)] if path else [DEFAULT_MAP]
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            data = yaml.safe_load(read_text_bounded(
                candidate, config.INPUT_MAX_BYTES, "context map")) or {}
        except Exception:
            continue
        routes = data.get("routes", [])
        if isinstance(routes, list):
            return [r for r in routes if isinstance(r, dict)]
    return []


def _path_matches(path: str, pattern: str) -> bool:
    import fnmatch
    norm = path.replace("\\", "/")
    return fnmatch.fnmatch(norm, pattern.replace("\\", "/")) or \
        fnmatch.fnmatch(norm, pattern + "/**")


def match_context(text: str, paths: Optional[list] = None,
                  context_map: Optional[list] = None) -> dict:
    """(matches, files, missing) for the given task text and touched paths."""
    routes = context_map if context_map is not None else load_context_map()
    lowered = (text or "").lower()
    files, missing = [], []
    for route in routes:
        when = route.get("when", {}) if isinstance(route.get("when"), dict) else {}
        path_patterns = when.get("paths", [])
        terms = when.get("task_contains", [])
        path_hit = bool(paths) and any(
            _path_matches(p, str(pattern)) for p in paths for pattern in path_patterns)
        term_hit = any(_keyword_hit(lowered, str(t)) for t in terms)
        if not (path_hit or (term_hit and not path_patterns)):
            continue
        for file in route.get("load", []):
            target = Path(str(file))
            if target.exists():
                files.append(str(target))
            else:
                missing.append(f"{file} (route matched, file missing)")
    # dedupe preserving order
    seen, unique = set(), []
    for f in files:
        if f not in seen:
            seen.add(f)
            unique.append(f)
    return {"files": unique, "missing": missing, "routes_matched": len(routes) > 0}


def format_context(manifest: dict) -> str:
    """Human-readable context manifest for prompt injection."""
    lines = ["## Project context to load (context router)"]
    if not manifest["files"]:
        if manifest["missing"]:
            lines.append("- matched routes, but the referenced documents do "
                         "not exist yet:")
            for missing in manifest["missing"]:
                lines.append(f"    - MISSING: {missing}")
        else:
            lines.append("- (none matched — load nothing extra)")
        return "\n".join(lines)
    for file in manifest["files"]:
        lines.append(f"- {file}")
    for missing in manifest["missing"]:
        lines.append(f"- MISSING: {missing}")
    return "\n".join(lines)


def context_main(argv: list) -> None:
    """`pi-batch context "<task>" [--paths P1,P2] [--json]`."""
    import argparse
    parser = argparse.ArgumentParser(
        prog="pi-batch.py context",
        description="Route the project documents a task needs (Context "
                    "Engineering; docs/agent-context/context-map.yaml).")
    parser.add_argument("task", nargs="*", default=[""], help="task prompt text")
    parser.add_argument("--paths", default="",
                        help="comma-separated touched file paths (glob ok)")
    parser.add_argument("--map", default="", help="context map YAML override")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    args = parser.parse_args(argv)
    text = " ".join(args.task)
    paths = [p.strip() for p in args.paths.split(",") if p.strip()]
    manifest = match_context(text, paths or None,
                             load_context_map(args.map) if args.map else None)
    if args.json:
        print(json.dumps(manifest, ensure_ascii=False, indent=2))
        return
    print(format_context(manifest))
    if manifest["missing"]:
        for item in manifest["missing"]:
            log.warning("context route references a missing file: %s", item)
