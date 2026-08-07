#!/usr/bin/env python3
"""Code organization quality scanner for ai-dev (pure stdlib, no deps).

Enforces the same organization budgets the repo applies to Go files:
- functions max 50 lines (report the longest offenders)
- cyclomatic complexity max 15 (if/elif/for/while/except/with/and/or/bool)
- files max 1000 lines (warn-level; pi-batch.py is exempted until split)
- duplicate function bodies (normalized AST fingerprint, len >= 8 stmts)

Usage:
    python quality.py [paths...]

Exit code 0 when no violations; 1 otherwise. Used as a validator:
    validators: { pyquality: "python {cwd}/quality.py {cwd}" }
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

MAX_FUNC_LINES = 50
MAX_COMPLEXITY = 15
MAX_FILE_LINES = 1000
MIN_DUP_STMTS = 8

# Files that predate the budgets and are scheduled for splitting; the
# scanner reports their overruns but does not fail the run on them.
_LEGACY_OVERRUN_FILES = {"pi-batch.py"}


def _complexity(node: ast.AST) -> int:
    """McCabe-style count of decision points in a function body."""
    score = 0
    for n in ast.walk(node):
        if isinstance(n, (ast.If, ast.For, ast.While, ast.ExceptHandler,
                          ast.With, ast.BoolOp, ast.comprehension)):
            score += 1
        elif isinstance(n, ast.IfExp):
            score += 1
    return score


def _body_lines(node: ast.AST) -> int:
    """Source lines spanned by a function/class body."""
    return max(0, (node.end_lineno or 0) - (node.lineno or 0) + 1)


def _fingerprint(node: ast.AST) -> str:
    """Normalized AST dump: names/constants stripped so equivalent bodies
    with different identifiers compare equal."""
    dumped = ast.dump(node, annotate_fields=True)
    import re
    return re.sub(r"(Name|Constant|Attribute) id='[^']*'|value=\\d+|id='[^']*'",
                  "", dumped)


def scan_file(path: Path, fail_on_legacy: bool) -> tuple[list[str], int]:
    """Return (violations, violation_count) for one Python file. Test files
    get doubled budgets: long scenario tests are the norm, not a smell."""
    violations: list[str] = []
    try:
        text = path.read_text(encoding="utf-8")
        tree = ast.parse(text)
    except SyntaxError as e:
        return [f"{path}: SYNTAX ERROR {e}"], 1
    except Exception as e:  # encoding or other parse issues
        return [f"{path}: cannot parse ({e})"], 1

    lines = text.splitlines()
    legacy = path.name in _LEGACY_OVERRUN_FILES and not fail_on_legacy
    is_test = "/tests/" in str(path) or path.name.startswith("test_")

    if len(lines) > MAX_FILE_LINES * (2 if is_test else 1) and not legacy:
        violations.append(f"{path}: {len(lines)} lines > {MAX_FILE_LINES} budget")

    violations.extend(_scan_functions(tree, path, is_test, legacy))
    violations.extend(_scan_duplicates(tree, path))

    if legacy and violations:
        violations.append(f"{path}: legacy overruns (scheduled for split)")
    return violations, len(violations)


def _scan_functions(tree: ast.AST, path: Path, is_test: bool, legacy: bool) -> list[str]:
    """Function length and complexity budget violations (doubled for tests)."""
    scale = 2 if is_test else 1
    violations = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        n_lines = _body_lines(node)
        cx = _complexity(node)
        if n_lines > MAX_FUNC_LINES * scale and not legacy:
            violations.append(f"{path}:{node.lineno} func '{node.name}' "
                              f"{n_lines} lines > {MAX_FUNC_LINES} budget")
        if cx > MAX_COMPLEXITY * scale and not legacy:
            violations.append(f"{path}:{node.lineno} func '{node.name}' "
                              f"complexity {cx} > {MAX_COMPLEXITY} budget")
    return violations


def _scan_duplicates(tree: ast.AST, path: Path) -> list[str]:
    """Duplicate-body detection: group normalized fingerprints of function
    bodies (excluding tiny getters/setters) and report duplicates."""
    bodies: dict[str, list[tuple[str, int]]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if _body_lines(node) < MIN_DUP_STMTS:
            continue
        fp = _fingerprint(ast.Module(body=node.body, type_ignores=[]))
        bodies.setdefault(fp, []).append((node.name, node.lineno))
    violations = []
    for fp, hits in bodies.items():
        if len(hits) > 1 and not fp.startswith("[Pass"):
            names = ", ".join(f"{n}@{ln}" for n, ln in hits)
            violations.append(f"{path}: duplicated function bodies: {names}")
    return violations


def main(argv: list[str]) -> int:
    # Standalone repo adaptation: upstream defaulted to "ai-dev"; here the
    # root IS the tool, so a bare run scans the current directory.
    # --exclude=PATH or --exclude PATH (repeatable) skips trees that
    # legitimately carry legacy/vendored code (e.g. ai-dev/).
    argv_list = list(argv)
    excludes: list = []
    index = 0
    while index < len(argv_list):
        arg = argv_list[index]
        if arg == "--exclude" and index + 1 < len(argv_list):
            excludes.append(Path(argv_list[index + 1]).resolve())
            argv_list.pop(index)
            argv_list.pop(index)
            continue
        if arg.startswith("--exclude="):
            excludes.append(Path(arg[len("--exclude="):]).resolve())
            argv_list.pop(index)
            continue
        index += 1
    targets = [Path(a) for a in (argv_list or ["."]) if Path(a).exists()]
    if not targets:
        print("quality: no targets found", file=sys.stderr)
        return 1
    fail_on_legacy = "--strict" in argv_list
    total = 0
    for t in targets:
        files = sorted(t.rglob("*.py")) if t.is_dir() else [t]
        for f in files:
            if any(f.resolve().is_relative_to(ex) for ex in excludes):
                continue
            violations, count = scan_file(f, fail_on_legacy)
            for v in violations:
                print(v)
            total += count
    if total:
        print(f"quality: {total} violation(s) found")
        return 1
    print("quality: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
