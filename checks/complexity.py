"""Complexity and size budgets for Go production sources.

The function body is measured by the closing brace at column 0, not by the
next ``func`` line. The previous implementation ended a function at the next
function header, so every top-level ``type``/``var``/``const`` declaration and
doc comment that happened to follow a function was charged to that function:
``SourceSystem.AllowsClient`` in internal/domain/models.go measured 258 lines
for 16 lines of code, and the worst offender in the repository was always the
last function before a block of type declarations. The metric could also be
gamed in the other direction, by inserting declarations after a function that
had already crossed the budget.

Comment lines and blank lines do not count toward the length budget, and
branch keywords are matched as whole words in code (``if(`` and ``} else if``
count, occurrences inside string literals do not), so the number reflects the
code an author has to reason about.
"""

import re
import sys
from pathlib import Path
from .config import get_config, ROOT

FUNCTION = re.compile(r"^func (?:\([^)]*\) )?(\w+)")
BRANCH = re.compile(r"\b(?:if|for|switch|case)\b|&&|\|\|")
LINE_COMMENT = re.compile(r"//.*$")
STRING_LITERAL = re.compile(r'"(?:\\.|[^"\\])*"')


def code_line(line: str) -> str:
    """Strip a line comment, string literals and surrounding whitespace."""
    return STRING_LITERAL.sub('""', LINE_COMMENT.sub("", line)).strip()


def function_spans(lines: list[str]) -> list[tuple[str, int, list[str]]]:
    """Return (name, start_line_index, body) for every top-level function.

    A body ends at the first closing brace in column 0, which is where gofmt
    puts the end of a top-level declaration. Files without that brace (never
    produced by gofmt) fall back to the next function header, so a malformed
    tree degrades to the old measurement instead of crashing the gate.
    """
    starts = [(index, match.group(1)) for index, line in enumerate(lines)
              if (match := FUNCTION.match(line))]
    spans: list[tuple[str, int, list[str]]] = []
    for position, (start, name) in enumerate(starts):
        fallback = starts[position + 1][0] if position + 1 < len(starts) else len(lines)
        end = fallback
        for index in range(start + 1, fallback + 1):
            if index < len(lines) and lines[index].startswith("}"):
                end = index
                break
        spans.append((name, start, [code_line(line) for line in lines[start:end]
                                    if code_line(line)]))
    return spans


def decisions(body: list[str]) -> int:
    """Count branch points in a function body.

    code_line is applied here as well so the count cannot depend on the
    caller: a branch word inside a comment or a string literal is not a
    decision, and ``if(`` or ``} else if`` is.
    """
    return sum(len(BRANCH.findall(code_line(line))) for line in body)


def check_source(path: Path, root: Path = ROOT) -> list[str]:
    """Return a failure line per over-budget function in one file."""
    config = get_config()
    lines = path.read_text(encoding="utf-8").splitlines()
    failures = []
    for name, start, body in function_spans(lines):
        count, branches = len(body), decisions(body)
        if count > config.max_function_lines or branches > config.max_decisions:
            failures.append(
                f"FAIL complexity: {path.relative_to(root)}:{start + 1} {name} "
                f"lines={count} decisions={branches}"
            )
    return failures


def run() -> int:
    failures: list[str] = []
    for path in sorted(ROOT.rglob("*.go")):
        if "_test.go" in path.name or "/bin/" in str(path):
            continue
        failures.extend(check_source(path))
    for failure in failures:
        print(failure)
    print("PASS: complexity" if not failures else f"FAIL: complexity ({len(failures)})")
    return int(bool(failures))


if __name__ == "__main__":
    sys.exit(run())
