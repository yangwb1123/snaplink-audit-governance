"""Locate ingest invariants by function, not by file path.

The ingest path carries three mechanical invariants that the quality gate
pins with source markers: the tenant-mismatch check fires before the server
strips a client stream_id, the strip precedes the stream derivation, and the
server stamp follows it. checks/tenant_consistency.py and
checks/stream_consistency.py both pinned them to a single path
(`internal/service/service.go`).

That is a maintenance trap, not a contract: the service package is split by
concern (service.go orchestration, ingest_validation.go, query_match.go,
export.go, ...), and the file holding those markers moves for reasons that
have nothing to do with this invariant. A guard that fails on a rename
teaches people to re-pin the path instead of reading the failure, and a guard
that only reads one file stops seeing a second copy of the markers.

This helper locates the ingest function by declaration across the package and
returns the lines of its body, so the ordering assertions keep their meaning
while being independent of where the code lives.
"""

from __future__ import annotations

import re

# The receiver method that owns the ingest invariants. Matched loosely so a
# receiver rename (Service -> AuditService) does not silently disable the
# guards: the error message then names the missing function.
INGEST_DECL = re.compile(r"^func\s+\([^)]*\)\s+Ingest\s*\(")


def ingest_body(root, receiver: str = "Ingest") -> tuple[str, int, int] | None:
    """Return (source, first_line, last_line) of the receiver method `receiver`
    found in any non-test Go file of the package that `root` is a path into,
    or None when it is not defined.

    `root` is a file inside the package (as the callers already hold); the
    package directory is its parent. Files are scanned in sorted order so the
    result is deterministic, and a second definition of the same method (which
    Go would not compile) is reported by returning the first.
    """
    package = root.resolve().parent
    if root.suffix == ".go":
        root = root.resolve()
    else:  # a directory was passed
        package = root.resolve()
    for path in sorted(package.glob("*.go")):
        if path.name.endswith("_test.go"):
            continue
        lines = path.read_text(encoding="utf-8").splitlines()
        for index, line in enumerate(lines):
            if not INGEST_DECL.match(line) or receiver not in line:
                continue
            depth, end = 0, index
            for cursor in range(index, len(lines)):
                depth += lines[cursor].count("{") - lines[cursor].count("}")
                end = cursor
                if depth <= 0 and cursor > index:
                    break
            return "\n".join(lines), index + 1, end + 1
    return None
