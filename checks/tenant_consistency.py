import sys
from pathlib import Path

from .ingest_invariants import ingest_body

# B1-8 (DS-08) mechanical guard: the ingest path must never re-stamp a
# mismatched envelope tenant. The only legal assignment
# `event.TenantID = tenantID` in Ingest is the server-side stamping of an
# EMPTY envelope tenant, which must appear AFTER the ErrTenantMismatch
# consistency check. If the assignment ever moves before the check (or the
# check disappears), this gate fails.

# The invariants are located inside Service.Ingest, whatever file it lives in
# (see checks/ingest_invariants.py), not in one pinned path.
PACKAGE = "internal/service"
FUNCTION = "Ingest"
CHECK_MARKER = "ErrTenantMismatch"
STAMP = "event.TenantID = tenantID"


def run(root=None) -> int:
    if root is None:
        root = Path(__file__).resolve().parents[1]
    located = ingest_body(root / PACKAGE / "service.go", FUNCTION)
    if located is None:
        print(f"FAIL: tenant consistency check — func ({FUNCTION}) not found in {PACKAGE}")
        return 1
    source, first, last = located
    lines = source.splitlines()[first - 1:last]
    check_line = None
    stamps = []
    for lineno, line in enumerate(lines, start=1):
        if check_line is None and CHECK_MARKER in line:
            check_line = lineno
        if STAMP in line:
            stamps.append((lineno, line.strip()))
    if check_line is None:
        print(f"FAIL: tenant consistency check — {CHECK_MARKER} check missing in {FUNCTION}")
        return 1
    if not stamps:
        print(f"FAIL: tenant consistency check — no {STAMP} server stamping found in {FUNCTION}")
        return 1
    for lineno, text in stamps:
        if lineno <= check_line:
            print(f"FAIL: tenant consistency check — {PACKAGE} Ingest:{first + lineno - 1}: {text} appears before the tenant-mismatch check (line {check_line}); silent re-stamping is forbidden (DS-08)")
            return 1
    print(f"PASS: tenant consistency check (mismatch check line {check_line}; {len(stamps)} server stamping site(s) after it)")
    return 0


if __name__ == "__main__":
    sys.exit(run())
