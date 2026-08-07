import sys
from pathlib import Path

# B1-8 (DS-08) mechanical guard: the ingest path must never re-stamp a
# mismatched envelope tenant. The only legal assignment
# `event.TenantID = tenantID` in Ingest is the server-side stamping of an
# EMPTY envelope tenant, which must appear AFTER the ErrTenantMismatch
# consistency check. If the assignment ever moves before the check (or the
# check disappears), this gate fails.

TARGET = "internal/service/service.go"
CHECK_MARKER = "ErrTenantMismatch"
STAMP = "event.TenantID = tenantID"


def run() -> int:
    root = Path(__file__).resolve().parents[1]
    path = root / TARGET
    if not path.exists():
        print(f"FAIL: tenant consistency check — {TARGET} missing")
        return 1
    lines = path.read_text(encoding="utf-8").splitlines()
    check_line = None
    stamps = []
    for lineno, line in enumerate(lines, start=1):
        if check_line is None and CHECK_MARKER in line:
            check_line = lineno
        if STAMP in line:
            stamps.append((lineno, line.strip()))
    if check_line is None:
        print(f"FAIL: tenant consistency check — {CHECK_MARKER} check missing in {TARGET}")
        return 1
    if not stamps:
        print(f"FAIL: tenant consistency check — no {STAMP} server stamping found in {TARGET}")
        return 1
    for lineno, text in stamps:
        if lineno <= check_line:
            print(f"FAIL: tenant consistency check — {TARGET}:{lineno}: {text} appears before the tenant-mismatch check (line {check_line}); silent re-stamping is forbidden (DS-08)")
            return 1
    print(f"PASS: tenant consistency check (mismatch check line {check_line}; {len(stamps)} server stamping site(s) after it)")
    return 0


if __name__ == "__main__":
    sys.exit(run())
