import sys
from pathlib import Path

# Stream consistency (strip) mechanical guard: the ingest path must never let
# a client-supplied stream_id participate in stream resolution. The only
# legal assignment `event.StreamID = ""` in Ingest is the server-side strip
# of an envelope-supplied value, and it must appear AFTER the
# ErrTenantMismatch consistency check (DS-08 fires first) and BEFORE the
# first `event.Stream()` derivation. The server stamp
# `event.StreamID = streamID` must remain after the derivation so events and
# receipts always carry the server-derived stream. If the strip is removed,
# moved after the derivation, or the stamp disappears, this gate fails.

TARGET = "internal/service/service.go"
TENANT_CHECK_MARKER = "ErrTenantMismatch"
STRIP = "event.StreamID = \"\""
DERIVE = "event.Stream()"
STAMP = "event.StreamID = streamID"


def run(root=None) -> int:
    if root is None:
        root = Path(__file__).resolve().parents[1]
    path = root / TARGET
    if not path.exists():
        print(f"FAIL: stream consistency check — {TARGET} missing")
        return 1
    lines = path.read_text(encoding="utf-8").splitlines()
    tenant_check_line = None
    strip_line = None
    derive_line = None
    stamp_line = None
    for lineno, line in enumerate(lines, start=1):
        if tenant_check_line is None and TENANT_CHECK_MARKER in line:
            tenant_check_line = lineno
        if strip_line is None and STRIP in line:
            strip_line = lineno
        if derive_line is None and DERIVE in line:
            derive_line = lineno
        if stamp_line is None and STAMP in line:
            stamp_line = lineno
    if strip_line is None:
        print(f"FAIL: stream consistency check — no {STRIP} strip found in {TARGET}")
        return 1
    if tenant_check_line is None:
        print(f"FAIL: stream consistency check — {TENANT_CHECK_MARKER} check missing in {TARGET}")
        return 1
    if strip_line <= tenant_check_line:
        print(f"FAIL: stream consistency check — strip (line {strip_line}) does not follow the tenant-mismatch check (line {tenant_check_line}); DS-08 must fire before stream stripping")
        return 1
    if derive_line is None:
        print(f"FAIL: stream consistency check — {DERIVE} derivation call missing in {TARGET}")
        return 1
    if strip_line >= derive_line:
        print(f"FAIL: stream consistency check — strip (line {strip_line}) does not precede the stream derivation (line {derive_line}); a client stream_id must never reach event.Stream()")
        return 1
    if stamp_line is None:
        print(f"FAIL: stream consistency check — no {STAMP} server stamping found in {TARGET}")
        return 1
    if stamp_line <= derive_line:
        print(f"FAIL: stream consistency check — {TARGET}:{stamp_line}: {STAMP} appears before the stream derivation (line {derive_line}); the server stamp must follow derivation")
        return 1
    print(f"PASS: stream consistency check (strip line {strip_line}; derivation line {derive_line}; stamp line {stamp_line})")
    return 0


if __name__ == "__main__":
    sys.exit(run())
