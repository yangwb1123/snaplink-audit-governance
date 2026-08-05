import sys
from pathlib import Path
from .config import ROOT


def run() -> int:
    source = "\n".join(path.read_text(encoding="utf-8") for path in (ROOT / "internal").rglob("*.go"))
    requirements = {
        "tenant context": "TenantID = tenantID",
        "sensitive-field rejection": "rejectSensitive",
        "hash-chain linkage": "PrevHash",
        "request size limit": "MaxBytesReader",
    }
    missing = [name for name, needle in requirements.items() if needle not in source]
    if missing:
        print("FAIL: invariants", *missing, sep="\n  ")
        return 1
    print("PASS: invariants")
    return 0


if __name__ == "__main__":
    sys.exit(run())

