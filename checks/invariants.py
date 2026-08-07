import sys
from pathlib import Path
from .config import ROOT

# The well-known development secrets must exist only in
# internal/service/secrets.go (non-test Go). Any other occurrence (a binary
# envOr fallback, a scanner, a wrapper) would split the single source of
# truth for the deny-list and could silently re-introduce hard-coded
# defaults.
DEFAULT_SECRETS = ("development-signing-key-change-me", "development-encryption-key-change-me")
DEFAULT_SECRETS_SOURCE = ROOT / "internal" / "service" / "secrets.go"


def default_secrets_single_source() -> list[str]:
    allowed = {str(DEFAULT_SECRETS_SOURCE.resolve())}
    offenders: list[str] = []
    for directory in (ROOT / "internal", ROOT / "cmd"):
        for path in sorted(directory.rglob("*.go")):
            if path.name.endswith("_test.go"):
                continue
            if str(path.resolve()) in allowed:
                continue
            text = path.read_text(encoding="utf-8")
            for secret in DEFAULT_SECRETS:
                if secret in text:
                    offenders.append(f"{path.relative_to(ROOT)} contains {secret!r}")
    return offenders


def run() -> int:
    source = "\n".join(path.read_text(encoding="utf-8") for path in (ROOT / "internal").rglob("*.go"))
    requirements = {
        "tenant context": "TenantID = tenantID",
        "sensitive-field rejection": "rejectSensitive",
        "hash-chain linkage": "PrevHash",
        "request size limit": "MaxBytesReader",
    }
    missing = [name for name, needle in requirements.items() if needle not in source]
    offenders = default_secrets_single_source()
    if missing:
        print("FAIL: invariants", *missing, sep="\n  ")
        return 1
    if offenders:
        print("FAIL: default secrets outside single source", *offenders, sep="\n  ")
        return 1
    print("PASS: invariants")
    return 0


if __name__ == "__main__":
    sys.exit(run())
