import re
import sys
from pathlib import Path

# B1-1 (S1) manifest gate: no non-verify deployment manifest may enable
# development authentication. Dev auth is environment-allowlisted by design
# (AUDIT_ALLOW_DEV_AUTH=true is the only legal way to run it), so a manifest
# that hard-codes it would silently bless a configuration the runtime treats
# as production.
#
# Files whose names contain "verify" (docker-compose.verify.yml,
# prometheus.verify.yml, prometheus-rules.verify.yml) are the explicit
# local-only validation stack and are exempt: they declare the allowlist
# environment variable, which is the sanctioned dev path. Any future
# production manifest (helm values, terraform, plain compose) with dev auth
# enabled fails the gate here.

DEV_AUTH_TRUE = re.compile(
    r"(AUDIT_ALLOW_DEV_AUTH|allow-dev-auth)\s*[:=]\s*(true|1|\"true\"|'true')",
    re.IGNORECASE,
)


def run() -> int:
    root = Path(__file__).resolve().parents[1]
    failures = []
    scanned = 0
    for path in sorted((root / "deploy").glob("*.yml")):
        if "verify" in path.name:
            continue
        scanned += 1
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if DEV_AUTH_TRUE.search(line):
                failures.append(f"{path.relative_to(root)}:{lineno}: development auth enabled in a non-verify deployment manifest")
    if failures:
        print("FAIL: dev-auth manifest scan", *failures, sep="\n  ")
        return 1
    print(f"PASS: dev-auth manifest scan (non-verify manifests scanned: {scanned})")
    return 0


if __name__ == "__main__":
    sys.exit(run())
