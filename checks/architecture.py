import sys
from pathlib import Path
from .config import ROOT

FORBIDDEN = {
    "internal/domain": {"internal/service", "internal/httpapi", "internal/grpcapi"},
    "internal/security": {"internal/service", "internal/httpapi", "internal/grpcapi"},
    "internal/auth": {"internal/service", "internal/httpapi", "internal/grpcapi"},
    "internal/store": {"internal/service", "internal/httpapi", "internal/grpcapi"},
}


def run() -> int:
    failures = []
    for package, forbidden in FORBIDDEN.items():
        for path in (ROOT / package).glob("*.go"):
            text = path.read_text(encoding="utf-8")
            for target in forbidden:
                if f'"github.com/snaplink/audit-governance/{target}' in text:
                    failures.append(f"{path.relative_to(ROOT)} imports {target}")
    if failures:
        print("FAIL: architecture", *failures, sep="\n  ")
        return 1
    print("PASS: architecture")
    return 0


if __name__ == "__main__":
    sys.exit(run())

