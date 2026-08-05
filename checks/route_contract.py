import re
import sys
from pathlib import Path
from .config import ROOT


def normalize(path: str) -> str:
    return re.sub(r"\{[^}]+\}", "{}", path)


def run() -> int:
    runtime = set()
    for path in (ROOT / "internal").rglob("*.go"):
        runtime.update(normalize(value) for value in re.findall(r'HandleFunc\("(?:GET|POST|PUT|DELETE|PATCH) ([^" ]+)', path.read_text(encoding="utf-8")) if value.startswith("/api/"))
    spec = ROOT / "api/openapi/openapi.yaml"
    documented = set(normalize(value) for value in re.findall(r"^  (/api/.*):\s*$", spec.read_text(encoding="utf-8"), re.MULTILINE))
    missing = sorted(runtime - documented)
    extra = sorted(documented - runtime)
    if missing or extra:
        print("FAIL: route contract", *(missing + extra), sep="\n  ")
        return 1
    print("PASS: route contract")
    return 0


if __name__ == "__main__":
    sys.exit(run())

