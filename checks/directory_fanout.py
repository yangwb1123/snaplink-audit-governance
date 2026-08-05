import sys
from pathlib import Path
from .config import get_config, ROOT


def run() -> int:
    limit = get_config().max_subdirs
    failures = []
    for directory in [ROOT] + [path for path in ROOT.rglob("*") if path.is_dir()]:
        if any(part in {".git", "bin", "vendor", "__pycache__", ".trends"} for part in directory.parts):
            continue
        count = sum(path.is_dir() for path in directory.iterdir())
        if count > limit:
            failures.append(f"{directory.relative_to(ROOT)} has {count} subdirectories")
    if failures:
        print("FAIL: directory fanout", *failures, sep="\n  ")
        return 1
    print(f"PASS: directory fanout (max {limit})")
    return 0


if __name__ == "__main__":
    sys.exit(run())

