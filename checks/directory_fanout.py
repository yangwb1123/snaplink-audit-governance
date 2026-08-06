import subprocess
import sys
from pathlib import Path

from .config import get_config, ROOT

# Static skip set: git's own .git directory is never reported by
# `git check-ignore`, and the gate must tolerate local build/output dirs even
# when git metadata is unavailable (fallback path below).
STATIC_SKIP = {".git", "bin", "vendor", "__pycache__", ".trends"}


def ignored_directories() -> set[Path]:
    """Return every directory git considers ignored (gitignore-aware).

    One batched `git check-ignore --stdin` invocation keeps the recursive
    scan cheap. Falls back to an empty set (static skip set only) when git
    is unavailable, so a checkout without git still passes.
    """
    directories = [ROOT] + [path for path in ROOT.rglob("*") if path.is_dir()]
    candidates = [directory for directory in directories
                  if not any(part in STATIC_SKIP for part in directory.parts)]
    if not candidates:
        return set()
    try:
        result = subprocess.run(
            ["git", "check-ignore", "--stdin"],
            input="\n".join(directory.as_posix() for directory in candidates) + "\n",
            capture_output=True, text=True, check=False, cwd=str(ROOT),
        )
    except OSError:
        return set()
    ignored = set()
    for line in result.stdout.splitlines():
        if line:
            ignored.add(Path(line))
    return ignored


def run() -> int:
    limit = get_config().max_subdirs
    ignored = ignored_directories()
    failures = []
    for directory in [ROOT] + [path for path in ROOT.rglob("*") if path.is_dir()]:
        if directory in ignored or any(part in STATIC_SKIP for part in directory.parts):
            continue
        count = 0
        for child in directory.iterdir():
            if not child.is_dir():
                continue
            if child in ignored or any(part in STATIC_SKIP for part in child.parts):
                continue
            count += 1
        if count > limit:
            failures.append(f"{directory.relative_to(ROOT)} has {count} subdirectories")
    if failures:
        print("FAIL: directory fanout", *failures, sep="\n  ")
        return 1
    print(f"PASS: directory fanout (max {limit})")
    return 0


if __name__ == "__main__":
    sys.exit(run())
