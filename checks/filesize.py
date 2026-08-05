import sys
from pathlib import Path
from .config import get_config, ROOT


def check_file(path: Path, root: Path = ROOT) -> bool:
    if path.suffix != ".go" or "_test.go" in path.name:
        return True
    rel = str(path.resolve().relative_to(root.resolve()))
    if any(part in rel for part in ("/.git/", "/bin/", "/vendor/")):
        return True
    lines = len(path.read_text(encoding="utf-8").splitlines())
    if lines > get_config().max_go_lines:
        print(f"FAIL filesize: {rel} has {lines} lines")
        return False
    return True


def run(files: list[Path] | None = None) -> int:
    paths = files or sorted(ROOT.rglob("*.go"))
    ok = all(check_file(path) for path in paths)
    print("PASS: filesize" if ok else "FAIL: filesize")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(run([Path(value) for value in sys.argv[1:]] or None))

