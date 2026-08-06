import sys
from pathlib import Path
from .config import ROOT

ALLOWED = {".git", ".gitignore", ".dockerignore", "Dockerfile", "README.md", "AGENTS.md",
           "Makefile", "go.mod", "go.sum", "cli.py", "engineering.yaml", "api", "cmd",
           "deploy", "docs", "internal", "migrations", "checks", "bin", "test",
           ".trends", "__pycache__"}


def run() -> int:
    unexpected = sorted(path.name for path in ROOT.iterdir() if path.name not in ALLOWED)
    if unexpected:
        print("FAIL: root files", *unexpected, sep="\n  ")
        return 1
    print("PASS: root files")
    return 0


if __name__ == "__main__":
    sys.exit(run())
