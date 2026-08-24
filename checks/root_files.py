import sys
from pathlib import Path
from .config import ROOT

ALLOWED = {".git", ".gitignore", ".dockerignore", "Dockerfile", "README.md", "AGENTS.md",
           "Makefile", "go.mod", "go.sum", "cli.py", "engineering.yaml", "api", "cmd",
           "deploy", "docs", "internal", "migrations", "checks", "bin", "test",
           "scripts", "web", ".trends", ".pi-batch", "__pycache__",
           # ai-batch-runner tooling synced by the tooling-sync commit: quality
           # gate, pipeline shell, and the runner package. logs/ and
           # .pi-batch.lock are runner runtime artifacts (gitignored, present
           # during local runs); the scan must tolerate them like .trends/.
           "quality.py", "pi-batch.py", "pi-batch.yaml", "pbatch", "logs", ".pi-batch.lock"}


def run() -> int:
    unexpected = sorted(
        path.name
        for path in ROOT.iterdir()
        if path.name not in ALLOWED and not path.name.startswith(".pi-batch.lock")
    )
    if unexpected:
        print("FAIL: root files", *unexpected, sep="\n  ")
        return 1
    print("PASS: root files")
    return 0


if __name__ == "__main__":
    sys.exit(run())
