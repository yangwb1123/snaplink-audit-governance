import subprocess
import sys
from pathlib import Path
from .config import ROOT

# The batch runner (ai-batch-runner) is used from its own checkout: its engine
# (pi-batch.py + pbatch/), validator registry (quality.py) and campaign configs
# are NOT vendored here. run-campaign.sh syncs only the campaign config and
# pipeline template into the gitignored .pi-batch/ directory at run time, so no
# runner entry belongs in ALLOWED and the gate now fails closed if one is
# re-vendored into the repository root.
ALLOWED = {".git", ".gitignore", ".dockerignore", "Dockerfile", "README.md", "AGENTS.md",
           "Makefile", "go.mod", "go.sum", "cli.py", "engineering.yaml", "api", "cmd",
           "deploy", "docs", "internal", "migrations", "checks", "bin", "test",
           "scripts", "web", ".github", ".trends", ".pi-batch", "__pycache__"}

# Locally produced, gitignored runtime state. The root policy governs the
# tracked repository layout, so a directory that only exists because the
# service (or the batch runner) ran here must not make the gate
# environment-dependent: `data/` is the default -state/-archive path of
# audit-api and audit-governance-worker, so requiring its absence would fail
# the gate on exactly the machines that used the service.
RUNTIME = {"data", "bin", ".trends", ".pi-batch", "logs", ".pi-batch.lock", "__pycache__"}


def _gitignored(name: str) -> bool:
    """True when git ignores this root entry. Keeps the gate honest for any
    other locally generated artifact, without replacing the explicit RUNTIME
    set above (that set also covers a checkout without a .git directory)."""
    try:
        return subprocess.run(
            ["git", "check-ignore", "-q", name], cwd=str(ROOT), check=False
        ).returncode == 0
    except OSError:  # git unavailable: fall back to the static sets
        return False


def run() -> int:
    unexpected = sorted(
        path.name
        for path in ROOT.iterdir()
        if path.name not in ALLOWED
        and path.name not in RUNTIME
        and not _gitignored(path.name)
    )
    if unexpected:
        print("FAIL: root files", *unexpected, sep="\n  ")
        return 1
    print("PASS: root files")
    return 0


if __name__ == "__main__":
    sys.exit(run())
