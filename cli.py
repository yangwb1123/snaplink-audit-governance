#!/usr/bin/env python3
"""Snaplink Audit Governance engineering CLI.

This keeps the command surface of the sibling Snaplink project while using
checks that are native to this repository.  Run ``python cli.py help`` for
the complete list.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def run(*args: str, cwd: Path = ROOT, check: bool = False) -> int:
    print("$", " ".join(args))
    return subprocess.run(list(args), cwd=str(cwd), check=False).returncode


def gofmt_files() -> list[str]:
    result = subprocess.run(
        ["gofmt", "-l", "."], cwd=str(ROOT), text=True,
        capture_output=True, check=False
    )
    return [line for line in result.stdout.splitlines() if line and ".git/" not in line]


def _defined_in_cli(command) -> bool:
    """True when the callable is implemented in this file.

    __module__ is "cli" when the CLI is imported (the gate's own suite, or a
    test) and "__main__" when it is run as a script, so a stage defined here
    has to be recognised by its source file rather than by its module name.
    """
    code = getattr(command, "__code__", None)
    if code is None:
        return False
    return Path(code.co_filename).resolve() == Path(__file__).resolve()


def stage_label(command) -> str:
    """CLI name of a gate stage, and therefore the name of the command that
    re-runs exactly that stage.

    Gate stages are imported as ``from checks.<stage> import run as <stage>``,
    so ``__name__`` is always "run" and the stage has to be named after the
    module that defines it; stages defined in this file (fmt, vet, test, ...)
    fall back to the function name. Every label must be a command in COMMANDS:
    a gate failure prints ``FAIL: <label>``, and that message is only useful if
    ``python3 cli.py <label>`` re-runs the stage.
    """
    module = "" if _defined_in_cli(command) else getattr(command, "__module__", "")
    stem = "" if module in {"__main__", "cli", "", "builtins"} else module.rsplit(".", 1)[-1]
    return (stem or command.__name__).removeprefix("cmd_").replace("_", "-")


def cmd_fmt() -> int:
    files = gofmt_files()
    if files:
        print("Unformatted files:", file=sys.stderr)
        print("\n".join(files), file=sys.stderr)
        return 1
    print("gofmt: clean")
    return 0


def cmd_test() -> int:
    return run("go", "test", "./...")


def cmd_race() -> int:
    return run("go", "test", "-race", "-count=1", "./...")


def cmd_bench() -> int:
    return run("go", "test", "-run=^$", "-bench=.", "-benchmem", "./...")


def cmd_vet() -> int:
    return run("go", "vet", "./...")


def cmd_build() -> int:
    bin_dir = ROOT / "bin"
    bin_dir.mkdir(exist_ok=True)
    targets = (("audit-api", "./cmd/audit-api"),
               ("audit-governance-worker", "./cmd/audit-governance-worker"),
               ("audit-outbox-relay", "./cmd/audit-outbox-relay"),
               ("audit-kafka-consumer", "./cmd/audit-kafka-consumer"),
               ("audit-kafka-dlq-replay", "./cmd/audit-kafka-dlq-replay"),
               ("audit-projector", "./cmd/audit-projector"),
               ("audit-pg-migrate", "./cmd/audit-pg-migrate"))
    for name, package in targets:
        target = bin_dir / name
        if run("go", "build", "-o", str(target), package) != 0:
            return 1
    return 0


def cmd_consistency_check() -> int:
    """Compare the pure signer/archive keys from the built API and worker."""
    from checks.consistency_key import run as consistency_key
    return consistency_key(root=ROOT)


def cmd_predeploy() -> int:
    """Build release binaries, then run the network-free parity gate."""
    if cmd_build() != 0:
        return 1
    return cmd_consistency_check()


def cmd_check_filesize() -> int:
    # Single implementation lives in checks/filesize.py, which is what the
    # quality gate runs. This launcher used to carry a second copy of the same
    # budget (and a second parser of engineering.yaml); it now delegates so a
    # command and the gate can never disagree.
    from checks.filesize import run
    return run()


def cmd_complexity() -> int:
    # Single implementation lives in checks/complexity.py (the stage the gate
    # runs). The inlined copy diverged: it scanned _test.go files and applied
    # its own engineering.yaml parsing.
    from checks.complexity import run
    return run()


def cmd_check_routes() -> int:
    # Single shared implementation lives in checks/route_contract.py; this
    # reads cli.ROOT at call time so the monkeypatch test pattern works.
    from checks.route_contract import run
    return run(root=ROOT)


def cmd_openapi_contract() -> int:
    """Validate the checked-in OpenAPI document with the pinned Go validator."""
    return run("go", "run", "./cmd/openapi-contract")


def cmd_check_root() -> int:
    # Single implementation lives in checks/root_files.py, which is what the
    # quality gate runs. This launcher used to carry a second, smaller copy of
    # the same policy that had already drifted (it rejected the vendored
    # runner tooling on a clean tree), so it now delegates instead of
    # re-deriving the allow-list.
    from checks.root_files import run
    return run()


def cmd_generate() -> int:
    from checks.proto_sync import run as proto_sync
    required = [ROOT / "api" / "openapi" / "openapi.yaml",
                ROOT / "api" / "asyncapi" / "asyncapi.yaml",
                ROOT / "api" / "proto" / "audit.proto"]
    missing = [str(path.relative_to(ROOT)) for path in required if not path.exists()]
    if missing:
        print("missing engineering contracts:", *missing, sep="\n  ")
        return 1
    # R7: the generate command reflects the real sync state (descriptor
    # comparison + version pins + replay) instead of reporting a false
    # "no regeneration required" success.
    return proto_sync(ROOT)


def cmd_architecture() -> int:
    # Single implementation lives in checks/architecture.py, the dependency
    # direction stage the gate runs. The inlined copy below used to re-declare
    # the same forbidden-import map.
    from checks.architecture import run
    return run()


def cmd_diagnose() -> int:
    print(f"root: {ROOT}")
    print(f"go files: {len(list(ROOT.rglob('*.go')))}")
    print(f"test files: {len(list(ROOT.rglob('*_test.go')))}")
    print(f"python: {sys.executable}")
    return run("go", "env", "GOVERSION", "GOMOD")


def cmd_health_report() -> int:
    checks = {
        "contracts": cmd_generate,
        "format": cmd_fmt,
        "architecture": cmd_architecture,
        "routes": cmd_check_routes,
    }
    failures = 0
    for name, check in checks.items():
        code = check()
        print(f"health {name}: {'OK' if code == 0 else 'FAIL'}")
        failures += int(code != 0)
    return int(failures != 0)


def cmd_trend() -> int:
    path = ROOT / ".trends" / "engineering.jsonl"
    path.parent.mkdir(exist_ok=True)
    record = {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "go_files": len(list(ROOT.rglob("*.go"))),
        "test_files": len(list(ROOT.rglob("*_test.go"))),
        "gofmt_clean": not bool(gofmt_files()),
    }
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, sort_keys=True) + "\n")
    print(f"trend: recorded {path.relative_to(ROOT)}")
    return 0


def cmd_check_exemptions() -> int:
    # Single implementation lives in checks/exemptions.py (the gate stage).
    from checks.exemptions import run
    return run()


def cmd_check_invariants() -> int:
    # Single implementation lives in checks/invariants.py, the stage the gate
    # runs (including the default-secrets single-source scan).
    from checks.invariants import run
    return run()


def cmd_adr_compliance() -> int:
    # Single implementation lives in checks/adr_compliance.py (the gate stage).
    # The inlined copy required a different evidence set than the stage the
    # gate actually runs, so the command could pass while the gate failed.
    from checks.adr_compliance import run
    return run()


def cmd_self_test() -> int:
    required = {"check", "test", "race", "vet", "fmt", "build", "check-routes", "consistency-check", "help"}
    missing = sorted(required - COMMANDS.keys())
    if missing:
        print("CLI self-test failed:", *missing, sep="\n  ")
        return 1
    return cmd_check_routes()


def cmd_skill_test() -> int:
    skills = ROOT / "skills"
    if not skills.exists():
        print("skill-test: no local skills configured")
        return 0
    failures = 0
    for path in sorted(skills.glob("*/test_skill.py")):
        failures += int(run(sys.executable, "-m", "pytest", str(path)) != 0)
    return int(failures != 0)


def cmd_modules() -> int:
    print("module catalog: Go module")
    return run("go", "list", "./...")


def cmd_capabilities() -> int:
    contracts = [ROOT / "api/openapi/openapi.yaml", ROOT / "api/asyncapi/asyncapi.yaml", ROOT / "api/proto/audit.proto"]
    missing = [str(path.relative_to(ROOT)) for path in contracts if not path.exists()]
    if missing:
        print("capability contracts missing:", *missing, sep="\n  ")
        return 1
    print(f"capabilities: {len(contracts)} contracts present")
    return 0


def cmd_sdk_surface() -> int:
    return check_module("route_contract")


def cmd_python_checks() -> int:
    # Regression tests deliberately exercise failing-check output. Buffer
    # successful tests so those expected negative cases do not look like a
    # failing quality gate in CI logs; unittest still reports their output if
    # the test itself fails.
    return run(sys.executable, "-m", "unittest", "discover", "-b", "-s", "checks", "-p", "test_*.py")


def check_module(name: str) -> int:
    return run(sys.executable, "-m", f"checks.{name}")


def cmd_coverage() -> int:
    from checks.coverage import run
    return run()


def cmd_check() -> int:
    for command in (cmd_fmt, cmd_check_filesize, cmd_check_routes,
                    cmd_openapi_contract, cmd_vet, cmd_test):
        if command() != 0:
            return 1
    return 0


def cmd_accept() -> int:
    return cmd_quality()


def cmd_quality() -> int:
    """Run the full post-change engineering gate."""
    # Recursion guard: the gate runs the checks unittest suite
    # (cmd_python_checks), whose route-contract tests invoke `cli.py quality`
    # and `cli.py check` end-to-end. Mark the run so those tests skip and
    # cannot re-enter the gate (infinite recursion / wasted minutes).
    os.environ["AUDIT_QUALITY_ACTIVE"] = "1"
    from checks.adr_compliance import run as adr_compliance
    from checks.architecture import run as architecture
    from checks.build import run as build
    from checks.complexity import run as complexity
    from checks.directory_fanout import run as directory_fanout
    from checks.exemptions import run as exemptions
    from checks.filesize import run as filesize
    from checks.invariants import run as invariants
    from checks.make_help import run as make_help
    from checks.root_business_code import run as root_business_code
    from checks.root_files import run as root_files
    from checks.asyncapi_channels import run as asyncapi_channels
    from checks.contract_fields import run as contract_fields
    from checks.dev_auth_manifest import run as dev_auth_manifest
    from checks.replay_round_reset import run as replay_round_reset
    from checks.prometheus_rules import run as prometheus_rules
    from checks.proto_sync import run as proto_sync
    from checks.route_contract import run as route_contract
    from checks.sensitive_logging import run as sensitive_logging
    from checks.stream_consistency import run as stream_consistency
    from checks.tenant_consistency import run as tenant_consistency
    from checks.tenant_scope import run as tenant_scope
    for command in (cmd_fmt, filesize, complexity, architecture, directory_fanout,
                    root_files, root_business_code, invariants, exemptions,
                    adr_compliance, make_help, route_contract, cmd_openapi_contract,
                    contract_fields,
                    asyncapi_channels,
                    proto_sync,  # proto drift guard: fails fast, before the ~95s Go stages
                    dev_auth_manifest, tenant_consistency, tenant_scope, stream_consistency,
                    replay_round_reset,
                    prometheus_rules,
                    sensitive_logging,
                    cmd_vet, cmd_python_checks, cmd_test, cmd_race, build):
        # Label every stage and report its cost: a bare loop left a failing
        # run with only the last check's output, and no way to tell whether
        # the ~95s Go stages or the fast static guards were to blame. The label
        # is also the re-runnable command (`python3 cli.py <label>`).
        label = stage_label(command)
        started = time.monotonic()
        code = command()
        elapsed = time.monotonic() - started
        if code != 0:
            print(f"FAIL: {label} ({elapsed:.1f}s)", file=sys.stderr)
            return code
        print(f"PASS: {label} ({elapsed:.1f}s)")
    print("QUALITY PASS")
    return 0


def cmd_harness() -> int:
    # cmd_check now includes the route stage (cmd_check_routes), so listing it
    # again here would run the gate twice per harness run.
    for command in (cmd_check, cmd_complexity):
        if command() != 0:
            return 1
    return 0


def cmd_lint() -> int:
    if shutil.which("golangci-lint"):
        return run("golangci-lint", "run", "--timeout", "5m")
    print("golangci-lint is not installed; use `go install` or run `go vet ./...`.", file=sys.stderr)
    return 2


def cmd_security_scan() -> int:
    if not shutil.which("govulncheck") and not shutil.which("gosec"):
        print("security scanners are not installed (govulncheck/gosec).", file=sys.stderr)
        return 2
    if shutil.which("govulncheck") and run("govulncheck", "./...") != 0:
        return 1
    if shutil.which("gosec"):
        return run("gosec", "-quiet", "./...")
    return 0


def cmd_review() -> int:
    for path in (ROOT / "docs").glob("*VALIDATION*"):
        print(path.read_text(encoding="utf-8"))
    return 0


def cmd_help() -> int:
    print(__doc__.strip())
    print("\nCommands:")
    for name in sorted(COMMANDS):
        print(f"  {name}")
    return 0


COMMANDS = {
    "generate": cmd_generate,
    "check": cmd_check,
    "check-filesize": cmd_check_filesize,
    "directory-fanout": lambda: check_module("directory_fanout"),
    "accept": cmd_accept,
    "ci": cmd_quality,
    "harness": cmd_harness,
    "quality": cmd_quality,
    "complexity": cmd_complexity,
    "architecture": cmd_architecture,
    "coverage": cmd_coverage,
    "evaluate": cmd_coverage,
    "review": cmd_review,
    "diagnose": cmd_diagnose,
    "trend": cmd_trend,
    "health-report": cmd_health_report,
    "check-exemptions": cmd_check_exemptions,
    "self-test": cmd_self_test,
    "check-invariants": cmd_check_invariants,
    "check-routes": cmd_check_routes,
    "check-openapi": cmd_openapi_contract,
    # Aliases that match the label cmd_quality prints for these stages, so the
    # "FAIL: <label>" line is always a runnable command.
    "openapi-contract": cmd_openapi_contract,
    "python-checks": cmd_python_checks,
    "check-root": cmd_check_root,
    "root-files": lambda: check_module("root_files"),
    "root-business-code": lambda: check_module("root_business_code"),
    "make-help": lambda: check_module("make_help"),
    "adr-compliance": cmd_adr_compliance,
    "acceptance": lambda: check_module("acceptance"),
    "check-test": cmd_test,
    "skill-test": cmd_skill_test,
    "test": cmd_test,
    "race": cmd_race,
    "bench": cmd_bench,
    "vet": cmd_vet,
    "fmt": cmd_fmt,
    "build": cmd_build,
    "consistency-check": cmd_consistency_check,
    "predeploy": cmd_predeploy,
    "configure": cmd_generate,
    "modules": cmd_modules,
    "capabilities": cmd_capabilities,
    "sdk-surface": cmd_sdk_surface,
    "profiles": cmd_build,
    "lint": cmd_lint,
    "security-scan": cmd_security_scan,
    "help": cmd_help,
}

# Every gate stage gets a re-runnable entrypoint. A stage that can only fail
# inside `python3 cli.py quality` costs a full gate cycle (~10 min) to
# re-check one thing, so each checks/ module behind a stage is reachable by
# name. checks/test_quality_checks.py asserts that coverage stays complete.
# Only modules without an entrypoint are added: an existing command already
# wraps the same policy (cmd_architecture, cmd_coverage, cmd_build,
# cmd_consistency_check, ...), and replacing a wrapper with a bare module call
# would silently change its arguments.
STAGE_MODULES = (
    "adr_compliance", "asyncapi_channels", "complexity", "contract_fields",
    "coverage", "dev_auth_manifest", "directory_fanout", "exemptions",
    "filesize", "health_report", "invariants", "make_help", "prometheus_rules",
    "proto_sync", "replay_round_reset", "root_business_code", "root_files",
    "route_contract", "sensitive_logging", "stream_consistency",
    "tenant_consistency", "tenant_scope",
)
COMMANDS.update({
    name: handler
    for name, handler in (
        (module.replace("_", "-"), (lambda m: lambda: check_module(m))(module))
        for module in STAGE_MODULES
    )
    if name not in COMMANDS
})


def main() -> int:
    parser = argparse.ArgumentParser(prog="agent", add_help=False)
    parser.add_argument("command", nargs="?", default="help")
    parsed, _ = parser.parse_known_args()
    command = "help" if parsed.command in {"-h", "--help"} else parsed.command
    handler = COMMANDS.get(command)
    if handler is None:
        print(f"ERROR: unknown command '{command}'. Use 'python cli.py help'.", file=sys.stderr)
        return 1
    return handler()


if __name__ == "__main__":
    sys.exit(main())
