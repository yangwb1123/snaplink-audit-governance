#!/usr/bin/env python3
"""Snaplink Audit Governance engineering CLI.

This keeps the command surface of the sibling Snaplink project while using
checks that are native to this repository.  Run ``python cli.py help`` for
the complete list.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def quality_limit(name: str, fallback: int) -> int:
    config = ROOT / "engineering.yaml"
    if not config.exists():
        return fallback
    match = re.search(rf"^\s*{re.escape(name)}:\s*(\d+)\s*$", config.read_text(encoding="utf-8"), re.MULTILINE)
    return int(match.group(1)) if match else fallback


QUALITY_MAX_GO_LINES = quality_limit("max_go_lines", 1500)
QUALITY_MAX_FUNCTION_LINES = quality_limit("max_function_lines", 260)
QUALITY_MAX_DECISIONS = quality_limit("max_decisions", 45)


def run(*args: str, cwd: Path = ROOT, check: bool = False) -> int:
    print("$", " ".join(args))
    return subprocess.run(list(args), cwd=str(cwd), check=False).returncode


def gofmt_files() -> list[str]:
    result = subprocess.run(
        ["gofmt", "-l", "."], cwd=str(ROOT), text=True,
        capture_output=True, check=False
    )
    return [line for line in result.stdout.splitlines() if line and ".git/" not in line]


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
               ("audit-projector", "./cmd/audit-projector"))
    for name, package in targets:
        target = bin_dir / name
        if run("go", "build", "-o", str(target), package) != 0:
            return 1
    return 0


def cmd_check_filesize() -> int:
    # The sibling CLI treats this as a gate.  Keep a generous threshold for
    # service implementations while still catching accidental generated blobs.
    # Test suites are exempt, matching checks/filesize.py (the checker the
    # quality gate runs): the line budget governs production code only.
    limit = QUALITY_MAX_GO_LINES
    failed = False
    for path in sorted(ROOT.rglob("*.go")):
        if any(part in {".git", "bin", "vendor"} for part in path.parts):
            continue
        if "_test.go" in path.name:
            continue
        lines = len(path.read_text(encoding="utf-8").splitlines())
        if lines > limit:
            print(f"file-size: {path.relative_to(ROOT)} has {lines} lines (limit {limit})")
            failed = True
    if not failed:
        print(f"filesize: PASS (max {limit} lines)")
    return int(failed)


def cmd_complexity() -> int:
    # Lightweight, dependency-free guard for pathological functions.  It is
    # deliberately conservative; detailed cyclomatic analysis remains an
    # optional golangci-lint concern.
    function = re.compile(r"^func \([^)]*\) (\w+)|^func (\w+)")
    starts: list[tuple[str, int]] = []
    failed = False
    for path in sorted(ROOT.rglob("*.go")):
        if any(part in {".git", "bin", "vendor"} for part in path.parts):
            continue
        lines = path.read_text(encoding="utf-8").splitlines()
        for index, line in enumerate(lines):
            match = function.match(line)
            if match:
                starts.append((match.group(1) or match.group(2), index))
        for position, (name, start) in enumerate(starts):
            end = starts[position + 1][1] if position + 1 < len(starts) else len(lines)
            body = lines[start:end]
            decisions = sum(line.count(token) for line in body for token in (" if ", " for ", "switch ", "case ", "&&", "||"))
            if len(body) > QUALITY_MAX_FUNCTION_LINES or decisions > QUALITY_MAX_DECISIONS:
                print(f"complexity: {path.relative_to(ROOT)}:{start + 1} {name} is large/branch-heavy (lines={len(body)}, decisions={decisions})")
                failed = True
        starts.clear()
    if not failed:
        print(f"complexity: PASS (max {QUALITY_MAX_FUNCTION_LINES} function lines, {QUALITY_MAX_DECISIONS} decisions)")
    return int(failed)


def normalized_route(path: str) -> str:
    return re.sub(r"\{[^}]+\}", "{}", path)


def cmd_check_routes() -> int:
    go_routes: set[str] = set()
    for path in (ROOT / "internal").rglob("*.go"):
        text = path.read_text(encoding="utf-8")
        go_routes.update(normalized_route(value) for value in re.findall(r'HandleFunc\("(?:GET|POST|PUT|DELETE|PATCH) ([^" ]+)', text) if value.startswith("/api/"))
    openapi = ROOT / "api" / "openapi" / "openapi.yaml"
    api_routes = set()
    if openapi.exists():
        api_routes.update(normalized_route(value) for value in re.findall(r"^  (/api/.*):\s*$", openapi.read_text(encoding="utf-8"), re.MULTILINE))
    missing = sorted(go_routes - api_routes)
    extra = sorted(api_routes - go_routes)
    if missing:
        print("routes missing from OpenAPI:", *missing, sep="\n  ")
    if extra:
        print("OpenAPI routes missing at runtime:", *extra, sep="\n  ")
    return int(bool(missing or extra))


def cmd_check_root() -> int:
    allowed = {".git", ".gitignore", ".dockerignore", "Dockerfile", "README.md", "AGENTS.md",
               "Makefile", "go.mod", "go.sum", "cli.py", "api", "cmd", "deploy",
               "docs", "internal", "migrations", "checks", "test", "bin", "scripts",
               ".trends", ".pi-batch", "engineering.yaml", "__pycache__"}
    unexpected = [path.name for path in ROOT.iterdir() if path.name not in allowed and not path.name.startswith(".pi-batch.lock")]
    if unexpected:
        print("root directories/files:", ", ".join(sorted(unexpected)))
    return int(bool(unexpected))


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


def go_imports(path: Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    values = set(re.findall(r'^\s*"([^"]+)"\s*$', text, re.MULTILINE))
    values.update(re.findall(r'import\s+"([^"]+)"', text))
    return values


def cmd_architecture() -> int:
    # Keep the dependency direction explicit for the reference service:
    # domain/security/auth/store are lower layers and cannot import API or
    # service packages.
    forbidden = {
        "internal/domain": {"internal/service", "internal/httpapi", "internal/grpcapi"},
        "internal/security": {"internal/service", "internal/httpapi", "internal/grpcapi"},
        "internal/auth": {"internal/service", "internal/httpapi", "internal/grpcapi"},
        "internal/store": {"internal/service", "internal/httpapi", "internal/grpcapi"},
    }
    errors: list[str] = []
    for package, disallowed in forbidden.items():
        directory = ROOT / package
        if not directory.exists():
            continue
        for path in directory.glob("*.go"):
            for imported in go_imports(path):
                for target in disallowed:
                    if imported.endswith(target):
                        errors.append(f"{path.relative_to(ROOT)} imports {target}")
    if errors:
        print("architecture violations:", *errors, sep="\n  ")
        return 1
    print("architecture: dependency direction is clean")
    return 0


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
    # Exemptions are intentionally explicit.  A missing registry is valid for
    # this small reference implementation; silently malformed entries are not.
    path = ROOT / "engineering-exemptions.yaml"
    if not path.exists():
        print("exemptions: none")
        return 0
    text = path.read_text(encoding="utf-8")
    if "reason:" not in text or "owner:" not in text:
        print(f"invalid exemption registry: {path}")
        return 1
    print(f"exemptions: validated {path.relative_to(ROOT)}")
    return 0


def default_secrets_single_source() -> list[str]:
    # The well-known development secrets must exist only in
    # internal/service/secrets.go (non-test Go). Any other occurrence (a
    # binary envOr fallback, a scanner, a wrapper) would split the single
    # source of truth for the deny-list and could silently re-introduce
    # hard-coded defaults.
    allowed = {str((ROOT / "internal" / "service" / "secrets.go").resolve())}
    offenders = []
    for directory in (ROOT / "internal", ROOT / "cmd"):
        for path in sorted(directory.rglob("*.go")):
            if path.name.endswith("_test.go") or str(path.resolve()) in allowed:
                continue
            text = path.read_text(encoding="utf-8")
            for secret in ("development-signing-key-change-me", "development-encryption-key-change-me"):
                if secret in text:
                    offenders.append(f"{path.relative_to(ROOT)} contains {secret!r}")
    return offenders


def cmd_check_invariants() -> int:
    checks = {
        "tenant-context": ("TenantID = tenantID", "tenant context assignment"),
        "sensitive-field-filter": ("rejectSensitive", "sensitive field rejection"),
        "hash-chain": ("PrevHash", "hash chain linkage"),
        "request-size-limit": ("MaxBytesReader", "HTTP request limit"),
    }
    source = "\n".join(path.read_text(encoding="utf-8") for path in (ROOT / "internal").rglob("*.go"))
    missing = [label for needle, label in checks.values() if needle not in source]
    offenders = default_secrets_single_source()
    if missing:
        print("missing security invariants:", *missing, sep="\n  ")
    if offenders:
        print("default secrets outside single source:", *offenders, sep="\n  ")
    if missing or offenders:
        return 1
    print("security invariants: clean")
    return 0


def cmd_adr_compliance() -> int:
    required = [ROOT / "docs" / "ARCHITECTURE_PLAN.md",
                ROOT / "docs" / "VALIDATION_PLAN.md",
                ROOT / "api" / "openapi" / "openapi.yaml",
                ROOT / "api" / "asyncapi" / "asyncapi.yaml"]
    missing = [str(path.relative_to(ROOT)) for path in required if not path.exists()]
    if missing:
        print("missing architecture evidence:", *missing, sep="\n  ")
        return 1
    print("architecture evidence: present")
    return 0


def cmd_self_test() -> int:
    required = {"check", "test", "race", "vet", "fmt", "build", "check-routes", "help"}
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


def cmd_skill() -> int:
    print("skill: no local skills configured")
    return 1


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
    return run(sys.executable, "-m", "unittest", "discover", "-s", "checks", "-p", "test_*.py")


def check_module(name: str) -> int:
    return run(sys.executable, "-m", f"checks.{name}")


def cmd_coverage() -> int:
    with tempfile.NamedTemporaryFile(prefix="audit-coverage-", suffix=".out") as file:
        return run("go", "test", "-coverprofile=" + file.name, "./...")


def cmd_check() -> int:
    for command in (cmd_fmt, cmd_check_filesize, cmd_vet, cmd_test):
        if command() != 0:
            return 1
    return 0


def cmd_accept() -> int:
    return cmd_quality()


def cmd_quality() -> int:
    """Run the full post-change engineering gate."""
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
    for command in (cmd_fmt, filesize, complexity, architecture, directory_fanout,
                    root_files, root_business_code, invariants, exemptions,
                    adr_compliance, make_help, route_contract, contract_fields,
                    asyncapi_channels,
                    proto_sync,  # proto drift guard: fails fast, before the ~95s Go stages
                    dev_auth_manifest, tenant_consistency, stream_consistency,
                    replay_round_reset,
                    prometheus_rules,
                    sensitive_logging,
                    cmd_vet, cmd_python_checks, cmd_test, cmd_race, build):
        if command() != 0:
            return 1
    print("QUALITY PASS")
    return 0


def cmd_harness() -> int:
    for command in (cmd_check, cmd_complexity, cmd_check_routes):
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
    "configure": cmd_generate,
    "modules": cmd_modules,
    "capabilities": cmd_capabilities,
    "sdk-surface": cmd_sdk_surface,
    "profiles": cmd_build,
    "lint": cmd_lint,
    "security-scan": cmd_security_scan,
    "skill": cmd_skill,
    "help": cmd_help,
}


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
