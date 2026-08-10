#!/usr/bin/env python3
"""Regenerate api/proto generated code with the pinned toolchain.

Single implementation of "regenerate with pinned tools", shared by
``make proto`` (in-place regeneration), the quality-gate replay
(``--check``, byte-compare against the checked-in files), and the drift
simulation restore step.

Pins are read from the ``proto:`` block of ``engineering.yaml`` (the single
pin manifest).  Tools are bootstrapped into the gitignored repo-local
``bin/`` directory and are never installed system-wide:

- ``bin/protoc-<pin>/bin/protoc``: official release zip from
  github.com/protocolbuffers/protobuf (protobuf switched release tags at
  3.21: pin ``v3.21.12`` maps to GitHub tag ``v21.12``);
- ``bin/protoc-gen-go`` / ``bin/protoc-gen-go-grpc``: ``go install
  <module>@<pin>`` with GOBIN pointed at ``bin/`` (does not touch
  go.mod/go.sum).

Exit codes:

- 0  success
- 1  ``--check``: regenerated output differs from the checked-in files
- 2  bootstrap/generation failure (missing pins, download failure, ...)
- 3  ``--check``: pinned toolchain not present (caller treats as skip)
"""

from __future__ import annotations

import difflib
import os
import platform
import re
import subprocess
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PIN_KEYS = ("protoc", "protoc-gen-go", "protoc-gen-go-grpc")

GENERATOR_MODULES = {
    "protoc-gen-go": "google.golang.org/protobuf/cmd/protoc-gen-go",
    "protoc-gen-go-grpc": "google.golang.org/grpc/cmd/protoc-gen-go-grpc",
}


class ProtoGenError(Exception):
    """Bootstrap/generation failure; maps to exit code 2."""


def proto_pins(root: Path) -> dict[str, str]:
    """Read the ``proto:`` block of ``engineering.yaml`` (single pin manifest)."""
    path = root / "engineering.yaml"
    if not path.exists():
        raise ProtoGenError("engineering.yaml missing (proto pins)")
    lines = path.read_text(encoding="utf-8").splitlines()
    block: list[str] = []
    found = False
    for index, line in enumerate(lines):
        if re.match(r"^\s*proto:\s*$", line):
            found = True
            for member in lines[index + 1:]:
                if member and not member[0].isspace():
                    break
                block.append(member)
            break
    if not found:
        raise ProtoGenError("engineering.yaml missing 'proto:' pin block")
    pins: dict[str, str] = {}
    for line in block:
        match = re.match(r"^\s*(protoc|protoc-gen-go|protoc-gen-go-grpc):\s*(v?[\d.]+)\s*$", line)
        if match:
            version = match.group(2)
            pins[match.group(1)] = version if version.startswith("v") else "v" + version
    missing = [key for key in PIN_KEYS if key not in pins]
    if missing:
        raise ProtoGenError(f"engineering.yaml proto: block missing pin(s): {', '.join(missing)}")
    return pins


# ---------------------------------------------------------------------------
# Tool bootstrap (repo-local, gitignored bin/)
# ---------------------------------------------------------------------------

def protoc_release_asset(pin: str) -> tuple[str, str]:
    """Map a pin like ``v3.21.12`` to (GitHub release tag, asset name).

    protobuf switched release tags at 3.21: ``v3.21.12`` is released as tag
    ``v21.12`` with asset ``protoc-21.12-linux-x86_64.zip``.  Versions below
    3.21 keep the ``v3.x.y`` tag.
    """
    match = re.fullmatch(r"v3\.(\d+)\.(\d+)", pin)
    if match and int(match.group(1)) >= 21:
        tag = f"v{match.group(1)}.{match.group(2)}"
    else:
        tag = pin
    machine = platform.machine().lower()
    if sys.platform.startswith("linux"):
        arch = {"x86_64": "x86_64", "amd64": "x86_64", "aarch64": "aarch_64", "arm64": "aarch_64"}.get(machine)
        if arch is None:
            raise ProtoGenError(f"unsupported platform for pinned protoc: linux {machine}")
        platform_name = f"linux-{arch}"
    elif sys.platform == "darwin":
        arch = {"x86_64": "x86_64", "aarch64": "aarch_64", "arm64": "aarch_64"}.get(machine)
        if arch is None:
            raise ProtoGenError(f"unsupported platform for pinned protoc: darwin {machine}")
        platform_name = f"osx-{arch}"
    else:
        raise ProtoGenError(f"unsupported platform for pinned protoc: {sys.platform}")
    return tag, f"protoc-{tag[1:]}-{platform_name}.zip"


def tool_version(binary: Path) -> str | None:
    """``--version`` output of a generator binary, normalized (e.g. v1.36.11)."""
    try:
        result = subprocess.run([str(binary), "--version"], text=True,
                                capture_output=True, check=False, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    match = re.search(r"(protoc-gen-go|protoc-gen-go-grpc)\s+(v?[\d.]+)", result.stdout)
    if not match:
        return None
    version = match.group(2)
    return version if version.startswith("v") else "v" + version


def protoc_version(binary: Path) -> str | None:
    try:
        result = subprocess.run([str(binary), "--version"], text=True,
                                capture_output=True, check=False, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    match = re.search(r"libprotoc\s+([\d.]+)", result.stdout)
    return "v" + match.group(1) if match else None


def ensure_protoc(root: Path, pin: str) -> Path:
    target = root / "bin" / f"protoc-{pin}"
    binary = target / "bin" / "protoc"
    if binary.exists() and protoc_version(binary) == pin:
        return binary
    tag, asset = protoc_release_asset(pin)
    url = f"https://github.com/protocolbuffers/protobuf/releases/download/{tag}/{asset}"
    print(f"proto-gen: downloading {url}")
    staging = Path(tempfile.mkdtemp(prefix="protoc-", dir=str(root / "bin")))
    try:
        archive = staging / asset
        try:
            with urllib.request.urlopen(url, timeout=120) as response:
                archive.write_bytes(response.read())
        except OSError as error:
            raise ProtoGenError(
                f"failed to download pinned protoc from {url} ({error}); "
                f"offline workaround: place the extracted release in bin/protoc-{pin}/") from error
        with zipfile.ZipFile(archive) as zf:
            zf.extractall(staging)
        candidate = staging / "bin" / "protoc"
        if not candidate.exists():
            raise ProtoGenError(f"downloaded archive {asset} has no bin/protoc")
        # Python's zipfile does not restore the executable bit from the
        # release archive's permissions.
        candidate.chmod(0o755)
        if protoc_version(candidate) != pin:
            raise ProtoGenError(f"downloaded protoc version mismatch: expected {pin}")
        if target.exists():
            import shutil
            shutil.rmtree(target)
        candidate.parent.parent.rename(target)
    finally:
        import shutil
        shutil.rmtree(staging, ignore_errors=True)
    return target / "bin" / "protoc"


def ensure_generator(root: Path, name: str, pin: str) -> Path:
    binary = root / "bin" / name
    if binary.exists() and tool_version(binary) == pin:
        return binary
    module = GENERATOR_MODULES[name]
    print(f"proto-gen: go install {module}@{pin} (GOBIN={root / 'bin'})")
    env = dict(os.environ)
    env["GOBIN"] = str(root / "bin")
    result = subprocess.run(["go", "install", f"{module}@{pin}"], cwd=str(root),
                            env=env, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        raise ProtoGenError(
            f"go install {module}@{pin} failed: {result.stderr.strip() or result.stdout.strip()}")
    if tool_version(binary) != pin:
        raise ProtoGenError(f"installed {name} version does not match pin {pin}")
    return binary


def ensure_toolchain(root: Path, pins: dict[str, str]) -> dict[str, Path]:
    """Bootstrap the pinned toolchain into bin/ (downloads / go installs)."""
    (root / "bin").mkdir(exist_ok=True)
    tools = {
        "protoc": ensure_protoc(root, pins["protoc"]),
        "protoc-gen-go": ensure_generator(root, "protoc-gen-go", pins["protoc-gen-go"]),
        "protoc-gen-go-grpc": ensure_generator(root, "protoc-gen-go-grpc", pins["protoc-gen-go-grpc"]),
    }
    return tools


def find_toolchain(root: Path, pins: dict[str, str]) -> dict[str, Path] | None:
    """Locate the pinned toolchain without bootstrapping; None if absent."""
    protoc = root / "bin" / f"protoc-{pins['protoc']}" / "bin" / "protoc"
    go_plugin = root / "bin" / "protoc-gen-go"
    grpc_plugin = root / "bin" / "protoc-gen-go-grpc"
    if not protoc.exists() or not go_plugin.exists() or not grpc_plugin.exists():
        return None
    if protoc_version(protoc) != pins["protoc"]:
        raise ProtoGenError(
            f"bin/protoc-{pins['protoc']}/bin/protoc version mismatch: expected {pins['protoc']}")
    if tool_version(go_plugin) != pins["protoc-gen-go"]:
        raise ProtoGenError(
            f"bin/protoc-gen-go version mismatch: expected {pins['protoc-gen-go']}")
    if tool_version(grpc_plugin) != pins["protoc-gen-go-grpc"]:
        raise ProtoGenError(
            f"bin/protoc-gen-go-grpc version mismatch: expected {pins['protoc-gen-go-grpc']}")
    return {"protoc": protoc, "protoc-gen-go": go_plugin, "protoc-gen-go-grpc": grpc_plugin}


# ---------------------------------------------------------------------------
# Regeneration
# ---------------------------------------------------------------------------

def protoc_command(root: Path, pins: dict[str, str], tools: dict[str, Path],
                   go_out: str, grpc_out: str) -> list[str]:
    proto_dir = root / "api" / "proto"
    return [
        str(tools["protoc"]), "-I", str(proto_dir),
        f"--plugin=protoc-gen-go={tools['protoc-gen-go']}",
        f"--plugin=protoc-gen-go-grpc={tools['protoc-gen-go-grpc']}",
        f"--go_out={go_out}", "--go_opt=paths=source_relative",
        f"--go-grpc_out={grpc_out}", "--go-grpc_opt=paths=source_relative",
        str(proto_dir / "audit.proto"),
    ]


def regenerate(root: Path, pins: dict[str, str], tools: dict[str, Path],
               go_out: str, grpc_out: str) -> None:
    command = protoc_command(root, pins, tools, go_out, grpc_out)
    result = subprocess.run(command, cwd=str(root), text=True,
                            capture_output=True, check=False)
    if result.returncode != 0:
        raise ProtoGenError(
            f"protoc failed: {result.stderr.strip() or result.stdout.strip()}")


def check_replay(root: Path, pins: dict[str, str], tools: dict[str, Path]) -> int:
    """Regenerate into a temp dir and byte-compare against api/proto."""
    with tempfile.TemporaryDirectory(prefix="proto-replay-") as tmp:
        tmp_path = Path(tmp)
        regenerate(root, pins, tools, str(tmp_path), str(tmp_path))
        differing = []
        for name in ("audit.pb.go", "audit_grpc.pb.go"):
            generated = (tmp_path / name).read_bytes()
            checked_in = (root / "api" / "proto" / name).read_bytes()
            if generated != checked_in:  # byte-exact, never normalized (R9)
                differing.append(name)
                print(f"proto-gen: {name} differs from pinned regeneration "
                      f"({len(generated)} vs {len(checked_in)} bytes)")
                diff = difflib.unified_diff(
                    (root / "api" / "proto" / name).read_text(encoding="utf-8").splitlines(),
                    generated.decode("utf-8").splitlines(),
                    fromfile=f"api/proto/{name} (checked in)",
                    tofile=f"api/proto/{name} (pinned regeneration)", n=3)
                print("\n".join(list(diff)[:60]))
        if differing:
            print("FAIL: proto-gen --check: run `make proto` to reconcile")
            return 1
    print(f"proto-gen: replay OK (audit.pb.go, audit_grpc.pb.go byte-identical, "
          f"protoc {pins['protoc']}, protoc-gen-go {pins['protoc-gen-go']}, "
          f"protoc-gen-go-grpc {pins['protoc-gen-go-grpc']})")
    return 0


def main(argv: list[str] | None = None, root: Path | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    root = root or ROOT
    try:
        pins = proto_pins(root)
        check_mode = "--check" in argv
        if check_mode:
            tools = find_toolchain(root, pins)
            if tools is None:
                print("proto-gen: replay skipped: pinned toolchain not in bin/ "
                      "(run `make proto` to bootstrap); descriptor + version checks still enforced")
                return 3
            return check_replay(root, pins, tools)
        tools = ensure_toolchain(root, pins)
        regenerate(root, pins, tools, str(root / "api" / "proto"), str(root / "api" / "proto"))
    except ProtoGenError as error:
        print(f"FAIL: proto-gen: {error}", file=sys.stderr)
        return 2
    print(f"proto-gen: regenerated api/proto/audit.pb.go and api/proto/audit_grpc.pb.go "
          f"(protoc {pins['protoc']}, protoc-gen-go {pins['protoc-gen-go']}, "
          f"protoc-gen-go-grpc {pins['protoc-gen-go-grpc']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
