#!/usr/bin/env python3
"""Run and summarize the real internal/service test package.

This is an observation tool, not a performance gate.  It always runs the
whole package with ``go test -json -count=1 -timeout=10m ./internal/service``
(optionally with ``-race``); it never adds ``-run`` or changes test
assertions, deadlines, fixtures, or production code.

The summary is deliberately narrow: test/subtest names, status, the Go
reported per-test Elapsed, package Elapsed, and the timing fields already
emitted by selected tests (``file_phase``, ``file_io``,
``archive_boundary``).  Raw JSON and stderr are private operator artifacts
outside the repository; arbitrary test output, payloads, credentials,
owners, object keys and the environment are never printed.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import math
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

PACKAGE = "./internal/service"
TEST_TIMEOUT = "10m"
DEFAULT_TOP = 20
SCRIPT = Path(__file__).resolve()
ROOT = SCRIPT.parents[1]

SAFE_LABEL_RE = re.compile(r"^[A-Za-z0-9_.:+-]{1,96}$")
KV_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)=([^\s]+)")
FILE_PHASE_RE = re.compile(r"(?:^|\s)file_phase\s+(.*)$")
FILE_IO_RE = re.compile(r"(?:^|\s)file_io\s+(.*)$")
ARCHIVE_BOUNDARY_RE = re.compile(r"(?:^|\s)archive_boundary\s+(.*)$")
PG_SKIP_MARKERS = (
    "set AUDIT_TEST_POSTGRES_EXPORT_LEASE_DSN to a disposable database",
    "set AUDIT_TEST_POSTGRES_DSN to run PostgreSQL backend tests",
)


class TimingError(RuntimeError):
    """A fail-closed local setup or output error."""


class TimingInterrupted(Exception):
    def __init__(self, signum: int) -> None:
        super().__init__(f"signal {signum}")
        self.signum = signum


@dataclass
class PhaseSample:
    stage: str
    elapsed_ms: float
    order: int


@dataclass
class FileIOSample:
    operation: str
    kind: str
    success: bool
    elapsed_ms: float
    order: int


@dataclass
class ArchiveSample:
    values: dict[str, float]
    order: int


@dataclass
class TestRecord:
    name: str
    order: int
    status: str = "INCOMPLETE"
    elapsed: float | None = None
    started: bool = False
    terminal: bool = False
    pg_skip_hint: bool = False
    skip_category: str | None = None
    phases: list[PhaseSample] = field(default_factory=list)
    file_io: list[FileIOSample] = field(default_factory=list)
    archives: list[ArchiveSample] = field(default_factory=list)


@dataclass
class PackageRecord:
    name: str
    status: str = "INCOMPLETE"
    elapsed: float | None = None
    terminal: bool = False


class GoTestParser:
    """Incrementally parse ``go test -json`` without retaining output text."""

    def __init__(self) -> None:
        self.tests: dict[str, TestRecord] = {}
        self.packages: dict[str, PackageRecord] = {}
        self.build_failures: set[str] = set()
        self.event_count = 0
        self.malformed_json_lines = 0
        self.non_object_lines = 0
        self.truncated_final_line = False
        self.panic_observed = False
        self.timeout_observed = False
        self.reader_error: str | None = None
        self.capture_errors: set[str] = set()
        self._order = 0
        self._observation_order = 0

    def test(self, name: str) -> TestRecord:
        record = self.tests.get(name)
        if record is None:
            self._order += 1
            record = TestRecord(name=name, order=self._order)
            self.tests[name] = record
        return record

    def _next_observation(self) -> int:
        self._observation_order += 1
        return self._observation_order

    @staticmethod
    def _number(value: Any) -> float | None:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(number) or number < 0:
            return None
        return number

    @staticmethod
    def _safe_label(value: Any) -> str | None:
        return value if isinstance(value, str) and SAFE_LABEL_RE.fullmatch(value) else None

    @staticmethod
    def _kv(text: str) -> dict[str, str]:
        return {key: value for key, value in KV_RE.findall(text)}

    def feed_line(self, line: str) -> None:
        if not line.strip():
            return
        try:
            event = json.loads(line)
        except (json.JSONDecodeError, UnicodeError):
            self.malformed_json_lines += 1
            return
        if not isinstance(event, dict):
            self.non_object_lines += 1
            return
        self.event_count += 1
        self._feed_event(event)

    def _feed_event(self, event: dict[str, Any]) -> None:
        action = event.get("Action")
        package = event.get("Package")
        package = package if isinstance(package, str) and package else None
        import_path = event.get("ImportPath")
        import_path = import_path if isinstance(import_path, str) and import_path else None
        test_name = event.get("Test")
        test_name = test_name if isinstance(test_name, str) and test_name else None

        if action == "output":
            output = event.get("Output")
            if isinstance(output, str):
                self._feed_output(output, self.test(test_name) if test_name else None)
            return
        if action == "build-output":
            return
        if action == "build-fail":
            self.build_failures.add(import_path or package or "<unknown>")
            return
        if action in {"start", "pause", "cont", "bench"}:
            if action == "start" and package and not test_name:
                self._package(package).terminal = False
            return
        if action not in {"run", "pass", "fail", "skip"}:
            return

        if test_name:
            record = self.test(test_name)
            if package:
                self._package(package)
            if action == "run":
                record.started = True
                if not record.terminal:
                    record.status = "INCOMPLETE"
                return
            record.started = True
            record.terminal = True
            record.status = {"pass": "PASS", "fail": "FAIL", "skip": "SKIP"}[action]
            record.elapsed = self._number(event.get("Elapsed"))
            if action == "skip":
                record.skip_category = "postgres_env_unset" if record.pg_skip_hint else "other"
            return

        if package:
            record = self._package(package)
            record.terminal = True
            record.status = {"pass": "PASS", "fail": "FAIL", "skip": "SKIP"}[action]
            record.elapsed = self._number(event.get("Elapsed"))

    def _package(self, name: str) -> PackageRecord:
        record = self.packages.get(name)
        if record is None:
            record = PackageRecord(name=name)
            self.packages[name] = record
        return record

    def _feed_output(self, output: str, test: TestRecord | None) -> None:
        for line in output.splitlines():
            stripped = line.strip()
            lowered = stripped.lower()
            if stripped.startswith(("panic:", "fatal error:")):
                self.panic_observed = True
            if "test timed out after " in lowered or "*** test killed" in lowered:
                self.timeout_observed = True
            if test is None:
                continue
            if any(marker in line for marker in PG_SKIP_MARKERS):
                test.pg_skip_hint = True
            self._feed_observations(line, test)

    def _feed_observations(self, line: str, test: TestRecord) -> None:
        match = FILE_PHASE_RE.search(line)
        if match:
            values = self._kv(match.group(1))
            stage = self._safe_label(values.get("stage"))
            elapsed = self._number(values.get("elapsed_ms"))
            if stage is not None and elapsed is not None:
                test.phases.append(PhaseSample(stage, elapsed, self._next_observation()))
            return

        match = FILE_IO_RE.search(line)
        if match:
            values = self._kv(match.group(1))
            operation = self._safe_label(values.get("operation"))
            kind = self._safe_label(values.get("kind"))
            success_text = values.get("success")
            elapsed = self._number(values.get("elapsed_ms"))
            if (operation is not None and kind is not None
                    and success_text in {"true", "false"} and elapsed is not None):
                test.file_io.append(FileIOSample(
                    operation=operation,
                    kind=kind,
                    success=success_text == "true",
                    elapsed_ms=elapsed,
                    order=self._next_observation(),
                ))
            return

        match = ARCHIVE_BOUNDARY_RE.search(line)
        if match:
            values = self._kv(match.group(1))
            keys = ("ingest_ms", "before_put_ms", "put_ms", "after_put_ms")
            parsed = {key: self._number(values.get(key)) for key in keys}
            if all(value is not None for value in parsed.values()):
                test.archives.append(ArchiveSample(
                    values={key: value for key, value in parsed.items() if value is not None},
                    order=self._next_observation(),
                ))

    def finalize(self) -> None:
        for record in self.tests.values():
            if not record.terminal:
                record.status = "INCOMPLETE"
        for record in self.packages.values():
            if not record.terminal:
                record.status = "INCOMPLETE"

    def status_counts(self) -> Counter[str]:
        return Counter(record.status for record in self.tests.values())

    def incomplete_tests(self) -> list[TestRecord]:
        return [record for record in self.tests.values() if record.status == "INCOMPLETE"]

    def failed_tests(self) -> list[TestRecord]:
        return [record for record in self.tests.values() if record.status == "FAIL"]

    def skipped_tests(self) -> list[TestRecord]:
        return [record for record in self.tests.values() if record.status == "SKIP"]


def phase_deltas(samples: Iterable[PhaseSample]) -> list[tuple[str, float, float | None]]:
    """Return observed stages ordered by the embedded cumulative timestamp.

    Deltas come only from adjacent ``file_phase.elapsed_ms`` values.  The
    first observed stage has no predecessor and therefore no delta.  No
    timestamp from when a buffered log line arrived is used.
    """
    ordered = sorted(enumerate(samples), key=lambda pair: (pair[1].elapsed_ms, pair[0]))
    result: list[tuple[str, float, float | None]] = []
    previous: float | None = None
    for _, sample in ordered:
        delta = None if previous is None else sample.elapsed_ms - previous
        result.append((sample.stage, sample.elapsed_ms, delta))
        previous = sample.elapsed_ms
    return result


def go_test_args(race: bool) -> list[str]:
    """The full package invocation; there is intentionally no -run knob."""
    args = ["test", "-json"]
    if race:
        args.append("-race")
    args.extend(["-count=1", f"-timeout={TEST_TIMEOUT}", PACKAGE])
    return args


def declared_go_version(go_mod: Path = ROOT / "go.mod") -> str:
    try:
        for line in go_mod.read_text(encoding="utf-8").splitlines():
            match = re.match(r"^go\s+(\S+)\s*$", line)
            if match:
                return match.group(1)
    except OSError:
        pass
    return "unobserved"


def default_output_base() -> Path:
    state = os.environ.get("XDG_STATE_HOME")
    if state:
        return Path(state) / "snaplink-audit-service-timing"
    home = os.environ.get("HOME") or tempfile.gettempdir()
    return Path(home) / ".local" / "state" / "snaplink-audit-service-timing"


def ensure_outside_repo(path: Path, repo: Path = ROOT) -> Path:
    repo_real = repo.resolve()
    path_real = path.resolve(strict=False)
    if path_real == repo_real:
        raise TimingError("SERVICE_TIMING_OUT must be outside the repository")
    try:
        inside = os.path.commonpath((str(repo_real), str(path_real))) == str(repo_real)
    except ValueError:
        inside = False
    if inside:
        raise TimingError("SERVICE_TIMING_OUT must be outside the repository")
    return path_real


def create_run_dir(requested: str | None) -> Path:
    base = Path(requested).expanduser() if requested else default_output_base()
    if not base.is_absolute():
        base = Path.cwd() / base
    base = ensure_outside_repo(base)
    try:
        base.mkdir(mode=0o700, parents=True, exist_ok=True)
    except OSError as exc:
        raise TimingError(f"cannot create output directory: {exc.strerror or type(exc).__name__}") from exc
    base = ensure_outside_repo(base)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    for suffix in range(100):
        suffix_text = "" if suffix == 0 else f"-{suffix}"
        run_dir = base / f"{stamp}-{os.getpid()}{suffix_text}"
        candidate = ensure_outside_repo(run_dir)
        try:
            candidate.mkdir(mode=0o700)
        except FileExistsError:
            continue
        except OSError as exc:
            raise TimingError(f"cannot create private run directory: {exc.strerror or type(exc).__name__}") from exc
        try:
            os.chmod(candidate, 0o700)
        except OSError as exc:
            raise TimingError(f"cannot secure private run directory: {exc.strerror or type(exc).__name__}") from exc
        return candidate
    raise TimingError("cannot allocate a unique private run directory")


def _open_private(path: Path):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.fchmod(fd, 0o600)
    except OSError:
        pass
    return os.fdopen(fd, "w", encoding="utf-8", errors="replace", buffering=1)


def _write_metadata(path: Path, metadata: dict[str, Any]) -> None:
    with _open_private(path) as stream:
        json.dump(metadata, stream, indent=2, sort_keys=True)
        stream.write("\n")


def _go_identity(go_command: list[str], cwd: Path) -> tuple[str, str]:
    executable = shutil.which(go_command[0])
    if executable is None:
        raise TimingError("Go executable from GO was not found")
    resolved = str(Path(executable).resolve())
    try:
        result = subprocess.run(
            [*go_command, "version"],
            cwd=str(cwd),
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=30,
            check=False,
        )
        version = (result.stdout or result.stderr).strip().splitlines()
        go_version = version[0] if version else "unobserved"
    except (OSError, subprocess.SubprocessError):
        go_version = "unobserved"
    return resolved, go_version


def _go_environment(go_command: list[str], cwd: Path) -> dict[str, str]:
    """Record only selected effective-toolchain settings, never the environment."""
    keys = ("GOROOT", "GOVERSION", "GOTOOLCHAIN", "GOFLAGS")
    try:
        result = subprocess.run(
            [*go_command, "env", "-json", *keys],
            cwd=str(cwd),
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=30,
            check=False,
        )
        data = json.loads(result.stdout) if result.returncode == 0 else {}
    except (OSError, subprocess.SubprocessError, ValueError):
        data = {}
    if not isinstance(data, dict):
        data = {}
    return {key: str(data.get(key) or "unobserved") for key in keys}


def _pump(stream, raw, parser: GoTestParser) -> None:
    """Drain stdout fully even if the private artifact cannot be written."""
    last_line = ""
    write_failed = False
    for line in stream:
        last_line = line
        if not write_failed:
            try:
                raw.write(line)
                raw.flush()
            except Exception as exc:  # noqa: BLE001 - keep draining the pipe
                parser.capture_errors.add("stdout")
                parser.reader_error = type(exc).__name__
                write_failed = True
        try:
            parser.feed_line(line)
        except Exception as exc:  # noqa: BLE001 - the parser must never stall the child
            parser.reader_error = type(exc).__name__
    if last_line and not last_line.endswith("\n"):
        parser.truncated_final_line = True


def _pump_stderr(stream, raw, parser: GoTestParser) -> None:
    failed = False
    for line in stream:
        if failed:
            continue
        try:
            raw.write(line)
            raw.flush()
        except Exception:  # noqa: BLE001 - keep draining the pipe
            parser.capture_errors.add("stderr")
            failed = True


def _group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _signal_group(pgid: int, signum: int) -> bool:
    try:
        os.killpg(pgid, signum)
    except OSError:
        return False
    return True


def _terminate_tree(process: subprocess.Popen) -> None:
    """Reap only the process tree started by this invocation.

    ``Popen(..., start_new_session=True)`` makes the child the leader of a new
    process group.  Signals are sent to that group only, never to our own group
    or to unrelated processes.  The Go launcher and the test binary it spawns
    both inherit the group.
    """
    pgid = process.pid
    if not _signal_group(pgid, signal.SIGTERM) and process.poll() is None:
        try:
            process.terminate()
        except OSError:
            pass
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if process.poll() is None:
            try:
                process.wait(timeout=0.1)
            except subprocess.TimeoutExpired:
                pass
        if not _group_exists(pgid):
            return
        time.sleep(0.05)
    if not _signal_group(pgid, signal.SIGKILL) and process.poll() is None:
        try:
            process.kill()
        except OSError:
            pass
    try:
        process.wait(timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        pass


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _seconds(value: float | None) -> str:
    return "unobserved" if value is None else f"{value:.3f}s"


def _display_name(name: str, limit: int = 240) -> str:
    safe = "".join(char if 32 <= ord(char) != 127 else "?" for char in name)
    return safe if len(safe) <= limit else safe[: limit - 3] + "..."


def _parents(tests: dict[str, TestRecord]) -> set[str]:
    parents: set[str] = set()
    for name in tests:
        if "/" in name:
            parents.add(name.rsplit("/", 1)[0])
    return parents


def _has_problem(parser: GoTestParser, metadata: dict[str, Any]) -> bool:
    """The single FAIL/PASS decision shared by the summary and exit code."""
    counts = parser.status_counts()
    go_exit = metadata.get("go_exit_code")
    return bool(
        metadata.get("interrupted")
        or (isinstance(go_exit, int) and go_exit != 0)
        or parser.build_failures
        or parser.malformed_json_lines
        or parser.non_object_lines
        or parser.truncated_final_line
        or parser.reader_error
        or parser.capture_errors
        or parser.panic_observed
        or parser.timeout_observed
        or counts.get("FAIL", 0)
        or counts.get("INCOMPLETE", 0)
        or not parser.tests
        or not parser.packages
        or any(package.status != "PASS" for package in parser.packages.values())
    )


def render_summary(parser: GoTestParser, metadata: dict[str, Any], top: int = DEFAULT_TOP) -> str:
    """Render only whitelisted timing/status fields and safe run metadata."""
    parser.finalize()
    counts = parser.status_counts()
    lines: list[str] = []
    interrupted = bool(metadata.get("interrupted"))
    go_exit = metadata.get("go_exit_code")
    has_problem = _has_problem(parser, metadata)
    result = "FAIL" if has_problem else "PASS"
    if result == "PASS" and counts.get("SKIP", 0):
        result = "PASS (completed with skips)"
    lines.append(f"service-timing: {result}")
    lines.append(f"mode: {'race' if metadata.get('race') else 'plain'}")
    lines.append(f"go_launcher: {metadata.get('go_path', 'unobserved')}")
    lines.append(f"go_version: {metadata.get('go_version', 'unobserved')}")
    lines.append(f"go_toolchain: {metadata.get('go_toolchain', 'unobserved')}")
    lines.append(f"go_goroot: {metadata.get('go_goroot', 'unobserved')}")
    lines.append(f"go_flags: {metadata.get('go_flags', 'unobserved')}")
    lines.append(f"go_maxprocs: {metadata.get('go_maxprocs', 'unobserved')}")
    lines.append(f"declared_go_version: {metadata.get('declared_go_version', 'unobserved')}")
    command = metadata.get("command") or []
    lines.append("command: " + " ".join(_display_name(str(part), 300) for part in command))
    lines.append(f"package: {PACKAGE}")
    lines.append(f"wall_seconds: {_seconds(metadata.get('wall_seconds'))}")
    lines.append(f"go_exit_code: {go_exit if go_exit is not None else 'unobserved'}")
    lines.append(f"interrupted: {str(interrupted).lower()}")
    if metadata.get("signal") is not None:
        lines.append(f"signal: {metadata['signal']}")
    lines.append(f"raw_json: {metadata.get('raw_json', 'unobserved')}")
    lines.append(f"raw_stderr: {metadata.get('raw_stderr', 'unobserved')}")
    lines.append(f"metadata: {metadata.get('metadata_path', 'unobserved')}")
    lines.append("")

    if parser.packages:
        for package in sorted(parser.packages.values(), key=lambda item: item.name):
            lines.append(
                f"package {package.name}: status={package.status} "
                f"elapsed={_seconds(package.elapsed)}"
            )
    else:
        lines.append("package: unobserved (no terminal package event)")
    if parser.build_failures:
        lines.append(f"build_failures: {len(parser.build_failures)} (details only in raw JSON)")
    lines.append(
        "tests: "
        f"total={len(parser.tests)} pass={counts.get('PASS', 0)} "
        f"fail={counts.get('FAIL', 0)} skip={counts.get('SKIP', 0)} "
        f"incomplete={counts.get('INCOMPLETE', 0)}"
    )
    pg_skips = sum(1 for record in parser.tests.values()
                   if record.skip_category == "postgres_env_unset")
    if pg_skips:
        lines.append(
            f"postgres_skips: {pg_skips} (existing t.Skip; no DSN was used or provisioned)"
        )
    if parser.incomplete_tests():
        lines.append("incomplete_tests: " + ", ".join(
            _display_name(record.name) for record in parser.incomplete_tests()
        ))
    lines.append("elapsed_values_are_per_test; parent values include their subtests and are never summed")
    lines.append("")

    lines.append("== per-test / per-subtest results ==")
    for record in sorted(parser.tests.values(), key=lambda item: item.order):
        lines.append(
            f"{record.status:<10} {_seconds(record.elapsed):>12}  {_display_name(record.name)}"
        )
    if not parser.tests:
        lines.append("unobserved (no test events)")
    lines.append("")

    lines.append(f"== slowest {top} per-test elapsed observations (not additive) ==")
    timed = [record for record in parser.tests.values() if record.elapsed is not None]
    parents = _parents(parser.tests)
    for index, record in enumerate(
            sorted(timed, key=lambda item: (-(item.elapsed or 0.0), item.order))[:top], start=1):
        scope = "parent" if record.name in parents else ("subtest" if "/" in record.name else "top")
        lines.append(
            f"{index:>2}. {_seconds(record.elapsed):>12}  "
            f"[{scope}] {_display_name(record.name)}"
        )
    if not timed:
        lines.append("unobserved")
    lines.append("")

    lines.append("== file_phase (existing cumulative ms -> adjacent observed delta) ==")
    observed_phases = 0
    for record in sorted(parser.tests.values(), key=lambda item: item.order):
        if not record.phases:
            continue
        observed_phases += 1
        lines.append(f"{_display_name(record.name)}:")
        for stage, cumulative, delta in phase_deltas(record.phases):
            delta_text = "unobserved" if delta is None else f"{delta:g}"
            lines.append(
                f"  stage={stage} cumulative_ms={cumulative:g} delta_ms={delta_text}"
            )
    if not observed_phases:
        lines.append("unobserved (no file_phase instrumentation logs; nothing inferred)")
    else:
        lines.append(
            f"tests_with_file_phase={observed_phases}; "
            f"tests_without_file_phase={len(parser.tests) - observed_phases} (unobserved)"
        )
    lines.append("")

    lines.append("== file_io (each entry is one operation) ==")
    observed_io = 0
    for record in sorted(parser.tests.values(), key=lambda item: item.order):
        if not record.file_io:
            continue
        observed_io += 1
        lines.append(f"{_display_name(record.name)}: count={len(record.file_io)}")
        for sample in sorted(record.file_io, key=lambda item: item.order):
            lines.append(
                f"  operation={sample.operation} kind={sample.kind} "
                f"success={str(sample.success).lower()} elapsed_ms={sample.elapsed_ms:g}"
            )
    if not observed_io:
        lines.append("unobserved (no file_io instrumentation logs; nothing inferred)")
    lines.append("")

    lines.append("== archive_boundary (explicit segments preserved) ==")
    observed_archive = 0
    for record in sorted(parser.tests.values(), key=lambda item: item.order):
        if not record.archives:
            continue
        observed_archive += 1
        lines.append(f"{_display_name(record.name)}:")
        for sample in sorted(record.archives, key=lambda item: item.order):
            fields = " ".join(
                f"{key}={sample.values[key]:g}"
                for key in ("ingest_ms", "before_put_ms", "put_ms", "after_put_ms")
                if key in sample.values
            )
            lines.append(f"  {fields}")
    if not observed_archive:
        lines.append("unobserved (no archive_boundary instrumentation logs; nothing inferred)")
    lines.append("")

    lines.append("== diagnostics ==")
    lines.append(f"malformed_json_lines: {parser.malformed_json_lines}")
    lines.append(f"non_object_json_lines: {parser.non_object_lines}")
    lines.append(f"truncated_final_json: {str(parser.truncated_final_line).lower()}")
    lines.append(f"panic_observed: {str(parser.panic_observed).lower()}")
    lines.append(f"timeout_observed: {str(parser.timeout_observed).lower()}")
    if parser.reader_error:
        lines.append(f"reader_error: {parser.reader_error}")
    if parser.capture_errors:
        lines.append("capture_errors: " + ",".join(sorted(parser.capture_errors)))
    lines.append("raw output is private and is not echoed into this summary")
    return "\n".join(lines)


def _exit_code(
    parser: GoTestParser,
    go_exit: int,
    interrupted_signal: int | None,
    metadata: dict[str, Any] | None = None,
) -> int:
    if interrupted_signal is not None:
        return 128 + interrupted_signal
    if go_exit < 0:
        return 128 + abs(go_exit)
    if go_exit != 0:
        return go_exit if go_exit <= 255 else 1
    if _has_problem(parser, metadata or {}):
        return 1
    return 0


def _install_signal_handlers() -> tuple[Any, Any]:
    def handler(signum, _frame):
        raise TimingInterrupted(signum)

    old_int = signal.getsignal(signal.SIGINT)
    old_term = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    return old_int, old_term


def _restore_signal_handlers(old: tuple[Any, Any]) -> None:
    signal.signal(signal.SIGINT, old[0])
    signal.signal(signal.SIGTERM, old[1])


def run(requested_out: str | None, race: bool, top: int = DEFAULT_TOP) -> int:
    os.umask(0o077)
    metadata: dict[str, Any] = {
        "script": "scripts/service_timing.py",
        "package": PACKAGE,
        "test_timeout": TEST_TIMEOUT,
        "race": race,
        "command": [],
        "go_path": "unobserved",
        "go_version": "unobserved",
        "go_toolchain": "unobserved",
        "go_goroot": "unobserved",
        "go_flags": "unobserved",
        "go_maxprocs": os.environ.get("GOMAXPROCS", "unobserved"),
        "declared_go_version": declared_go_version(),
        "started_at": _iso_now(),
        "finished_at": None,
        "wall_seconds": None,
        "go_exit_code": None,
        "interrupted": False,
        "signal": None,
        "malformed_json_lines": 0,
        "truncated_final_json": False,
        "build_failures": 0,
        "panic_observed": False,
        "timeout_observed": False,
        "raw_json": "unobserved",
        "raw_stderr": "unobserved",
        "metadata_path": "unobserved",
    }
    parser = GoTestParser()
    try:
        run_dir = create_run_dir(requested_out)
    except TimingError as exc:
        print(f"service-timing: FAIL: {exc}", file=sys.stderr)
        return 2

    raw_json = run_dir / "go-test.jsonl"
    raw_stderr = run_dir / "stderr.log"
    metadata_path = run_dir / "metadata.json"
    lock_path = run_dir.parent / ".service-timing.lock"
    metadata.update({
        "run_dir": str(run_dir),
        "raw_json": str(raw_json),
        "raw_stderr": str(raw_stderr),
        "metadata_path": str(metadata_path),
        "lock": str(lock_path),
    })
    _write_metadata(metadata_path, metadata)

    go_text = os.environ.get("GO", "go").strip() or "go"
    try:
        go_command = shlex.split(go_text)
    except ValueError as exc:
        print(f"service-timing: FAIL: invalid GO value: {exc}", file=sys.stderr)
        return 2
    if not go_command:
        print("service-timing: FAIL: GO is empty", file=sys.stderr)
        return 2
    metadata["go_command"] = go_text
    try:
        go_path, go_version = _go_identity(go_command, ROOT)
    except TimingError as exc:
        metadata.update({"finished_at": _iso_now(), "setup_error": str(exc)})
        _write_metadata(metadata_path, metadata)
        print(f"service-timing: FAIL: {exc}", file=sys.stderr)
        return 127
    metadata.update({"go_path": go_path, "go_version": go_version})
    go_env = _go_environment(go_command, ROOT)
    metadata.update({
        "go_toolchain": go_env.get("GOTOOLCHAIN", "unobserved"),
        "go_goroot": go_env.get("GOROOT", "unobserved"),
        "go_flags": go_env.get("GOFLAGS", "unobserved"),
        "go_goversion": go_env.get("GOVERSION", "unobserved"),
    })

    command = [*go_command, *go_test_args(race)]
    metadata["command"] = command
    _write_metadata(metadata_path, metadata)

    old_handlers = _install_signal_handlers()
    lock_stream = None
    process: subprocess.Popen | None = None
    stdout_thread: threading.Thread | None = None
    stderr_thread: threading.Thread | None = None
    interrupted_signal: int | None = None
    started_monotonic = 0.0
    try:
        try:
            lock_stream = _open_private(lock_path)
            fcntl.flock(lock_stream.fileno(), fcntl.LOCK_EX)
        except OSError as exc:
            metadata.update({
                "finished_at": _iso_now(),
                "setup_error": f"cannot acquire resource lock: {exc.strerror or type(exc).__name__}",
            })
            _write_metadata(metadata_path, metadata)
            print("service-timing: FAIL: cannot acquire resource lock", file=sys.stderr)
            return 2

        try:
            with _open_private(raw_json) as json_stream, _open_private(raw_stderr) as stderr_stream:
                started_monotonic = time.monotonic()
                try:
                    process = subprocess.Popen(
                        command,
                        cwd=str(ROOT),
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                        bufsize=1,
                        start_new_session=True,
                    )
                except OSError as exc:
                    metadata.update({
                        "finished_at": _iso_now(),
                        "setup_error": f"cannot start Go: {exc.strerror or type(exc).__name__}",
                    })
                    _write_metadata(metadata_path, metadata)
                    print("service-timing: FAIL: cannot start Go", file=sys.stderr)
                    return 127

                stdout_thread = threading.Thread(
                    target=_pump,
                    args=(process.stdout, json_stream, parser),
                    name="service-timing-stdout",
                    daemon=True,
                )
                stderr_thread = threading.Thread(
                    target=_pump_stderr,
                    args=(process.stderr, stderr_stream, parser),
                    name="service-timing-stderr",
                    daemon=True,
                )
                try:
                    stdout_thread.start()
                    stderr_thread.start()
                except BaseException:
                    _terminate_tree(process)
                    raise
                try:
                    go_exit = process.wait()
                except TimingInterrupted as exc:
                    interrupted_signal = exc.signum
                    metadata["interrupted"] = True
                    metadata["signal"] = exc.signum
                    _terminate_tree(process)
                    go_exit = process.wait()
                finally:
                    if stdout_thread is not None:
                        stdout_thread.join()
                    if stderr_thread is not None:
                        stderr_thread.join()
                    for stream in (process.stdout, process.stderr):
                        if stream is not None:
                            try:
                                stream.close()
                            except OSError:
                                pass
        except OSError as exc:
            metadata.update({
                "finished_at": _iso_now(),
                "setup_error": f"cannot open private capture files: {exc.strerror or type(exc).__name__}",
            })
            _write_metadata(metadata_path, metadata)
            print("service-timing: FAIL: cannot open private capture files", file=sys.stderr)
            return 2
    finally:
        _restore_signal_handlers(old_handlers)
        if lock_stream is not None:
            try:
                fcntl.flock(lock_stream.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            lock_stream.close()

    wall_seconds = time.monotonic() - started_monotonic
    parser.finalize()
    metadata.update({
        "finished_at": _iso_now(),
        "wall_seconds": round(wall_seconds, 3),
        "go_exit_code": go_exit,
        "malformed_json_lines": parser.malformed_json_lines,
        "truncated_final_json": parser.truncated_final_line,
        "build_failures": len(parser.build_failures),
        "panic_observed": parser.panic_observed,
        "timeout_observed": parser.timeout_observed,
        "capture_errors": sorted(parser.capture_errors),
    })
    if parser.reader_error:
        metadata["reader_error"] = parser.reader_error
    _write_metadata(metadata_path, metadata)

    summary = render_summary(parser, metadata, top=top)
    print(summary)
    code = _exit_code(parser, go_exit, interrupted_signal, metadata)
    if code != 0 and not summary.startswith("service-timing: FAIL"):
        # Defensive: the summary must never claim success when the exit is non-zero.
        print("service-timing: FAIL (non-zero result)", file=sys.stderr)
    return code


def _race_from_environment() -> bool:
    value = os.environ.get("SERVICE_TIMING_RACE", "").strip()
    if value not in {"", "0", "1"}:
        raise TimingError("SERVICE_TIMING_RACE must be 0 or 1")
    return value == "1"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="service-timing",
        description="Run the full internal/service test package and summarize timing fields.",
    )
    parser.add_argument("--race", action="store_true", help="add -race to the full package run")
    parser.add_argument(
        "--out",
        default=None,
        metavar="DIR",
        help="private output base outside the repository (or SERVICE_TIMING_OUT)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=DEFAULT_TOP,
        help=f"number of slowest per-test observations to show (default {DEFAULT_TOP})",
    )
    args = parser.parse_args(argv)
    if args.top < 1:
        parser.error("--top must be positive")
    try:
        race = args.race or _race_from_environment()
    except TimingError as exc:
        print(f"service-timing: FAIL: {exc}", file=sys.stderr)
        return 2
    requested_out = args.out if args.out is not None else os.environ.get("SERVICE_TIMING_OUT") or None
    try:
        return run(requested_out, race, top=args.top)
    except TimingInterrupted as exc:
        print(f"service-timing: interrupted by signal {exc.signum}", file=sys.stderr)
        return 128 + exc.signum


if __name__ == "__main__":
    sys.exit(main())
