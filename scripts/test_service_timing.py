#!/usr/bin/env python3
"""Standard-library unittest coverage for scripts/service_timing.py."""

from __future__ import annotations

import importlib.util
import io
import json
import os
import stat
import subprocess
import sys
import tempfile
import time
import unittest
import contextlib
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).with_name("service_timing.py")
SPEC = importlib.util.spec_from_file_location("service_timing_under_test", SCRIPT)
timing = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = timing
SPEC.loader.exec_module(timing)


def event(action: str, **fields):
    return json.dumps({"Action": action, **fields})


def metadata(**updates):
    value = {
        "race": False,
        "go_path": "/usr/bin/go",
        "go_version": "go version go1.26.10 linux/amd64",
        "declared_go_version": "1.26.10",
        "command": ["go", "test", "-json", "-count=1", "-timeout=10m", "./internal/service"],
        "go_exit_code": 0,
        "wall_seconds": 1.0,
        "raw_json": "/private/go-test.jsonl",
        "raw_stderr": "/private/stderr.log",
        "metadata_path": "/private/metadata.json",
        "interrupted": False,
    }
    value.update(updates)
    return value


def parse(lines):
    parser = timing.GoTestParser()
    for line in lines:
        parser.feed_line(line)
    parser.finalize()
    return parser


RACE_ALLOCATION_FIXTURE = (
    event("start", Package="example/service"),
    event("run", Package="example/service",
          Test="TestRestoreReadControlFileAllocationDoesNotScaleWithUnrelatedEvidence"),
    event("output", Package="example/service",
          Test="TestRestoreReadControlFileAllocationDoesNotScaleWithUnrelatedEvidence",
          Output=("    restore_read_control_file_test.go:147: "
                  "restore_control_read_allocation hot_samples=1,32 "
                  "measured_calls_each=4 allocs_small=165 allocs_large=205\n")),
    event("output", Package="example/service",
          Test="TestRestoreReadControlFileAllocationDoesNotScaleWithUnrelatedEvidence",
          Output=("    restore_read_control_file_test.go:150: restore metadata read "
                  "allocated in proportion to unrelated hot/cold evidence\n")),
    event("fail", Package="example/service",
          Test="TestRestoreReadControlFileAllocationDoesNotScaleWithUnrelatedEvidence", Elapsed=1.19),
    event("run", Package="example/service",
          Test="TestRetentionReadControlFileAllocationDoesNotScaleWithUnrelatedEvidence"),
    event("output", Package="example/service",
          Test="TestRetentionReadControlFileAllocationDoesNotScaleWithUnrelatedEvidence",
          Output=("    retention_read_control_file_test.go:144: "
                  "retention_control_read_allocation hot_samples=1,32 "
                  "measured_calls_each=4 allocs_small=168 allocs_large=190\n")),
    event("output", Package="example/service",
          Test="TestRetentionReadControlFileAllocationDoesNotScaleWithUnrelatedEvidence",
          Output=("    retention_read_control_file_test.go:146: policy metadata read "
                  "allocated in proportion to unrelated hot/cold evidence\n")),
    event("fail", Package="example/service",
          Test="TestRetentionReadControlFileAllocationDoesNotScaleWithUnrelatedEvidence", Elapsed=1.22),
    event("fail", Package="example/service", Elapsed=378.751),
)


class FailingWriter(io.StringIO):
    def write(self, _text):
        raise OSError("private capture is full")

    def flush(self):
        raise OSError("private capture is full")


def process_running(pid: int) -> bool:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return False
    return len(fields) > 2 and fields[2] != "Z"


def wait_for(predicate, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


class CommandAndVersionTest(unittest.TestCase):
    def test_plain_command_is_the_full_package_with_fixed_timeout(self):
        args = timing.go_test_args(False)
        self.assertEqual(
            args,
            ["test", "-json", "-count=1", "-timeout=10m", "./internal/service"],
        )
        self.assertNotIn("-run", args)

    def test_race_command_only_adds_race(self):
        args = timing.go_test_args(True)
        self.assertEqual(
            args,
            ["test", "-json", "-race", "-count=1", "-timeout=10m", "./internal/service"],
        )
        self.assertNotIn("-run", args)
        self.assertEqual(args[-1], "./internal/service")

    def test_declared_version_comes_from_go_mod(self):
        with tempfile.TemporaryDirectory() as tmp:
            go_mod = Path(tmp) / "go.mod"
            go_mod.write_text("module example.test\n\ngo 1.26.10\n", encoding="utf-8")
            self.assertEqual(timing.declared_go_version(go_mod), "1.26.10")
            go_mod.write_text("module example.test\n", encoding="utf-8")
            self.assertEqual(timing.declared_go_version(go_mod), "unobserved")


class ParserTest(unittest.TestCase):
    def test_statuses_and_subtests_keep_go_elapsed_values(self):
        parser = parse([
            event("start", Package="example/service"),
            event("run", Package="example/service", Test="TestParent"),
            event("run", Package="example/service", Test="TestParent/fast"),
            event("pass", Package="example/service", Test="TestParent/fast", Elapsed=0.25),
            event("run", Package="example/service", Test="TestParent/slow"),
            event("pass", Package="example/service", Test="TestParent/slow", Elapsed=1.5),
            event("pass", Package="example/service", Test="TestParent", Elapsed=2.0),
            event("skip", Package="example/service", Test="TestSkipped", Elapsed=0),
            event("pass", Package="example/service", Elapsed=2.5),
        ])
        self.assertEqual(parser.tests["TestParent/fast"].status, "PASS")
        self.assertEqual(parser.tests["TestParent/fast"].elapsed, 0.25)
        self.assertEqual(parser.tests["TestParent/slow"].status, "PASS")
        self.assertEqual(parser.tests["TestSkipped"].status, "SKIP")
        self.assertEqual(parser.packages["example/service"].elapsed, 2.5)
        self.assertEqual(parser.status_counts()["PASS"], 3)
        self.assertEqual(parser.status_counts()["SKIP"], 1)

    def test_parent_elapsed_is_not_summed_into_package_wall_time(self):
        parser = parse([
            event("run", Package="p", Test="TestParent"),
            event("run", Package="p", Test="TestParent/a"),
            event("pass", Package="p", Test="TestParent/a", Elapsed=7.0),
            event("run", Package="p", Test="TestParent/b"),
            event("pass", Package="p", Test="TestParent/b", Elapsed=2.0),
            event("pass", Package="p", Test="TestParent", Elapsed=10.0),
            event("pass", Package="p", Elapsed=12.0),
        ])
        summary = timing.render_summary(parser, metadata())
        self.assertIn("package p: status=PASS elapsed=12.000s", summary)
        self.assertIn("not additive", summary)
        self.assertNotIn("19.000s", summary)  # child sum must not become a total
        self.assertIn("[parent] TestParent", summary)

    def test_interleaved_parallel_subtests_keep_each_reported_elapsed(self):
        parser = parse([
            event("start", Package="p"),
            event("run", Package="p", Test="TestParallel"),
            event("run", Package="p", Test="TestParallel/a"),
            event("run", Package="p", Test="TestParallel/b"),
            event("pass", Package="p", Test="TestParallel/a", Elapsed=0.4),
            event("pass", Package="p", Test="TestParallel/b", Elapsed=0.2),
            event("pass", Package="p", Test="TestParallel", Elapsed=0.7),
            event("pass", Package="p", Elapsed=0.8),
        ])
        summary = timing.render_summary(parser, metadata())
        self.assertEqual(parser.tests["TestParallel/a"].elapsed, 0.4)
        self.assertEqual(parser.tests["TestParallel/b"].elapsed, 0.2)
        self.assertEqual(parser.tests["TestParallel"].elapsed, 0.7)
        self.assertIn("package p: status=PASS elapsed=0.800s", summary)
        self.assertNotIn("1.300s", summary)
        self.assertTrue(summary.startswith("service-timing: PASS"))
        self.assertEqual(timing._exit_code(parser, 0, None), 0)

    def test_incomplete_test_is_retained_and_summary_is_not_pass(self):
        parser = parse([
            event("start", Package="p"),
            event("run", Package="p", Test="TestNeverFinished"),
        ])
        summary = timing.render_summary(parser, metadata())
        self.assertEqual(parser.tests["TestNeverFinished"].status, "INCOMPLETE")
        self.assertIn("INCOMPLETE", summary)
        self.assertIn("TestNeverFinished", summary)
        self.assertTrue(summary.startswith("service-timing: FAIL"))
        self.assertNotEqual(timing._exit_code(parser, 0, None), 0)

    def test_missing_package_terminal_state_is_fail_closed(self):
        parser = parse([
            event("run", Package="p", Test="TestFinished"),
            event("pass", Package="p", Test="TestFinished", Elapsed=0.1),
        ])
        summary = timing.render_summary(parser, metadata())
        self.assertEqual(parser.packages["p"].status, "INCOMPLETE")
        self.assertIn("package p: status=INCOMPLETE", summary)
        self.assertTrue(summary.startswith("service-timing: FAIL"))
        self.assertNotEqual(timing._exit_code(parser, 0, None), 0)

    def test_summary_result_and_exit_code_agree_for_pass_skip_and_fail(self):
        passing = parse([
            event("run", Package="p", Test="TestPass"),
            event("pass", Package="p", Test="TestPass", Elapsed=0.1),
            event("pass", Package="p", Elapsed=0.2),
        ])
        self.assertTrue(timing.render_summary(passing, metadata()).startswith(
            "service-timing: PASS"))
        self.assertEqual(timing._exit_code(passing, 0, None), 0)

        skipping = parse([
            event("run", Package="p", Test="TestSkip"),
            event("skip", Package="p", Test="TestSkip", Elapsed=0),
            event("pass", Package="p", Elapsed=0.2),
        ])
        summary = timing.render_summary(skipping, metadata())
        self.assertTrue(summary.startswith("service-timing: PASS (completed with skips)"))
        self.assertEqual(timing._exit_code(skipping, 0, None), 0)

        failing = parse([
            event("run", Package="p", Test="TestFail"),
            event("fail", Package="p", Test="TestFail", Elapsed=0.1),
            event("fail", Package="p", Elapsed=0.2),
        ])
        self.assertTrue(timing.render_summary(failing, metadata()).startswith(
            "service-timing: FAIL"))
        self.assertNotEqual(timing._exit_code(failing, 0, None), 0)

    def test_build_failure_is_a_failure_without_the_compiler_log(self):
        parser = parse([
            event("build-output", ImportPath="example/service", Output="SECRET_COMPILER_TEXT\n"),
            event("build-fail", ImportPath="example/service"),
        ])
        summary = timing.render_summary(parser, metadata(go_exit_code=1))
        self.assertTrue(summary.startswith("service-timing: FAIL"))
        self.assertIn("build_failures: 1", summary)
        self.assertNotIn("SECRET_COMPILER_TEXT", summary)
        self.assertNotEqual(timing._exit_code(parser, 1, None), 0)

    def test_panic_and_timeout_are_observed_without_echoing_output(self):
        parser = parse([
            event("start", Package="p"),
            event("run", Package="p", Test="TestStuck"),
            event("output", Package="p", Test="TestStuck",
                  Output="panic: test timed out after 10m0s\nSECRET_STACK_FRAME\n"),
            event("fail", Package="p", Elapsed=600.0),
        ])
        summary = timing.render_summary(parser, metadata(go_exit_code=1))
        self.assertTrue(parser.panic_observed)
        self.assertTrue(parser.timeout_observed)
        self.assertIn("panic_observed: true", summary)
        self.assertIn("timeout_observed: true", summary)
        self.assertIn("TestStuck", summary)
        self.assertNotIn("SECRET_STACK_FRAME", summary)

    def test_malformed_and_truncated_json_are_fail_closed(self):
        parser = timing.GoTestParser()
        parser.feed_line('{"Action":"run","Test":"TestA"')
        self.assertEqual(parser.malformed_json_lines, 1)
        parser.feed_line(event("run", Package="p", Test="TestA"))
        parser.truncated_final_line = True
        parser.finalize()
        summary = timing.render_summary(parser, metadata())
        self.assertTrue(summary.startswith("service-timing: FAIL"))
        self.assertIn("malformed_json_lines: 1", summary)
        self.assertIn("truncated_final_json: true", summary)
        self.assertNotEqual(timing._exit_code(parser, 0, None), 0)

    def test_stdout_pump_marks_an_unterminated_final_line(self):
        parser = timing.GoTestParser()
        raw = io.StringIO()
        timing._pump(io.StringIO('{"Action":"start","Package":"p"}'), raw, parser)
        self.assertTrue(parser.truncated_final_line)
        self.assertEqual(raw.getvalue(), '{"Action":"start","Package":"p"}')

    def test_postgres_skip_is_recorded_as_a_skip_category(self):
        parser = parse([
            event("run", Package="p", Test="TestPostgresExample"),
            event("output", Package="p", Test="TestPostgresExample",
                  Output="    x_test.go:1: set AUDIT_TEST_POSTGRES_DSN to run PostgreSQL backend tests\n"),
            event("skip", Package="p", Test="TestPostgresExample", Elapsed=0),
            event("pass", Package="p", Elapsed=0.1),
        ])
        record = parser.tests["TestPostgresExample"]
        self.assertEqual(record.status, "SKIP")
        self.assertEqual(record.skip_category, "postgres_env_unset")
        summary = timing.render_summary(parser, metadata())
        self.assertIn("postgres_skips: 1", summary)
        self.assertIn("SKIP", summary)

    def test_interrupted_run_returns_signal_status(self):
        parser = parse([event("run", Package="p", Test="TestA")])
        self.assertEqual(timing._exit_code(parser, -15, 15), 143)


class ObservationTest(unittest.TestCase):
    def test_file_phase_uses_cumulative_values_and_adjacent_deltas(self):
        parser = parse([
            event("run", Package="p", Test="TestPhases"),
            event("output", Package="p", Test="TestPhases",
                  Output="    x_test.go:1: file_phase stage=created members=2 elapsed_ms=40\n"),
            event("output", Package="p", Test="TestPhases",
                  Output="    x_test.go:2: file_phase stage=fixture_ready elapsed_ms=10\n"),
            event("output", Package="p", Test="TestPhases",
                  Output="    x_test.go:3: file_phase stage=terminal elapsed_ms=65\n"),
            event("pass", Package="p", Test="TestPhases", Elapsed=0.1),
            event("pass", Package="p", Elapsed=0.2),
        ])
        rows = timing.phase_deltas(parser.tests["TestPhases"].phases)
        self.assertEqual(rows, [
            ("fixture_ready", 10, None),
            ("created", 40, 30),
            ("terminal", 65, 25),
        ])
        summary = timing.render_summary(parser, metadata())
        self.assertIn("stage=fixture_ready cumulative_ms=10 delta_ms=unobserved", summary)
        self.assertIn("stage=created cumulative_ms=40 delta_ms=30", summary)
        self.assertIn("stage=terminal cumulative_ms=65 delta_ms=25", summary)

    def test_file_io_is_kept_as_each_single_operation(self):
        parser = parse([
            event("run", Package="p", Test="TestIO"),
            event("output", Package="p", Test="TestIO",
                  Output="    x_test.go:1: file_io operation=get kind=events success=true elapsed_ms=4 since_start_ms=100\n"),
            event("output", Package="p", Test="TestIO",
                  Output="    x_test.go:2: file_io operation=put kind=exports success=false elapsed_ms=9 since_start_ms=140\n"),
            event("pass", Package="p", Test="TestIO", Elapsed=0.1),
            event("pass", Package="p", Elapsed=0.2),
        ])
        samples = parser.tests["TestIO"].file_io
        self.assertEqual([sample.elapsed_ms for sample in samples], [4, 9])
        self.assertEqual([sample.operation for sample in samples], ["get", "put"])
        summary = timing.render_summary(parser, metadata())
        self.assertIn("operation=get kind=events success=true elapsed_ms=4", summary)
        self.assertIn("operation=put kind=exports success=false elapsed_ms=9", summary)
        self.assertIn("each entry is one operation", summary)

    def test_archive_boundary_preserves_explicit_segments(self):
        parser = parse([
            event("run", Package="p", Test="TestArchive"),
            event("output", Package="p", Test="TestArchive",
                  Output="    x_test.go:1: archive_boundary ingest_ms=5000 before_put_ms=12 put_ms=4900 after_put_ms=88\n"),
            event("pass", Package="p", Test="TestArchive", Elapsed=5.0),
            event("pass", Package="p", Elapsed=5.1),
        ])
        summary = timing.render_summary(parser, metadata())
        self.assertIn(
            "ingest_ms=5000 before_put_ms=12 put_ms=4900 after_put_ms=88",
            summary,
        )

    def test_no_instrumentation_is_reported_as_unobserved(self):
        parser = parse([
            event("run", Package="p", Test="TestNoInstrumentation"),
            event("pass", Package="p", Test="TestNoInstrumentation", Elapsed=0.1),
            event("pass", Package="p", Elapsed=0.2),
        ])
        summary = timing.render_summary(parser, metadata())
        self.assertIn("unobserved (no file_phase instrumentation logs; nothing inferred)", summary)
        self.assertIn("unobserved (no file_io instrumentation logs; nothing inferred)", summary)
        self.assertIn("unobserved (no archive_boundary instrumentation logs; nothing inferred)", summary)
        self.assertNotIn("delta_ms=0", summary)


class PrivacyAndOutputTest(unittest.TestCase):
    def test_summary_does_not_echo_arbitrary_payload_or_secret_fields(self):
        parser = parse([
            event("run", Package="p", Test="TestPrivacy"),
            event("output", Package="p", Test="TestPrivacy",
                  Output=("    x_test.go:1: file_phase stage=created elapsed_ms=12 "
                          "owner=alice object_key=objects/secret payload=TOPSECRET\n")),
            event("output", Package="p", Test="TestPrivacy",
                  Output="raw failure payload=TOPSECRET credential=TOPSECRET\n"),
            event("skip", Package="p", Test="TestPrivacy", Elapsed=0),
            event("pass", Package="p", Elapsed=0.1),
        ])
        summary = timing.render_summary(parser, metadata())
        for secret in ("TOPSECRET", "owner=alice", "object_key", "credential"):
            self.assertNotIn(secret, summary)
        self.assertIn("stage=created cumulative_ms=12", summary)

    def test_output_directory_must_be_outside_the_repository(self):
        with self.assertRaises(timing.TimingError):
            timing.ensure_outside_repo(timing.ROOT / "private-timing", timing.ROOT)
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                timing.ensure_outside_repo(Path(tmp), timing.ROOT).resolve(),
                Path(tmp).resolve(),
            )

    def test_symlink_resolving_back_into_the_repository_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            link = Path(tmp) / "repo-link"
            link.symlink_to(timing.ROOT, target_is_directory=True)
            with self.assertRaises(timing.TimingError):
                timing.ensure_outside_repo(link / "private-timing", timing.ROOT)

    def test_capture_write_failures_force_fail_even_when_events_all_pass(self):
        passing_events = [
            event("start", Package="p"),
            event("run", Package="p", Test="TestPass"),
            event("pass", Package="p", Test="TestPass", Elapsed=0.1),
            event("pass", Package="p", Elapsed=0.2),
        ]
        for channel in ("stdout", "stderr"):
            with self.subTest(channel=channel):
                parser = parse(passing_events)
                payload = "".join(line + "\n" for line in passing_events)
                if channel == "stdout":
                    timing._pump(io.StringIO(payload), FailingWriter(), parser)
                else:
                    timing._pump_stderr(io.StringIO("secret stderr\n"), FailingWriter(), parser)
                parser.finalize()
                summary = timing.render_summary(parser, metadata(go_exit_code=0))
                self.assertIn(f"capture_errors: {channel}", summary)
                self.assertTrue(summary.startswith("service-timing: FAIL"))
                self.assertNotIn("service-timing: PASS", summary)
                self.assertNotEqual(timing._exit_code(parser, 0, None), 0)
                self.assertNotIn("secret stderr", summary)


class EndToEndFakeGoTest(unittest.TestCase):
    def test_private_raw_files_metadata_race_and_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_go = root / "fake-go"
            fake_go.write_text(
                "#!/usr/bin/env python3\n"
                "import json, sys\n"
                "if sys.argv[1:2] == ['version']:\n"
                "    print('go version go1.99.0 fake/linux')\n"
                "    raise SystemExit(0)\n"
                "if sys.argv[1:2] == ['env']:\n"
                "    print(json.dumps({'GOROOT':'/fake/goroot','GOVERSION':'go1.99.0',"
                "'GOTOOLCHAIN':'go1.99.0','GOFLAGS':''}))\n"
                "    raise SystemExit(0)\n"
                "events = [\n"
                "  {'Action':'start','Package':'example/service'},\n"
                "  {'Action':'run','Package':'example/service','Test':'TestOne'},\n"
                "  {'Action':'output','Package':'example/service','Test':'TestOne',"
                "'Output':'    x_test.go:1: file_phase stage=ready elapsed_ms=3\\n'},\n"
                "  {'Action':'output','Package':'example/service','Test':'TestOne',"
                "'Output':'raw payload=TOPSECRET\\n'},\n"
                "  {'Action':'pass','Package':'example/service','Test':'TestOne','Elapsed':0.01},\n"
                "  {'Action':'pass','Package':'example/service','Elapsed':0.02},\n"
                "]\n"
                "for item in events:\n"
                "    print(json.dumps(item), flush=True)\n"
                "print('stderr-only-SECRET', file=sys.stderr, flush=True)\n"
                "raise SystemExit(0)\n",
                encoding="utf-8",
            )
            fake_go.chmod(fake_go.stat().st_mode | stat.S_IXUSR)
            out = root / "timing-out"
            environment = os.environ.copy()
            environment.update({
                "GO": str(fake_go),
                "SERVICE_TIMING_OUT": str(out),
                "SERVICE_TIMING_RACE": "1",
            })
            result = subprocess.run(
                [sys.executable, str(SCRIPT)],
                env=environment,
                text=True,
                capture_output=True,
                timeout=30,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("service-timing: PASS", result.stdout)
            self.assertIn("mode: race", result.stdout)
            self.assertNotIn("TOPSECRET", result.stdout)
            self.assertNotIn("stderr-only-SECRET", result.stdout)

            runs = [path for path in out.iterdir() if path.is_dir()]
            self.assertEqual(len(runs), 1)
            run_dir = runs[0]
            self.assertEqual(stat.S_IMODE(run_dir.stat().st_mode), 0o700)
            for name in ("go-test.jsonl", "stderr.log", "metadata.json"):
                path = run_dir / name
                self.assertTrue(path.is_file(), name)
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertIn("TOPSECRET", (run_dir / "go-test.jsonl").read_text(encoding="utf-8"))
            self.assertIn("stderr-only-SECRET", (run_dir / "stderr.log").read_text(encoding="utf-8"))

            data = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
            self.assertIn("-race", data["command"])
            self.assertNotIn("-run", data["command"])
            self.assertEqual(data["declared_go_version"], timing.declared_go_version())
            self.assertEqual(data["go_exit_code"], 0)
            self.assertEqual(data["go_path"], str(fake_go.resolve()))

    def test_capture_file_write_failure_is_nonzero_even_if_go_exits_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_go = root / "fake-go"
            fake_go.write_text(
                "#!/usr/bin/env python3\n"
                "import json, sys\n"
                "if sys.argv[1:2] == ['version']:\n"
                "    print('go version go1.99.0 fake/linux')\n"
                "    raise SystemExit(0)\n"
                "if sys.argv[1:2] == ['env']:\n"
                "    print(json.dumps({'GOROOT':'/fake/goroot','GOVERSION':'go1.99.0',"
                "'GOTOOLCHAIN':'go1.99.0','GOFLAGS':''}))\n"
                "    raise SystemExit(0)\n"
                "events = [\n"
                "  {'Action':'start','Package':'example/service'},\n"
                "  {'Action':'run','Package':'example/service','Test':'TestOne'},\n"
                "  {'Action':'pass','Package':'example/service','Test':'TestOne','Elapsed':0.01},\n"
                "  {'Action':'pass','Package':'example/service','Elapsed':0.02},\n"
                "]\n"
                "for item in events:\n"
                "    print(json.dumps(item), flush=True)\n"
                "print('stderr-only-SECRET', file=sys.stderr, flush=True)\n"
                "raise SystemExit(0)\n",
                encoding="utf-8",
            )
            fake_go.chmod(fake_go.stat().st_mode | stat.S_IXUSR)
            real_open = timing._open_private
            for channel, filename in (("stdout", "go-test.jsonl"), ("stderr", "stderr.log")):
                with self.subTest(channel=channel):
                    out = root / f"timing-out-{channel}"

                    def fake_open(path, _filename=filename):
                        if Path(path).name == _filename:
                            return FailingWriter()
                        return real_open(path)

                    stdout = io.StringIO()
                    with mock.patch.dict(os.environ, {"GO": str(fake_go)}, clear=False), \
                            mock.patch.object(timing, "_open_private", side_effect=fake_open), \
                            contextlib.redirect_stdout(stdout):
                        code = timing.run(str(out), False, top=5)
                    text = stdout.getvalue()
                    self.assertNotEqual(code, 0)
                    self.assertTrue(text.startswith("service-timing: FAIL"))
                    self.assertIn(f"capture_errors: {channel}", text)
                    self.assertNotIn("service-timing: PASS", text)
                    self.assertNotIn("stderr-only-SECRET", text)
                    runs = [path for path in out.iterdir() if path.is_dir()]
                    self.assertEqual(len(runs), 1)
                    data = json.loads((runs[0] / "metadata.json").read_text(encoding="utf-8"))
                    self.assertEqual(data["go_exit_code"], 0)
                    self.assertIn(channel, data["capture_errors"])


class RaceEvidenceFixtureTest(unittest.TestCase):
    def test_sanitized_old_race_allocations_stay_failed(self):
        parser = parse(RACE_ALLOCATION_FIXTURE)
        summary = timing.render_summary(parser, metadata(race=True, go_exit_code=1))
        self.assertEqual(parser.tests[
            "TestRestoreReadControlFileAllocationDoesNotScaleWithUnrelatedEvidence"].status,
            "FAIL")
        self.assertEqual(parser.tests[
            "TestRetentionReadControlFileAllocationDoesNotScaleWithUnrelatedEvidence"].status,
            "FAIL")
        self.assertIn("fail=2", summary)
        self.assertIn("TestRestoreReadControlFileAllocationDoesNotScaleWithUnrelatedEvidence",
                      summary)
        self.assertIn("TestRetentionReadControlFileAllocationDoesNotScaleWithUnrelatedEvidence",
                      summary)
        self.assertTrue(summary.startswith("service-timing: FAIL"))
        self.assertNotIn("service-timing: PASS", summary)
        self.assertNotEqual(timing._exit_code(parser, 1, None), 0)
        for secret in ("owner=", "object_key=", "payload=", "TOPSECRET", "DSN"):
            self.assertNotIn(secret, "\n".join(RACE_ALLOCATION_FIXTURE))
            self.assertNotIn(secret, summary)


class InterruptionTest(unittest.TestCase):
    def test_sigterm_keeps_partial_raw_and_incomplete_test(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_go = root / "slow-fake-go"
            fake_go.write_text(
                "#!/usr/bin/env python3\n"
                "import json, sys, time\n"
                "if sys.argv[1:2] == ['version']:\n"
                "    print('go version go1.99.0 fake/linux')\n"
                "    raise SystemExit(0)\n"
                "if sys.argv[1:2] == ['env']:\n"
                "    print(json.dumps({'GOROOT':'/fake/goroot','GOVERSION':'go1.99.0',"
                "'GOTOOLCHAIN':'go1.99.0','GOFLAGS':''}))\n"
                "    raise SystemExit(0)\n"
                "for item in [\n"
                "  {'Action':'start','Package':'example/service'},\n"
                "  {'Action':'run','Package':'example/service','Test':'TestSlow'},\n"
                "  {'Action':'output','Package':'example/service','Test':'TestSlow',"
                "'Output':'    x_test.go:1: file_phase stage=ready elapsed_ms=3\\n'},\n"
                "]:\n"
                "    print(json.dumps(item), flush=True)\n"
                "time.sleep(60)\n",
                encoding="utf-8",
            )
            fake_go.chmod(fake_go.stat().st_mode | stat.S_IXUSR)
            out = root / "timing-out"
            environment = os.environ.copy()
            environment.update({
                "GO": str(fake_go),
                "SERVICE_TIMING_OUT": str(out),
                "SERVICE_TIMING_RACE": "",
            })
            process = subprocess.Popen(
                [sys.executable, str(SCRIPT)],
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                deadline = time.monotonic() + 10
                run_dir = None
                while time.monotonic() < deadline:
                    candidates = [path for path in out.iterdir() if path.is_dir()] if out.exists() else []
                    if candidates and (candidates[0] / "go-test.jsonl").exists():
                        run_dir = candidates[0]
                        if (run_dir / "go-test.jsonl").stat().st_size > 0:
                            break
                    time.sleep(0.05)
                self.assertIsNotNone(run_dir)
                process.send_signal(timing.signal.SIGTERM)
                stdout, stderr = process.communicate(timeout=20)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()
            self.assertEqual(process.returncode, 143, stderr)
            self.assertIn("service-timing: FAIL", stdout)
            self.assertIn("interrupted: true", stdout)
            self.assertIn("signal: 15", stdout)
            self.assertIn("INCOMPLETE", stdout)
            self.assertIn("TestSlow", stdout)
            self.assertNotIn("service-timing: PASS", stdout)
            raw = (run_dir / "go-test.jsonl").read_text(encoding="utf-8")
            self.assertIn('"Action": "run"', raw)
            self.assertIn('"Test": "TestSlow"', raw)
            data = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
            self.assertTrue(data["interrupted"])
            self.assertEqual(data["signal"], 15)

    def test_sigterm_reaps_only_the_own_child_tree(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_go = root / "tree-fake-go"
            fake_go.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os, subprocess, sys, time\n"
                "if sys.argv[1:2] == ['version']:\n"
                "    print('go version go1.99.0 fake/linux')\n"
                "    raise SystemExit(0)\n"
                "if sys.argv[1:2] == ['env']:\n"
                "    print(json.dumps({'GOROOT':'/fake/goroot','GOVERSION':'go1.99.0',"
                "'GOTOOLCHAIN':'go1.99.0','GOFLAGS':''}))\n"
                "    raise SystemExit(0)\n"
                "for item in [\n"
                "  {'Action':'start','Package':'example/service'},\n"
                "  {'Action':'run','Package':'example/service','Test':'TestTree'},\n"
                "]:\n"
                "    print(json.dumps(item), flush=True)\n"
                "child = subprocess.Popen([sys.executable, '-c', "
                "'import time; time.sleep(60)'])\n"
                "with open(os.environ['FAKE_CHILD_PID_FILE'], 'w', encoding='utf-8') as f:\n"
                "    f.write(str(child.pid))\n"
                "time.sleep(60)\n",
                encoding="utf-8",
            )
            fake_go.chmod(fake_go.stat().st_mode | stat.S_IXUSR)
            out = root / "timing-out"
            pid_file = root / "child.pid"
            environment = os.environ.copy()
            environment.update({
                "GO": str(fake_go),
                "SERVICE_TIMING_OUT": str(out),
                "SERVICE_TIMING_RACE": "",
                "FAKE_CHILD_PID_FILE": str(pid_file),
            })
            unrelated = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                start_new_session=True,
            )
            process = subprocess.Popen(
                [sys.executable, str(SCRIPT)],
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
            child_pid = None
            try:
                self.assertTrue(wait_for(
                    lambda: pid_file.exists() and pid_file.stat().st_size > 0, timeout=10))
                child_pid = int(pid_file.read_text(encoding="utf-8"))
                self.assertTrue(process_running(child_pid))
                process.send_signal(timing.signal.SIGTERM)
                stdout, stderr = process.communicate(timeout=30)
                self.assertTrue(wait_for(lambda: not process_running(child_pid), timeout=10))
                self.assertTrue(process_running(unrelated.pid))
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()
                if unrelated.poll() is None:
                    unrelated.terminate()
                    try:
                        unrelated.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        unrelated.kill()
                        unrelated.wait()
                if child_pid is not None and process_running(child_pid):
                    try:
                        os.kill(child_pid, timing.signal.SIGKILL)
                    except OSError:
                        pass
            self.assertEqual(process.returncode, 143, stderr)
            self.assertIn("service-timing: FAIL", stdout)
            self.assertIn("interrupted: true", stdout)
            self.assertIn("INCOMPLETE", stdout)
            self.assertIn("TestTree", stdout)
            self.assertNotIn("service-timing: PASS", stdout)
            run_dir = next(path for path in out.iterdir() if path.is_dir())
            raw = (run_dir / "go-test.jsonl").read_text(encoding="utf-8")
            self.assertIn('"Action": "run"', raw)
            self.assertIn('"Test": "TestTree"', raw)
            data = json.loads((run_dir / "metadata.json").read_text(encoding="utf-8"))
            self.assertTrue(data["interrupted"])
            self.assertEqual(data["signal"], 15)


if __name__ == "__main__":
    unittest.main()
