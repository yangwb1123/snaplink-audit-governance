import hashlib
import importlib.util
import io
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

try:
    from google.protobuf import descriptor_pb2  # type: ignore
    HAS_ORACLE = True
except ImportError:  # pragma: no cover - environment without the oracle
    HAS_ORACLE = False


def run_capture(check, *args, **kwargs):
    stream = io.StringIO()
    with redirect_stdout(stream):
        code = check(*args, **kwargs)
    return code, stream.getvalue()


def real_pins_text() -> str:
    from checks.proto_sync import proto_pins
    pins = proto_pins(ROOT)
    return "proto:\n" + "".join(f"  {key}: {value}\n" for key, value in pins.items())


def next_free_number(proto_text: str) -> int:
    match = re.search(r"message EventEnvelope \{(.*?)\n\}", proto_text, re.S)
    numbers = [int(n) for n in re.findall(r"= (\d+);", match.group(1))]
    return max(numbers) + 1


def inject_drift_field(root: Path, field_name: str = "drift_probe") -> int:
    """Append a field to EventEnvelope in a temp tree's .proto (R8 rule:
    next free number scanned, never hard-coded)."""
    path = root / "api/proto/audit.proto"
    text = path.read_text(encoding="utf-8")
    match = re.search(r"message EventEnvelope \{(.*?)\n\}", text, re.S)
    body = match.group(1)
    number = max(int(n) for n in re.findall(r"= (\d+);", body)) + 1
    new_body = body.rstrip() + f"\n  string {field_name} = {number};\n"
    path.write_text(text[:match.start(1)] + new_body + text[match.end(1):], encoding="utf-8")
    return number


def in_git_repo(root: Path = ROOT) -> bool:
    return subprocess.run(["git", "-C", str(root), "rev-parse", "--git-dir"],
                          capture_output=True, check=False).returncode == 0


def pinned_toolchain_present() -> bool:
    from checks.proto_sync import proto_pins
    pins = proto_pins(ROOT)
    return ((ROOT / "bin" / f"protoc-{pins['protoc']}" / "bin" / "protoc").exists()
            and (ROOT / "bin" / "protoc-gen-go").exists()
            and (ROOT / "bin" / "protoc-gen-go-grpc").exists())


class ProtoSyncTest(unittest.TestCase):
    """proto_sync: descriptor-parse drift guard + version pins + replay."""

    def make_sync_tree(self):
        """Temp root with the real api/proto + real pins + minimal go.mod."""
        tree = tempfile.TemporaryDirectory()
        root = Path(tree.name)
        (root / "api").mkdir()
        shutil.copytree(ROOT / "api" / "proto", root / "api" / "proto")
        (root / "engineering.yaml").write_text(real_pins_text(), encoding="utf-8")
        from checks.proto_sync import proto_pins
        protobuf_pin = proto_pins(ROOT)["protoc-gen-go"]
        (root / "go.mod").write_text(
            f"module example\n\ngo 1.26\n\nrequire (\n\tgoogle.golang.org/protobuf {protobuf_pin}\n)\n",
            encoding="utf-8")
        self.addCleanup(tree.cleanup)
        return Path(tree.name)

    def make_tree_with_replay_script(self, symlink_bin=False):
        root = self.make_sync_tree()
        (root / "scripts").mkdir()
        shutil.copy(ROOT / "scripts" / "proto-gen.py", root / "scripts" / "proto-gen.py")
        if symlink_bin:
            os.symlink(ROOT / "bin", root / "bin")
        return root

    # -- R1 gate membership ------------------------------------------------

    def test_proto_sync_in_quality_tuple(self):
        source = (ROOT / "cli.py").read_text(encoding="utf-8")
        self.assertIn("from checks.proto_sync import run as proto_sync", source)
        quality = source[source.index("def cmd_quality"):source.index("QUALITY PASS")]
        match = re.search(r"for command in \((.*?)\):", quality, re.S)
        self.assertIsNotNone(match, "cmd_quality command tuple not found")
        commands = [name.strip() for name in match.group(1).split(",") if name.strip()]
        self.assertIn("proto_sync", commands)
        self.assertGreater(commands.index("proto_sync"), commands.index("contract_fields"),
                           "proto_sync must run immediately after contract_fields (fails fast)")

    # -- R2a descriptor comparison -----------------------------------------

    def test_baseline_run_pass(self):
        from checks.proto_sync import run
        code, output = run_capture(run, ROOT)
        self.assertEqual(code, 0, output)
        self.assertIn("PASS: proto-sync", output)

    def test_drift_detected_by_generated_code(self):
        from checks.proto_sync import run
        root = self.make_sync_tree()
        inject_drift_field(root)
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("drift_probe", output)
        self.assertIn("EventEnvelope", output)

    def test_stale_generated_field_detected(self):
        from checks.proto_sync import run
        root = self.make_sync_tree()
        path = root / "api/proto/audit.proto"
        text = path.read_text(encoding="utf-8")
        self.assertIn("  bytes payload_json = 21;", text)
        path.write_text(text.replace("  bytes payload_json = 21;\n", ""), encoding="utf-8")
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("payload_json", output)

    def test_parse_failure_fails_closed(self):
        from checks.proto_sync import run
        root = self.make_sync_tree()
        path = root / "api/proto/audit.pb.go"
        text = path.read_text(encoding="utf-8")
        self.assertIn(r"\x1b", text)
        path.write_text(text.replace(r"\x1b", r"\xZZ", 1), encoding="utf-8")
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("parse error", output)

    # -- R3 version pins ---------------------------------------------------

    def test_generator_version_pins(self):
        from checks.proto_sync import run, proto_pins
        pins = proto_pins(ROOT)
        root = self.make_sync_tree()

        pb_path = root / "api/proto/audit.pb.go"
        text = pb_path.read_text(encoding="utf-8")
        pb_path.write_text(text.replace(f"protoc-gen-go {pins['protoc-gen-go']}",
                                        "protoc-gen-go v9.9.9"), encoding="utf-8")
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("v9.9.9", output)
        self.assertIn(pins["protoc-gen-go"], output)

        grpc_path = root / "api/proto/audit_grpc.pb.go"
        text = grpc_path.read_text(encoding="utf-8")
        grpc_path.write_text(text.replace(f"protoc-gen-go-grpc {pins['protoc-gen-go-grpc']}",
                                          "protoc-gen-go-grpc v9.9.9"), encoding="utf-8")
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("v9.9.9", output)

        root2 = self.make_sync_tree()
        text = (root2 / "api/proto/audit.pb.go").read_text(encoding="utf-8")
        (root2 / "api/proto/audit.pb.go").write_text(
            text.replace(f"protoc        {pins['protoc']}",
                         f"protoc        v9.9.9"), encoding="utf-8")
        code, output = run_capture(run, root2)
        self.assertNotEqual(code, 0)
        self.assertIn("v9.9.9", output)

        root3 = self.make_sync_tree()
        (root3 / "go.mod").write_text(
            (root3 / "go.mod").read_text(encoding="utf-8").replace(
                pins["protoc-gen-go"], "v9.9.9"), encoding="utf-8")
        code, output = run_capture(run, root3)
        self.assertNotEqual(code, 0)
        self.assertIn("go.mod", output)

    # -- R4 contract_fields uses the generated descriptor ------------------

    def _make_contract_tree(self):
        root = self.make_sync_tree()
        (root / "internal" / "domain").mkdir(parents=True)
        shutil.copy(ROOT / "internal/domain/models.go", root / "internal/domain/models.go")
        (root / "api" / "openapi").mkdir(parents=True)
        shutil.copy(ROOT / "api/openapi/openapi.yaml", root / "api/openapi/openapi.yaml")
        (root / "api" / "asyncapi").mkdir(parents=True)
        shutil.copy(ROOT / "api/asyncapi/asyncapi.yaml", root / "api/asyncapi/asyncapi.yaml")
        return root

    def test_contract_fields_uses_generated_code(self):
        from checks.contract_fields import run as contract_fields
        from checks.proto_sync import envelope_fields, run as proto_sync
        root = self._make_contract_tree()
        code, output = run_capture(contract_fields, root)
        self.assertEqual(code, 0, output)
        inject_drift_field(root)
        # The uncompiled field must not be in the descriptor-derived set...
        self.assertNotIn("drift_probe", envelope_fields(root))
        # ...proto_sync must fail on it...
        code, output = run_capture(proto_sync, root)
        self.assertNotEqual(code, 0)
        # ...and the business-field parity loop must still pass (R4 point:
        # the source-text path could previously be satisfied by an
        # uncompiled field; the descriptor path cannot).
        code, output = run_capture(contract_fields, root)
        self.assertEqual(code, 0, output)

    def test_envelope_fields_parse_error_propagates(self):
        from checks.contract_fields import run as contract_fields
        from checks.proto_sync import ProtoSyncParseError, envelope_fields
        root = self._make_contract_tree()
        path = root / "api/proto/audit.pb.go"
        text = path.read_text(encoding="utf-8")
        path.write_text(text.replace(r"\x1b", r"\xZZ", 1), encoding="utf-8")
        with self.assertRaises(ProtoSyncParseError):
            envelope_fields(root)
        code, output = run_capture(contract_fields, root)
        self.assertNotEqual(code, 0)

    # -- R5 make proto -----------------------------------------------------

    def test_make_proto_registered(self):
        makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
        phony = next(line for line in makefile.splitlines() if line.startswith(".PHONY:"))
        self.assertIn("proto", phony.split())
        help_line = next(line for line in makefile.splitlines() if "make targets:" in line)
        self.assertIn("proto", help_line)
        self.assertIn("proto:\n\tpython3 scripts/proto-gen.py", makefile)

    @unittest.skipUnless(pinned_toolchain_present(), "pinned toolchain not bootstrapped in bin/")
    @unittest.skipUnless(in_git_repo(), "requires the git repository")
    def test_make_proto_idempotent(self):
        before = {name: hashlib.sha256((ROOT / "api/proto" / name).read_bytes()).hexdigest()
                  for name in ("audit.pb.go", "audit_grpc.pb.go")}
        first = subprocess.run(["make", "proto"], cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
        second = subprocess.run(["make", "proto"], cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
        after = {name: hashlib.sha256((ROOT / "api/proto" / name).read_bytes()).hexdigest()
                 for name in ("audit.pb.go", "audit_grpc.pb.go")}
        self.assertEqual(before, after, "make proto must be byte-idempotent (R5/R9)")
        status = subprocess.run(["git", "status", "--porcelain", "api/proto"],
                                cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(status.stdout.strip(), "",
                         "make proto must leave api/proto byte-identical (CI no-diff assertion)")

    # -- R6 replay ---------------------------------------------------------

    @unittest.skipUnless(pinned_toolchain_present(), "pinned toolchain not bootstrapped in bin/")
    def test_replay_no_diff(self):
        result = subprocess.run([sys.executable, str(ROOT / "scripts/proto-gen.py"), "--check"],
                                cwd=ROOT, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("byte-identical", result.stdout)

        root = self.make_tree_with_replay_script(symlink_bin=True)
        inject_drift_field(root)
        result = subprocess.run([sys.executable, str(root / "scripts/proto-gen.py"), "--check"],
                                cwd=root, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("differs", result.stdout)

    @unittest.skipUnless(pinned_toolchain_present(), "pinned toolchain not bootstrapped in bin/")
    def test_replay_byte_exact(self):
        root = self.make_tree_with_replay_script(symlink_bin=True)
        # Byte-exactness (R9): a single trailing newline must fail the replay.
        path = root / "api/proto/audit.pb.go"
        path.write_bytes(path.read_bytes() + b"\n")
        result = subprocess.run([sys.executable, str(root / "scripts/proto-gen.py"), "--check"],
                                cwd=root, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("audit.pb.go", result.stdout)

    def test_replay_unavailable_fail_closed(self):
        from checks.proto_sync import run
        # (a) clean tree, no toolchain: R2a+R3 still enforced, replay skipped
        # with an explicit notice (never a silent pass).
        root = self.make_tree_with_replay_script(symlink_bin=False)
        code, output = run_capture(run, root)
        self.assertEqual(code, 0, output)
        self.assertIn("replay skipped", output)
        # (b) drifted tree, no toolchain: the descriptor check fires before
        # the replay skip -- fail-closed ordering.
        root2 = self.make_tree_with_replay_script(symlink_bin=False)
        inject_drift_field(root2)
        code, output = run_capture(run, root2)
        self.assertNotEqual(code, 0)
        self.assertIn("drift_probe", output)

    def test_replay_exit_code_contract(self):
        from checks.proto_sync import run
        # Exit 3 = "toolchain unavailable": proto_sync must treat it as a
        # skip, not a failure, and must say so in its output.
        root = self.make_sync_tree()
        (root / "scripts").mkdir()
        (root / "scripts/proto-gen.py").write_text(
            "#!/usr/bin/env python3\nimport sys\nprint('proto-gen: replay unavailable')\nsys.exit(3)\n",
            encoding="utf-8")
        code, output = run_capture(run, root)
        self.assertEqual(code, 0, output)
        self.assertIn("replay skipped", output)
        # Exit 1 = replay drift: proto_sync must fail echoing the script's
        # first-differing-file message.
        root2 = self.make_sync_tree()
        (root2 / "scripts").mkdir()
        (root2 / "scripts/proto-gen.py").write_text(
            "#!/usr/bin/env python3\nimport sys\n"
            "print('proto-gen: api/proto/audit_grpc.pb.go differs from pinned regeneration')\n"
            "sys.exit(1)\n",
            encoding="utf-8")
        code, output = run_capture(run, root2)
        self.assertNotEqual(code, 0)
        self.assertIn("audit_grpc.pb.go differs", output)

    # -- descriptor walker fuzz (oracle: google.protobuf) ------------------

    @unittest.skipUnless(HAS_ORACLE, "google.protobuf oracle not installed")
    def test_descriptor_parser_fuzz(self):
        from checks.proto_sync import ProtoSyncParseError, extract_raw_desc, parse_descriptor
        raw = extract_raw_desc((ROOT / "api/proto/audit.pb.go").read_text(encoding="utf-8"))
        baseline = parse_descriptor(raw)

        def oracle_messages(file_proto):
            def normalize(field):
                if field.type in (10, 11, 14):
                    return field.type_name.lstrip(".").rsplit(".", 1)[-1]
                return field.type
            result = {}

            def walk(messages):
                for message in messages:
                    result[message.name] = {
                        field.number: (field.name, field.label, normalize(field))
                        for field in message.field}
                    walk(message.nested_type)
            walk(file_proto.message_type)
            return result

        sanity = descriptor_pb2.FileDescriptorProto()
        sanity.ParseFromString(raw)
        self.assertEqual(oracle_messages(sanity), baseline, "walker vs oracle on pristine bytes")

        rng = random.Random(20260810)
        checked = 0

        def assert_parse(mutated, label):
            """Property: either a clean ProtoSyncParseError, or -- when the
            oracle (google.protobuf) parses the same bytes -- an
            oracle-identical message view, or -- when the oracle rejects
            them -- the pristine baseline (corruption confined to regions
            the walker does not inspect, e.g. FileOptions).  Anything else
            is a silently different field mapping."""
            try:
                result = parse_descriptor(mutated)
            except ProtoSyncParseError:
                return
            oracle = descriptor_pb2.FileDescriptorProto()
            try:
                oracle.ParseFromString(mutated)
            except Exception:
                self.assertEqual(result, baseline,
                                 f"{label}: accepted bytes the oracle rejects with a novel mapping")
                return
            self.assertEqual(result, oracle_messages(oracle), f"{label} diverges from oracle")

        # Truncation at every offset of the (<= 4 KiB) descriptor.
        for cut in range(len(raw)):
            assert_parse(raw[:cut], f"truncation at {cut}")
            checked += 1
        # >= 100 single-byte flips over the whole buffer.
        for _ in range(150):
            offset = rng.randrange(len(raw))
            mutated = bytearray(raw)
            mutated[offset] ^= 1 << rng.randrange(8)
            assert_parse(bytes(mutated), f"flip at {offset}")
            checked += 1
        self.assertGreaterEqual(checked, 100)

    # -- R7 cmd_generate honesty -------------------------------------------

    def test_cmd_generate_reflects_sync(self):
        import cli
        original_root = cli.ROOT
        self.addCleanup(setattr, cli, "ROOT", original_root)
        root = self.make_sync_tree()
        for directory, filename in (("openapi", "openapi.yaml"), ("asyncapi", "asyncapi.yaml")):
            target = root / "api" / directory
            target.mkdir()
            shutil.copy(ROOT / "api" / directory / filename, target / filename)
        inject_drift_field(root)
        cli.ROOT = root
        code, output = run_capture(cli.cmd_generate)
        self.assertNotEqual(code, 0)
        self.assertNotIn("no generated files require regeneration", output)
        self.assertIn("drift_probe", output)
        cli.ROOT = original_root
        code, output = run_capture(cli.cmd_generate)
        self.assertEqual(code, 0, output)

    # -- AC-3d README CI assertion -----------------------------------------

    def test_readme_documents_ci_assertion(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn('make proto && test -z "$(git status --porcelain api/proto)"', readme)

    # -- pin parsers agree -------------------------------------------------

    def _import_proto_gen(self):
        spec = importlib.util.spec_from_file_location("proto_gen", ROOT / "scripts/proto-gen.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_pins_parsers_agree(self):
        module = self._import_proto_gen()
        from checks.proto_sync import proto_pins as check_pins
        self.assertEqual(check_pins(ROOT), module.proto_pins(ROOT),
                         "proto_sync and proto-gen.py must read the same pin manifest")

    def test_bootstrap_offline(self):
        """F8: download / go install failure -> exit 2 with the URL printed,
        no partial state in bin/ (fail closed, never a silent skip)."""
        module = self._import_proto_gen()
        root = self.make_sync_tree()
        (root / "bin").mkdir()

        # (a) protoc zip download failure.
        def boom(url, timeout=None):
            raise OSError("offline")
        original_urlopen = module.urllib.request.urlopen
        module.urllib.request.urlopen = boom
        try:
            stream = io.StringIO()
            with redirect_stdout(stream), redirect_stderr(stream):
                code = module.main([], root=root)
        finally:
            module.urllib.request.urlopen = original_urlopen
        self.assertEqual(code, 2)
        self.assertIn("github.com/protocolbuffers/protobuf/releases/download", stream.getvalue())
        self.assertFalse((root / "bin" / "protoc-3.21.12").exists(),
                         "failed download must leave no partial bin tree")

        # (b) go install failure (protoc already available).
        module.ensure_protoc = lambda root, pin: root / "bin" / "protoc"
        real_run = module.subprocess.run

        def fake_run(command, *args, **kwargs):
            if isinstance(command, list) and command and command[0] == "go":
                return subprocess.CompletedProcess(command, 1, stdout="", stderr="go: offline")
            return real_run(command, *args, **kwargs)
        module.subprocess.run = fake_run
        try:
            code = module.main([], root=root)
        finally:
            module.subprocess.run = real_run
        self.assertEqual(code, 2)
        self.assertFalse((root / "bin" / "protoc-gen-go").exists(),
                         "failed go install must leave no generator binary")


class ProtoDriftSimulationTest(unittest.TestCase):
    """Gate-level drift scenarios.  Both tests drive ONE implementation,
    scripts/drift-simulate.sh -- the script never touches the committed tree
    (git archive HEAD copy only)."""

    def _write_stub(self, path: Path, body: str):
        path.write_text(body, encoding="utf-8")
        os.chmod(path, 0o755)

    def test_drift_simulate_script(self):
        """Stub-gate runs of the real script (fast, deterministic)."""
        if not in_git_repo():
            self.skipTest("requires the git repository (git archive HEAD)")
        stub_dir = Path(tempfile.mkdtemp(prefix="drift-stub-"))
        self.addCleanup(shutil.rmtree, str(stub_dir))

        # 1) stub fails-then-passes (naming the drifted field) -> PASS.
        state = stub_dir / "state"
        self._write_stub(stub_dir / "fail-then-pass.sh", (
            "#!/usr/bin/env bash\n"
            "f=\"${GATE_STATE}\"\n"
            "if [ ! -f \"$f\" ]; then\n"
            "  echo 1 > \"$f\"\n"
            "  echo \"FAIL: proto-sync: EventEnvelope field 'drift_probe' present in audit.proto but missing from generated descriptor\" >&2\n"
            "  exit 1\n"
            "fi\n"
            "echo \"PASS: everything\"\n"
            "exit 0\n"))
        env = dict(os.environ, GATE_CMD=f"bash {stub_dir / 'fail-then-pass.sh'}",
                   GATE_STATE=str(state))
        result = subprocess.run(["bash", str(ROOT / "scripts/drift-simulate.sh")], cwd=ROOT,
                                capture_output=True, text=True, env=env)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("drift simulation: PASS", result.stdout)

        # 2) stub always-passes -> the script must fail loudly.
        self._write_stub(stub_dir / "always-pass.sh",
                         "#!/usr/bin/env bash\necho \"PASS: everything\"\nexit 0\n")
        env = dict(os.environ, GATE_CMD=f"bash {stub_dir / 'always-pass.sh'}")
        result = subprocess.run(["bash", str(ROOT / "scripts/drift-simulate.sh")], cwd=ROOT,
                                capture_output=True, text=True, env=env)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("gate passed despite proto drift", result.stdout + result.stderr)

        # 3) gate fails without naming the field -> loud failure.
        self._write_stub(stub_dir / "fail-generic.sh",
                         "#!/usr/bin/env bash\necho \"FAIL: something else\" >&2\nexit 1\n")
        env = dict(os.environ, GATE_CMD=f"bash {stub_dir / 'fail-generic.sh'}")
        result = subprocess.run(["bash", str(ROOT / "scripts/drift-simulate.sh")], cwd=ROOT,
                                capture_output=True, text=True, env=env)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("did not name drifted field", result.stdout + result.stderr)

        # 4) missing anchor line -> loud failure before any gate run.
        env = dict(os.environ, GATE_CMD=f"bash {stub_dir / 'always-pass.sh'}",
                   ANCHOR="  string does_not_exist = 99;")
        result = subprocess.run(["bash", str(ROOT / "scripts/drift-simulate.sh")], cwd=ROOT,
                                capture_output=True, text=True, env=env)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("anchor", result.stdout + result.stderr)

    @unittest.skipUnless(os.environ.get("AUDIT_DRIFT_SIM") == "1",
                         "opt-in: AUDIT_DRIFT_SIM=1 python3 -m unittest checks.test_proto_sync")
    @unittest.skipUnless(in_git_repo(), "requires the git repository")
    def test_gate_fails_on_proto_drift(self):
        """AC-1 via the single drift-sim implementation: inject in a git
        archive copy, the full gate must fail naming the field, restore, and
        the full gate must pass.  ~2x the gate duration; opt-in by design."""
        result = subprocess.run(["bash", str(ROOT / "scripts/drift-simulate.sh")], cwd=ROOT,
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("drift simulation: PASS", result.stdout)


if __name__ == "__main__":
    unittest.main()
