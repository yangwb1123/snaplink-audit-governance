import io
import stat
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

from checks import consistency_key


class ConsistencyKeyCheckTest(unittest.TestCase):
    def make_tree(self):
        tree = tempfile.TemporaryDirectory()
        root = Path(tree.name)
        binary_dir = root / "bin"
        binary_dir.mkdir()
        for name in consistency_key.BINARY_NAMES:
            path = binary_dir / name
            path.write_text("test binary", encoding="utf-8")
            path.chmod(path.stat().st_mode | stat.S_IXUSR)
        self.addCleanup(tree.cleanup)
        return root

    def run_capture(self, root, results, env=None):
        output = io.StringIO()
        with patch("checks.consistency_key.subprocess.run", side_effect=results) as mocked:
            with redirect_stdout(output):
                code = consistency_key.run(root=root, env=env)
        return code, output.getvalue(), mocked

    def test_extract_key_requires_one_nonempty_value(self):
        self.assertEqual(consistency_key.extract_key("prefix consistency_key=HMAC-SHA256|file:/archive\n"), "HMAC-SHA256|file:/archive")
        self.assertIsNone(consistency_key.extract_key(""))
        self.assertIsNone(consistency_key.extract_key("consistency_key=one\nconsistency_key=two\n"))
        self.assertIsNone(consistency_key.extract_key("consistency_key=\n"))

    def test_identical_keys_pass_and_use_built_binaries(self):
        root = self.make_tree()
        env = {"AUDIT_SIGNING_SECRET": "same", "AUDIT_ENCRYPTION_KEY": "different"}
        results = [
            CompletedProcess([], 0, stdout="consistency_key=HMAC-SHA256|file:/archive\n", stderr=""),
            CompletedProcess([], 0, stdout="consistency_key=HMAC-SHA256|file:/archive\n", stderr=""),
        ]
        code, output, mocked = self.run_capture(root, results, env)
        self.assertEqual(code, 0, output)
        self.assertIn("PASS: consistency_key identical across processes", output)
        self.assertEqual(mocked.call_count, 2)
        self.assertEqual(mocked.call_args_list[0].args[0], [str(root / "bin" / "audit-api"), "-consistency-key"])
        self.assertEqual(mocked.call_args_list[1].args[0], [str(root / "bin" / "audit-governance-worker"), "-consistency-key"])
        self.assertEqual(mocked.call_args_list[0].kwargs["env"], env)
        self.assertEqual(mocked.call_args_list[1].kwargs["env"], env)

    def test_mismatch_fails_and_prints_both_values(self):
        root = self.make_tree()
        results = [
            CompletedProcess([], 0, stdout="consistency_key=HMAC-SHA256|s3:worm-a\n", stderr=""),
            CompletedProcess([], 0, stdout="consistency_key=vault-transit:key|s3:worm-b\n", stderr=""),
        ]
        code, output, _ = self.run_capture(root, results, {})
        self.assertEqual(code, 1)
        self.assertIn("FAIL: consistency_key mismatch", output)
        self.assertIn("audit-api:           HMAC-SHA256|s3:worm-a", output)
        self.assertIn("audit-governance-worker: vault-transit:key|s3:worm-b", output)

    def test_nonzero_or_missing_key_fails_closed(self):
        root = self.make_tree()
        results = [
            CompletedProcess([], 1, stdout="", stderr="configuration failed"),
            CompletedProcess([], 0, stdout="ready but no key\n", stderr=""),
        ]
        code, output, _ = self.run_capture(root, results, {})
        self.assertEqual(code, 1)
        self.assertIn("FAIL: consistency_key check", output)
        self.assertIn("audit-api: <missing>", output)
        self.assertIn("audit-governance-worker: <missing>", output)

    def test_missing_binary_fails_without_executing_it(self):
        tree = tempfile.TemporaryDirectory()
        root = Path(tree.name)
        (root / "bin").mkdir()
        self.addCleanup(tree.cleanup)
        with patch("checks.consistency_key.subprocess.run") as mocked:
            output = io.StringIO()
            with redirect_stdout(output):
                code = consistency_key.run(root=root, env={})
        self.assertEqual(code, 1)
        self.assertIn("built binary not found", output.getvalue())
        mocked.assert_not_called()


if __name__ == "__main__":
    unittest.main()
