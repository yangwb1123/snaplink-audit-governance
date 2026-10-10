"""A sourced token fixture must never copy bearer credentials into CI logs."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class TokenFixtureTests(unittest.TestCase):
    def test_sourced_is_silent_and_direct_execution_keeps_eval_interface(self):
        with tempfile.TemporaryDirectory() as directory:
            curl = Path(directory) / "curl"
            curl.write_text('#!/bin/sh\nprintf \'{"access_token":"test-fixture-token"}\\n\'\n')
            curl.chmod(0o700)
            environment = dict(os.environ, PATH=directory + ":" + os.environ["PATH"],
                               AUDIT_IDP_TOKEN_URL="http://localhost/token",
                               AUDIT_IDP_CLIENT_ID="fixture", AUDIT_IDP_CLIENT_SECRET="fixture")
            environment.pop("AUDIT_IDP_RESOURCE", None)
            sourced = subprocess.run(
                ["bash", "-c", 'source "$1"; test "$AUDIT_E2E_TOKEN" = test-fixture-token; test "$AUDIT_OUTBOX_TOKEN" = test-fixture-token',
                 "fixture", str(ROOT / "scripts/mint-token.sh")],
                env=environment, text=True, capture_output=True, check=True)
            self.assertEqual(sourced.stdout, "")
            self.assertEqual(sourced.stderr, "")
            direct = subprocess.run(["bash", str(ROOT / "scripts/mint-token.sh")],
                                    env=environment, text=True, capture_output=True, check=True)
            self.assertEqual(direct.stdout.count("export AUDIT_"), 2)
            self.assertEqual(direct.stderr, "")
