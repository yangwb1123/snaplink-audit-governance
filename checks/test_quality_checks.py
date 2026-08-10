import unittest
from pathlib import Path

from checks.architecture import run as architecture
from checks.config import get_config
from checks.invariants import default_secrets_single_source
from checks.route_contract import normalize


class QualityChecksTest(unittest.TestCase):
    def test_config_has_quality_limits(self):
        config = get_config()
        self.assertGreater(config.max_go_lines, 0)
        self.assertGreater(config.max_function_lines, 0)
        self.assertGreater(config.max_decisions, 0)

    def test_route_parameter_names_are_normalized(self):
        self.assertEqual(normalize("/api/v1/events/{eventID}"), "/api/v1/events/{}")

    def test_architecture_is_clean(self):
        self.assertEqual(architecture(), 0)

    def test_default_secrets_are_single_source(self):
        self.assertEqual(default_secrets_single_source(), [])


if __name__ == "__main__":
    unittest.main()



class DevAuthManifestCheckTest(unittest.TestCase):
    """dev_auth_manifest: non-verify manifests enabling dev auth fail the
    gate; verify files are exempt."""

    def make_tree(self, files):
        import tempfile
        tree = tempfile.TemporaryDirectory()
        deploy = Path(tree.name) / "deploy"
        deploy.mkdir()
        for name, content in files.items():
            (deploy / name).write_text(content, encoding="utf-8")
        return tree

    def test_verify_manifest_exempt(self):
        from checks.dev_auth_manifest import run
        tree = self.make_tree({"docker-compose.verify.yml": "AUDIT_ALLOW_DEV_AUTH: 'true'\n"})
        self.addCleanup(tree.cleanup)
        self.assertEqual(run(Path(tree.name)), 0)

    def test_non_verify_manifest_with_dev_auth_fails(self):
        from checks.dev_auth_manifest import run
        tree = self.make_tree({"compose.prod.yml": "AUDIT_ALLOW_DEV_AUTH: 'true'\n"})
        self.addCleanup(tree.cleanup)
        self.assertNotEqual(run(Path(tree.name)), 0)

    def test_clean_manifest_passes(self):
        from checks.dev_auth_manifest import run
        tree = self.make_tree({"compose.prod.yml": "AUDIT_ALLOW_DEV_AUTH: 'false'\n"})
        self.addCleanup(tree.cleanup)
        self.assertEqual(run(Path(tree.name)), 0)


class TenantConsistencyCheckTest(unittest.TestCase):
    """tenant_consistency: the server-side tenant stamping must appear after
    the ErrTenantMismatch check; re-stamping before it fails the gate."""

    GOOD = (
        "func ingest() {\n"
        "\tif event.TenantID != \"\" && event.TenantID != tenantID {\n"
        "\t\treturn ErrTenantMismatch\n"
        "\t}\n"
        "\tevent.TenantID = tenantID\n"
        "}\n"
    )
    BAD = (
        "func ingest() {\n"
        "\tevent.TenantID = tenantID\n"
        "\tif event.TenantID != \"\" && event.TenantID != tenantID {\n"
        "\t\treturn ErrTenantMismatch\n"
        "\t}\n"
        "}\n"
    )

    def make_tree(self, content):
        import tempfile
        tree = tempfile.TemporaryDirectory()
        internal = Path(tree.name) / "internal" / "service"
        internal.mkdir(parents=True)
        (internal / "service.go").write_text(content, encoding="utf-8")
        return tree

    def test_check_before_stamp_passes(self):
        from checks.tenant_consistency import run
        tree = self.make_tree(self.GOOD)
        self.addCleanup(tree.cleanup)
        self.assertEqual(run(Path(tree.name)), 0)

    def test_stamp_before_check_fails(self):
        from checks.tenant_consistency import run
        tree = self.make_tree(self.BAD)
        self.addCleanup(tree.cleanup)
        self.assertNotEqual(run(Path(tree.name)), 0)

    def test_missing_check_fails(self):
        from checks.tenant_consistency import run
        tree = self.make_tree("func ingest() {}\n")
        self.addCleanup(tree.cleanup)
        self.assertNotEqual(run(Path(tree.name)), 0)


class StreamConsistencyCheckTest(unittest.TestCase):
    """stream_consistency: the client stream_id strip must appear after the
    DS-08 tenant check, before the first event.Stream() derivation, and the
    server stamp must follow the derivation; any reorder fails the gate."""

    GOOD = (
        "func ingest() {\n"
        "\tif event.TenantID != \"\" && event.TenantID != tenantID {\n"
        "\t\treturn ErrTenantMismatch\n"
        "\t}\n"
        "\tevent.StreamID = \"\"\n"
        "\tstreamID := event.Stream()\n"
        "\tevent.StreamID = streamID\n"
        "}\n"
    )
    BAD_ORDER = (
        "func ingest() {\n"
        "\tif event.TenantID != \"\" && event.TenantID != tenantID {\n"
        "\t\treturn ErrTenantMismatch\n"
        "\t}\n"
        "\tstreamID := event.Stream()\n"
        "\tevent.StreamID = \"\"\n"
        "\tevent.StreamID = streamID\n"
        "}\n"
    )
    BAD_NO_STRIP = (
        "func ingest() {\n"
        "\tif event.TenantID != \"\" && event.TenantID != tenantID {\n"
        "\t\treturn ErrTenantMismatch\n"
        "\t}\n"
        "\tstreamID := event.Stream()\n"
        "\tevent.StreamID = streamID\n"
        "}\n"
    )
    BAD_NO_STAMP = (
        "func ingest() {\n"
        "\tif event.TenantID != \"\" && event.TenantID != tenantID {\n"
        "\t\treturn ErrTenantMismatch\n"
        "\t}\n"
        "\tevent.StreamID = \"\"\n"
        "\tstreamID := event.Stream()\n"
        "}\n"
    )
    BAD_STRIP_BEFORE_TENANT = (
        "func ingest() {\n"
        "\tevent.StreamID = \"\"\n"
        "\tif event.TenantID != \"\" && event.TenantID != tenantID {\n"
        "\t\treturn ErrTenantMismatch\n"
        "\t}\n"
        "\tstreamID := event.Stream()\n"
        "\tevent.StreamID = streamID\n"
        "}\n"
    )

    def make_tree(self, content):
        import tempfile
        tree = tempfile.TemporaryDirectory()
        internal = Path(tree.name) / "internal" / "service"
        internal.mkdir(parents=True)
        (internal / "service.go").write_text(content, encoding="utf-8")
        return tree

    def test_strip_before_derivation_passes(self):
        from checks.stream_consistency import run
        tree = self.make_tree(self.GOOD)
        self.addCleanup(tree.cleanup)
        self.assertEqual(run(Path(tree.name)), 0)

    def test_strip_after_derivation_fails(self):
        from checks.stream_consistency import run
        tree = self.make_tree(self.BAD_ORDER)
        self.addCleanup(tree.cleanup)
        self.assertNotEqual(run(Path(tree.name)), 0)

    def test_missing_strip_fails(self):
        from checks.stream_consistency import run
        tree = self.make_tree(self.BAD_NO_STRIP)
        self.addCleanup(tree.cleanup)
        self.assertNotEqual(run(Path(tree.name)), 0)

    def test_missing_stamp_fails(self):
        from checks.stream_consistency import run
        tree = self.make_tree(self.BAD_NO_STAMP)
        self.addCleanup(tree.cleanup)
        self.assertNotEqual(run(Path(tree.name)), 0)

    def test_strip_before_tenant_check_fails(self):
        from checks.stream_consistency import run
        tree = self.make_tree(self.BAD_STRIP_BEFORE_TENANT)
        self.addCleanup(tree.cleanup)
        self.assertNotEqual(run(Path(tree.name)), 0)
