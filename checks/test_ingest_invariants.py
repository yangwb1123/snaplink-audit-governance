import textwrap
import unittest
from pathlib import Path

from checks.ingest_invariants import ingest_body

INGEST = """\
package service

func (s *Service) Ingest(ctx context.Context) error {
	if err := s.check(); err != nil {
		return err
	}
	return nil
}

func (s *Service) Other(ctx context.Context) error {
	return nil
}
"""

OTHER_ONLY = """\
package service

func (s *Service) IngestOther(ctx context.Context) error {
	return nil
}
"""


def tree(files: dict[str, str], name: str) -> Path:
    import tempfile

    directory = Path(tempfile.mkdtemp())
    package = directory / "internal" / "service"
    package.mkdir(parents=True)
    for file_name, content in files.items():
        (package / file_name).write_text(content, encoding="utf-8")
    return directory


class IngestInvariantsTest(unittest.TestCase):
    def test_locates_the_receiver_method_and_its_body(self):
        root = tree({"service.go": INGEST}, "svc")
        self.addCleanup(__import__("shutil").rmtree, root)
        located = ingest_body(root / "internal" / "service" / "service.go")
        self.assertIsNotNone(located)
        source, first, last = located
        body = source.splitlines()[first - 1:last]
        self.assertTrue(body[0].startswith("func (s *Service) Ingest("), body[0])
        self.assertIn("if err := s.check()", "\n".join(body))
        self.assertEqual(body[-1].strip(), "}")

    def test_finds_the_function_in_another_file_of_the_package(self):
        # The point of resolving by declaration: the ingest path may live in
        # any file of the package, and both ingest guards keep working.
        root = tree({"service.go": "package service\n", "ingest_path.go": INGEST}, "ing")
        self.addCleanup(__import__("shutil").rmtree, root)
        self.assertIsNotNone(ingest_body(root / "internal" / "service" / "service.go"))

    def test_missing_function_is_reported_not_defaulted(self):
        root = tree({"service.go": OTHER_ONLY}, "other")
        self.addCleanup(__import__("shutil").rmtree, root)
        self.assertIsNone(ingest_body(root / "internal" / "service" / "service.go"))

    def test_test_files_are_not_scanned(self):
        root = tree({"service.go": "package service\n", "service_test.go": INGEST}, "tests")
        self.addCleanup(__import__("shutil").rmtree, root)
        self.assertIsNone(ingest_body(root / "internal" / "service" / "service.go"))

    def test_nested_braces_do_not_end_the_body_early(self):
        source = textwrap.dedent(
            """\
            package service

            func (s *Service) Ingest(ctx context.Context) error {
            \tif true {
            \t\tfor i := 0; i < 3; i++ {
            \t\t\t_ = i
            \t\t}
            \t}
            \treturn nil
            }
            """
        )
        root = tree({"service.go": source}, "braces")
        self.addCleanup(__import__("shutil").rmtree, root)
        _, first, last = ingest_body(root / "internal" / "service" / "service.go")
        body = "\n".join(source.splitlines()[first - 1:last])
        self.assertIn("for i := 0", body)
        self.assertIn("return nil", body)


if __name__ == "__main__":
    unittest.main()
