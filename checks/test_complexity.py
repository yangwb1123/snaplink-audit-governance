import textwrap
import unittest
from pathlib import Path

from checks.complexity import code_line, decisions, function_spans

TRAILING_DECLARATIONS = textwrap.dedent(
    """\
    package domain

    // AllowsClient reports whether the source accepts the client.
    func (s SourceSystem) AllowsClient(clientID string) bool {
    \tif clientID == "" {
    \t\treturn false
    \t}
    \treturn clientID == s.ID
    }

    // A long doc comment for the next type must not be charged to the
    // function above it, and neither must the declaration itself.
    //
    // Type Principal carries only server-derived identity.
    type Principal struct {
    \tClientID string
    \tTenantID string
    \tRoles    []string
    \tScopes   []string
    }
    """
)


class ComplexityMetricTest(unittest.TestCase):
    def test_trailing_declarations_are_not_charged_to_a_function(self):
        # Regression: the old metric ended a function at the next `func` line
        # and therefore charged every following type/var/const declaration and
        # doc comment to it. SourceSystem.AllowsClient in internal/domain
        # measured 258 lines for 16 lines of code, and the worst offender in
        # the repository was always the last function before a block of types.
        spans = function_spans(TRAILING_DECLARATIONS.splitlines())
        self.assertEqual([name for name, _, _ in spans], ["AllowsClient"])
        _, _, body = spans[0]
        self.assertLess(len(body), 20, body)

    def test_brace_in_column_zero_ends_the_function(self):
        source = textwrap.dedent(
            """\
            package p
            func a() {
            \tx := 1
            }
            func b() {
            \ty := 2
            }
            """
        )
        spans = function_spans(source.splitlines())
        self.assertEqual([(name, len(body)) for name, _, body in spans],
                         [("a", 2), ("b", 2)])

    def test_comments_and_blanks_do_not_count_toward_length(self):
        source = textwrap.dedent(
            """\
            package p
            func a() {
            \t// a comment that is much longer than the code it describes
            \t//
            \t// and another one

            \tx := 1
            }
            """
        )
        _, _, body = function_spans(source.splitlines())[0]
        self.assertEqual(len(body), 2, body)  # the signature plus `x := 1`

    def test_branch_keywords_are_matched_as_whole_words(self):
        # `if(` and `} else if` are branches; `ifdef` and a branch word inside
        # a string literal are not. The old token counting (" if ", "&&", ...)
        # missed every one of these.
        self.assertEqual(decisions(["if x {", "} else if y {"]), 2)
        self.assertEqual(decisions(["for i := range x {", "switch x {", "case 1:"]), 3)
        self.assertEqual(decisions(["ifdefined", "ifdef", "format"]), 0)
        self.assertEqual(decisions(["a && b", "c || d"]), 2)
        self.assertEqual(decisions(['log("if the tenant is not ready")']), 0)
        self.assertEqual(decisions(['name := "switch"']), 0)

    def test_code_line_strips_comments_and_string_literals(self):
        self.assertEqual(code_line('\tname := "a" // trailing comment'), 'name := ""')
        self.assertEqual(code_line("   "), "")

    def test_malformed_tree_falls_back_to_the_next_function(self):
        # A file without a column-0 closing brace (never produced by gofmt)
        # must degrade to the previous behaviour instead of raising.
        source = ["package p", "func a() {", "\tx := 1", "func b() {", "\ty := 2"]
        spans = function_spans(source)
        self.assertEqual([(name, len(body)) for name, _, body in spans],
                         [("a", 2), ("b", 2)])

    def test_repository_worst_case_is_inside_the_budget(self):
        # Guards the documented calibration: the budgets in engineering.yaml
        # must stay above the real worst case, and the real worst case must
        # stay the function the comment names. If a refactor splits Ingest,
        # lower the budgets in the same change.
        from checks.complexity import check_source
        from checks.config import get_config

        root = Path(__file__).resolve().parents[1]
        config = get_config()
        worst = (0, "")
        for path in sorted(root.rglob("*.go")):
            if "_test.go" in path.name or "/bin/" in str(path):
                continue
            for name, start, body in function_spans(path.read_text(encoding="utf-8").splitlines()):
                if decisions(body) > worst[0]:
                    worst = (decisions(body), f"{path.relative_to(root)}:{start + 1} {name}")
        self.assertEqual(check_source(root / "internal/service/service.go"), [])
        self.assertLessEqual(worst[0], config.max_decisions, worst[1])
        self.assertTrue(worst[1].endswith("Ingest"), worst[1])
        self.assertTrue(worst[1].startswith("internal/service/service.go:"), worst[1])


if __name__ == "__main__":
    unittest.main()
