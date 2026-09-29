"""Static invariants for the quality gate's own wiring.

The gate used to assert itself with two tests that spawned `cli.py check` and
`cli.py quality` as subprocesses. Both skipped themselves when the gate was
running (cmd_quality exports AUDIT_QUALITY_ACTIVE to stop the recursion), so
in CI — whose single job is `python3 cli.py quality` — they never executed,
and running the suite by hand spawned a second full gate instead. The gate
proves itself by running; what needed a test was the wiring an operator
relies on when a stage fails:

- CI really runs the gate;
- every stage the gate runs is reachable as `python3 cli.py <label>`, because
  that is the instruction the failure line prints;
- the cheap `cli.py check` subset cannot drift into enforcing something the
  gate does not;
- the recursion guard is still installed before the suite stage, and no test
  reintroduces a nested full-gate run.
"""

import ast
import re
import unittest
from pathlib import Path

import cli

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "quality.yml"


def _stage_names(function_name: str) -> list[str]:
    """Names in the `for command in (...)` tuple of a cli.py stage loop."""
    tree = ast.parse((ROOT / "cli.py").read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            for inner in ast.walk(node):
                if (isinstance(inner, ast.For) and isinstance(inner.iter, ast.Tuple)
                        and isinstance(inner.target, ast.Name)
                        and inner.target.id == "command"):
                    return [element.id for element in inner.iter.elts
                            if isinstance(element, ast.Name)]
    raise AssertionError(f"no stage loop found in cli.{function_name}()")


def _check_modules(name: str) -> set[str]:
    """checks.* modules a stage runs, directly or through a wrapper.

    A stage name that is not a `cmd_*` function is the alias of a
    `from checks.<name> import run as <name>` import inside cmd_quality, so the
    stage runs that module.
    """
    if not name.startswith("cmd_"):
        return {name}
    # Parse the wrapper's body from the source file: ast.unparse cannot
    # decompile a function object.
    source = (ROOT / "cli.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    body = next(node for node in tree.body
                if isinstance(node, ast.FunctionDef) and node.name == name)
    return {node.module.split(".", 1)[1]
            for node in ast.walk(body)
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("checks.")}


def _stages(function_name: str) -> list[tuple[str, str, object]]:
    """(label, source_name, callable) for each stage of a cli.py loop, using
    the same label cmd_quality prints."""
    stages = []
    for name in _stage_names(function_name):
        if name.startswith("cmd_"):
            stages.append((cli.stage_label(getattr(cli, name)), name, getattr(cli, name)))
        else:
            label = name.replace("_", "-")
            stages.append((label, name, cli.COMMANDS[label]))
    return stages


class GateWiringTest(unittest.TestCase):
    def test_ci_runs_the_quality_gate(self):
        # The gate cannot assert itself from inside itself, so the assertion
        # moves here: the CI job is the gate run, and it must stay one.
        self.assertIn("python3 cli.py quality", WORKFLOW.read_text(encoding="utf-8"))

    def test_every_gate_stage_is_rerunnable_by_its_label(self):
        # cmd_quality prints "FAIL: <label>"; that instruction is only honest
        # if the label is a command.
        stages = _stages("cmd_quality")
        self.assertGreater(len(stages), 20)
        for label, _, command in stages:
            self.assertIn(label, cli.COMMANDS, f"gate stage {label} has no command")
            self.assertTrue(callable(cli.COMMANDS[label]))
            self.assertTrue(callable(command))

    def test_check_stages_are_enforced_by_the_gate(self):
        # `cli.py check` is the cheap subset. A stage may not appear there
        # unless the gate runs it too, otherwise the quick command would
        # enforce something the post-change gate does not.
        gate_modules = set()
        for _, name, _ in _stages("cmd_quality"):
            gate_modules |= _check_modules(name)
        for name in _stage_names("cmd_check"):
            for module in _check_modules(name):
                self.assertIn(module, gate_modules,
                              f"cli.py check runs {module}, the gate does not")

    def test_recursion_guard_precedes_the_checks_suite(self):
        source = (ROOT / "cli.py").read_text(encoding="utf-8")
        quality = source[source.index("def cmd_quality"):source.index("QUALITY PASS")]
        guard = quality.index('os.environ["AUDIT_QUALITY_ACTIVE"] = "1"')
        # Match the stage tuple, not the comment that names cmd_python_checks.
        suite = re.search(r"cmd_python_checks, cmd_test", quality)
        self.assertIsNotNone(suite, "the checks suite is no longer a gate stage")
        self.assertLess(guard, suite.start(), "the suite stage would recurse into the gate")

    def test_no_check_test_spawns_the_whole_gate(self):
        # A test that shells out to the full gate can only ever run outside
        # the gate, i.e. never in CI, while costing a full gate when a
        # developer runs the suite. Keep the suite free of them.
        needle = '"cli.py", ' + '"quality"'
        for path in sorted((ROOT / "checks").glob("test_*.py")):
            if path.name == Path(__file__).name:
                continue
            self.assertNotIn(needle, path.read_text(encoding="utf-8"), path.name)

    def test_gate_labels_match_their_module(self):
        # Guards the label derivation itself: a stage imported as `run` must
        # be named after checks/<module>.py, not after the alias or "run".
        for label, name, _ in _stages("cmd_quality"):
            if not name.startswith("cmd_"):
                self.assertEqual(label, name.replace("_", "-"))


if __name__ == "__main__":
    unittest.main()
