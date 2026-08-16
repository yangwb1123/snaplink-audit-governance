import io
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from checks.route_contract import documented_statuses, run

ROOT = Path(__file__).resolve().parents[1]


def run_capture(check, *args, **kwargs):
    stream = io.StringIO()
    with redirect_stdout(stream):
        code = check(*args, **kwargs)
    return code, stream.getvalue()


SPEC_GET_EVENTS = (
    "openapi: 3.1.0\n"
    "paths:\n"
    "  /api/v1/events:\n"
    "    get:\n"
    "      operationId: queryEvents\n"
    "      responses: { '200': { description: ok } }\n"
)

SPEC_POST_EVENTS = SPEC_GET_EVENTS.replace("    get:", "    post:")

GO_METHOD_DRIFT = (
    "package httpapi\n"
    "\n"
    'import "net/http"\n'
    "\n"
    "func (s *Server) Handler() http.Handler {\n"
    "\tmux := http.NewServeMux()\n"
    "\tmux.HandleFunc(\"METHOD /api/v1/events\", s.spanWrap(s.postEvent))\n"
    "\treturn mux\n"
    "}\n"
    "\n"
    "func (s *Server) postEvent(w http.ResponseWriter, r *http.Request) {\n"
    "\twriteJSON(w, http.StatusOK, map[string]any{})\n"
    "}\n"
)


class RouteContractTest(unittest.TestCase):
    """Method- and response-aware route contract gate.

    Fixture trees follow the test_proto_sync pattern: a temp root with the
    two inputs the gate reads (api/openapi/openapi.yaml and internal/**/*.go)
    plus addCleanup(tree.cleanup)."""

    def make_tree(self, spec: str, go_source: str) -> Path:
        tree = tempfile.TemporaryDirectory()
        root = Path(tree.name)
        (root / "api" / "openapi").mkdir(parents=True)
        (root / "internal" / "httpapi").mkdir(parents=True)
        (root / "api" / "openapi" / "openapi.yaml").write_text(spec, encoding="utf-8")
        (root / "internal" / "httpapi" / "server.go").write_text(go_source, encoding="utf-8")
        self.addCleanup(tree.cleanup)
        return root

    # -- AC-1: method drift -----------------------------------------------

    def test_method_drift_runtime_post_spec_get(self):
        root = self.make_tree(SPEC_GET_EVENTS, GO_METHOD_DRIFT.replace("METHOD", "POST"))
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("POST /api/v1/events", output)

    def test_method_drift_mirror(self):
        root = self.make_tree(SPEC_POST_EVENTS, GO_METHOD_DRIFT.replace("METHOD", "GET"))
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("GET /api/v1/events", output)

    def test_method_match_control(self):
        root = self.make_tree(SPEC_GET_EVENTS, GO_METHOD_DRIFT.replace("METHOD", "GET"))
        code, output = run_capture(run, root)
        self.assertEqual(code, 0, output)

    def test_cmd_check_includes_route_stage(self):
        import cli
        import inspect
        self.assertIn("cmd_check_routes", inspect.getsource(cli.cmd_check))

    def test_cmd_check_routes_monkeypatched_root(self):
        import cli
        original = cli.ROOT
        self.addCleanup(setattr, cli, "ROOT", original)
        root = self.make_tree(SPEC_GET_EVENTS, GO_METHOD_DRIFT.replace("METHOD", "POST"))
        cli.ROOT = root
        code, output = run_capture(cli.cmd_check_routes)
        self.assertNotEqual(code, 0)
        self.assertIn("POST /api/v1/events", output)

    # -- AC-2: undocumented runtime status --------------------------------

    SPEC_DOWNLOAD = (
        "openapi: 3.1.0\n"
        "paths:\n"
        "  /api/v1/exports/{jobId}/download:\n"
        "    get:\n"
        "      operationId: downloadExport\n"
        "      responses:\n"
        "        '200': { description: ok }\n"
        "        '409': { $ref: '#/components/responses/Error' }\n"
        "        '500': { $ref: '#/components/responses/Error' }\n"
        "components:\n"
        "  responses:\n"
        "    Error: { description: Error }\n"
    )

    GO_DOWNLOAD = (
        "package httpapi\n"
        "\n"
        'import "net/http"\n'
        "\n"
        "func (s *Server) Handler() http.Handler {\n"
        "\tmux := http.NewServeMux()\n"
        "\tmux.HandleFunc(\"GET /api/v1/exports/{jobID}/download\", s.spanWrap(s.downloadExport))\n"
        "\treturn mux\n"
        "}\n"
        "\n"
        "func (s *Server) downloadExport(w http.ResponseWriter, r *http.Request) {\n"
        "\ts.writeError(w, r, http.StatusNotFound, nil)\n"
        "\twriteJSON(w, http.StatusOK, map[string]any{})\n"
        "}\n"
    )

    def test_undocumented_status_fails(self):
        root = self.make_tree(self.SPEC_DOWNLOAD, self.GO_DOWNLOAD)
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("404", output)
        self.assertIn("/api/v1/exports/{jobId}/download", output)

    def test_documented_status_passes(self):
        spec = self.SPEC_DOWNLOAD.replace(
            "        '200': { description: ok }\n",
            "        '200': { description: ok }\n"
            "        '404': { $ref: '#/components/responses/Error' }\n",
        )
        root = self.make_tree(spec, self.GO_DOWNLOAD)
        code, output = run_capture(run, root)
        self.assertEqual(code, 0, output)
        self.assertEqual(
            documented_statuses(root, "GET", "/api/v1/exports/{jobId}/download"),
            {200, 404, 409, 500},
        )

    def test_real_repo_undocumented_404_fails(self):
        """The drift the gate exists to catch is still caught on the real
        artifacts: with downloadExport's documented 404 removed from a copy
        of the real spec, run() fails naming the 404 (AC-2/AC-4's
        precondition demonstrated on the repo itself)."""
        tree = tempfile.TemporaryDirectory()
        root = Path(tree.name)
        self.addCleanup(tree.cleanup)
        (root / "api" / "openapi").mkdir(parents=True)
        spec = (ROOT / "api/openapi/openapi.yaml").read_text(encoding="utf-8")
        drifted = spec.replace(
            "        '200': { description: Export JSON Lines download, content: { application/x-ndjson: { schema: { type: string, format: binary } } } }\n        '404': { $ref: '#/components/responses/Error' }\n",
            "        '200': { description: Export JSON Lines download, content: { application/x-ndjson: { schema: { type: string, format: binary } } } }\n",
        )
        self.assertNotEqual(drifted, spec, "downloadExport 404 line not found in the spec")
        (root / "api/openapi/openapi.yaml").write_text(drifted, encoding="utf-8")
        shutil.copytree(ROOT / "internal", root / "internal")
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("404", output)
        self.assertIn("download", output)

    # -- AC-3: schema integrity -------------------------------------------

    def test_dangling_ref_fails(self):
        spec = (
            "openapi: 3.1.0\n"
            "paths:\n"
            "  /api/v1/events:\n"
            "    get:\n"
            "      operationId: queryEvents\n"
            "      responses: { '200': { description: ok, content: { application/json: { schema: { $ref: '#/components/schemas/Ghost' } } } } }\n"
        )
        root = self.make_tree(spec, GO_METHOD_DRIFT.replace("METHOD", "GET"))
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("dangling $ref", output)
        self.assertIn("Ghost", output)

    def test_unreferenced_schema_fails(self):
        spec = (
            "openapi: 3.1.0\n"
            "paths:\n"
            "  /api/v1/events:\n"
            "    get:\n"
            "      operationId: queryEvents\n"
            "      responses: { '200': { description: ok } }\n"
            "components:\n"
            "  schemas:\n"
            "    Ghost: { type: object }\n"
        )
        root = self.make_tree(spec, GO_METHOD_DRIFT.replace("METHOD", "GET"))
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("defined-but-unreferenced schema Ghost", output)

    def test_schema_control(self):
        spec = (
            "openapi: 3.1.0\n"
            "paths:\n"
            "  /api/v1/events:\n"
            "    get:\n"
            "      operationId: queryEvents\n"
            "      responses: { '200': { description: ok, content: { application/json: { schema: { $ref: '#/components/schemas/Event' } } } } }\n"
            "components:\n"
            "  schemas:\n"
            "    Event: { type: object }\n"
        )
        root = self.make_tree(spec, GO_METHOD_DRIFT.replace("METHOD", "GET"))
        code, output = run_capture(run, root)
        self.assertEqual(code, 0, output)

    def test_security_scheme_not_flagged_as_schema(self):
        """R3 scoping: bearerAuth lives under securitySchemes, not schemas;
        a whole-file name scan would falsely flag it (design §0 refinement 3)."""
        spec = (
            "openapi: 3.1.0\n"
            "paths:\n"
            "  /api/v1/events:\n"
            "    get:\n"
            "      operationId: queryEvents\n"
            "      responses: { '200': { description: ok } }\n"
            "components:\n"
            "  securitySchemes:\n"
            "    bearerAuth: { type: http, scheme: bearer }\n"
        )
        root = self.make_tree(spec, GO_METHOD_DRIFT.replace("METHOD", "GET"))
        code, output = run_capture(run, root)
        self.assertEqual(code, 0, output)

    def test_flow_style_section_header_tolerated(self):
        """L2: flow-style empty section headers (schemas: {}) must not break
        section tracking for the component names that follow."""
        from checks.route_contract import _component_names
        names = _component_names(
            "components:\n"
            "  schemas: {}\n"
            "  responses:\n"
            "    Error: { description: Error }\n"
        )
        self.assertIn("schemas", names)
        self.assertEqual(names["responses"], {"Error"})

    # -- H2: unmappable status constant fails loudly, never a traceback ----

    def test_unmappable_status_constant_fails_loudly(self):
        spec = (
            "openapi: 3.1.0\n"
            "paths:\n"
            "  /api/v1/events:\n"
            "    get:\n"
            "      operationId: queryEvents\n"
            "      responses: { '418': { description: ok } }\n"
        )
        go = GO_METHOD_DRIFT.replace("METHOD", "GET").replace(
            "writeJSON(w, http.StatusOK, map[string]any{})",
            "writeJSON(w, http.StatusTeapot, map[string]any{})",
        )
        root = self.make_tree(spec, go)
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("StatusTeapot", output)
        self.assertNotIn("Traceback", output)

    # -- M2: WriteHeader-only handler shape is pinned ----------------------

    GO_WRITEHEADER = GO_METHOD_DRIFT.replace("METHOD", "GET").replace(
        "writeJSON(w, http.StatusOK, map[string]any{})",
        "w.WriteHeader(http.StatusTooManyRequests)\n\treturn",
    )

    SPEC_WRITEHEADER = (
        "openapi: 3.1.0\n"
        "paths:\n"
        "  /api/v1/events:\n"
        "    get:\n"
        "      operationId: queryEvents\n"
        "      responses: { '200': { description: ok }, '429': { $ref: '#/components/responses/Error' } }\n"
        "components:\n"
        "  responses:\n"
        "    Error: { description: Error }\n"
    )

    def test_writeheader_shape_documented_passes(self):
        root = self.make_tree(self.SPEC_WRITEHEADER, self.GO_WRITEHEADER)
        code, output = run_capture(run, root)
        self.assertEqual(code, 0, output)

    def test_writeheader_shape_undocumented_fails(self):
        spec = self.SPEC_WRITEHEADER.replace(
            "{ '200': { description: ok }, '429': { $ref: '#/components/responses/Error' } }",
            "{ '200': { description: ok } }",
        )
        root = self.make_tree(spec, self.GO_WRITEHEADER)
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("429", output)

    # -- M3: multi-status flow-style response line is pinned ---------------

    SPEC_FLOW_MULTI = (
        "openapi: 3.1.0\n"
        "paths:\n"
        "  /api/v1/events/{eventId}:\n"
        "    get:\n"
        "      operationId: getEvent\n"
        "      responses: { '200': { description: ok }, '404': { $ref: '#/components/responses/Error' }, '500': { $ref: '#/components/responses/Error' } }\n"
        "components:\n"
        "  responses:\n"
        "    Error: { description: Error }\n"
    )

    GO_FLOW_MULTI = (
        "package httpapi\n"
        "\n"
        'import "net/http"\n'
        "\n"
        "func (s *Server) Handler() http.Handler {\n"
        "\tmux := http.NewServeMux()\n"
        "\tmux.HandleFunc(\"GET /api/v1/events/{eventID}\", s.spanWrap(s.getEvent))\n"
        "\treturn mux\n"
        "}\n"
        "\n"
        "func (s *Server) getEvent(w http.ResponseWriter, r *http.Request) {\n"
        "\ts.writeError(w, r, http.StatusInternalServerError, nil)\n"
        "}\n"
    )

    def test_flow_style_multi_status_documented_passes(self):
        root = self.make_tree(self.SPEC_FLOW_MULTI, self.GO_FLOW_MULTI)
        code, output = run_capture(run, root)
        self.assertEqual(code, 0, output)

    def test_flow_style_multi_status_undocumented_fails(self):
        spec = self.SPEC_FLOW_MULTI.replace(
            ", '500': { $ref: '#/components/responses/Error' }", "")
        root = self.make_tree(spec, self.GO_FLOW_MULTI)
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("500", output)
        self.assertIn("/api/v1/events/{eventId}", output)

    # -- M4: one-directional semantics are pinned --------------------------

    def test_documented_status_without_runtime_literal_passes(self):
        """One-directional by design: a documented status the handler never
        emits literally (middleware 401, statusForError 404, ...) is NOT a
        finding — only runtime-literal ⊆ documented is enforced (design
        §3.2 reverse-direction non-goal)."""
        spec = (
            "openapi: 3.1.0\n"
            "paths:\n"
            "  /api/v1/events:\n"
            "    get:\n"
            "      operationId: queryEvents\n"
            "      responses: { '200': { description: ok }, '401': { $ref: '#/components/responses/Error' } }\n"
            "components:\n"
            "  responses:\n"
            "    Error: { description: Error }\n"
        )
        root = self.make_tree(spec, GO_METHOD_DRIFT.replace("METHOD", "GET"))
        code, output = run_capture(run, root)
        self.assertEqual(code, 0, output)

    # -- M5: robustness paths ----------------------------------------------

    def test_missing_spec_fails_cleanly(self):
        tree = tempfile.TemporaryDirectory()
        root = Path(tree.name)
        self.addCleanup(tree.cleanup)
        (root / "internal" / "httpapi").mkdir(parents=True)
        (root / "internal" / "httpapi" / "server.go").write_text(
            GO_METHOD_DRIFT.replace("METHOD", "GET"), encoding="utf-8")
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("not found", output)
        self.assertNotIn("Traceback", output)

    def test_empty_internal_reports_unregistered(self):
        tree = tempfile.TemporaryDirectory()
        root = Path(tree.name)
        self.addCleanup(tree.cleanup)
        (root / "api" / "openapi").mkdir(parents=True)
        (root / "internal").mkdir(parents=True)
        (root / "api" / "openapi" / "openapi.yaml").write_text(
            SPEC_GET_EVENTS, encoding="utf-8")
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("not registered at runtime", output)
        self.assertNotIn("Traceback", output)

    def test_both_empty_vacuous_pass(self):
        tree = tempfile.TemporaryDirectory()
        root = Path(tree.name)
        self.addCleanup(tree.cleanup)
        (root / "api" / "openapi").mkdir(parents=True)
        (root / "internal").mkdir(parents=True)
        (root / "api" / "openapi" / "openapi.yaml").write_text(
            "openapi: 3.1.0\npaths: {}\n", encoding="utf-8")
        code, output = run_capture(run, root)
        self.assertEqual(code, 0, output)

    def test_duplicate_registration_fails_loudly(self):
        """F4: two files registering the same (method, path) fail loudly
        instead of silently picking the last one."""
        tree = tempfile.TemporaryDirectory()
        root = Path(tree.name)
        self.addCleanup(tree.cleanup)
        (root / "api" / "openapi").mkdir(parents=True)
        (root / "internal" / "httpapi").mkdir(parents=True)
        (root / "api" / "openapi" / "openapi.yaml").write_text(
            SPEC_GET_EVENTS, encoding="utf-8")
        go = GO_METHOD_DRIFT.replace("METHOD", "GET")
        (root / "internal" / "httpapi" / "server.go").write_text(go, encoding="utf-8")
        (root / "internal" / "httpapi" / "server2.go").write_text(
            go.replace("queryEvents", "other"), encoding="utf-8")
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("duplicate registration", output)
        self.assertNotIn("Traceback", output)

    def test_duplicate_handler_name_fails_loudly(self):
        """F4: the same receiver-method name in two bodies is ambiguous;
        the gate fails loudly instead of attributing statuses to the first
        match."""
        tree = tempfile.TemporaryDirectory()
        root = Path(tree.name)
        self.addCleanup(tree.cleanup)
        (root / "api" / "openapi").mkdir(parents=True)
        (root / "internal" / "httpapi").mkdir(parents=True)
        (root / "api" / "openapi" / "openapi.yaml").write_text(
            SPEC_GET_EVENTS, encoding="utf-8")
        go = GO_METHOD_DRIFT.replace("METHOD", "GET")
        (root / "internal" / "httpapi" / "server.go").write_text(go, encoding="utf-8")
        # A second file with a different route but the same method name and a
        # distinct registrar, so the duplicate is the handler itself.
        second = GO_METHOD_DRIFT.replace("METHOD", "POST").replace(
            "/api/v1/events", "/api/v1/other").replace(
            "Handler() http.Handler", "Routes2() http.Handler")
        (root / "internal" / "httpapi" / "server2.go").write_text(second, encoding="utf-8")
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("receiver method", output)
        self.assertNotIn("Traceback", output)

    # -- AC-4: corrected real repo passes ---------------------------------

    def test_real_repo_clean_and_cli_wiring(self):
        code, output = run_capture(run)
        self.assertEqual(code, 0, output)
        import cli
        code, output = run_capture(cli.cmd_check_routes)
        self.assertEqual(code, 0, output)

    def test_real_repo_cli_check_passes(self):
        # cli.py check now includes the route stage; skipped when the quality
        # gate itself is running this suite (it exercises check separately).
        if os.environ.get("AUDIT_QUALITY_ACTIVE"):
            self.skipTest("already inside the quality gate")
        result = subprocess.run(
            [sys.executable, "cli.py", "check"], cwd=ROOT,
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout[-3000:] + result.stderr[-3000:])

    def test_quality_gate(self):
        # AGENTS.md gate. The recursion guard prevents `cli.py quality` ->
        # unittest suite -> `cli.py quality` -> ... when this suite is run
        # from inside the gate (cmd_quality sets AUDIT_QUALITY_ACTIVE).
        if os.environ.get("AUDIT_QUALITY_ACTIVE"):
            self.skipTest("quality gate already active; run `python3 cli.py quality` directly")
        result = subprocess.run(
            [sys.executable, "cli.py", "quality"], cwd=ROOT,
            capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout[-4000:] + result.stderr[-4000:])


if __name__ == "__main__":
    unittest.main()
