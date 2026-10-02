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

    # -- C1/C2: the withdrawal class is extracted explicitly, not silently --

    # A statusForError carrying the two caller-withdrawal arms, each returning
    # the package-local 499 constant. Everything else is irrelevant to the
    # extractor except that it must not bleed across arms.
    GO_WITHDRAWAL_MAPPING = (
        "package httpapi\n"
        "\n"
        "import (\n"
        '\t"context"\n'
        '\t"errors"\n'
        '\t"net/http"\n'
        ")\n"
        "\n"
        "const statusClientClosedRequest = 499\n"
        "\n"
        "func (s *Server) Handler() http.Handler {\n"
        "\tmux := http.NewServeMux()\n"
        '\tmux.HandleFunc("GET /api/v1/events/{eventID}", s.spanWrap(s.getEvent))\n'
        "\treturn mux\n"
        "}\n"
        "\n"
        "func (s *Server) getEvent(w http.ResponseWriter, r *http.Request) {\n"
        "\ts.writeError(w, r, statusForError(domain.ErrNotFound), nil)\n"
        "}\n"
        "\n"
        "func statusForError(err error) int {\n"
        "\tswitch {\n"
        "\tcase errors.Is(err, domain.ErrNotFound):\n"
        "\t\treturn http.StatusNotFound\n"
        "\tcase errors.Is(err, store.ErrOperationDeadline):\n"
        "\t\treturn http.StatusServiceUnavailable\n"
        "\tcase errors.Is(err, context.Canceled):\n"
        "\t\treturn statusClientClosedRequest\n"
        "\tcase errors.Is(err, context.DeadlineExceeded):\n"
        "\t\treturn statusClientClosedRequest\n"
        "\tdefault:\n"
        "\t\treturn http.StatusInternalServerError\n"
        "\t}\n"
        "}\n"
    )

    def withdrawal_spec(self, *mappings: str) -> str:
        """A minimal opt-in executable-contract document (x-runtime-handler
        enables stage 4) carrying the given error-code declarations."""
        declared = ", ".join(
            "{ code: %s, status: %d }" % item for item in mappings
        )
        return (
            "openapi: 3.1.0\n"
            "x-contract-version: 1\n"
            "paths:\n"
            "  /api/v1/events/{eventId}:\n"
            "    get:\n"
            "      operationId: getEvent\n"
            "      x-runtime-handler: getEvent\n"
            "      x-required-permission: audit:event:read\n"
            "      x-error-mappings: [%s]\n"
            "      responses:\n"
            "        '200': { description: ok, content: { application/json: { schema: { $ref: '#/components/schemas/Event' } } } }\n"
            "        '404': { $ref: '#/components/responses/Error' }\n"
            "        '499': { $ref: '#/components/responses/Error' }\n"
            "        '500': { $ref: '#/components/responses/Error' }\n"
            "        '503': { $ref: '#/components/responses/Error' }\n"
            "components:\n"
            "  schemas:\n"
            "    Event: { type: object }\n"
            "    Error: { type: object }\n"
            "  responses:\n"
            "    Error: { description: Error, content: { application/json: { schema: { $ref: '#/components/schemas/Error' } } } }\n"
            % declared
        )

    WITHDRAWAL_MAPPINGS = (
        ("not_found", 404),
        ("store_timeout", 503),
        ("internal_error", 500),
        ("client_cancelled", 499),
        ("client_deadline_exceeded", 499),
    )

    def test_withdrawal_arm_is_extracted_and_declared_passes(self):
        """The new arms resolve through the explicit local-constant allow-list,
        so a spec that declares them at 499 satisfies stage 4."""
        root = self.make_tree(self.withdrawal_spec(*self.WITHDRAWAL_MAPPINGS), self.GO_WITHDRAWAL_MAPPING)
        code, output = run_capture(run, root)
        self.assertEqual(code, 0, output)

    def test_withdrawal_code_absent_from_spec_fails(self):
        """F3: the arms are extracted, so omitting the declaration fails loudly
        instead of the new codes being invisible to the gate."""
        spec = self.withdrawal_spec(
            *(m for m in self.WITHDRAWAL_MAPPINGS if m[0] != "client_cancelled"))
        root = self.make_tree(spec, self.GO_WITHDRAWAL_MAPPING)
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("client_cancelled", output)
        self.assertIn("want runtime status 499", output)

    def test_withdrawal_wrong_status_fails(self):
        spec = self.withdrawal_spec(
            ("not_found", 404), ("store_timeout", 503), ("internal_error", 500),
            ("client_cancelled", 400), ("client_deadline_exceeded", 499))
        root = self.make_tree(spec, self.GO_WITHDRAWAL_MAPPING)
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("client_cancelled", output)
        self.assertIn("want runtime status 499", output)

    def test_extraction_is_arm_bounded(self):
        """C1: a sentinel arm whose body returns no status must fail loudly
        instead of silently adopting a LATER arm's return (which would model
        a 499-arm sentinel as 500)."""
        go = self.GO_WITHDRAWAL_MAPPING.replace(
            "\tcase errors.Is(err, context.Canceled):\n"
            "\t\treturn statusClientClosedRequest\n",
            "\tcase errors.Is(err, context.Canceled):\n",
        )
        root = self.make_tree(self.withdrawal_spec(*self.WITHDRAWAL_MAPPINGS), go)
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("Canceled", output)
        self.assertNotIn("Traceback", output)

    def test_store_arm_keeps_its_own_503_next_to_the_499_arms(self):
        """Regression: store_timeout must stay 503 even though the following
        arms return a non-net/http constant."""
        from checks.route_contract import _runtime_error_mappings
        mappings = _runtime_error_mappings(self.real_withdrawal_tree())
        self.assertEqual(mappings["store_timeout"], 503)
        self.assertEqual(mappings["client_cancelled"], 499)
        self.assertEqual(mappings["client_deadline_exceeded"], 499)

    def real_withdrawal_tree(self) -> Path:
        tree = tempfile.TemporaryDirectory()
        root = Path(tree.name)
        self.addCleanup(tree.cleanup)
        (root / "internal" / "httpapi").mkdir(parents=True)
        (root / "internal" / "httpapi" / "authz.go").write_text(
            self.GO_WITHDRAWAL_MAPPING, encoding="utf-8")
        return root

    def test_comment_mentioned_sentinel_is_not_extracted(self):
        """A sentinel named only in a comment must not create a mapping: the
        extractor blanks comment lines before scanning."""
        go = self.GO_WITHDRAWAL_MAPPING.replace(
            "\tcase errors.Is(err, context.Canceled):",
            "\t// case errors.Is(err, context.Canceled): would return 499",
        ).replace(
            "\t\treturn statusClientClosedRequest\n\tcase errors.Is(err, context.DeadlineExceeded):",
            "\t// (see the withdrawn arm)\n\tcase errors.Is(err, context.DeadlineExceeded):",
        )
        from checks.route_contract import _runtime_error_mappings
        mappings = _runtime_error_mappings(self.go_only_tree(go))
        self.assertNotIn("client_cancelled", mappings)
        self.assertEqual(mappings["client_deadline_exceeded"], 499)

    def go_only_tree(self, go: str) -> Path:
        tree = tempfile.TemporaryDirectory()
        root = Path(tree.name)
        self.addCleanup(tree.cleanup)
        (root / "internal" / "httpapi").mkdir(parents=True)
        (root / "internal" / "httpapi" / "authz.go").write_text(go, encoding="utf-8")
        return root

    def test_unmappable_local_status_constant_fails_loudly(self):
        """H2 preserved: only statusClientClosedRequest is allow-listed. Any
        other package-local constant still fails the gate by name."""
        go = self.GO_WITHDRAWAL_MAPPING.replace(
            "return statusClientClosedRequest", "return statusTeapot")
        root = self.make_tree(self.withdrawal_spec(*self.WITHDRAWAL_MAPPINGS), go)
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("statusTeapot", output)
        self.assertNotIn("Traceback", output)

    # -- AC-4: corrected real repo passes ---------------------------------

    def make_real_contract_tree(self) -> Path:
        tree = tempfile.TemporaryDirectory()
        root = Path(tree.name)
        self.addCleanup(tree.cleanup)
        (root / "api" / "openapi").mkdir(parents=True)
        shutil.copytree(ROOT / "internal", root / "internal")
        (root / "api" / "openapi" / "openapi.yaml").write_text(
            (ROOT / "api" / "openapi" / "openapi.yaml").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        return root

    def test_executable_contract_missing_permission_fails(self):
        root = self.make_real_contract_tree()
        path = root / "api" / "openapi" / "openapi.yaml"
        spec = path.read_text(encoding="utf-8")
        spec = spec.replace(
            "x-required-permission: audit:event:read",
            "x-required-permission: ''",
            1,
        )
        path.write_text(spec, encoding="utf-8")
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("missing x-required-permission", output)

    def test_executable_contract_status_mapping_must_match_runtime(self):
        root = self.make_real_contract_tree()
        path = root / "api" / "openapi" / "openapi.yaml"
        spec = path.read_text(encoding="utf-8")
        spec = spec.replace(
            "x-error-mappings: [{ code: invalid_request, status: 400 }",
            "x-error-mappings: [{ code: invalid_request, status: 401 }",
            1,
        )
        path.write_text(spec, encoding="utf-8")
        code, output = run_capture(run, root)
        self.assertNotEqual(code, 0)
        self.assertIn("invalid_request", output)
        self.assertIn("want runtime status 400", output)

    def test_real_repo_clean_and_cli_wiring(self):
        code, output = run_capture(run)
        self.assertEqual(code, 0, output)
        import cli
        code, output = run_capture(cli.cmd_check_routes)
        self.assertEqual(code, 0, output)

    # `cli.py check` and `cli.py quality` are not re-run from here. They spawn
    # the Go toolchain, and the gate already runs both stage sets (this suite
    # included), so a nested invocation could only ever execute in a manual
    # run of the suite, never in the CI job that runs the gate. Their wiring
    # is asserted statically in checks/test_gate_wiring.py instead.


if __name__ == "__main__":
    unittest.main()
