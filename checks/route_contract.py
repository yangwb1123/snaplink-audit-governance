import re
import sys
from http import HTTPStatus
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - the quality image provides PyYAML
    yaml = None

from .config import ROOT


class RouteContractError(ValueError):
    """Fail-loud condition. The gate never hides a drift behind a traceback:
    run() converts this into a FAIL line naming the offending symbol."""


def normalize(path: str) -> str:
    return re.sub(r"\{[^}]+\}", "{}", path)


# Runtime side: every explicit-method route registration in the reference
# implementation goes through spanWrap; the method group is kept (the old
# gate discarded it, which let GET/POST drift pass unnoticed).
ROUTE_REGEX = re.compile(r'HandleFunc\("(GET|POST|PUT|DELETE|PATCH) ([^" ]+)", s\.spanWrap\(s\.(\w+)\)\)')
# Spec side: 2-space path lines and 4-space operation lines are a fixed
# convention of api/openapi/openapi.yaml (same path-line regex as before).
PATH_REGEX = re.compile(r"^  (/api/.*):\s*$")
OPERATION_REGEX = re.compile(r"^    (get|post|put|delete|patch):")
# Response keys are quoted 'NNN': in both block and flow styles.
STATUS_KEY_REGEX = re.compile(r"'(\d{3})':")
# Top-level component names are flow-style definitions on their own line.
COMPONENT_NAME_REGEX = re.compile(r"^    (\w+):\s*\{")
REF_REGEX = re.compile(r"#/components/(schemas|responses|parameters)/(\w+)")
# Literal status writes attributable to the handler body itself. Helpers
# (writeError/writeJSON/statusForError/errorBody/decodeBody/parseQuery) are
# separate \nfunc blocks and never match the receiver-scoped scan.
LITERAL_STATUS_REGEX = re.compile(
    r"(?:s\.writeError\(w, r, |writeJSON\(w, |WriteHeader\()http\.Status(\w+)")
# Anchored at the start of a \nfunc part: a receiver-method body starts with
# "(s *Server) Name(" right after the split. Searching instead of matching
# would let a comment or string mentioning "func (s *Server) X" misattribute
# a body (server_test.go Fatalf-string hazard, F5).
FUNC_HEADER_REGEX = re.compile(r"\(s \*Server\) (\w+)\(")


def _strip_line_comments(text: str) -> str:
    """Blank full-line // comments so they can never be scanned as function
    headers, route registrations, or status literals. Line count is preserved
    and inline comments are kept: a real literal is a real finding."""
    return "\n".join(
        "" if line.lstrip().startswith("//") else line
        for line in text.splitlines()
    )


def _build_index(root: Path) -> tuple[dict[tuple[str, str], str], dict[str, set[int]]]:
    """Single pass over internal/**/*.go (F1: replaces the per-operation
    full-repo rescan). Returns (routes, handler_statuses_map):

      routes[(METHOD.upper(), normalize(path))] = handler_name
      handler_statuses_map[handler_name] = {literal http.StatusX codes}

    Duplicate registrations or duplicate receiver-method names fail loudly
    (last-write-wins would silently pick the wrong handler body — the
    design's own fail-loud rule, F4)."""
    routes: dict[tuple[str, str], str] = {}
    statuses: dict[str, set[int]] = {}
    for path in sorted((root / "internal").rglob("*.go")):
        text = _strip_line_comments(path.read_text(encoding="utf-8"))
        for method, route, handler in ROUTE_REGEX.findall(text):
            if not route.startswith("/api/"):
                continue
            key = (method.upper(), normalize(route))
            if key in routes:
                raise RouteContractError(
                    f"duplicate registration {method} {route}: "
                    f"{routes[key]} vs {handler}")
            routes[key] = handler
        for part in text.split("\nfunc "):
            match = FUNC_HEADER_REGEX.match(part)
            if not match:
                continue
            name = match.group(1)
            if name in statuses:
                raise RouteContractError(
                    f"receiver method {name} defined in more than one body")
            statuses[name] = {
                _status_code(constant)
                for constant in LITERAL_STATUS_REGEX.findall(part)
            }
    return routes, statuses


def runtime_operations(root: Path) -> dict[tuple[str, str], str]:
    """{(METHOD.upper(), normalize(path)): handler_name} for every registered
    /api/ route across internal/**/*.go (including _test.go, as the previous
    gate did; test-file mentions are comments/Fatalf strings that do not
    match the registration regex). Single shared scan: see _build_index."""
    routes, _ = _build_index(root)
    return routes


def spec_path_spellings(root: Path) -> dict[str, str]:
    """{normalize(path): original path} — used so response findings name the
    spec's parameter spelling (e.g. {jobId}) instead of the normalized form."""
    spec = root / "api/openapi/openapi.yaml"
    if not spec.exists():
        return {}
    spellings: dict[str, str] = {}
    for line in spec.read_text(encoding="utf-8").splitlines():
        match = PATH_REGEX.match(line)
        if match:
            spellings.setdefault(normalize(match.group(1)), match.group(1))
    return spellings


def spec_operations(root: Path) -> dict[tuple[str, str], set[int]]:
    """{(METHOD.upper(), normalize(path)): {documented status codes}} parsed
    from api/openapi/openapi.yaml. Response keys are collected per operation
    block, so both block-style and flow-style responses parse."""
    spec = root / "api/openapi/openapi.yaml"
    if not spec.exists():
        return {}
    operations: dict[tuple[str, str], set[int]] = {}
    current_path: str | None = None
    current_key: tuple[str, str] | None = None
    for line in spec.read_text(encoding="utf-8").splitlines():
        path_match = PATH_REGEX.match(line)
        if path_match:
            current_path = normalize(path_match.group(1))
            current_key = None
            continue
        op_match = OPERATION_REGEX.match(line)
        if op_match and current_path is not None:
            current_key = (op_match.group(1).upper(), current_path)
            operations.setdefault(current_key, set())
            continue
        if current_key is not None:
            statuses = STATUS_KEY_REGEX.findall(line)
            if statuses:
                operations[current_key].update(int(code) for code in statuses)
    return operations


def documented_statuses(root: Path, method: str, path: str) -> set[int]:
    """R2 lookup wrapper: the documented status codes for a (method, path)."""
    return spec_operations(root).get((method.upper(), normalize(path)), set())


def _status_code(constant: str) -> int:
    """http.StatusNotFound -> 404. The Go constant name is not a Python
    HTTPStatus member name (StatusBadRequest vs BAD_REQUEST), so convert:
    strip the Status prefix, snake_case the camel part, upper-case it.
    An unmappable constant fails loudly with its name — never silently
    skipped, because a skip would hide drift (H2)."""
    key = re.sub(r"([a-z])([A-Z])", r"\1_\2", constant).upper()
    try:
        return HTTPStatus.__members__[key].value
    except KeyError:
        raise RouteContractError(
            f"unmappable http.Status{constant} constant") from None


def handler_statuses(root: Path, handler: str) -> set[int]:
    """Literal http.StatusX codes written inside the named (s *Server) method
    body. Public helper: run() uses the prebuilt index so the repository is
    scanned once per gate run, not once per operation (F1). A handler whose
    body is not found yields an empty set (one-directional soundness: never a
    false positive)."""
    if not handler:
        return set()
    _, statuses = _build_index(root)
    return statuses.get(handler, set())


def _component_names(text: str) -> dict[str, set[str]]:
    """{namespace: set(names)} for components.schemas/responses/parameters.
    The name regex is scoped to each 2-space component section; a whole-file
    application would falsely flag securitySchemes names like bearerAuth.
    Flow-style empty section headers (schemas: {}) are tolerated so they do
    not silently break section tracking (L2)."""
    names: dict[str, set[str]] = {}
    section: str | None = None
    for line in text.splitlines():
        section_match = re.match(r"^  (\w+):\s*(\{\})?\s*$", line)
        if section_match:
            section = section_match.group(1)
            names.setdefault(section, set())
            continue
        if section in ("schemas", "responses", "parameters"):
            name_match = COMPONENT_NAME_REGEX.match(line)
            if name_match:
                names[section].add(name_match.group(1))
    return names


EXECUTABLE_CONTRACT_VERSION = 1
OPENAPI_METHODS = frozenset({"get", "post", "put", "delete", "patch"})
ERROR_CODE_BY_SENTINEL = {
    "ErrOccurredAtOutOfRange": "occurred_at_out_of_range",
    "ErrInvalid": "invalid_request",
    "ErrUnauthorized": "unauthorized",
    "ErrForbidden": "forbidden",
    "ErrNotFound": "not_found",
    "ErrConflict": "conflict",
    "ErrQuotaExceeded": "quota_exceeded",
    "ErrSchemaNotFound": "schema_not_found",
    "ErrTenantMismatch": "tenant_mismatch",
    "ErrSnapshotConflict": "snapshot_conflict",
}


def _load_openapi(root: Path) -> dict:
    """Load the executable contract without allowing a parser failure to pass."""
    if yaml is None:
        raise RouteContractError(
            "executable OpenAPI contract requires the pinned quality environment "
            "to provide PyYAML"
        )
    spec = root / "api/openapi/openapi.yaml"
    try:
        document = yaml.safe_load(spec.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise RouteContractError(f"OpenAPI YAML cannot be loaded: {error}") from None
    if not isinstance(document, dict):
        raise RouteContractError("OpenAPI document must be a mapping")
    return document


def _openapi_operations(document: dict):
    """Yield (path, method, operation) entries from a parsed OpenAPI document."""
    paths = document.get("paths")
    if not isinstance(paths, dict):
        raise RouteContractError("OpenAPI paths must be a mapping")
    for path, path_item in paths.items():
        if not isinstance(path, str) or not isinstance(path_item, dict):
            raise RouteContractError(f"invalid OpenAPI path item {path!r}")
        for method, operation in path_item.items():
            if method.lower() not in OPENAPI_METHODS:
                continue
            if not isinstance(operation, dict):
                raise RouteContractError(
                    f"{method.upper()} {path} operation must be a mapping"
                )
            yield path, method.lower(), operation


def _response_schema(document: dict, response: object) -> object | None:
    """Resolve a local response reference and require a media schema."""
    if not isinstance(response, dict):
        return None
    if "$ref" in response:
        reference = response["$ref"]
        prefix = "#/components/responses/"
        if not isinstance(reference, str) or not reference.startswith(prefix):
            return None
        responses = document.get("components", {}).get("responses", {})
        response = responses.get(reference[len(prefix):])
        if not isinstance(response, dict):
            return None
    content = response.get("content")
    if not isinstance(content, dict) or not content:
        return None
    for media in content.values():
        if isinstance(media, dict) and isinstance(media.get("schema"), dict):
            return media["schema"]
    return None


def _runtime_error_mappings(root: Path) -> dict[str, int]:
    """Extract statusForError's ordered sentinel mapping plus its default."""
    source = root / "internal/httpapi/server.go"
    try:
        text = source.read_text(encoding="utf-8")
    except OSError as error:
        raise RouteContractError(f"cannot read statusForError source: {error}") from None
    function = re.search(
        r"func statusForError\(err error\) int \{(?P<body>.*?\n\})",
        text,
        re.DOTALL,
    )
    if not function:
        raise RouteContractError("statusForError function is not discoverable")
    mappings: dict[str, int] = {}
    for match in re.finditer(
        r"case\s+errors\.Is\(err,\s*(?:domain|store)\.(Err\w+)\):"
        r".*?return\s+http\.Status(\w+)",
        function.group("body"),
        re.DOTALL,
    ):
        sentinel, status_name = match.groups()
        code = ERROR_CODE_BY_SENTINEL.get(sentinel)
        if code is None:
            raise RouteContractError(f"unmapped statusForError sentinel {sentinel}")
        mappings[code] = _status_code(status_name)
    if not mappings:
        raise RouteContractError("statusForError has no discoverable sentinel mappings")
    mappings["internal_error"] = HTTPStatus.INTERNAL_SERVER_ERROR
    return mappings


def _executable_contract_violations(root: Path) -> list[str]:
    """Validate the opt-in executable OpenAPI metadata and response contract."""
    document = _load_openapi(root)
    operations = list(_openapi_operations(document))
    enabled = document.get("x-contract-version") == EXECUTABLE_CONTRACT_VERSION
    enabled = enabled or any(
        "x-runtime-handler" in operation for _, _, operation in operations
    )
    if not enabled:
        return []

    runtime_ops, _ = _build_index(root)
    runtime_errors = _runtime_error_mappings(root)
    findings: list[str] = []
    operation_ids: dict[str, str] = {}
    for path, method, operation in operations:
        display = f"{method.upper()} {path}"
        operation_id = operation.get("operationId")
        if not isinstance(operation_id, str) or not operation_id:
            findings.append(f"{display} missing operationId")
        elif operation_id in operation_ids:
            findings.append(
                f"duplicate operationId {operation_id} ({operation_ids[operation_id]} and {display})"
            )
        else:
            operation_ids[operation_id] = display

        key = (method.upper(), normalize(path))
        expected_handler = runtime_ops.get(key)
        handler = operation.get("x-runtime-handler")
        if not isinstance(handler, str) or not handler:
            findings.append(f"{display} missing x-runtime-handler")
        elif expected_handler and handler != expected_handler:
            findings.append(
                f"{display} maps to {handler}, runtime registers {expected_handler}"
            )
        if not isinstance(operation.get("x-required-permission"), str) or not operation[
            "x-required-permission"
        ].strip():
            findings.append(f"{display} missing x-required-permission")

        responses = operation.get("responses")
        mappings = operation.get("x-error-mappings")
        if not isinstance(responses, dict) or not responses:
            findings.append(f"{display} missing responses")
            continue
        if not isinstance(mappings, list) or not mappings:
            findings.append(f"{display} missing x-error-mappings")
            mappings = []

        status_codes: set[int] = set()
        for raw_status, response in responses.items():
            try:
                status = int(raw_status)
            except (TypeError, ValueError):
                findings.append(f"{display} has non-numeric response status {raw_status!r}")
                continue
            status_codes.add(status)
            if 200 <= status < 300 and status != 204:
                if _response_schema(document, response) is None:
                    findings.append(f"{display} 2xx response {status} has no payload schema")
            elif 400 <= status < 600 and _response_schema(document, response) is None:
                findings.append(f"{display} error response {status} has no payload schema")

        declared: dict[str, int] = {}
        for mapping in mappings:
            if not isinstance(mapping, dict):
                findings.append(f"{display} has malformed x-error-mappings entry")
                continue
            code = mapping.get("code")
            status = mapping.get("status")
            if not isinstance(code, str) or not code:
                findings.append(f"{display} has error mapping without code")
                continue
            try:
                status = int(status)
            except (TypeError, ValueError):
                findings.append(f"{display} maps {code} to non-numeric status")
                continue
            if code in declared:
                findings.append(f"{display} duplicates error mapping {code}")
            declared[code] = status
            if status not in status_codes:
                findings.append(f"{display} maps {code} to undocumented status {status}")

        for code, status in runtime_errors.items():
            if declared.get(code) != status:
                findings.append(
                    f"{display} maps {code} to {declared.get(code)!r}, want runtime status {status}"
                )
        for code in sorted(set(declared) - set(runtime_errors)):
            findings.append(f"{display} maps unknown runtime error code {code}")
    return findings


def schema_violations(root: Path) -> list[str]:
    """R3: dangling $refs (schemas/responses/parameters) and defined-but-
    unreferenced top-level schemas. Both directions are safe: atypical
    spacing or block-style definitions escape the regexes (no false finds)."""
    spec = root / "api/openapi/openapi.yaml"
    if not spec.exists():
        return []
    text = spec.read_text(encoding="utf-8")
    names = _component_names(text)
    violations: list[str] = []
    refs = REF_REGEX.findall(text)
    for namespace, name in refs:
        if name not in names.get(namespace, set()):
            violations.append(f"dangling $ref #/components/{namespace}/{name}")
    referenced_schemas = {name for namespace, name in refs if namespace == "schemas"}
    for name in sorted(names.get("schemas", set()) - referenced_schemas):
        violations.append(f"defined-but-unreferenced schema {name}")
    return violations


def run(root: Path | None = None) -> int:
    """Method- and response-aware route contract gate.

    Stage 1 (operations): compare (METHOD, normalized path) pairs between the
    runtime registrations and the OpenAPI spec — method drift now fails.
    Stage 2 (responses): every literal http.StatusX written by a handler body
    must be documented for that operation (one-directional; the dynamic
    statusForError mapping is out of scope by design).
    Stage 3 (schema): every $ref must resolve; every top-level schema must be
    referenced.
    Stage 4 (executable contract): the marked OpenAPI document must expose
    unique operation IDs, runtime handlers, permissions, response schemas, and
    the complete statusForError mapping.

    root defaults to the repository root (computed from checks/config.py), so
    zero-arg callers (checks/self_test.py, cmd_quality) keep working and the
    gate is never cwd-dependent.
    """
    if root is None:
        root = ROOT
    spec = root / "api/openapi/openapi.yaml"
    if not spec.exists():
        print("FAIL: route contract (spec): api/openapi/openapi.yaml not found")
        return 1

    try:
        runtime_ops, handler_statuses_map = _build_index(root)
        findings = _collect_findings(root, runtime_ops, handler_statuses_map)
    except RouteContractError as error:
        # Fail loudly with the offending symbol; never surface a traceback.
        print(f"FAIL: route contract: {error}")
        return 1

    if findings:
        for stage, message in findings:
            print(f"FAIL: route contract ({stage}): {message}")
        return 1
    print("PASS: route contract")
    return 0


def _collect_findings(
    root: Path,
    runtime_ops: dict[tuple[str, str], str],
    handler_statuses_map: dict[str, set[int]],
) -> list[tuple[str, str]]:
    """Stages 1-3 of the gate against the prebuilt runtime index."""
    findings: list[tuple[str, str]] = []
    spec_ops = spec_operations(root)
    spellings = spec_path_spellings(root)

    # Stage 1: (method, path) operation matching.
    for method, path in sorted(set(runtime_ops) - set(spec_ops)):
        findings.append(("operations", f"runtime {method} {path} not documented"))
    for method, path in sorted(set(spec_ops) - set(runtime_ops)):
        findings.append(("operations", f"spec {method} {path} not registered at runtime"))

    # Stage 2: runtime-literal statuses must be documented per operation.
    for (method, path), handler in sorted(runtime_ops.items()):
        documented = spec_ops.get((method, path), set())
        for code in sorted(handler_statuses_map.get(handler, set()) - documented):
            display = spellings.get(path, path)
            findings.append(("responses", f"{method} {display} emits {code} not documented"))

    # Stage 3: schema integrity.
    for violation in schema_violations(root):
        findings.append(("schema", violation))

    # Stage 4: opt-in executable contract. Minimal historical fixtures do not
    # carry the marker/extensions and retain the original route-gate contract;
    # the checked-in OpenAPI document opts in explicitly.
    for violation in _executable_contract_violations(root):
        findings.append(("executable", violation))
    return findings


if __name__ == "__main__":
    sys.exit(run())
