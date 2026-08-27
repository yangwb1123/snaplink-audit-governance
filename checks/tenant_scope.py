"""Fail-closed OpenAPI tenant-scope contract checker.

The runtime has a deliberately small, explicit scope matrix. Keeping the
allowlist here prevents a newly added route from inheriting an accidental
empty-tenant meaning or from becoming an undocumented cross-tenant endpoint.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from .config import ROOT

ALL_TENANT = {
    "querySnaplinkConsoleAuditEvents",
    "querySnaplinkConsoleAuditFacets",
    "listAdminActions",
}
BODY_SCOPED = {"writeEvent", "writeBatch"}
PLATFORM_MANAGEMENT = {"createTenant", "listTenants"}
EXPECTED_OPERATIONS = {
    "querySnaplinkConsoleAuditEvents",
    "getSnaplinkConsoleAuditEvent",
    "querySnaplinkConsoleAuditFacets",
    "writeEvent",
    "queryEvents",
    "getEvent",
    "getReceipt",
    "writeBatch",
    "getOperation",
    "getOperationTimeline",
    "replayOperation",
    "getAggregateTimeline",
    "createExport",
    "getExport",
    "downloadExport",
    "verifyIntegrity",
    "createLegalHold",
    "listLegalHolds",
    "releaseLegalHold",
    "previewRestore",
    "createRestore",
    "getRestore",
    "approveRestore",
    "rejectRestore",
    "listAdminActions",
    "createTenant",
    "listTenants",
    "createSource",
    "listSources",
    "updateSource",
    "createSchema",
    "listSchemas",
    "setRetentionPolicy",
    "getRetentionPolicy",
    "evaluateRetention",
}
QUERY_SCOPED = EXPECTED_OPERATIONS - ALL_TENANT - BODY_SCOPED - PLATFORM_MANAGEMENT
TENANT_PARAMETER = "#/components/parameters/tenantId"

PATH_RE = re.compile(r"^  (/api/.*):\s*$")
METHOD_RE = re.compile(r"^    (get|post|put|patch|delete):\s*$")
OP_RE = re.compile(r"^      operationId:\s*([A-Za-z0-9_]+)\s*$")
SCOPE_RE = re.compile(r"^      x-tenant-scope:\s*\{(.*)\}\s*$")
MANAGEMENT_RE = re.compile(r"^      x-platform-management:\s*\{(.*)\}\s*$")


def _value_map(raw: str) -> dict[str, str]:
    """Read the constrained flow mappings used for contract extensions."""
    result: dict[str, str] = {}
    for key, quoted, plain in re.findall(
        r"([A-Za-z_]+):\s*(?:'([^']*)'|([^,}]+))", raw
    ):
        result[key] = quoted or plain.strip()
    return result


def operations(root: Path) -> dict[str, dict[str, object]]:
    spec = root / "api/openapi/openapi.yaml"
    result: dict[str, dict[str, object]] = {}
    path = ""
    method = ""
    operation = ""
    for line in spec.read_text(encoding="utf-8").splitlines():
        path_match = PATH_RE.match(line)
        if path_match:
            path = path_match.group(1)
            method = ""
            operation = ""
            continue
        method_match = METHOD_RE.match(line)
        if method_match and path:
            method = method_match.group(1).upper()
            operation = ""
            continue
        op_match = OP_RE.match(line)
        if op_match and method:
            operation = op_match.group(1)
            if operation in result:
                result[operation]["duplicate"] = True
                operation = ""
                continue
            result[operation] = {
                "method": method,
                "path": path,
                "scope": None,
                "management": False,
                "tenant_param": False,
                "description": "",
                "duplicate": False,
            }
            continue
        if not operation:
            continue
        description_match = re.match(r"^      description:\s*(.*)$", line)
        if description_match:
            result[operation]["description"] = description_match.group(1).strip()
        scope_match = SCOPE_RE.match(line)
        if scope_match:
            result[operation]["scope"] = _value_map(scope_match.group(1))
        management_match = MANAGEMENT_RE.match(line)
        if management_match:
            result[operation]["management"] = (
                _value_map(management_match.group(1)).get("enabled") == "true"
            )
        if TENANT_PARAMETER in line:
            result[operation]["tenant_param"] = True
    return result


def validate(root: Path) -> list[str]:
    failures: list[str] = []
    found = operations(root)
    names = set(found)
    if names != EXPECTED_OPERATIONS:
        failures.append(
            f"operation allowlist mismatch: found={sorted(names)} expected={sorted(EXPECTED_OPERATIONS)}"
        )
        return failures
    for name in sorted(EXPECTED_OPERATIONS):
        item = found[name]
        if item.get("duplicate"):
            failures.append(f"{name}: operationId is duplicated")
        scope = item["scope"]
        if name in PLATFORM_MANAGEMENT:
            if scope is not None or not item["management"]:
                failures.append(f"{name}: must have x-platform-management and no tenant scope")
            if "platform" not in str(item.get("description", "")).lower():
                failures.append(f"{name}: description must state platform management")
            continue
        if not isinstance(scope, dict):
            failures.append(f"{name}: missing x-tenant-scope")
            continue
        description = str(item.get("description", "")).lower()
        if "tenant" not in description:
            failures.append(f"{name}: description must state tenant policy")
        policy = scope.get("policy")
        selector = scope.get("selector")
        if name in ALL_TENANT:
            expected = ("all-tenants", "optional-query-filter", False, True)
        elif name in BODY_SCOPED:
            expected = ("scoped", "body", True, False)
        else:
            expected = ("scoped", "query", True, False)
        actual = (
            policy,
            selector,
            scope.get("platform_selector_required") == "true",
            scope.get("tenant_token_source"),
            scope.get("empty_platform_behavior"),
        )
        expected = (*expected[:3], "signed-claim", "all-tenants" if expected[3] else "reject")
        if actual != expected:
            failures.append(f"{name}: invalid scope metadata {scope}")
        if name in BODY_SCOPED:
            wanted_path = "tenant_id" if name == "writeEvent" else "events[*].tenant_id"
            if scope.get("selector_path") != wanted_path:
                failures.append(f"{name}: selector_path must be {wanted_path!r}")
        if name in ALL_TENANT or name in QUERY_SCOPED:
            if not item["tenant_param"]:
                failures.append(f"{name}: missing reusable tenantId query parameter")
        elif item["tenant_param"]:
            failures.append(f"{name}: body/management operation must not declare tenantId query parameter")
    if len(ALL_TENANT) != 3 or len(BODY_SCOPED) != 2 or len(PLATFORM_MANAGEMENT) != 2 or len(QUERY_SCOPED) != 28:
        failures.append("scope matrix cardinality changed unexpectedly")
    parameter_text = (root / "api/openapi/openapi.yaml").read_text(encoding="utf-8")
    if "tenantId: { name: tenant_id" not in parameter_text:
        failures.append("components.parameters.tenantId is missing")
    return failures


def run(root: Path | None = None) -> int:
    root = root or ROOT
    spec = root / "api/openapi/openapi.yaml"
    if not spec.exists():
        print("FAIL: tenant scope: api/openapi/openapi.yaml not found")
        return 1
    failures = validate(root)
    if failures:
        print("FAIL: tenant scope", *failures, sep="\n  ")
        return 1
    print("PASS: tenant scope (35 operations; 3 all-tenant, 28 query, 2 body, 2 management)")
    return 0


if __name__ == "__main__":
    sys.exit(run())
