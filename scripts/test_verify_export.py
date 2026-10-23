#!/usr/bin/env python3
"""Regression tests for scripts/verify_export.py.

All fixtures are real files under a private temporary directory.  Mutation
tests work on copies and compare the original bytes and permission bits
before and after every CLI subprocess.
"""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("verify_export.py")
SPEC = importlib.util.spec_from_file_location("verify_export_under_test", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
verify_export = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = verify_export
SPEC.loader.exec_module(verify_export)

TENANT = "tenant-验证"
SENTINEL = "SENTINEL_DO_NOT_ECHO_9f3a"
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


def event(
    event_id: str,
    *,
    tenant: str = TENANT,
    source: str = "crm",
    stream: str = "stream-a",
    sequence: int = 1,
    **extra,
) -> dict:
    value = {
        "event_id": event_id,
        "tenant_id": tenant,
        "source_system": source,
        "event_type": "demo.order@v1",
        "schema_id": "demo.order",
        "schema_version": 1,
        "occurred_at": "2026-01-01T00:00:00Z",
        "received_at": "2026-01-01T00:00:01Z",
        "stream_id": stream,
        "sequence": sequence,
        "actor": {"id": SENTINEL, "type": "service"},
        "action": "order.created",
        "outcome": "success",
        "data_classification": "internal",
        "retention_class": "standard",
        "idempotency_key": "idem-" + event_id,
        "prev_hash": "0" * 64,
        "hash": "a" * 64,
        "payload": {"note": "第一行\n第二行", "marker": SENTINEL},
    }
    value.update(extra)
    return value


def encode_lines(events: list[dict], *, ensure_ascii: bool = False) -> bytes:
    lines = [
        json.dumps(value, ensure_ascii=ensure_ascii, separators=(",", ":"))
        for value in events
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


def sha256_hex(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def metadata(digest: str, count: int, **updates) -> dict:
    value = {
        "id": "export-job-本地-1",
        "tenant_id": TENANT,
        "requested_by": SENTINEL,
        "query": {
            "from": "2026-01-01T00:00:00Z",
            "to": "2026-01-02T00:00:00Z",
            "actor_id": SENTINEL,
            "payload_field": SENTINEL,
        },
        "status": "completed",
        "created_at": "2026-01-01T00:00:01Z",
        "finished_at": "2026-01-01T00:00:02Z",
        "object_path": "/private/" + SENTINEL,
        "digest": digest,
        "event_count": count,
        "error": SENTINEL,
    }
    value.update(updates)
    return value


def json_fragment(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def duplicate_member(
    document: dict, key: str, final: object, first: object
) -> bytes:
    """Insert a duplicate member before its final, valid occurrence."""
    raw = json_fragment(document)
    marker = f"{json_fragment(key)}:{json_fragment(final)}"
    duplicate = f"{json_fragment(key)}:{json_fragment(first)},{marker}"
    if raw.count(marker) != 1:
        raise AssertionError("fixture member is not unique")
    return raw.replace(marker, duplicate, 1).encode("utf-8")


def fingerprint(path: Path) -> dict:
    info = path.stat()
    return {
        "bytes": path.read_bytes(),
        "mode": stat.S_IMODE(info.st_mode),
        "size": info.st_size,
    }


class VerifyExportCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.events = [
            event(
                "evt-甲",
                source="crm",
                stream="stream-a",
                sequence=2,
                reason="多行\n理由",
            ),
            event(
                "evt-乙",
                source="aero-vault",
                stream="stream-b",
                sequence=9,
                payload={"note": "文件\n第二行", "marker": SENTINEL},
            ),
            event(
                "evt-丙",
                source="crm",
                stream="stream-c",
                sequence=17,
                payload={"note": "非连续序号", "marker": SENTINEL},
            ),
        ]
        self.raw = encode_lines(self.events)
        self.digest = sha256_hex(self.raw)
        self.jsonl = self.root / ("export-" + SENTINEL + ".jsonl")
        self.jsonl.write_bytes(self.raw)
        os.chmod(self.jsonl, 0o640)
        self.job = self.root / ("job-" + SENTINEL + ".json")
        self.write_metadata(self.job, metadata(self.digest, len(self.events)))
        os.chmod(self.job, 0o640)

    def write_metadata(self, path: Path, value: dict | bytes) -> None:
        if isinstance(value, bytes):
            path.write_bytes(value)
        else:
            path.write_text(
                json.dumps(value, ensure_ascii=False), encoding="utf-8"
            )

    def run_cli(
        self,
        *,
        job: Path | None = None,
        jsonl: Path | None = None,
        tenant: str | None = TENANT,
        extra: tuple[str, ...] = (),
        timeout: float = 20.0,
    ) -> subprocess.CompletedProcess:
        argv = [
            sys.executable,
            "-B",
            str(SCRIPT),
            "--job-file",
            str(job if job is not None else self.job),
            "--jsonl",
            str(jsonl if jsonl is not None else self.jsonl),
        ]
        if tenant is not None:
            argv.extend(["--tenant", tenant])
        argv.extend(extra)
        return subprocess.run(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )

    def result(self, process: subprocess.CompletedProcess) -> dict:
        self.assertEqual(process.stderr, b"", "stderr must stay empty")
        text = process.stdout.decode("utf-8", "replace")
        lines = text.splitlines()
        self.assertEqual(len(lines), 1, f"expected one JSON result line, got {text!r}")
        value = json.loads(lines[0])
        self.assertIsInstance(value, dict)
        return value

    def assert_failure(
        self, process: subprocess.CompletedProcess, expected_code: str
    ) -> dict:
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(process.returncode, verify_export.EXIT_CODES[expected_code])
        value = self.result(process)
        self.assertEqual(value, {"ok": False, "code": expected_code})
        self.assertNotIn(SENTINEL, process.stdout.decode("utf-8", "replace"))
        self.assertNotIn(SENTINEL, process.stderr.decode("utf-8", "replace"))
        return value

    def test_valid_export_with_non_ascii_multiline_sources_and_streams(self):
        before_jsonl = fingerprint(self.jsonl)
        before_job = fingerprint(self.job)
        process = self.run_cli()
        self.assertEqual(process.returncode, 0)
        value = self.result(process)
        self.assertEqual(
            value,
            {
                "ok": True,
                "code": "verified",
                "event_count": 3,
                "sha256": self.digest,
            },
        )
        self.assertNotIn(SENTINEL, process.stdout.decode("utf-8", "replace"))
        self.assertEqual(fingerprint(self.jsonl), before_jsonl)
        self.assertEqual(fingerprint(self.job), before_job)

    def test_empty_export_is_valid(self):
        path = self.root / "empty.jsonl"
        path.write_bytes(b"")
        job = self.root / "empty.json"
        self.write_metadata(job, metadata(EMPTY_SHA256, 0))
        process = self.run_cli(job=job, jsonl=path)
        self.assertEqual(process.returncode, 0)
        self.assertEqual(
            self.result(process),
            {
                "ok": True,
                "code": "verified",
                "event_count": 0,
                "sha256": EMPTY_SHA256,
            },
        )

    def test_digest_is_over_exact_bytes_not_a_reencoding(self):
        # Same logical objects, deliberately different bytes (ASCII escapes and
        # spaces).  A tool that parsed then re-serialized would compute another
        # digest and fail this fixture.
        raw = encode_lines(self.events, ensure_ascii=True)
        self.assertNotEqual(raw, self.raw)
        path = self.root / "exact-bytes.jsonl"
        path.write_bytes(raw)
        job = self.root / "exact-bytes.json"
        self.write_metadata(job, metadata(sha256_hex(raw), len(self.events)))
        process = self.run_cli(job=job, jsonl=path)
        self.assertEqual(process.returncode, 0)
        value = self.result(process)
        self.assertEqual(value["sha256"], sha256_hex(raw))
        self.assertEqual(value["event_count"], len(self.events))

    def test_copy_byte_change_truncation_and_reorder_fail_without_touching_original(self):
        lines = self.raw.splitlines(keepends=True)
        mutations = {
            "byte": (
                self.raw.replace(
                    b'"source_system":"crm"', b'"source_system":"crmX"', 1
                ),
                "digest_mismatch",
            ),
            "truncate": (self.raw[:-7], "invalid_json"),
            "reorder": (lines[1] + lines[0] + b"".join(lines[2:]), "digest_mismatch"),
        }
        self.assertNotEqual(mutations["byte"][0], self.raw)
        self.assertNotEqual(mutations["reorder"][0], self.raw)
        before_jsonl = fingerprint(self.jsonl)
        before_job = fingerprint(self.job)
        for name, (raw, expected) in mutations.items():
            with self.subTest(name=name):
                path = self.root / f"mutated-{name}.jsonl"
                shutil.copyfile(self.jsonl, path)
                os.chmod(path, stat.S_IMODE(self.jsonl.stat().st_mode))
                path.write_bytes(raw)
                process = self.run_cli(jsonl=path)
                self.assert_failure(process, expected)
                self.assertEqual(fingerprint(self.jsonl), before_jsonl)
                self.assertEqual(fingerprint(self.job), before_job)

    def test_structural_failures_have_matching_digest_and_are_not_masked(self):
        tenant_bytes = TENANT.encode("utf-8")
        cases = {
            "invalid_json": (b'{"event_id": "' + SENTINEL.encode() + b'"\n', 1),
            "invalid_utf8": (
                b'{"event_id":"e1","tenant_id":"'
                + tenant_bytes
                + b'","x":"\xff","marker":"'
                + SENTINEL.encode()
                + b'"}\n',
                1,
            ),
            "non_object": (b'["' + SENTINEL.encode() + b'"]\n', 1),
            "missing_final_lf": (self.raw[:-1], len(self.events)),
            "duplicate_event_id": (
                encode_lines(
                    [
                        event("duplicate-id", source="crm"),
                        event("duplicate-id", source="aero"),
                    ]
                ),
                2,
            ),
            "missing_event_id": (
                encode_lines([{"tenant_id": TENANT, "marker": SENTINEL}]),
                1,
            ),
            "mixed_tenant": (
                encode_lines(
                    [
                        event("tenant-a-event"),
                        event("tenant-b-event", tenant="tenant-other"),
                    ]
                ),
                2,
            ),
        }
        for name, (raw, count) in cases.items():
            with self.subTest(name=name):
                path = self.root / f"structure-{name}.jsonl"
                path.write_bytes(raw)
                job = self.root / f"structure-{name}.json"
                self.write_metadata(job, metadata(sha256_hex(raw), count))
                process = self.run_cli(job=job, jsonl=path)
                expected = {
                    "invalid_json": "invalid_json",
                    "invalid_utf8": "invalid_utf8",
                    "non_object": "invalid_jsonl_object",
                    "missing_final_lf": "invalid_json",
                    "duplicate_event_id": "duplicate_event_id",
                    "missing_event_id": "missing_event_id",
                    "mixed_tenant": "tenant_mismatch",
                }[name]
                self.assert_failure(process, expected)

    def test_duplicate_jsonl_object_keys_are_rejected_even_when_last_value_is_valid(self):
        # json.loads keeps the last duplicate.  Each fixture therefore ends
        # with the valid tenant/event and matches its own raw-byte digest and
        # count, which the old parser wrongly accepted.
        tenant_bytes = json_fragment(TENANT).encode("utf-8")
        sentinel_bytes = json_fragment(SENTINEL).encode("utf-8")
        cases = {
            "tenant_id": (
                b'{"event_id":"dup-tenant","tenant_id":' + sentinel_bytes
                + b',"tenant_id":' + tenant_bytes + b'}\n'
            ),
            "event_id": (
                b'{"event_id":' + sentinel_bytes
                + b',"event_id":"dup-event","tenant_id":' + tenant_bytes
                + b'}\n'
            ),
            "nested_actor": (
                b'{"event_id":"nested-actor","tenant_id":' + tenant_bytes
                + b',"actor":{"id":' + sentinel_bytes
                + b',"id":"second"}}\n'
            ),
            "nested_payload": (
                b'{"event_id":"nested-payload","tenant_id":' + tenant_bytes
                + b',"payload":{"note":' + sentinel_bytes
                + b',"note":"second"}}\n'
            ),
            "nested_array_object": (
                b'{"event_id":"nested-array","tenant_id":' + tenant_bytes
                + b',"payload":{"items":[{"id":' + sentinel_bytes
                + b',"id":"second"}]}}\n'
            ),
            "same_value_duplicate": (
                b'{"event_id":"same-value","tenant_id":' + tenant_bytes
                + b',"tenant_id":' + tenant_bytes + b'}\n'
            ),
            "escaped_tenant_key": (
                b'{"event_id":"escaped-key","\\u0074enant_id":'
                + sentinel_bytes + b',"tenant_id":' + tenant_bytes + b'}\n'
            ),
            "nested_actor_escaped_key": (
                b'{"event_id":"nested-escaped","tenant_id":' + tenant_bytes
                + b',"actor":{"\\u0069d":' + sentinel_bytes
                + b',"id":"second"}}\n'
            ),
        }
        before_jsonl = fingerprint(self.jsonl)
        before_job = fingerprint(self.job)
        for name, raw in cases.items():
            with self.subTest(name=name):
                path = self.root / f"duplicate-key-{name}.jsonl"
                path.write_bytes(raw)
                job = self.root / f"duplicate-key-{name}.json"
                self.write_metadata(job, metadata(sha256_hex(raw), 1))
                self.assert_failure(
                    self.run_cli(job=job, jsonl=path), "invalid_json"
                )
        self.assertEqual(fingerprint(self.jsonl), before_jsonl)
        self.assertEqual(fingerprint(self.job), before_job)

    def test_same_key_names_across_objects_and_inside_strings_are_allowed(self):
        events = [
            event(
                "evt-same-a",
                actor={"id": "actor-a", "type": "service"},
                payload={
                    "id": "payload-a",
                    "note": "tenant_id status digest event_count event_id",
                },
            ),
            event(
                "evt-same-b",
                actor={"id": "actor-b", "type": "service"},
                payload={
                    "id": "payload-b",
                    "note": "actor.id payload.id repeated spelling only",
                },
            ),
        ]
        raw = encode_lines(events)
        path = self.root / "repeat-across-objects.jsonl"
        path.write_bytes(raw)
        os.chmod(path, 0o640)
        job = self.root / "repeat-across-objects.json"
        self.write_metadata(job, metadata(sha256_hex(raw), len(events)))
        before_jsonl = fingerprint(path)
        before_job = fingerprint(job)
        process = self.run_cli(job=job, jsonl=path)
        self.assertEqual(process.returncode, 0)
        value = self.result(process)
        self.assertEqual(value["event_count"], len(events))
        self.assertEqual(value["sha256"], sha256_hex(raw))
        self.assertEqual(fingerprint(path), before_jsonl)
        self.assertEqual(fingerprint(job), before_job)

    def test_count_mismatch_fails_with_matching_digest(self):
        job = self.root / "wrong-count.json"
        self.write_metadata(job, metadata(self.digest, len(self.events) + 1))
        self.assert_failure(self.run_cli(job=job), "count_mismatch")

    def test_metadata_evidence_and_shape_failures(self):
        valid = metadata(self.digest, len(self.events))
        summary_only = {
            "id": valid["id"],
            "tenant_id": TENANT,
            "requested_by": SENTINEL,
            "status": "completed",
            "created_at": valid["created_at"],
        }
        cases = {
            "bad_json": b'{"id": "' + SENTINEL.encode() + b'"',
            "bad_utf8": b'{"id":"' + SENTINEL.encode() + b'\xff"}',
            "list_summary": json.dumps([valid]).encode("utf-8"),
            "summary_only": json.dumps(summary_only).encode("utf-8"),
            "not_completed": metadata(self.digest, len(self.events), status="running"),
            "wrong_tenant": metadata(self.digest, len(self.events), tenant_id="tenant-other"),
            "uppercase_digest": metadata("A" * 64, len(self.events)),
            "short_digest": metadata("abc", len(self.events)),
            "negative_count": metadata(self.digest, -1),
            "bool_count": metadata(self.digest, True),
            "float_count": metadata(self.digest, float(len(self.events))),
            "empty_id": metadata(self.digest, len(self.events), id=""),
        }
        for name, value in cases.items():
            with self.subTest(name=name):
                job = self.root / f"metadata-{name}.json"
                self.write_metadata(job, value)
                self.assert_failure(self.run_cli(job=job), "invalid_metadata")

    def test_duplicate_metadata_members_are_rejected_even_when_last_value_is_valid(self):
        # The final status/digest/count all match the JSONL and tenant; only a
        # parser that rejects duplicate object keys can tell this apart from
        # the valid metadata fixture.
        valid = metadata(self.digest, len(self.events))
        cases = {
            "status": ("status", valid["status"], "running-" + SENTINEL),
            "status_same_value": (
                "status",
                valid["status"],
                valid["status"],
            ),
            "digest": ("digest", valid["digest"], SENTINEL),
            "event_count": ("event_count", valid["event_count"], SENTINEL),
            "nested_query_from": (
                "from",
                valid["query"]["from"],
                SENTINEL,
            ),
        }
        before_jsonl = fingerprint(self.jsonl)
        before_job = fingerprint(self.job)
        for name, (key, final, first) in cases.items():
            with self.subTest(name=name):
                job = self.root / f"duplicate-metadata-{name}.json"
                job.write_bytes(duplicate_member(valid, key, final, first))
                self.assert_failure(self.run_cli(job=job), "invalid_metadata")
        self.assertEqual(fingerprint(self.jsonl), before_jsonl)
        self.assertEqual(fingerprint(self.job), before_job)

    def test_missing_symlink_directory_and_fifo_inputs_are_rejected(self):
        before_jsonl = fingerprint(self.jsonl)
        before_job = fingerprint(self.job)
        missing = self.root / ("missing-" + SENTINEL)
        self.assert_failure(
            self.run_cli(job=missing), "input_unavailable"
        )
        self.assert_failure(
            self.run_cli(jsonl=missing), "input_unavailable"
        )

        symlink = self.root / ("leaf-link-" + SENTINEL)
        os.symlink(self.jsonl, symlink)
        self.assert_failure(
            self.run_cli(jsonl=symlink), "input_not_regular"
        )
        job_link = self.root / ("job-link-" + SENTINEL)
        os.symlink(self.job, job_link)
        self.assert_failure(
            self.run_cli(job=job_link), "input_not_regular"
        )

        directory = self.root / ("directory-" + SENTINEL)
        directory.mkdir()
        self.assert_failure(
            self.run_cli(jsonl=directory), "input_not_regular"
        )
        self.assert_failure(
            self.run_cli(job=directory), "input_not_regular"
        )

        if hasattr(os, "mkfifo"):
            fifo = self.root / ("fifo-" + SENTINEL)
            os.mkfifo(fifo)
            self.assert_failure(
                self.run_cli(jsonl=fifo, timeout=10), "input_not_regular"
            )
            self.assert_failure(
                self.run_cli(job=fifo, timeout=10), "input_not_regular"
            )

        self.assertEqual(fingerprint(self.jsonl), before_jsonl)
        self.assertEqual(fingerprint(self.job), before_job)

    @unittest.skipIf(os.geteuid() == 0, "root can read mode-000 files")
    def test_unreadable_input_fails_without_echoing_its_name(self):
        unreadable = self.root / ("private-" + SENTINEL + ".jsonl")
        shutil.copyfile(self.jsonl, unreadable)
        os.chmod(unreadable, 0)
        self.assert_failure(
            self.run_cli(jsonl=unreadable), "input_unavailable"
        )

    def test_metadata_total_and_line_limits_fail_closed(self):
        oversized_metadata = self.root / "oversized-metadata.json"
        oversized_metadata.write_bytes(
            b" " * (verify_export.METADATA_BYTES_LIMIT + 1)
        )
        self.assert_failure(
            self.run_cli(job=oversized_metadata), "metadata_too_large"
        )

        # Sparse regular file: the size precheck rejects it without reading a
        # gigabyte or blocking on an unbounded input.
        oversized_jsonl = self.root / "oversized.jsonl"
        with oversized_jsonl.open("wb") as stream:
            stream.truncate(verify_export.TOTAL_BYTES_LIMIT + 1)
        job = self.root / "oversized-job.json"
        self.write_metadata(job, metadata(EMPTY_SHA256, 0))
        self.assert_failure(
            self.run_cli(job=job, jsonl=oversized_jsonl), "file_too_large"
        )

        long_raw = (
            b'{"event_id":"'
            + b"x" * verify_export.LINE_BYTES_LIMIT
            + b'","tenant_id":"'
            + TENANT.encode("utf-8")
            + b'"}\n'
        )
        long_jsonl = self.root / "long-line.jsonl"
        long_jsonl.write_bytes(long_raw)
        long_job = self.root / "long-line.json"
        self.write_metadata(long_job, metadata(sha256_hex(long_raw), 1))
        self.assert_failure(
            self.run_cli(job=long_job, jsonl=long_jsonl), "line_too_large"
        )

    def test_arguments_and_metadata_sentinels_are_not_echoed(self):
        cases = [
            (
                [
                    "--job-file",
                    str(self.job),
                    "--jsonl",
                    str(self.jsonl),
                    "--tenant",
                    TENANT,
                    "--unknown-" + SENTINEL,
                    SENTINEL,
                ],
                "invalid_arguments",
            ),
            (
                ["--job-file", str(self.job), "--jsonl", str(self.jsonl)],
                "invalid_arguments",
            ),
            (
                [
                    "--job-file",
                    str(self.job),
                    "--jsonl",
                    str(self.jsonl),
                    "--tenant",
                    "",
                ],
                "invalid_arguments",
            ),
        ]
        for argv, expected in cases:
            with self.subTest(argv=argv[-1]):
                process = subprocess.run(
                    [sys.executable, "-B", str(SCRIPT), *argv],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=20,
                    check=False,
                )
                self.assert_failure(process, expected)
                self.assertNotIn(
                    SENTINEL, process.stdout.decode("utf-8", "replace")
                )
                self.assertNotIn(
                    SENTINEL, process.stderr.decode("utf-8", "replace")
                )

    def test_tool_source_has_no_network_or_credential_reads(self):
        source = SCRIPT.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".", 1)[0])
        for forbidden in ("urllib", "http", "requests", "socket", "ssl", "subprocess"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, imported)
        for forbidden in ("os.environ", "os.getenv", "getpass"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
