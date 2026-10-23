#!/usr/bin/env python3
"""Offline consistency check for an already-downloaded audit export.

The tool deliberately does not contact the API, read credentials, decrypt an
archive object, or write business/control state.  Given the ExportJob JSON an
operator previously obtained from the authorized detail endpoint and the raw
JSONL bytes previously obtained from the authorized download endpoint, it
checks only that those two local artifacts agree:

* the job is a completed job for the explicitly selected tenant, has a
  non-empty id, a lowercase SHA-256 digest and a non-negative integer
  ``event_count`` (an ExportSummary/list item is not sufficient evidence);
* SHA-256 is calculated over the exact bytes read from the JSONL stream; and
* every newline-terminated JSONL record is an object with a non-empty,
  unique ``event_id`` and a ``tenant_id`` equal to the selected tenant, and
  the number of records equals ``event_count``; and
* neither input contains a JSON object with a duplicate member name at any
  nesting depth.  Names are compared after JSON escape decoding, and a
  duplicate is rejected even when both occurrences carry the same value.

It does not re-encode/normalize the input, reconstruct plaintext, verify the
hash chain, evaluate Legal Hold, authenticate metadata provenance, or prove
WORM/full-history/business execution.  Inputs are opened read-only and a
leaf symlink, directory, FIFO, socket or other non-regular file is rejected
without blocking.  Output is one fixed, minimal JSON object; failure details
(including paths, payloads, query fields, actors and raw exceptions) are not
printed.
"""

from __future__ import annotations

import argparse
import errno
import hashlib
import hmac
import json
import os
import re
import stat
import sys
from typing import Any, BinaryIO

# Bounds are intentionally local to this offline tool.  They do not replace
# production limits or claim that every export produced by the service fits.
METADATA_BYTES_LIMIT = 1 * 1024 * 1024
TOTAL_BYTES_LIMIT = 1 * 1024 * 1024 * 1024
LINE_BYTES_LIMIT = 512 * 1024

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

EXIT_CODES = {
    "invalid_arguments": 2,
    "invalid_metadata": 3,
    "invalid_json": 4,
    "invalid_utf8": 5,
    "invalid_jsonl_object": 6,
    "missing_event_id": 7,
    "duplicate_event_id": 8,
    "tenant_mismatch": 9,
    "count_mismatch": 10,
    "digest_mismatch": 11,
    "input_unavailable": 12,
    "input_not_regular": 13,
    "metadata_too_large": 14,
    "file_too_large": 15,
    "line_too_large": 16,
    "internal_error": 70,
}


class VerificationFailure(Exception):
    """A fail-closed local check with a fixed, non-sensitive result code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code
        self.exit_code = EXIT_CODES[code]


class SafeArgumentParser(argparse.ArgumentParser):
    """Argument errors must not echo untrusted argument text."""

    def error(self, _message: str) -> None:
        raise VerificationFailure("invalid_arguments")


def _open_regular(path: str) -> tuple[int, os.stat_result]:
    """Open one leaf read-only and prove it is a regular file.

    O_NOFOLLOW rejects a leaf symlink without resolving it.  O_NONBLOCK keeps
    a FIFO from blocking the tool before fstat can reject it.
    """
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        if not getattr(os, "O_NOFOLLOW", None):
            if stat.S_ISLNK(os.lstat(path).st_mode):
                raise VerificationFailure("input_not_regular")
        fd = os.open(path, flags)
    except VerificationFailure:
        raise
    except OSError as error:
        if error.errno == errno.ELOOP:
            raise VerificationFailure("input_not_regular") from None
        raise VerificationFailure("input_unavailable") from None
    except (ValueError, TypeError):
        raise VerificationFailure("input_unavailable") from None
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise VerificationFailure("input_not_regular")
    except VerificationFailure:
        os.close(fd)
        raise
    except OSError:
        os.close(fd)
        raise VerificationFailure("input_unavailable") from None
    return fd, info


def _read_metadata(fd: int, info: os.stat_result) -> dict[str, Any]:
    if info.st_size > METADATA_BYTES_LIMIT:
        raise VerificationFailure("metadata_too_large")
    chunks: list[bytes] = []
    remaining = METADATA_BYTES_LIMIT + 1
    while remaining > 0:
        try:
            chunk = os.read(fd, min(65536, remaining))
        except OSError:
            raise VerificationFailure("input_unavailable") from None
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    raw = b"".join(chunks)
    if len(raw) > METADATA_BYTES_LIMIT:
        raise VerificationFailure("metadata_too_large")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise VerificationFailure("invalid_metadata") from None
    try:
        document = json.loads(
            text,
            parse_constant=_reject_constant,
            object_pairs_hook=_reject_duplicate_keys,
        )
    except (ValueError, RecursionError):
        raise VerificationFailure("invalid_metadata") from None
    if not isinstance(document, dict):
        raise VerificationFailure("invalid_metadata")
    return document


def _reject_constant(_value: str) -> Any:
    raise ValueError("non-standard JSON constant")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build one object, rejecting repeated decoded member names.

    ``object_pairs_hook`` receives the escaped member names after JSON
    decoding and is invoked separately for every object at every nesting
    depth, so ``"tenant_id"`` escaped as ``"\\u0074enant_id"`` still collides
    while equal names in unrelated objects do not.
    """
    document: dict[str, Any] = {}
    for key, value in pairs:
        if key in document:
            raise ValueError("duplicate JSON object key")
        document[key] = value
    return document


def _validate_metadata(document: dict[str, Any], tenant: str) -> tuple[str, int]:
    job_id = document.get("id")
    status = document.get("status")
    tenant_id = document.get("tenant_id")
    digest = document.get("digest")
    event_count = document.get("event_count")
    if not isinstance(job_id, str) or job_id == "":
        raise VerificationFailure("invalid_metadata")
    if status != "completed" or tenant_id != tenant:
        raise VerificationFailure("invalid_metadata")
    if not isinstance(digest, str) or SHA256_RE.fullmatch(digest) is None:
        raise VerificationFailure("invalid_metadata")
    if type(event_count) is not int or event_count < 0:
        raise VerificationFailure("invalid_metadata")
    return digest, event_count


def _parse_jsonl_line(line: bytes) -> dict[str, Any]:
    if not line.endswith(b"\n"):
        raise VerificationFailure("invalid_json")
    body = line[:-1]
    if not body.strip():
        raise VerificationFailure("invalid_json")
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        raise VerificationFailure("invalid_utf8") from None
    try:
        value = json.loads(
            text,
            parse_constant=_reject_constant,
            object_pairs_hook=_reject_duplicate_keys,
        )
    except (ValueError, RecursionError):
        raise VerificationFailure("invalid_json") from None
    if not isinstance(value, dict):
        raise VerificationFailure("invalid_jsonl_object")
    return value


def _scan_jsonl(
    fd: int,
    info: os.stat_result,
    tenant: str,
    expected_count: int,
    expected_digest: str,
) -> tuple[int, str]:
    if info.st_size > TOTAL_BYTES_LIMIT:
        raise VerificationFailure("file_too_large")
    digest = hashlib.sha256()
    seen: set[str] = set()
    count = 0
    total = 0
    try:
        stream: BinaryIO = os.fdopen(fd, "rb", closefd=False)
        try:
            while True:
                line = stream.readline(LINE_BYTES_LIMIT + 1)
                if not line:
                    break
                if len(line) > LINE_BYTES_LIMIT:
                    raise VerificationFailure("line_too_large")
                total += len(line)
                if total > TOTAL_BYTES_LIMIT:
                    raise VerificationFailure("file_too_large")
                digest.update(line)
                event = _parse_jsonl_line(line)
                event_id = event.get("event_id")
                if not isinstance(event_id, str) or event_id == "":
                    raise VerificationFailure("missing_event_id")
                if event_id in seen:
                    raise VerificationFailure("duplicate_event_id")
                seen.add(event_id)
                if event.get("tenant_id") != tenant:
                    raise VerificationFailure("tenant_mismatch")
                count += 1
        finally:
            stream.close()
    except VerificationFailure:
        raise
    except OSError:
        raise VerificationFailure("input_unavailable") from None
    if count != expected_count:
        raise VerificationFailure("count_mismatch")
    actual_digest = digest.hexdigest()
    if not hmac.compare_digest(actual_digest, expected_digest):
        raise VerificationFailure("digest_mismatch")
    return count, actual_digest


def verify(job_file: str, jsonl_file: str, tenant: str) -> tuple[int, str]:
    if not isinstance(tenant, str) or tenant == "":
        raise VerificationFailure("invalid_arguments")
    job_fd, job_info = _open_regular(job_file)
    try:
        document = _read_metadata(job_fd, job_info)
    finally:
        os.close(job_fd)
    expected_digest, expected_count = _validate_metadata(document, tenant)

    jsonl_fd, jsonl_info = _open_regular(jsonl_file)
    try:
        count, actual_digest = _scan_jsonl(
            jsonl_fd, jsonl_info, tenant, expected_count, expected_digest
        )
    finally:
        os.close(jsonl_fd)
    return count, actual_digest


def _emit(result: dict[str, Any]) -> None:
    line = json.dumps(
        result, ensure_ascii=True, separators=(",", ":"), sort_keys=True
    )
    try:
        sys.stdout.write(line + "\n")
        sys.stdout.flush()
    except OSError:
        pass


def main(argv: list[str] | None = None) -> int:
    parser = SafeArgumentParser(
        prog="verify_export.py",
        description=(
            "Offline check of local ExportJob JSON against raw downloaded JSONL; "
            "no network, credentials, decryption or state writes."
        ),
    )
    parser.add_argument("--job-file", required=True)
    parser.add_argument("--jsonl", required=True)
    parser.add_argument("--tenant", required=True)
    try:
        args = parser.parse_args(argv)
        count, digest = verify(args.job_file, args.jsonl, args.tenant)
    except VerificationFailure as failure:
        _emit({"ok": False, "code": failure.code})
        return failure.exit_code
    except KeyboardInterrupt:
        _emit({"ok": False, "code": "internal_error"})
        return EXIT_CODES["internal_error"]
    except Exception:
        _emit({"ok": False, "code": "internal_error"})
        return EXIT_CODES["internal_error"]
    _emit(
        {
            "ok": True,
            "code": "verified",
            "event_count": count,
            "sha256": digest,
        }
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
