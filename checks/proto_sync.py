"""Proto drift guard for ``api/proto`` (generated-code sync check).

The gRPC runtime ships checked-in generated code (``api/proto/audit.pb.go``,
``api/proto/audit_grpc.pb.go``) that is the compiled wire surface.  Nothing
regenerates or validates that code against its source ``api/proto/audit.proto``;
a field added to the ``.proto`` passes the old source-text check
(``contract_fields``) while the compiled runtime silently ignores it.

This module closes that gap with three layers (all pure stdlib):

1. **Descriptor parse (R2a, always on, zero external tools).**  Decode the
   protobuf wire format of the embedded file descriptor
   ``file_audit_proto_rawDesc`` in ``audit.pb.go`` (Go string-literal
   unescaping + varint/length-delimited walker) and compare, per message and
   per field, ``(name, number, repeated?, type)`` against a parse of
   ``audit.proto``.  This is a check of the generated code, not the source
   text: it fails exactly when a field added to the ``.proto`` never reaches
   the compiled runtime.
2. **Version-pin check (R3, always on).**  The generator versions in the
   generated headers must match the ``proto:`` block in ``engineering.yaml``
   (the single pin manifest), and the ``protoc-gen-go`` pin must equal the
   ``google.golang.org/protobuf`` version in ``go.mod``.
3. **Replay (R6, on when the pinned toolchain is present).**  Delegate to
   ``scripts/proto-gen.py --check``, which regenerates into a temp directory
   with the pinned tools and byte-compares.  Detects comment/format-only
   drift the descriptor check cannot see.  Tools absent -> explicit skip
   notice, never a silent pass (fail closed on the dangerous class).

Public surface:

- ``run(root=None) -> int``      gate entry: R2a + R3 + R6
- ``envelope_fields(root=None)`` EventEnvelope field names from the
                                 GENERATED descriptor (R4); raises on
                                 unparseable descriptor
- ``generator_versions(root)``   header version parse (testable)
- ``parse_descriptor(raw)``      R2a wire walker (testable)
- ``proto_pins(root)``           pins from engineering.yaml (testable)
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

# FieldDescriptorProto.Type values (descriptor.proto).  Scalar kinds are
# compared by number; message/enum/group kinds (which carry a type_name) are
# compared by unqualified name so `.proto`'s `google.protobuf.Timestamp`
# maps to the descriptor's `.google.protobuf.Timestamp`.
SCALAR_TYPES = {
    "double": 1, "float": 2, "int64": 3, "uint64": 4, "int32": 5,
    "fixed64": 6, "fixed32": 7, "bool": 8, "string": 9, "group": 10,
    "message": 11, "bytes": 12, "uint32": 13, "enum": 14, "sfixed32": 15,
    "sfixed64": 16, "sint32": 17, "sint64": 18,
}
NAMED_TYPE_KINDS = frozenset({10, 11, 14})  # group, message, enum
REPEATED_LABEL = 3
OPTIONAL_LABEL = 1

# Replay exit code contract with scripts/proto-gen.py: 3 means "pinned
# toolchain not present" and is a skip, not a failure.
REPLAY_UNAVAILABLE = 3


class ProtoSyncParseError(Exception):
    """Raised when a parse fails; the check fails closed on any of these."""


def default_root() -> Path:
    return Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# engineering.yaml pins (R3)
# ---------------------------------------------------------------------------

PIN_KEYS = ("protoc", "protoc-gen-go", "protoc-gen-go-grpc")


def proto_pins(root: Path | None = None) -> dict[str, str]:
    """Read the ``proto:`` block of ``engineering.yaml`` (single pin manifest).

    Raises ProtoSyncParseError when the block or any pin is missing.
    """
    root = Path(root) if root is not None else default_root()
    path = root / "engineering.yaml"
    if not path.exists():
        raise ProtoSyncParseError("engineering.yaml missing (proto pins)")
    lines = path.read_text(encoding="utf-8").splitlines()
    block: list[str] = []
    found = False
    for index, line in enumerate(lines):
        if re.match(r"^\s*proto:\s*$", line):
            found = True
            for member in lines[index + 1:]:
                if member and not member[0].isspace():
                    break
                block.append(member)
            break
    if not found:
        raise ProtoSyncParseError("engineering.yaml missing 'proto:' pin block")
    pins: dict[str, str] = {}
    for line in block:
        match = re.match(r"^\s*(protoc|protoc-gen-go|protoc-gen-go-grpc):\s*(v?[\d.]+)\s*$", line)
        if match:
            version = match.group(2)
            pins[match.group(1)] = version if version.startswith("v") else "v" + version
    missing = [key for key in PIN_KEYS if key not in pins]
    if missing:
        raise ProtoSyncParseError(f"engineering.yaml proto: block missing pin(s): {', '.join(missing)}")
    return pins


# ---------------------------------------------------------------------------
# .proto source parse (fail closed on unsupported constructs)
# ---------------------------------------------------------------------------

def _strip_comments(text: str) -> str:
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    return re.sub(r"//[^\n]*", "", text)


def parse_proto_text(text: str) -> dict[str, dict[int, tuple]]:
    """Parse top-level ``message`` blocks of a ``.proto`` file.

    Returns ``{message_name: {field_number: (name, label, type)}}`` where
    label is 3 for repeated fields and 1 otherwise, and type is the scalar
    FieldDescriptorProto.Type number or the unqualified name for message/enum
    types.  Unsupported constructs (maps, oneofs, groups, reserved,
    extensions) raise ProtoSyncParseError -- an unparseable source is never
    "in sync".
    """
    source = _strip_comments(text)
    for pattern, label in (
        (r"map<", "map fields"),
        (r"^\s*oneof\b", "oneofs"),
        (r"\breserved\b", "reserved"),
        (r"\bextension\b", "extensions"),
        (r"\bgroup\b", "groups"),
    ):
        if re.search(pattern, source, re.M):
            raise ProtoSyncParseError(f"audit.proto uses unsupported construct: {label}")
    messages: dict[str, dict[int, tuple]] = {}
    field_re = re.compile(r"^\s*(repeated\s+)?([\w.]+)\s+(\w+)\s*=\s*(\d+)\s*;", re.M)
    for match in re.finditer(r"message (\w+) \{(.*?)\n\}", source, re.S):
        name, body = match.group(1), match.group(2)
        fields: dict[int, tuple] = {}
        for field in field_re.finditer(body):
            repeated = field.group(1) is not None
            type_token = field.group(2)
            if type_token in SCALAR_TYPES:
                field_type: object = SCALAR_TYPES[type_token]
            else:
                field_type = type_token.rsplit(".", 1)[-1]
            number = int(field.group(4))
            fields[number] = (field.group(3), REPEATED_LABEL if repeated else OPTIONAL_LABEL, field_type)
        if not fields:
            raise ProtoSyncParseError(f"message {name}: no fields parsed")
        messages[name] = fields
    if not messages:
        raise ProtoSyncParseError("no message blocks parsed from audit.proto")
    return messages


# ---------------------------------------------------------------------------
# Generated descriptor parse (R2a): Go literal unescape + wire walker
# ---------------------------------------------------------------------------

def _go_unescape(literal: str) -> bytes:
    out = bytearray()
    index = 0
    length = len(literal)
    while index < length:
        char = literal[index]
        if char != "\\":
            out.append(ord(char))
            index += 1
            continue
        index += 1
        if index >= length:
            raise ProtoSyncParseError("truncated escape in rawDesc literal")
        escape = literal[index]
        if escape == "x":
            hexpart = literal[index + 1:index + 3]
            if len(hexpart) != 2 or not re.fullmatch(r"[0-9a-fA-F]{2}", hexpart):
                raise ProtoSyncParseError(f"bad \\x escape in rawDesc literal: \\x{hexpart}")
            out.append(int(hexpart, 16))
            index += 3
        elif escape in "01234567":
            end = index
            while end < length and end < index + 3 and literal[end] in "01234567":
                end += 1
            out.append(int(literal[index:end], 8))
            index = end
        else:
            mapping = {"n": 10, "t": 9, "a": 7, "v": 11, "f": 12, "r": 13,
                       "b": 8, '"': 34, "\\": 92}
            if escape not in mapping:
                raise ProtoSyncParseError(f"unsupported escape \\{escape} in rawDesc literal")
            out.append(mapping[escape])
            index += 1
    return bytes(out)


def extract_raw_desc(text: str) -> bytes:
    """Decode the concatenated Go string literal of file_audit_proto_rawDesc."""
    match = re.search(r'const file_audit_proto_rawDesc = "" \+(\n.*?)(?=\nvar \()', text, re.S)
    if not match:
        raise ProtoSyncParseError("file_audit_proto_rawDesc constant not found in audit.pb.go")
    parts = re.findall(r'"((?:[^"\\]|\\.)*)"', match.group(1))
    if not parts:
        raise ProtoSyncParseError("no string literals in file_audit_proto_rawDesc")
    return b"".join(_go_unescape(part) for part in parts)


def _decode(payload: bytes) -> str:
    try:
        return payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise ProtoSyncParseError(f"invalid UTF-8 in descriptor string: {error}") from error


def _take_length_delimited(data: bytes, pos: int) -> tuple[bytes, int]:
    """Read a length-delimited payload, failing closed on truncation.

    protobuf's parser rejects a field whose declared length overruns the
    buffer; silently slicing a short block would let a corrupted descriptor
    parse into a plausible-but-wrong message map.
    """
    length, pos = _read_varint(data, pos)
    end = pos + length
    if end > len(data):
        raise ProtoSyncParseError(
            f"length-delimited field overruns buffer ({pos} + {length} > {len(data)})")
    return data[pos:end], end


def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise ProtoSyncParseError("truncated varint")
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return value, pos
        shift += 7
        if shift > 70:
            raise ProtoSyncParseError("varint too long")


def _skip_field(data: bytes, pos: int, tag: int) -> int:
    wire = tag & 7
    if wire == 0:
        _, pos = _read_varint(data, pos)
    elif wire == 1:
        pos += 8
    elif wire == 2:
        length, pos = _read_varint(data, pos)
        pos += length
    elif wire == 5:
        pos += 4
    else:
        raise ProtoSyncParseError(f"unsupported wire type {wire}")
    if pos > len(data):
        raise ProtoSyncParseError("field overruns buffer")
    return pos


def _parse_field(block: bytes) -> tuple[int, tuple]:
    name = None
    number = None
    label = None
    field_type = None
    type_name = None
    pos = 0
    while pos < len(block):
        tag, pos = _read_varint(block, pos)
        field_number, wire = tag >> 3, tag & 7
        if wire != 2:
            pos = _skip_field(block, pos, tag)
            continue
        payload, pos = _take_length_delimited(block, pos)
        if field_number == 1:
            name = _decode(payload)
        elif field_number == 6:
            type_name = _decode(payload)
    # Re-walk for the varint members (field 3 number, 4 label, 5 type).
    pos = 0
    while pos < len(block):
        tag, pos = _read_varint(block, pos)
        field_number, wire = tag >> 3, tag & 7
        if field_number in (3, 4, 5) and wire == 0:
            value, pos = _read_varint(block, pos)
            if field_number == 3:
                number = value
            elif field_number == 4:
                label = value
            else:
                field_type = value
        else:
            pos = _skip_field(block, pos, tag)
    if name is None or number is None or label is None or field_type is None:
        raise ProtoSyncParseError(f"incomplete FieldDescriptorProto: name={name!r}")
    if label not in (1, 2, 3):
        raise ProtoSyncParseError(f"field {name}: unknown label value {label}")
    if field_type not in range(1, 19):
        raise ProtoSyncParseError(f"field {name}: unknown type value {field_type}")
    if field_type in NAMED_TYPE_KINDS:
        if not type_name:
            raise ProtoSyncParseError(f"message/enum field {name} without type_name")
        compare_type: object = type_name.lstrip(".").rsplit(".", 1)[-1]
    else:
        compare_type = field_type
    return number, (name, label, compare_type)


def _parse_message_type(block: bytes, messages: dict[str, dict[int, tuple]]) -> None:
    name = None
    fields: dict[int, tuple] = {}
    nested: list[bytes] = []
    pos = 0
    while pos < len(block):
        tag, pos = _read_varint(block, pos)
        field_number, wire = tag >> 3, tag & 7
        if wire != 2:
            pos = _skip_field(block, pos, tag)
            continue
        payload, pos = _take_length_delimited(block, pos)
        if field_number == 1:
            name = _decode(payload)
        elif field_number == 2:
            number, field = _parse_field(payload)
            fields[number] = field
        elif field_number == 3:
            nested.append(payload)
    if name is None:
        raise ProtoSyncParseError("descriptor message without name")
    messages[name] = fields
    for nested_block in nested:
        _parse_message_type(nested_block, messages)


def parse_descriptor(raw: bytes) -> dict[str, dict[int, tuple]]:
    """Walk the FileDescriptorProto wire format (message_type = field 4)."""
    messages: dict[str, dict[int, tuple]] = {}
    pos = 0
    while pos < len(raw):
        tag, pos = _read_varint(raw, pos)
        field_number, wire = tag >> 3, tag & 7
        if field_number == 4 and wire == 2:
            block, pos = _take_length_delimited(raw, pos)
            _parse_message_type(block, messages)
        else:
            pos = _skip_field(raw, pos, tag)
    return messages


def _format_field(field: tuple) -> str:
    name, label, field_type = field
    return f"{name} (label={label}, type={field_type})"


def compare_messages(proto_messages: dict, descriptor_messages: dict) -> list[str]:
    """Compare both sides per message and per field number; returns failures."""
    failures: list[str] = []
    for name in sorted(set(proto_messages) | set(descriptor_messages)):
        proto_fields = proto_messages.get(name)
        descriptor_fields = descriptor_messages.get(name)
        if proto_fields is None:
            failures.append(f"message {name} present in generated descriptor but missing from audit.proto")
            continue
        if descriptor_fields is None:
            failures.append(f"message {name} present in audit.proto but missing from generated descriptor")
            continue
        for number in sorted(set(proto_fields) | set(descriptor_fields)):
            proto_field = proto_fields.get(number)
            descriptor_field = descriptor_fields.get(number)
            if proto_field is None:
                failures.append(
                    f"message {name}: field {descriptor_field[0]} ({number}) present in generated "
                    "descriptor but missing from audit.proto")
                continue
            if descriptor_field is None:
                failures.append(
                    f"message {name}: field {proto_field[0]} ({number}) present in audit.proto "
                    "but missing from generated descriptor")
                continue
            if proto_field != descriptor_field:
                failures.append(
                    f"message {name}: field '{proto_field[0]}' ({number}) mismatch: "
                    f"proto {_format_field(proto_field)} vs generated {_format_field(descriptor_field)}")
    return failures


# ---------------------------------------------------------------------------
# Version pins (R3)
# ---------------------------------------------------------------------------

def generator_versions(root: Path | None = None) -> dict[str, str]:
    """Parse the pinned versions out of the generated file headers."""
    root = Path(root) if root is not None else default_root()
    pb = (root / "api/proto/audit.pb.go").read_text(encoding="utf-8")
    grpc = (root / "api/proto/audit_grpc.pb.go").read_text(encoding="utf-8")
    versions: dict[str, str] = {}
    match = re.search(r"protoc-gen-go v([\d.]+)", pb)
    if not match:
        raise ProtoSyncParseError("audit.pb.go header missing protoc-gen-go version")
    versions["protoc-gen-go"] = "v" + match.group(1)
    match = re.search(r"protoc-gen-go-grpc v([\d.]+)", grpc)
    if not match:
        raise ProtoSyncParseError("audit_grpc.pb.go header missing protoc-gen-go-grpc version")
    versions["protoc-gen-go-grpc"] = "v" + match.group(1)
    for label, text in (("audit.pb.go", pb), ("audit_grpc.pb.go", grpc)):
        match = re.search(r"^//[ \t-]+protoc[ \t]+v([\d.]+)", text, re.M)
        if not match:
            raise ProtoSyncParseError(f"{label} header missing protoc version")
        if "protoc" in versions and versions["protoc"] != "v" + match.group(1):
            raise ProtoSyncParseError(
                f"protoc header version mismatch: {versions['protoc']} vs v{match.group(1)}")
        versions["protoc"] = "v" + match.group(1)
    return versions


def go_mod_protobuf_version(root: Path | None = None) -> str:
    root = Path(root) if root is not None else default_root()
    text = (root / "go.mod").read_text(encoding="utf-8")
    match = re.search(r"^\s*google\.golang\.org/protobuf\s+v([\d.]+)", text, re.M)
    if not match:
        raise ProtoSyncParseError("go.mod missing google.golang.org/protobuf version")
    return "v" + match.group(1)


def check_version_pins(root: Path, pins: dict[str, str]) -> list[str]:
    versions = generator_versions(root)
    failures: list[str] = []
    for key in PIN_KEYS:
        if versions[key] != pins[key]:
            failures.append(
                f"generated header {key} {versions[key]}, pinned {pins[key]} (engineering.yaml)")
    go_mod_version = go_mod_protobuf_version(root)
    if pins["protoc-gen-go"] != go_mod_version:
        failures.append(
            f"protoc-gen-go pin {pins['protoc-gen-go']} != go.mod google.golang.org/protobuf {go_mod_version}")
    return failures


# ---------------------------------------------------------------------------
# Replay (R6)
# ---------------------------------------------------------------------------

def _replay(root: Path) -> tuple[bool, str]:
    """Byte-replay via scripts/proto-gen.py --check; exit 3 is a skip."""
    script = root / "scripts" / "proto-gen.py"
    if not script.exists():
        return True, ("proto-sync: replay skipped (scripts/proto-gen.py absent; "
                      "descriptor + version checks still enforced)")
    result = subprocess.run([sys.executable, str(script), "--check"], cwd=str(root),
                            text=True, capture_output=True, check=False)
    output = (result.stdout + result.stderr).strip()
    if result.returncode == REPLAY_UNAVAILABLE:
        return True, "proto-sync: replay skipped (pinned toolchain not in bin/; descriptor + version checks still enforced)"
    if result.returncode == 0:
        return True, f"proto-sync: {output or 'replay byte-identical'}"
    return False, f"proto-sync replay: {output or f'proto-gen.py --check exited {result.returncode}'}"


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------

def envelope_fields(root: Path | None = None) -> set[str]:
    """EventEnvelope field names from the GENERATED descriptor (R4).

    Raises ProtoSyncParseError when the descriptor cannot be parsed so
    contract_fields fails rather than silently passing.
    """
    root = Path(root) if root is not None else default_root()
    text = (root / "api/proto/audit.pb.go").read_text(encoding="utf-8")
    messages = parse_descriptor(extract_raw_desc(text))
    if "EventEnvelope" not in messages:
        raise ProtoSyncParseError("descriptor missing EventEnvelope")
    return {name for name, _, _ in messages["EventEnvelope"].values()}


def run(root: Path | None = None) -> int:
    """Gate entry: descriptor comparison (R2a) + version pins (R3) + replay (R6)."""
    root = Path(root) if root is not None else default_root()
    try:
        pins = proto_pins(root)
        proto_path = root / "api/proto/audit.proto"
        pb_path = root / "api/proto/audit.pb.go"
        grpc_path = root / "api/proto/audit_grpc.pb.go"
        for path in (proto_path, pb_path, grpc_path):
            if not path.exists():
                print(f"FAIL: proto-sync: missing {path.relative_to(root)}")
                return 1
        proto_messages = parse_proto_text(proto_path.read_text(encoding="utf-8"))
        descriptor_messages = parse_descriptor(extract_raw_desc(pb_path.read_text(encoding="utf-8")))
        failures = compare_messages(proto_messages, descriptor_messages)
        if failures:
            print("FAIL: proto-sync", *failures, sep="\n  ")
            return 1
        version_failures = check_version_pins(root, pins)
        if version_failures:
            print("FAIL: proto-sync generator version", *version_failures, sep="\n  ")
            return 1
    except ProtoSyncParseError as error:
        print(f"FAIL: proto-sync parse error: {error}")
        return 1
    replay_ok, replay_message = _replay(root)
    print(replay_message)
    if not replay_ok:
        print("FAIL: proto-sync replay")
        return 1
    field_count = sum(len(fields) for fields in descriptor_messages.values())
    print(f"PASS: proto-sync (audit.proto <-> generated code: {len(descriptor_messages)} messages, "
          f"{field_count} fields; generator pins ok)")
    return 0


if __name__ == "__main__":
    sys.exit(run())
