"""AsyncAPI channel <-> Go Kafka symbol drift guard (stdlib only).

The event contract in ``api/asyncapi/asyncapi.yaml`` declares Kafka topic
channels; the Go runtime names those topics in ``internal/**`` and ``cmd/**``
as ``const Topic* = "<address>"``. Nothing previously verified that the two
trees agree: a channel added to the spec with no Go symbol (or a Go topic
constant with no declared channel) drifts silently past the engineering gate.

The check enforces both directions, fail closed (unclassifiable input goes
red, never green):

- **Rule A (spec -> Go):** every channel referenced by a ``send``/``receive``
  operation must have a Go ``const <Symbol> = "<address>"`` whose value is
  exactly the channel address (a rename on either side is drift).
- **Rule B (Go -> spec):** every dotted ``Topic*`` constant found in non-test
  Go source under ``internal/``/``cmd/`` must equal a declared channel
  address.  Rule B is symmetric completeness on purpose: it also catches
  channel removal/rename drift from the Go side.  It deliberately stops at
  declaration -- no traffic or consumer enforcement, which is the companion
  post-ledger pipeline direction's scope.

Parser contract (stdlib-only).  ``engineering.yaml`` declares the gate
dependency-free and no sibling check imports PyYAML, so the spec is parsed
with a strict YAML-subset parser that reads only the shape this spec uses:

- mapping nodes (``key: value`` and ``key:``), plain/single-/double-quoted
  scalar values, flow containers ``{...}``/``[...]`` as opaque values (``$ref``
  is pulled out of a ``channel: { $ref: '...' }`` flow map when needed), block
  scalars (``key: >-`` / ``key: |`` and friends: following more-indented lines
  are consumed unparsed — the real spec's ``channels.accepted.description: >-``
  folded scalar is the regression shape), blank lines, and ``#`` comments.

Only ``channels.<name>.address``, ``operations.<op>.action`` and
``operations.<op>.channel.$ref`` are read, but every other line must still be
classifiable -- an unsupported construct (block sequence ``- item``, tab
indentation, unbalanced flow container, unterminated quote) fails closed
instead of being silently mis-parsed.

Fail-closed conditions (FM-1 ... FM-13):

- FM-1 spec file missing; FM-2 spec unparsable or empty; FM-3 root not a
  mapping.
- FM-4 ``channels`` missing; FM-5 ``channels`` not a mapping.
- FM-6 channel without a scalar ``address``; FM-7 templated (``{``) address.
- FM-8 operation without a resolvable ``channel.$ref``; FM-9 ``$ref``
  targets an undeclared channel.
- FM-10 operation ``action`` missing or not ``send``/``receive`` (the spec
  requires the enum; a typo must not drop a channel from the subject set).
- FM-11 ``operations`` missing while channels exist.
- FM-12 a declared channel is referenced by no operation (orphan).
- FM-13 Go source unreadable during the symbol scan.

Symbol derivation (repo convention, pinned by table-driven tests): drop a
trailing ``v<N>`` segment, take the last remaining segment, use
``KNOWN_ABBREVIATIONS`` verbatim when the segment matches exactly (``dlq`` ->
``DLQ`` reproduces ``TopicDLQ``), otherwise capitalize its first character,
and prefix ``Topic``.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Derivation table: segments whose Go constant keeps an all-caps spelling.
# Deliberately explicit -- a length threshold would silently map e.g. "hold"
# to TopicHOLD and force the Go author to guess the spelling; add a row only
# when a real channel address needs it.
KNOWN_ABBREVIATIONS = {"dlq": "DLQ"}

VERSION_SUFFIX_RE = re.compile(r"^v\d+$")

# Go const forms the scanner understands.  Comment/raw-string fakes are
# masked before scanning (see _mask_go_literals); interpreted strings cannot
# span lines, so a line-anchored const regex cannot match inside one.
CONST_RE = re.compile(r'^\s*const\s+(Topic\w+)\s*=\s*"([^"]+)"', re.MULTILINE)
CONST_BLOCK_RE = re.compile(r"(?m)^\s*const\s*\((.*?)^\s*\)", re.DOTALL)
CONST_BLOCK_ENTRY_RE = re.compile(r'^\s*(Topic\w+)\s*=\s*"([^"]+)"', re.M)

# Only `$ref: '#/channels/<name>'` (flow or bare) resolves.
REF_RE = re.compile(r"\$ref:\s*(?:'([^']+)'|\"([^\"]+)\")")

GO_SOURCE_DIRS = ("internal", "cmd")


class AsyncAPIError(Exception):
    """Raised on unclassifiable input; the gate fails closed on any of these."""


def default_root() -> Path:
    return Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Strict YAML-subset parser (see module docstring for the contract)
# ---------------------------------------------------------------------------

def _line_indent(raw: str, lineno: int) -> int:
    """Leading-space count of a line; tab indentation is illegal in YAML."""
    leading = raw[:len(raw) - len(raw.lstrip())]
    if "\t" in leading:
        raise AsyncAPIError(f"line {lineno}: tab indentation is not supported")
    return len(leading)


def _next_content_line(lines: list[str], index: int) -> int | None:
    """Index of the next non-blank, non-comment line, or None at EOF."""
    while index < len(lines):
        stripped = lines[index].strip()
        if stripped and not stripped.startswith("#"):
            return index
        index += 1
    return None


def _find_key_colon(line: str, lineno: int) -> int:
    """Locate the 'key:' separator colon (outside quotes/flow containers)."""
    quote = None
    depth = 0
    for pos, char in enumerate(line):
        if quote:
            if char == quote:
                quote = None
            continue
        if char in "\"'":
            quote = char
        elif char in "[{":
            depth += 1
        elif char in "]}":
            depth -= 1
            if depth < 0:
                raise AsyncAPIError(f"line {lineno}: unbalanced flow container")
        elif char == ":" and depth == 0:
            return pos
    if quote is not None:
        raise AsyncAPIError(f"line {lineno}: unterminated quote")
    if depth:
        raise AsyncAPIError(f"line {lineno}: unbalanced flow container")
    raise AsyncAPIError(f"line {lineno}: not a 'key: value' line")


def _check_flow(value: str, lineno: int) -> str:
    """Validate a flow container is balanced; returned as an opaque value."""
    quote = None
    stack: list[str] = []
    for pos, char in enumerate(value):
        if quote:
            if char == quote:
                quote = None
            continue
        if char in "\"'" and (pos == 0 or value[pos - 1] in ":, \t[{"):
            quote = char
        elif char in "[{":
            stack.append(char)
        elif char in "]}":
            if not stack or (char == "}" and stack[-1] != "{") or (
                    char == "]" and stack[-1] != "["):
                raise AsyncAPIError(f"line {lineno}: unbalanced flow container")
            stack.pop()
    if quote is not None or stack:
        raise AsyncAPIError(f"line {lineno}: unbalanced flow container")
    return value


def _unquote(value: str, lineno: int) -> str:
    """Strip one level of single/double quotes from a scalar value."""
    quote = value[0]
    if value[-1] != quote or len(value) < 2:
        raise AsyncAPIError(f"line {lineno}: unterminated quoted value")
    body = value[1:-1]
    if quote == "'":
        return body.replace("''", "'")
    return body.replace('\\"', '"').replace("\\\\", "\\")


def _parse_scalar(raw: str, lineno: int) -> str:
    """Parse one scalar/flow value; inline comments are stripped outside
    quotes.  Quotes only open at a token boundary (start, or after
    ``: , [ {`` or whitespace), so a bare apostrophe inside a plain scalar
    (e.g. the real spec's ``payload's``) never opens one."""
    value = raw.strip()
    quote = None
    for pos, char in enumerate(value):
        if quote:
            if char == quote:
                quote = None
            continue
        if char in "\"'" and (pos == 0 or value[pos - 1] in ":, \t[{"):
            quote = char
        elif char == "#" and pos > 0 and value[pos - 1].isspace():
            value = value[:pos].rstrip()
            break
    if quote is not None:
        raise AsyncAPIError(f"line {lineno}: unterminated quote")
    if not value:
        raise AsyncAPIError(f"line {lineno}: empty value")
    if value[0] in "[{":
        return _check_flow(value, lineno)
    if value[0] in "\"'":
        return _unquote(value, lineno)
    if ": " in value:
        raise AsyncAPIError(f"line {lineno}: unsupported compact nested mapping in value")
    return value


def _split_key_value(line: str, lineno: int) -> tuple[str, object, str]:
    """Split ``key: value``; kind is "scalar", "empty", or "block"."""
    colon = _find_key_colon(line, lineno)
    key = line[:colon].strip()
    if not key:
        raise AsyncAPIError(f"line {lineno}: empty key")
    rest = line[colon + 1:].strip()
    if not rest:
        return key, None, "empty"
    if re.fullmatch(r"[>|][+-]?\d*", rest):
        return key, None, "block"
    return key, _parse_scalar(rest, lineno), "scalar"


def _skip_block_scalar(lines: list[str], index: int, key_indent: int) -> int:
    """Consume the more-indented content of a block scalar; returns next index."""
    n = len(lines)
    while index < n:
        raw = lines[index]
        if not raw.strip():
            probe = index
            while probe < n and not lines[probe].strip():
                probe += 1
            if probe >= n or _line_indent(lines[probe], probe + 1) <= key_indent:
                break
            index = probe
            continue
        if _line_indent(raw, index + 1) <= key_indent:
            break
        index += 1
    return index


def _parse_mapping(lines: list[str], index: int, indent: int) -> tuple[dict, int]:
    """Parse mapping entries at exactly ``indent``; returns (mapping, next)."""
    mapping: dict = {}
    n = len(lines)
    while index < n:
        raw = lines[index]
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            index += 1
            continue
        line_indent = _line_indent(raw, index + 1)
        if line_indent < indent:
            break
        if line_indent > indent:
            raise AsyncAPIError(
                f"line {index + 1}: unexpected indentation (expected depth {indent})")
        key, value, kind = _split_key_value(raw, index + 1)
        if key in mapping:
            raise AsyncAPIError(f"line {index + 1}: duplicate key {key!r}")
        if kind == "block":
            mapping[key] = None
            index = _skip_block_scalar(lines, index + 1, indent)
        elif kind == "empty":
            child = _next_content_line(lines, index + 1)
            if child is None or _line_indent(lines[child], child + 1) <= indent:
                mapping[key] = None
                index += 1
            else:
                mapping[key], index = _parse_mapping(
                    lines, index + 1, _line_indent(lines[child], child + 1))
        else:
            mapping[key] = value
            index += 1
    return mapping, index


def parse_spec(text: str) -> dict:
    """Parse the strict YAML subset into nested mappings; raises
    AsyncAPIError on any unclassifiable input (fail closed)."""
    lines = text.splitlines()
    index = 0
    while index < len(lines):
        stripped = lines[index].strip()
        if not stripped or stripped.startswith("#"):
            index += 1
        else:
            break
    if index >= len(lines):
        raise AsyncAPIError("empty asyncapi.yaml (no mapping root)")
    root, index = _parse_mapping(lines, index, _line_indent(lines[index], index + 1))
    if index < len(lines):
        raise AsyncAPIError(f"line {index + 1}: trailing content after root mapping")
    return root


# ---------------------------------------------------------------------------
# Channel/operation extraction (validated, fail closed)
# ---------------------------------------------------------------------------

def _resolve_channel_ref(channel_ref: object, op_name: str) -> str:
    """Resolve an operation's channel reference to a channel name.

    Accepts both spec shapes: the nested mapping form
    (``channel:\n  $ref: '#/channels/x'``, as in the real spec) and the
    inline flow form (``channel: { $ref: '#/channels/x' }``)."""
    if isinstance(channel_ref, dict):
        target = channel_ref.get("$ref")
        if not isinstance(target, str):
            raise AsyncAPIError(f"operation {op_name}: channel $ref not found")
    elif isinstance(channel_ref, str) and channel_ref:
        match = REF_RE.search(channel_ref)
        if match:
            target = match.group(1) if match.group(1) is not None else match.group(2)
        elif channel_ref.startswith("#/channels/"):
            target = channel_ref
        else:
            raise AsyncAPIError(
                f"operation {op_name}: channel $ref not found in {channel_ref!r}")
    else:
        raise AsyncAPIError(f"operation {op_name}: missing channel $ref")
    prefix, _, channel_name = target.rpartition("/")
    if prefix != "#/channels" or not channel_name:
        raise AsyncAPIError(f"operation {op_name}: unsupported $ref target {target!r}")
    return channel_name


def _extract_channels_and_operations(spec: dict) -> tuple[dict, list]:
    """Validate the top-level shape; returns (channels, operations) where
    operations is a list of (channel_name, action) pairs."""
    channels_raw = spec.get("channels")
    if not isinstance(channels_raw, dict):
        raise AsyncAPIError("channels missing or not a mapping")
    operations_raw = spec.get("operations")
    if not isinstance(operations_raw, dict):
        raise AsyncAPIError("operations missing or not a mapping")
    channels: dict[str, str] = {}
    for name, block in channels_raw.items():
        if not isinstance(block, dict):
            raise AsyncAPIError(f"channel {name}: expected a mapping")
        address = block.get("address")
        if not isinstance(address, str) or not address:
            raise AsyncAPIError(f"channel {name}: missing 'address'")
        if "{" in address:
            raise AsyncAPIError(f"channel {name}: templated address not supported: {address!r}")
        channels[name] = address
    operations: list[tuple[str, str]] = []
    referenced: set[str] = set()
    for op_name, block in operations_raw.items():
        if not isinstance(block, dict):
            raise AsyncAPIError(f"operation {op_name}: expected a mapping")
        target = _resolve_channel_ref(block.get("channel"), op_name)
        if target not in channels:
            raise AsyncAPIError(f"operation {op_name}: $ref targets undeclared channel {target!r}")
        action = block.get("action")
        if action not in ("send", "receive"):
            raise AsyncAPIError(
                f"operation {op_name}: action must be 'send' or 'receive' (got {action!r}) "
                f"for channel {target} ({channels[target]})")
        referenced.add(target)
        operations.append((target, action))
    orphans = sorted(set(channels) - referenced)
    if orphans:
        raise AsyncAPIError(f"channel(s) not referenced by any operation: {', '.join(orphans)}")
    return channels, operations


# ---------------------------------------------------------------------------
# Go symbol scan (comment/raw-string fakes masked; test files excluded)
# ---------------------------------------------------------------------------

def _mask_go_literals(text: str) -> str:
    """Blank Go comments and raw strings (keeping newlines) so a fake
    ``const Topic*`` inside a comment or backtick string can never match."""
    chars = list(text)
    n = len(text)
    i = 0
    while i < n:
        char = text[i]
        if char == "/" and i + 1 < n and text[i + 1] == "/":
            i = _blank_line_comment(chars, text, i)
        elif char == "/" and i + 1 < n and text[i + 1] == "*":
            i = _blank_block_comment(chars, text, i)
        elif char == "`":
            i = _blank_raw_string(chars, text, i)
        elif char in "\"'":
            i = _skip_quoted(chars, text, i)
        else:
            i += 1
    return "".join(chars)


def _blank_line_comment(chars: list, text: str, i: int) -> int:
    while i < len(text) and text[i] != "\n":
        chars[i] = " "
        i += 1
    return i


def _blank_block_comment(chars: list, text: str, i: int) -> int:
    n = len(text)
    i += 2
    while i + 1 < n and not (text[i] == "*" and text[i + 1] == "/"):
        if text[i] != "\n":
            chars[i] = " "
        i += 1
    if i + 1 < n:
        chars[i] = " "
        chars[i + 1] = " "
        return i + 2
    return n


def _blank_raw_string(chars: list, text: str, i: int) -> int:
    end = text.find("`", i + 1)
    if end == -1:
        end = len(text)
    else:
        end += 1
    for k in range(i, end):
        if text[k] != "\n":
            chars[k] = " "
    return end


def _skip_quoted(chars: list, text: str, i: int) -> int:
    quote = text[i]
    n = len(text)
    i += 1
    while i < n:
        if text[i] == "\\":
            i += 2
        elif text[i] == quote:
            return i + 1
        else:
            i += 1
    return n


def _scan_go_symbols(root: Path) -> dict[str, list[str]]:
    """Map Topic symbol -> declared values across internal/ and cmd/.

    Returns symbol -> [values] (all declarations) so duplicate symbols with
    conflicting values are visible to the rules; a last-wins map would mask
    a non-conforming declaration (Rule B false pass).
    """
    symbols: dict[str, list[str]] = {}
    for source_dir in GO_SOURCE_DIRS:
        base = root / source_dir
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.go")):
            if path.name.endswith("_test.go"):
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except OSError as error:
                raise AsyncAPIError(f"cannot read {path.relative_to(root)}: {error}") from error
            masked = _mask_go_literals(text)
            for match in CONST_RE.finditer(masked):
                symbols.setdefault(match.group(1), []).append(match.group(2))
            for block in CONST_BLOCK_RE.finditer(masked):
                for entry in CONST_BLOCK_ENTRY_RE.finditer(block.group(1)):
                    symbols.setdefault(entry.group(1), []).append(entry.group(2))
    return symbols


# ---------------------------------------------------------------------------
# Symbol derivation and the two alignment rules
# ---------------------------------------------------------------------------

def derive_symbol(address: str) -> str:
    """Derive the expected Go constant name for a channel address."""
    segments = address.split(".")
    if len(segments) > 1 and VERSION_SUFFIX_RE.fullmatch(segments[-1]):
        segments = segments[:-1]
    last = segments[-1]
    if not last:
        raise AsyncAPIError(f"cannot derive symbol from address {address!r}")
    if last in KNOWN_ABBREVIATIONS:
        symbol = KNOWN_ABBREVIATIONS[last]
    else:
        symbol = last[:1].upper() + last[1:]
    return "Topic" + symbol


def _rule_a(channels: dict, operations: list, symbols: dict) -> list[str]:
    """Rule A: every send/receive channel needs a matching Go constant."""
    failures: list[str] = []
    by_channel: dict[str, set] = {}
    for target, action in operations:
        by_channel.setdefault(target, set()).add(action)
    for target in sorted(by_channel):
        actions = "/".join(sorted(by_channel[target]))
        address = channels[target]
        symbol = derive_symbol(address)
        values = symbols.get(symbol)
        if values is None or address not in values:
            failures.append(
                f"FAIL: asyncapi channels: {address} ({actions}) has no matching Go symbol "
                f"{symbol} — declare const {symbol} = \"{address}\" under internal/ or cmd/")
    return failures


def _rule_b(symbols: dict, declared: set) -> list[str]:
    """Rule B: every dotted Topic* constant must be a declared channel."""
    failures: list[str] = []
    for symbol in sorted(symbols):
        for value in sorted(set(symbols[symbol])):
            if "." not in value:
                continue
            if value not in declared:
                failures.append(
                    f"FAIL: asyncapi channels: Go symbol {symbol} = \"{value}\" is not "
                    "declared as a channel in api/asyncapi/asyncapi.yaml")
    return failures


# ---------------------------------------------------------------------------
# Gate entry
# ---------------------------------------------------------------------------

def run(root: Path | None = None) -> int:
    """Gate entry: Rule A + Rule B over the spec and the Go source trees."""
    root = Path(root) if root is not None else default_root()
    spec_path = root / "api" / "asyncapi" / "asyncapi.yaml"
    if not spec_path.exists():
        print("FAIL: asyncapi channels: missing api/asyncapi/asyncapi.yaml")
        return 1
    try:
        text = spec_path.read_text(encoding="utf-8")
        spec = parse_spec(text)
        channels, operations = _extract_channels_and_operations(spec)
        symbols = _scan_go_symbols(root)
    except AsyncAPIError as error:
        print(f"FAIL: asyncapi channels: {error}")
        return 1
    except OSError as error:
        print(f"FAIL: asyncapi channels: cannot read {spec_path.relative_to(root)}: {error}")
        return 1
    failures = _rule_a(channels, operations, symbols) + _rule_b(symbols, set(channels.values()))
    if failures:
        print("\n".join(failures))
        print(f"FAIL: asyncapi channels: {len(failures)} channel/symbol drift(s)")
        return 1
    send_count = sum(1 for _, action in operations if action == "send")
    aligned = len({symbol for symbol, values in symbols.items()
                   if any(value in set(channels.values()) for value in values)})
    print(f"PASS: asyncapi channels ({send_count} send channels, {aligned} symbols aligned, "
          "undeclared topics: 0)")
    return 0


if __name__ == "__main__":
    sys.exit(run())
