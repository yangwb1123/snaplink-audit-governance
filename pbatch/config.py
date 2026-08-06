"""Declarative configuration for pi-batch: pi-batch.yaml resolution,
agent defaults, session flags, and the named validator registry."""

from __future__ import annotations

import logging
import os
import re
import sys
from functools import lru_cache
from pathlib import Path
from typing import NamedTuple, Optional

try:
    import yaml
except ImportError:
    yaml = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pi-batch")

def _find_batch_config() -> Optional[dict]:
    """Locate and parse pi-batch.yaml: the entry script directory
    (PBATCH_SCRIPT_DIR, set by the pi-batch.py shim) wins, then the package
    directory, then the process working directory. None when absent."""
    if not yaml:
        return None
    candidates = []
    script_dir = os.environ.get("PBATCH_SCRIPT_DIR", "")
    if script_dir:
        candidates.append(Path(script_dir) / "pi-batch.yaml")
    candidates.append(Path(__file__).resolve().parent / "pi-batch.yaml")
    candidates.append(Path("pi-batch.yaml"))
    for p in candidates:
        if not p.exists():
            continue
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        if isinstance(data, dict):
            return data
    return None


def _load_batch_config(path: str = "pi-batch.yaml") -> dict:
    """Optional defaults for pi-batch. Missing file -> {} (built-in defaults
    below apply), so the tool still runs standalone with zero config -- copy
    pi-batch.yaml alongside pi-batch.py to point it at a different agent
    CLI."""
    data = _find_batch_config()
    return data if data is not None else {}


_BATCH_CFG = _load_batch_config()


def _section(name: str) -> dict:
    """Return a mapping config section, falling back on malformed input."""
    value = _BATCH_CFG.get(name, {})
    if isinstance(value, dict):
        return value
    log.warning("Config section '%s' must be a mapping; using defaults", name)
    return {}


def _int_setting(section: dict, key: str, default: int,
                 minimum: Optional[int] = None) -> int:
    """Read an integer without letting YAML type/value mistakes abort import."""
    value = section.get(key, default)
    try:
        if isinstance(value, bool):
            raise ValueError
        parsed = int(value)
    except (TypeError, ValueError):
        log.warning("Config value '%s' must be an integer; using %d", key, default)
        return default
    if minimum is not None and parsed < minimum:
        log.warning("Config value '%s' must be >= %d; using %d", key, minimum, default)
        return default
    return parsed


def _choice_setting(section: dict, key: str, default: str,
                    choices: tuple[str, ...]) -> str:
    """Normalize a small string enum, with a usable argparse-safe default."""
    value = str(section.get(key, default)).lower()
    if value in choices:
        return value
    log.warning("Config value '%s' must be one of %s; using %s",
                key, ", ".join(choices), default)
    return default


def _string_setting(section: dict, key: str, default: str,
                    allow_empty: bool = True) -> str:
    """Read a scalar string and reject YAML null/collection surprises."""
    value = section.get(key, default)
    if isinstance(value, str) and (allow_empty or value):
        return value
    kind = "a string" if allow_empty else "a non-empty string"
    log.warning("Config value '%s' must be %s; using %s", key, kind, default)
    return default


def _float_setting(section: dict, key: str, default: float,
                   minimum: Optional[float] = None,
                   maximum: Optional[float] = None) -> float:
    """Read a ratio-like float without letting YAML surprises abort import."""
    value = section.get(key, default)
    try:
        if isinstance(value, bool):
            raise ValueError
        parsed = float(value)
    except (TypeError, ValueError):
        log.warning("Config value '%s' must be a number; using %s", key, default)
        return default
    if (minimum is not None and parsed < minimum) or (maximum is not None and parsed > maximum):
        log.warning("Config value '%s' must be within [%s, %s]; using %s",
                    key, minimum, maximum, default)
        return default
    return parsed


_AGENT_CFG = _section("agent")
# Optional per-agent override sections: agent.agents.<bin>.* win over the
# global agent.* settings for that binary (G line: claude/codex/gemini have
# different failure signatures and session-flag syntax).
_AGENTS_CFG = _AGENT_CFG.get("agents")
if not isinstance(_AGENTS_CFG, dict):
    _AGENTS_CFG = {}


def _agent_section(bin_name: str) -> dict:
    """Per-agent config section for a binary name (agent.agents.<bin>),
    or {} when the section is absent/malformed."""
    if not bin_name:
        return {}
    section = _AGENTS_CFG.get(bin_name)
    return section if isinstance(section, dict) else {}


# Default provider/CLI failure signatures (G line). Per-agent overrides:
#   agent.error_patterns: [...]      (all agents)
#   agent.agents.<bin>.error_patterns: [...]   (one binary)
# Empty list disables signature rejection for that agent.
_DEFAULT_ERROR_PATTERNS = (
    # provider error codes (quota, rate limit, billing, auth)
    r"rate_?limit_?error",
    r"insufficient_?quota",
    r"quota_?exceeded",
    r"credit_?balance_?too_?low",
    r"insufficient\s+(?:balance|funds)",
    r"billing_?error",
    r"payment_?required",
    r"invalid_?api_?key",
    r"authentication_?error",
    r"context_?length_?exceeded",
    r"overloaded_?error",
    r"429 too many requests",
    # network connectivity failures (offline, DNS, TLS, proxy)
    r"network is unreachable",
    r"no route to host",
    r"temporary failure in name resolution",
    r"name or service not known",
    r"dns resolution failed",
    r"getaddrinfo",
    r"max retries exceeded",
    r"certificate verify failed",
    r"connectionerror",
    r"sslerror",
    r"proxyerror",
    r"econnrefused",
    r"econnreset",
    r"etimedout",
    r"curl: \(\d+\)",
    r"connection timed out",
    r"connect timed out",
    r"operation timed out",
    r"connection refused",
    r"connection reset",
    r"connection closed",
    r"broken pipe",
    r"failed to connect",
    r"unable to connect",
    r"could not connect",
    # CLI-level failure banners (inline (?im) adds MULTILINE to IGNORECASE)
    r"(?im)^\[?error\]?:",
    r"(?im)^fatal:",
)


@lru_cache(maxsize=16)
def _compile_patterns(patterns: tuple) -> tuple:
    return tuple(re.compile(pattern, re.IGNORECASE) for pattern in patterns)


def agent_error_patterns(bin_name: str = "") -> tuple:
    """Failure-signature patterns for the current agent binary: per-agent
    override (agent.agents.<bin>.error_patterns) > global override
    (agent.error_patterns) > built-in defaults. Reads config.AGENT_BIN at
    call time so a --agent-bin CLI override is honored."""
    name = bin_name or AGENT_BIN
    raw = _agent_section(name).get("error_patterns")
    if raw is None:
        raw = _AGENT_CFG.get("error_patterns")
    if raw is None:
        raw = _DEFAULT_ERROR_PATTERNS
    if isinstance(raw, str):
        raw = (raw,)
    if not isinstance(raw, (list, tuple)):
        log.warning("Config 'error_patterns' must be a string list; using defaults")
        raw = _DEFAULT_ERROR_PATTERNS
    patterns = tuple(str(p) for p in raw if str(p).strip())
    if not patterns:
        # explicit empty list disables signature rejection for this agent
        return ()
    return _compile_patterns(patterns)
_COMMIT_CFG = _section("commit")
_LOGGING_CFG = _section("logging")
_SESSION_CFG = _section("session")
_OUTPUT_CFG = _section("output")
_INPUT_CFG = _section("input")
_EVIDENCE_CFG = _section("evidence")
_PROMPT_CFG = _section("prompt")
_VALIDATION_CFG = _section("validation")
_COMMAND_CFG = _section("commands")
_MEMORY_CFG = _section("memory")
_CLASSIFIER_CFG = _section("classifier")
_LIMITS_CFG = _section("limits")


AGENT_BIN = _string_setting(_AGENT_CFG, "bin", "pi", allow_empty=False)


AGENT_DEFAULT_MODEL = _string_setting(_AGENT_CFG, "default_model", "")


AGENT_DEFAULT_TIMEOUT = _int_setting(_AGENT_CFG, "default_timeout", 900, 1)


AGENT_DEFAULT_WORKERS = _int_setting(_AGENT_CFG, "default_workers", 4, 1)


COMMIT_PREFIX_DEFAULT = _string_setting(_COMMIT_CFG, "prefix", "[pi-batch]")

# T3 (7x24 governance): log rotation thresholds; --log-max-bytes /
# --log-backups override per run. 5MB x 3 backups bounds a 24x7 log.
LOG_MAX_BYTES = _int_setting(_LOGGING_CFG, "max_bytes", 5 * 1024 * 1024, 0)
LOG_BACKUP_COUNT = _int_setting(_LOGGING_CFG, "backups", 3, 0)
# Model response bodies are artifacts/messages, not operational log records.
# auto streams them only to an interactive terminal; full/none are explicit
# overrides. A task without an output path still prints its final answer once.
STREAM_OUTPUT = _choice_setting(
    _LOGGING_CFG, "stream_output", "auto", ("auto", "full", "none"))

# T10 (7x24 governance): rotate (fork) the shared session past this size.
SESSION_MAX_BYTES = _int_setting(_SESSION_CFG, "max_bytes", 2 * 1024 * 1024, 1)
# Pi stores one JSON object per line. Normal assistant messages may approach
# OUTPUT_MAX_BYTES, so allow headroom for JSON escaping while refusing a
# pathological line before metering/compaction scans allocate it wholesale.
SESSION_LINE_MAX_BYTES = _int_setting(
    _SESSION_CFG, "line_max_bytes", 4 * 1024 * 1024, 1)

# T12e (governance): reject agent outputs above this size (default 2MB).
OUTPUT_MAX_BYTES = _int_setting(_OUTPUT_CFG, "max_bytes", 2 * 1024 * 1024, 1)

# External prompt/task/template files are trusted repository inputs, but a
# malformed or accidentally generated multi-gigabyte file must not be read
# wholesale before the prompt-size gate can reject it.  Keep this separate
# from OUTPUT_MAX_BYTES so operators can tune input and model-response
# budgets independently.
INPUT_MAX_BYTES = _int_setting(_INPUT_CFG, "max_bytes", 2 * 1024 * 1024, 1)

# Bound the total artifact content injected into any aggregate/meta prompt.
# Full artifacts stay on disk and truncation markers point back to them.
EVIDENCE_MAX_BYTES = _int_setting(_EVIDENCE_CFG, "max_bytes", 64 * 1024, 1)
# Bound path traversal and fence/manifest overhead independently of byte size.
EVIDENCE_MAX_SOURCES = _int_setting(_EVIDENCE_CFG, "max_sources", 64, 1)

# Agent prompts are passed as one `-p` argv value. Stay below Linux's usual
# 128KiB per-argument ceiling and reject predictably before subprocess spawn.
PROMPT_MAX_BYTES = _int_setting(_PROMPT_CFG, "max_bytes", 120 * 1024, 0)

# Validator diagnostics are useful only in small tails; cap both pipes so a
# noisy external gate cannot OOM the coordinator before its timeout.
VALIDATION_OUTPUT_MAX_BYTES = _int_setting(
    _VALIDATION_CFG, "output_max_bytes", 256 * 1024, 1)

# Post-stage hooks are repository-wide gates and may legitimately run longer
# than per-artifact validators. They still need the same bounded process-tree
# semantics so one noisy or orphan-producing hook cannot wedge a 7x24 run.
COMMAND_TIMEOUT = _int_setting(_COMMAND_CFG, "timeout", 600, 1)
COMMAND_OUTPUT_MAX_BYTES = _int_setting(
    _COMMAND_CFG, "output_max_bytes", 256 * 1024, 1)

# Progressive memory: preserve pi's raw JSONL sessions, while exposing only
# a small metadata manifest to the model unless it elects to read more.
MEMORY_MODE = _choice_setting(_MEMORY_CFG, "mode", "auto", ("auto", "on", "off"))
_MEMORY_AGENT_VALUE = _MEMORY_CFG.get("agent_names", ("pi",))
if isinstance(_MEMORY_AGENT_VALUE, str):
    _MEMORY_AGENT_VALUE = (_MEMORY_AGENT_VALUE,)
elif not isinstance(_MEMORY_AGENT_VALUE, (list, tuple)):
    log.warning("Config value 'agent_names' must be a string or list; using pi")
    _MEMORY_AGENT_VALUE = ("pi",)
MEMORY_AGENT_NAMES = tuple(str(value) for value in _MEMORY_AGENT_VALUE)
MEMORY_INDEX_FILE = _string_setting(
    _MEMORY_CFG, "index_file", ".pi-batch/memory/sessions.index.jsonl", allow_empty=False)
MEMORY_MANIFEST_RECENT = _int_setting(_MEMORY_CFG, "manifest_recent", 3, 0)
MEMORY_MANIFEST_SCAN = _int_setting(_MEMORY_CFG, "manifest_scan", 30, 1)
MEMORY_MANIFEST_MAX_BYTES = _int_setting(_MEMORY_CFG, "manifest_max_bytes", 4096, 1)
MEMORY_READ_MAX_BYTES = _int_setting(_MEMORY_CFG, "read_max_bytes", 65536, 1)
MEMORY_INDEX_LINE_MAX_BYTES = _int_setting(
    _MEMORY_CFG, "index_line_max_bytes", 256 * 1024, 1)

# F line (round-5 F): provider rate limiting. A process-wide token bucket
# serializes agent invocations across parallel workers / meta role fan-out;
# --min-interval only throttles successful SERIAL tasks. 0 = unlimited.
RATE_LIMIT_PER_SECOND = _float_setting(_LIMITS_CFG, "per_second", 0.0, 0.0)
RATE_LIMIT_BURST = _float_setting(_LIMITS_CFG, "burst", 1.0, 0.0)
_LIMIT_PROVIDERS = _LIMITS_CFG.get("providers")
if not isinstance(_LIMIT_PROVIDERS, dict):
    _LIMIT_PROVIDERS = {}
RATE_LIMIT_PROVIDERS = {
    str(key): value for key, value in _LIMIT_PROVIDERS.items()
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0
}


# Task type classifier (pbatch/classifier.py): deterministic keyword gate
# that runs BEFORE execution and routes frontend UI tasks to the UI
# generation pipeline (--classify / `pi-batch.py classify`).
CLASSIFIER_KEYWORDS = _string_setting(_CLASSIFIER_CFG, "keywords", "")
CLASSIFIER_FRONTEND_PIPELINE = _string_setting(
    _CLASSIFIER_CFG, "frontend_pipeline",
    "examples/frontend-implementation-pipeline.yaml", allow_empty=False)
CLASSIFIER_BACKEND_PIPELINE = _string_setting(
    _CLASSIFIER_CFG, "backend_pipeline",
    "examples/backend-implementation-pipeline.yaml", allow_empty=False)
# Minimum top score for a confident classification (2 = one strong hit).
CLASSIFIER_MIN_SCORE = _int_setting(_CLASSIFIER_CFG, "min_score", 2, 1)
# Route when this share of a batch's tasks classify as frontend UI.
CLASSIFIER_FRONTEND_RATIO = _float_setting(
    _CLASSIFIER_CFG, "frontend_ratio", 0.5, 0.0, 1.0)


_DEFAULT_SESSION_FLAGS = {
    "start": ["--session-id", "{session}", "--name", "{name}"],
    "continue": ["--session-id", "{session}"],
    "fork": ["--fork", "{session}"],
}


def agent_session_flags(key: str, session_id: str, session_name: str,
                        bin_name: str = "") -> list:
    """Resolve session flags for a call (start/continue/fork), replacing
    {session} and {name} placeholders. Per-agent override
    (agent.agents.<bin>.session_flags) wins over agent.session_flags,
    then pi-style defaults — so a non-pi agent CLI can point these at its
    own continue-session flags without touching shared config (G line)."""
    configured = _agent_section(bin_name or AGENT_BIN).get("session_flags")
    if configured is None:
        configured = _AGENT_CFG.get("session_flags")
    cfg = configured if isinstance(configured, dict) else {}
    flags = cfg.get(key)
    if (not isinstance(flags, (list, tuple)) or not flags
            or not all(isinstance(flag, str) for flag in flags)):
        if flags is not None:
            log.warning("Config session_flags.%s must be a string list; using defaults", key)
        flags = _DEFAULT_SESSION_FLAGS[key]
    return [f.replace("{session}", session_id).replace("{name}", session_name) for f in flags]


def _session_flags(key: str, session_id: str, session_name: str) -> list:
    """Backward-compatible alias resolving flags for the configured bin."""
    return agent_session_flags(key, session_id, session_name)


def _load_validators() -> dict:
    """Read the named validators registry from pi-batch.yaml (like the
    project's engineering.yaml declares gates for cli.py); empty when absent
    so the script stays portable."""
    data = _find_batch_config()
    if not data:
        return {}
    v = data.get("validators")
    return dict(v) if isinstance(v, dict) else {}


VALIDATORS = _load_validators()


class ValidatorSpec(NamedTuple):
    """One resolved validator entry.

    cmd:   shell command ({output}/{cwd} placeholders)
    judge: when True, the gate parses a VERDICT: PASS|FAIL - <reason> line
           from stdout (fail closed on missing/bare verdict) instead of
           trusting the exit code alone — LLM-as-judge protocol (H line)
    scope: "file" = run per artifact before it is committed (default);
           "repo" = full-tree gate deferred to stage end / batch end so it
           never races concurrent role agents writing the tree (N2)
    """
    cmd: str
    judge: bool = False
    scope: str = "file"


def _validator_spec(entry) -> Optional[ValidatorSpec]:
    """Normalize one registry entry: a string command, or a mapping with
    cmd/judge/scope keys. Malformed entries fall back to a raw command so a
    typo fails deterministically at execution, not at import."""
    if isinstance(entry, str):
        return ValidatorSpec(cmd=entry)
    if isinstance(entry, dict):
        cmd = entry.get("cmd")
        if isinstance(cmd, str) and cmd.strip():
            judge = bool(entry.get("judge", False))
            scope = str(entry.get("scope", "file")).lower()
            if scope not in ("file", "repo"):
                log.warning("Validator scope '%s' must be file or repo; using file", scope)
                scope = "file"
            return ValidatorSpec(cmd=cmd.strip(), judge=judge, scope=scope)
    log.warning("Validator entry must be a command string or a {cmd, judge, scope} "
                "mapping; skipping %r", entry)
    return None


VALIDATOR_SPECS: dict = {name: spec for name, entry in VALIDATORS.items()
                         if (spec := _validator_spec(entry)) is not None}


def _resolve_validator_specs(value: str) -> list:
    """Expand a comma-separated list into ValidatorSpecs: registry names are
    replaced by their pi-batch.yaml spec (cmd/judge/scope), anything else is
    used as a raw file-scoped shell command. Empty value -> no validation."""
    out = []
    for item in [x.strip() for x in (value or "").split(",") if x.strip()]:
        spec = VALIDATOR_SPECS.get(item)
        out.append(spec if spec is not None else ValidatorSpec(cmd=item))
    return out


def _resolve_validators(value: str) -> list:
    """Backward-compatible alias: command strings only (judge/scope ignored)."""
    return [spec.cmd for spec in _resolve_validator_specs(value)]
