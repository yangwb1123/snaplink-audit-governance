"""Keyword-based relevance scoring for meta-stage role selection."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from . import config
from .config import yaml
from .text_io import read_text_bounded


@dataclass(frozen=True)
class RoleScore:
    """A role's relevance score and the keywords that produced it."""

    role: str
    score: int
    matched: tuple[str, ...]


def _contains_cjk(value: str) -> bool:
    return bool(re.search(r"[\u3400-\u9fff]", value))


def _keyword_hit(text: str, keyword: str) -> bool:
    """CJK terms use substring matching; Latin terms use word boundaries."""
    term = keyword.strip().lower()
    if not term:
        return False
    if _contains_cjk(term):
        return term in text
    return bool(re.search(r"(?<!\w)" + re.escape(term) + r"(?!\w)", text))


def score_roles(content: str, roles: list[str], keywords: dict,
                points_per_hit: int = 2, cap: int = 10) -> list[RoleScore]:
    """Score roles by keyword overlap, preserving role order on score ties."""
    lowered = (content or "").lower()
    scored = []
    for index, role in enumerate(roles):
        terms = keywords.get(role, []) if isinstance(keywords, dict) else []
        hits = tuple(str(term) for term in terms if _keyword_hit(lowered, str(term)))
        scored.append((RoleScore(role, min(cap, len(hits) * points_per_hit), hits), index))
    scored.sort(key=lambda item: (-item[0].score, item[1]))
    return [item[0] for item in scored]


def _default_keyword_paths() -> list[Path]:
    return [Path(__file__).with_name("role_keywords.yaml")]


def load_role_keywords(path: str = "") -> dict:
    """Load a role keyword map; malformed or absent files disable scoring."""
    if not yaml:
        return {}
    candidates = [Path(path)] if path else _default_keyword_paths()
    for candidate in candidates:
        try:
            data = yaml.safe_load(read_text_bounded(
                candidate, config.INPUT_MAX_BYTES, "role keyword file")) or {}
        except Exception:  # absent, unreadable, or malformed YAML disables advisory scoring
            continue
        if isinstance(data, dict):
            return {str(role): list(terms) for role, terms in data.items()
                    if isinstance(terms, list)}
    return {}


def format_suggestions(scores: list[RoleScore], minimum: int = 1) -> str:
    """Render positive role scores for insertion into the meta prompt."""
    relevant = [item for item in scores if item.score >= minimum]
    if not relevant:
        return ""
    lines = ["Relevance suggestions (keyword evidence; prefer higher scores):"]
    for item in relevant:
        matched = ", ".join(item.matched)
        lines.append(f"- {item.role}: {item.score} (matched: {matched})")
    return "\n".join(lines)


def constrain_role_plan(plan: list[dict], limit: int, scores: list[RoleScore],
                        minimum: int = 0) -> tuple[list[dict], list[str]]:
    """Dedupe, relevance-filter named roles, and enforce the fan-out cap."""
    score_map = {item.role: item.score for item in scores}
    kept = []
    dropped = []
    seen = set()
    for item in plan:
        role = str(item.get("role", "")).strip()
        key = role.casefold()
        if not role or key in seen:
            dropped.append(role or "(empty)")
            continue
        seen.add(key)
        is_ad_hoc = bool(str(item.get("task", "")).strip())
        if minimum > 0 and not is_ad_hoc and score_map.get(role, 0) < minimum:
            dropped.append(role)
            continue
        if len(kept) >= max(1, limit):
            dropped.append(role)
            continue
        kept.append(item)
    return kept, dropped
