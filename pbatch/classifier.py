"""Task type classifier: a deterministic, bilingual keyword gate that runs
BEFORE execution and decides whether a task/batch is frontend UI work, so
the CLI can route it to the UI generation pipeline (--classify flag or the
`pi-batch.py classify` subcommand).

Scoring mirrors pbatch.relevance: CJK terms match by substring, Latin terms
by word boundaries, 2 points per hit, capped at 10 per type. A platform
hit (tsx/dart/vue/react-native) adds a +2 boost to frontend_ui because
"implement this page in flutter" is overwhelmingly a UI task even when the
rest of the sentence is business wording. Profile hits (erp/cms/oa/
dashboard/immersive/marketing/mobile) add +1 each (cap +4): "a marketing
landing page" is a UI signal even with no generic UI word.

Zero-score input classifies as unknown (never frontend on a tie). Routing
is a hint, not a gate: a false positive just adds spec/review stages via
the frontend pipeline; an explicit --pipeline always wins.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from . import config
from .config import log, yaml
from .relevance import _keyword_hit
from .text_io import read_text_bounded

UNKNOWN = "unknown"
FRONTEND = "frontend_ui"
PLATFORM_BOOST = 2

# Built-in defaults so the classifier works standalone (no YAML, no PyYAML);
# pbatch/task_keywords.yaml overrides sections wholesale when present.
_DEFAULT_KEYWORDS: dict = {
    "task_types": {
        "frontend_ui": [
            "page", "ui", "layout", "component", "spacing", "widget", "screen",
            "tsx", "dart", "flutter", "react", "vue", "css", "styled",
            "admin", "页面", "前端", "界面", "后台", "管理后台", "布局", "组件", "间距", "按钮", "表单",            "弹窗", "卡片", "导航", "侧边栏", "表格", "分页",
            "登录页", "列表页", "详情页", "设计稿", "视觉",
        ],
        "backend": [
            "api", "server", "endpoint", "database", "schema", "service",
            "后端", "接口", "服务端", "数据库", "表结构",
            "微服务", "分布式", "领域模型", "实体", "聚合", "事务",
            "消息队列", "对账", "库存", "工作流",
            "domain", "entity", "workflow", "transaction",
        ],
        "data": ["sql", "etl", "数据管道", "报表", "指标", "数据仓库"],
        "code_engineering": [
            "重构", "维护", "修复", "优化", "清理", "重构代码", "技术债",
            "refactor", "maintain", "fix", "optimize", "cleanup", "refactoring",
        ],
        "docs": ["documentation", "spec", "readme", "文档", "需求", "规范", "说明", "手册"],
        "devops": ["ci/cd", "deploy", "docker", "kubernetes", "部署", "运维", "发布", "流水线"],
        "analysis": ["review", "audit", "分析", "审查", "评审", "评估", "调研"],
    },
    "platforms": {
        "tsx": ["tsx", "react", "typescript", "antd", "前端组件"],
        "dart": ["dart", "flutter", "widget", "sizedbox", "material"],
        "vue": ["vue", "nuxt", "template"],
        "rn": ["react native", "rn"],
    },
    "profiles": {
        "erp": ["erp", "mes", "排程", "库存", "采购单", "工单", "单据"],
        "cms": ["cms", "内容管理", "文章", "审核", "发布"],
        "oa": ["oa", "审批", "待办", "流程", "通知公告"],
        "dashboard": ["dashboard", "大屏", "图表", "数据可视化", "看板"],
        "immersive": ["特效", "3d", "沉浸", "动画", "滚动叙事", "粒子"],
        "marketing": ["官网", "落地页", "营销", "转化", "landing"],
        "mobile": ["移动端", "app", "mobile"],
    },
}


@dataclass(frozen=True)
class TaskClassification:
    """One task's classification: dominant task type plus frontend
    platform/profile detail and the keyword evidence behind the score."""

    task_type: str
    score: int
    matched: tuple = ()
    platform: str = UNKNOWN
    profile: str = UNKNOWN
    confident: bool = False

    def to_dict(self) -> dict:
        return {
            "task_type": self.task_type,
            "score": self.score,
            "matched": list(self.matched),
            "platform": self.platform,
            "profile": self.profile,
            "confident": self.confident,
        }


def _default_keyword_paths() -> list[Path]:
    return [Path(__file__).with_name("task_keywords.yaml")]


def load_keywords(path: str = "") -> dict:
    """Load the keyword map: YAML entries are ADDITIVE to the built-in
    defaults (per type list, deduped) so projects can extend vocabulary
    without rewriting the full map; malformed/absent files degrade to
    defaults."""
    merged = {section: {name: list(terms) for name, terms in section_map.items()}
              for section, section_map in _DEFAULT_KEYWORDS.items()}
    if not yaml:
        return merged
    candidates = [Path(path)] if path else _default_keyword_paths()
    for candidate in candidates:
        try:
            data = yaml.safe_load(read_text_bounded(
                candidate, config.INPUT_MAX_BYTES, "task keyword file")) or {}
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        for section, section_map in data.items():
            if section not in merged or not isinstance(section_map, dict):
                continue
            for name, terms in section_map.items():
                if not isinstance(terms, list):
                    continue
                extra = [str(term) for term in terms if str(term) not in merged[section].get(name, [])]
                merged[section].setdefault(name, []).extend(extra)
    return merged


def _best_match(text: str, section: dict, lowered: str = "") -> tuple[str, tuple]:
    """Best-scoring entry in a keyword section (platform/profile maps)."""
    best, best_hits = UNKNOWN, ()
    for name, terms in section.items():
        hits = tuple(str(term) for term in terms if _keyword_hit(lowered, str(term)))
        if len(hits) > len(best_hits):
            best, best_hits = name, hits
    return best, best_hits


def _frontend_boost(platform: str, platform_hits: tuple,
                    profile: str, profile_hits: tuple) -> int:
    """Evidence boost for the frontend type: a platform mention (tsx/dart/
    flutter/vue/rn, +2) and profile hits (erp/cms/oa/..., +1 each, cap +4)."""
    boost = 0
    if platform != UNKNOWN and platform_hits:
        boost += PLATFORM_BOOST
    if profile != UNKNOWN and profile_hits:
        boost += min(4, len(profile_hits))
    return boost


def classify_text(text: str, keywords: Optional[dict] = None) -> TaskClassification:
    """Classify one prompt/task text into a task type (with frontend
    platform/profile detail). confident = top score >= the configured
    minimum (default 2, i.e. one strong keyword hit)."""
    kw = keywords or load_keywords()
    lowered = (text or "").lower()
    types = kw.get("task_types", {})
    platforms = kw.get("platforms", {})
    profiles = kw.get("profiles", {})

    scored = []
    for name, terms in types.items():
        hits = tuple(str(term) for term in terms if _keyword_hit(lowered, str(term)))
        scored.append((name, min(10, len(hits) * 2), hits))
    # Platform mention is strong frontend evidence: "implement in flutter".
    platform, platform_hits = _best_match(lowered, platforms, lowered)
    # Profile mention is UI evidence too: "a marketing landing page".
    profile, profile_hits = _best_match(lowered, profiles, lowered)
    boost = _frontend_boost(platform, platform_hits, profile, profile_hits)
    if boost:
        scored = [(n, s + (boost if n == FRONTEND else 0), h)
                  for n, s, h in scored]
    scored.sort(key=lambda item: (-item[1], 0 if item[0] == FRONTEND else 1, item[0]))
    best_type, best_score, best_hits = scored[0]
    if best_score <= 0:
        return TaskClassification(UNKNOWN, 0)

    matched = best_hits
    if best_type == FRONTEND:
        # Evidence transparency: include the platform/profile hits that
        # contributed the frontend boost.
        matched = best_hits + platform_hits + profile_hits
    return TaskClassification(
        task_type=best_type,
        score=best_score,
        matched=matched,
        platform=platform if best_type == FRONTEND else UNKNOWN,
        profile=profile if best_type == FRONTEND else UNKNOWN,
        confident=best_score >= config.CLASSIFIER_MIN_SCORE,
    )


def classify_tasks(tasks: list, keywords: Optional[dict] = None) -> tuple:
    """Classify a whole batch; returns (dominant, per_task). The dominant
    type is the majority of per-task top types (frontend_ui wins ties so a
    mixed batch errs toward the spec/review pipeline)."""
    per_task = []
    for t in tasks:
        if isinstance(t, TaskClassification):
            per_task.append(t)
        else:
            per_task.append(classify_text(getattr(t, "prompt", "") or "", keywords))
    counts: dict = {}
    for item in per_task:
        counts[item.task_type] = counts.get(item.task_type, 0) + 1
    best_count = max(counts.values()) if counts else 0
    tied = sorted(name for name, count in counts.items() if count == best_count)
    dominant_type = tied[0] if len(tied) == 1 else FRONTEND
    dominant = next((item for item in per_task if item.task_type == dominant_type),
                    per_task[0] if per_task else TaskClassification(UNKNOWN, 0))
    return dominant, per_task


def should_route_frontend(per_task: list, min_ratio: float = 0.5) -> bool:
    """Frontend routing decision: at least min_ratio of the batch's
    CONFIDENT top types are frontend_ui (a zero-score best guess must not
    divert a task into the UI pipeline)."""
    if not per_task:
        return False
    frontend = sum(1 for item in per_task
                   if item.task_type == FRONTEND and item.confident)
    return frontend / len(per_task) >= min_ratio


def format_classification(item: TaskClassification, index: int = 0) -> str:
    """One human-readable classification line with evidence."""
    label = f"task {index}: " if index else ""
    detail = ""
    if item.task_type == FRONTEND and (item.platform != UNKNOWN or item.profile != UNKNOWN):
        detail = " [%s/%s]" % (item.platform, item.profile)
    evidence = ", ".join(item.matched) or "-"
    flag = "confident" if item.confident else "best-guess"
    return f"{label}{item.task_type}{detail} (score {item.score}, {flag}; matched: {evidence})"


def _texts_from_file(src: Path) -> list:
    """Extract prompts from a YAML/JSON task file (tasks list) or treat
    the whole file as one prompt text."""
    text = read_text_bounded(src, config.INPUT_MAX_BYTES, "classify source")
    if yaml and src.suffix in (".yaml", ".yml"):
        try:
            data = yaml.safe_load(text) or {}
        except Exception:
            data = {}
        prompts = _task_prompts(data)
        if prompts:
            return prompts
    if src.suffix == ".json":
        try:
            prompts = _task_prompts(json.loads(text))
        except Exception:
            prompts = []
        if prompts:
            return prompts
    return [text]


def _task_prompts(data) -> list:
    """Prompts from a parsed task-file payload ({'tasks': [{prompt: ...}]})."""
    if not isinstance(data, dict):
        return []
    tasks = data.get("tasks", [])
    if not isinstance(tasks, list):
        return []
    return [str(t.get("prompt", "")) for t in tasks if isinstance(t, dict) and t.get("prompt")]


def _texts_from_dir(directory: Path, suffix: str) -> list:
    texts = []
    for p in sorted(directory.glob("*" + (suffix or ".md"))):
        try:
            texts.append(read_text_bounded(p, config.INPUT_MAX_BYTES, "classify source"))
        except Exception as exc:
            log.error("Cannot read %s: %s", p, exc)
    return texts


def _collect_texts(args) -> list[str]:
    """Gather prompt texts from a subcommand invocation without executing."""
    if args.prompt:
        return [args.prompt]
    if args.from_dir:
        return _texts_from_dir(Path(args.from_dir), args.suffix)
    if args.source:
        src = Path(args.source)
        if not src.exists():
            # Not a file on disk: treat as an inline prompt unless it looks
            # like a path (slash, dot-prefix, or a known file extension).
            looks_like_path = (" " not in args.source
                               and ("/" in args.source or "\\" in args.source
                                   or args.source.startswith(".")
                                   or Path(args.source).suffix in (".md", ".txt", ".yaml", ".yml", ".json")))
            if not looks_like_path:
                return [args.source]
            log.error("File not found: %s", src)
            sys.exit(1)
        return _texts_from_file(src)
    return []


def classify_main(argv: list) -> None:
    """`pi-batch.py classify [prompt|tasks.yaml|--from-dir DIR] [--json]`
    — print the classification and routing decision without executing."""
    import argparse
    parser = argparse.ArgumentParser(
        prog="pi-batch.py classify",
        description="Classify task(s) by type before execution (frontend UI detection).")
    parser.add_argument("source", nargs="?", help="prompt text, or a YAML/JSON/txt task file path")
    parser.add_argument("-p", "--prompt", help="inline prompt")
    parser.add_argument("--from-dir", dest="from_dir", help="classify every file in DIR")
    parser.add_argument("--suffix", default=".md", help="file suffix for --from-dir (default: .md)")
    parser.add_argument("--json", action="store_true", help="machine-readable JSON output")
    parser.add_argument("--index", default="", help="task keywords YAML override")
    args = parser.parse_args(argv)
    texts = _collect_texts(args)
    if not texts:
        parser.error("Provide a prompt (-p), a task file path, or --from-dir")
    keywords = load_keywords(args.index)
    per_task = [classify_text(text, keywords) for text in texts]
    dominant = classify_tasks(per_task, keywords)[0]
    route = should_route_frontend(per_task, config.CLASSIFIER_FRONTEND_RATIO)
    if args.json:
        payload = {
            "dominant": dominant.to_dict(),
            "route_frontend": route,
            "frontend_pipeline": config.CLASSIFIER_FRONTEND_PIPELINE if route else "",
            "tasks": [item.to_dict() for item in per_task],
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    for index, item in enumerate(per_task, 1):
        print(format_classification(item, index))
    print(f"dominant: {dominant.task_type}; route to frontend pipeline: {route}")
