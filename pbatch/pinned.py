"""Pinned context (AADM-G §19): 不可压缩上下文分层.

Pinned：安全规则、用户硬约束、公共接口契约、关键验收标准、禁止状态——
**不允许被摘要丢失**。机制：`.pi-batch/pinned.md`（或配置的额外文件）
中的内容总是完整注入；Retrievable 可摘要但保留来源指针；Ephemeral 完成
后丢弃。

摘要失效机制：跟踪源文件 mtime+size 指纹——被摘要的文档变化时，摘要缓存
必须作废并重新生成（否则 Agent 拿着过期摘要当真相）。

Usage:
    pi-batch pinned [--json]          # 渲染 Pinned 块 + 指纹有效性
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Optional

PINNED_DEFAULT = Path(".pi-batch") / "pinned.md"


def pinned_sources(cwd: str = "", extra_files: Optional[list] = None) -> list:
    """Pinned 源文件：默认 .pi-batch/pinned.md + 额外文件（存在的才返回）。"""
    root = Path(cwd or ".")
    candidates = [root / PINNED_DEFAULT]
    candidates += [Path(path) if Path(path).is_absolute() else root / path
                   for path in (extra_files or [])]
    return [str(path) for path in candidates if path.exists()]


def fingerprint_sources(sources: list) -> str:
    """源文件指纹（mtime+size）：摘要失效判据。"""
    digest = hashlib.sha256()
    for source in sorted(sources):
        try:
            stat = Path(source).stat()
        except OSError:
            continue
        digest.update(f"{source}|{stat.st_mtime_ns}|{stat.st_size}|".encode())
    return digest.hexdigest()[:16]


def render_pinned(cwd: str = "", extra_files: Optional[list] = None,
                  max_bytes: int = 256 * 1024) -> dict:
    """渲染 Pinned 块（有界读取；超限截断并标记——绝不静默丢失）。"""
    sources = pinned_sources(cwd, extra_files)
    chunks, total = [], 0
    truncated = False
    for source in sources:
        try:
            data = Path(source).read_text(encoding="utf-8",
                                          errors="replace")
        except OSError:
            continue
        chunk = f"\n## pinned: {source}\n{data}\n"
        if total + len(chunk.encode("utf-8")) > max_bytes:
            truncated = True
            remaining = max_bytes - total
            if remaining > 0:
                chunks.append(chunk[:remaining])
                total += remaining
            break
        chunks.append(chunk)
        total += len(chunk.encode("utf-8"))
    return {"sources": sources, "fingerprint": fingerprint_sources(sources),
            "content": "".join(chunks), "truncated": truncated}


def summary_valid(cached_fingerprint: str, cwd: str = "",
                  extra_files: Optional[list] = None) -> bool:
    """摘要缓存是否仍有效：源文件未变。"""
    return cached_fingerprint == fingerprint_sources(
        pinned_sources(cwd, extra_files))


def pinned_main(argv: Optional[list] = None) -> int:
    """`pi-batch pinned [--json] [--fingerprint CACHED]`。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py pinned",
        description="Pinned context: 不可压缩上下文 + 摘要失效检测")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--fingerprint", default="",
                        help="缓存的摘要指纹：校验是否需重新生成")
    parser.add_argument("--extra", default="",
                        help="逗号分隔的额外 pinned 文件")
    args = parser.parse_args(argv)
    extra = [f.strip() for f in args.extra.split(",") if f.strip()]
    report = render_pinned(extra_files=extra)
    if args.fingerprint:
        report["cache_valid"] = summary_valid(args.fingerprint,
                                              extra_files=extra)
    if args.json:
        report = {key: value for key, value in report.items()
                  if key != "content"}
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"pinned sources: {len(report['sources'])}")
        print(f"fingerprint: {report['fingerprint']}")
        if args.fingerprint:
            valid = report.get("cache_valid")
            print(f"summary cache: "
                  f"{'有效' if valid else '已失效——必须重新生成'}")
        if report["truncated"]:
            print("⚠ 超限截断（不会静默丢失，显式标记）")
    raise SystemExit(0)
