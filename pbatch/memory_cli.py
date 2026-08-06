"""Command-line adapter for progressive message memory queries."""

from __future__ import annotations

import argparse
import json

from . import memory


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pi-batch.py memory")
    parser.add_argument("--index", default="", help="memory index override")
    subs = parser.add_subparsers(dest="command", required=True)
    recent_parser = subs.add_parser("recent")
    recent_parser.add_argument("--limit", type=int, default=5)
    find_parser = subs.add_parser("find")
    find_parser.add_argument("query")
    find_parser.add_argument("--limit", type=int, default=5)
    read_parser = subs.add_parser("read")
    read_parser.add_argument("session_id")
    read_parser.add_argument("--max-bytes", type=int, default=0)
    read_parser.add_argument("--raw", action="store_true", help="do not redact likely secrets")
    ingest_parser = subs.add_parser("ingest")
    ingest_parser.add_argument("--session-dir", default="")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "limit", 0) < 0 or getattr(args, "max_bytes", 0) < 0:
        parser.error("--limit and --max-bytes must be non-negative")
    if args.command == "recent":
        result = memory.recent(limit=args.limit, override=args.index)
    elif args.command == "find":
        result = memory.find(args.query, limit=args.limit, override=args.index)
    elif args.command == "read":
        result = memory.read_session(args.session_id, max_bytes=args.max_bytes,
                                     override=args.index, redact=not args.raw)
    else:
        result = memory.ingest_sessions(directory=args.session_dir, override=args.index)
    print(json.dumps(result, ensure_ascii=False, indent=2))
