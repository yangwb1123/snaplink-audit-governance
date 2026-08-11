"""`pi-batch export-template [--specs SPECS] [-o OUT]` — 分发智能落地。

把规范资产打包为可分享、可移植的模板目录（Distribution Intelligence：
分发物必须可复现、自验证、可接手——README + 校验 + 版本清单）。

默认导出设计智能规范包（前端 6 + 后端 8 + 哲学 + 检查器），
`--checkers` 附带门禁脚本；`--all` 导出全部规范资产。
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

_DEFAULT_SPECS = [
    "ui-specs/design-intelligence",
    "backend-specs/design-intelligence",
    "docs/ENGINEERING_PHILOSOPHY.md",
]
_DEFAULT_CHECKERS = [
    "scripts/check-design-intelligence.py",
    "scripts/check-backend-experience.py",
    "scripts/check-knowledge-freshness.py",
]
_PIPELINE_TEMPLATES = [
    "examples/ui-generation-pipeline.yaml",
    "examples/backend-implementation-pipeline.yaml",
]


def _copy(src: Path, dest_root: Path) -> int:
    if src.is_dir():
        target = dest_root / src.name
        shutil.copytree(src, target, dirs_exist_ok=True)
        return sum(1 for _ in target.rglob("*") if _.is_file())
    target = dest_root / src.name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, target)
    return 1


def export_template(specs: list[Path], checkers: list[Path],
                    pipelines: list[Path], out: Path) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    manifest = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "source": "ai-batch-runner",
        "specs": [str(s) for s in specs],
        "checkers": [str(c) for c in checkers],
        "pipelines": [str(p) for p in pipelines],
        "files": {},
    }
    total = 0
    for src in specs + checkers + pipelines:
        if not src.exists():
            manifest["files"][str(src)] = "missing"
            continue
        count = _copy(src, out)
        manifest["files"][str(src)] = count
        total += count
    # README：可接手（分发质量第 3 条）
    readme = out / "README.md"
    readme.write_text(
        f"# Design Intelligence Pack\n\n"
        f"导出时间: {manifest['exported_at']}\n"
        f"规范 {len(specs)} 项 / 检查器 {len(checkers)} / 流水线 {len(pipelines)}\n"
        f"\n## 校验\n"
        f"```bash\n"
        f"python check-design-intelligence.py --selfcheck\n"
        f"python check-backend-experience.py --selfcheck\n"
        f"python check-knowledge-freshness.py --selfcheck\n"
        f"```\n",
        encoding="utf-8",
    )
    # 版本清单（可追溯第 4 条）
    (out / "VERSION.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8")
    return {"files": total, "manifest": str(out / "VERSION.json")}


def export_main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pi-batch.py export-template",
                                     description=__doc__)
    parser.add_argument("--all", action="store_true",
                        help="export every spec asset (ui/backend/docs)")
    parser.add_argument("--checkers", action="store_true",
                        help="include checker scripts")
    parser.add_argument("--pipelines", action="store_true",
                        help="include pipeline templates")
    parser.add_argument("-o", "--output", default="dist/design-intelligence-pack",
                        help="output directory")
    args = parser.parse_args(argv)
    specs = [ROOT / p for p in _DEFAULT_SPECS]
    if args.all:
        specs = [p for p in (ROOT / "ui-specs").rglob("*.md")]
        specs += [p for p in (ROOT / "backend-specs").rglob("*.md")]
        specs += [ROOT / "docs" / "ENGINEERING_PHILOSOPHY.md"]
    checkers = [ROOT / p for p in _DEFAULT_CHECKERS] if args.checkers else []
    pipelines = [ROOT / p for p in _PIPELINE_TEMPLATES] if args.pipelines else []
    out = ROOT / args.output
    report = export_template(specs, checkers, pipelines, out)
    print(f"exported {report['files']} files -> {out}")
    print(f"manifest: {report['manifest']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(export_main())
