"""ui-geometry 子命令 — UI 几何构图门禁（ui-specs/geometry.md）。

包装 scripts/check-ui-geometry.py 为 pi-batch 统一入口：
    pi-batch ui-geometry <artifact.tsx|.css|.dart|.vue>
    pi-batch ui-geometry --dir <项目前端根> [--all] [--json] [-o 报告.json]

拦截（fail closed）：裸数字小偏移（1-3px 未绑 --optical-*）、随机圆角
（非 tokens.json 圆角家族 4/8/12 或 50%/999px）、随机线条宽度（非 {1,2,3}）。
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def ui_geometry_main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="pi-batch ui-geometry",
        description="UI 几何构图门禁（ui-specs/geometry.md）",
    )
    ap.add_argument("artifact", nargs="?", help="单文件（.tsx/.css/.vue/.dart）")
    ap.add_argument("--dir", default="", help="项目前端源码目录")
    ap.add_argument("--all", action="store_true", help="递归扫描 --dir 下全部 UI 源文件")
    ap.add_argument("--json", action="store_true", help="JSON 输出")
    ap.add_argument("-o", default="", help="JSON 报告写入路径（与 --json 同格式）")
    ap.add_argument("--strict", action="store_true", help="同 --all（兼容别名）")
    args = ap.parse_args(argv)

    # 定位 check-ui-geometry.py（入口脚本旁 / 仓库根 scripts/）
    root = Path(__file__).resolve().parent.parent
    candidates = [
        root / "scripts" / "check-ui-geometry.py",
        Path(__file__).resolve().parent.parent.parent / "scripts" / "check-ui-geometry.py",
    ]
    script = next((c for c in candidates if c.is_file()), None)
    if script is None:
        print("ui-geometry: check-ui-geometry.py 未找到", file=sys.stderr)
        return 2

    cmd = [sys.executable, str(script)]
    if args.artifact:
        cmd.append(args.artifact)
    if args.dir:
        cmd += ["--dir", args.dir]
    if args.all or args.strict:
        cmd.append("--all")
    if args.json:
        cmd.append("--json")
    if args.o:
        cmd += ["-o", args.o]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    # 与 check/eval 等子命令一致：内部 sys.exit 透传退出码
    sys.exit(result.returncode)


if __name__ == "__main__":
    sys.exit(ui_geometry_main())
