"""测试分类报告（`pi-batch test-report`）— 测试分类治理.

用户规范：测试应分类为 Unit（函数正确性）/ Contract（模块接口稳定）/
Property（不变量始终成立）/ Scenario（完整业务流程）/ Chaos（故障注入
与演练）。本命令扫描 tests/ 统计各分类标记数 + 未分类文件（诚实标注）。

pytest 标记定义在 pytest.ini；用 `pytest -m <category>` 可过滤运行。

Usage:
    pi-batch test-report [--json]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Optional

CATEGORIES = ("unit", "contract", "property", "scenario", "chaos")
TESTS_DIR = Path("tests")


def classify_tests(tests_dir: Path = TESTS_DIR) -> dict:
    """扫描测试文件：分类标记计数 + 测试函数总数 + 未分类文件数。"""
    counts = {category: 0 for category in CATEGORIES}
    test_functions = 0
    unmarked_files = 0
    files = 0
    for path in sorted(tests_dir.glob("test_*.py")):
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        files += 1
        test_functions += len(re.findall(
            r"^def test_|^    def test_", text, re.M))
        marked = False
        for category in CATEGORIES:
            count = len(re.findall(
                rf"@pytest\.mark\.{category}\b", text))
            counts[category] += count
            if count:
                marked = True
        if not marked:
            unmarked_files += 1
    total_marked = sum(counts.values())
    return {
        "files": files, "test_functions": test_functions,
        "marked": total_marked,
        "mark_coverage": round(total_marked / test_functions, 2)
        if test_functions else 0.0,
        "unmarked_files": unmarked_files,
        "categories": counts,
        "_note": "标记是代表性分类（不是全量）；未标记测试默认视为 unit",
    }


def test_report_main(argv: Optional[list] = None) -> int:
    """`pi-batch test-report [--json]`：测试分类统计。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py test-report",
        description="测试分类报告（Unit/Contract/Property/Scenario/Chaos）")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    report = classify_tests()
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"# Test Classification（{report['test_functions']} 个测试，"
              f"{report['files']} 个文件）")
        print(f"标记覆盖: {report['marked']} 个代表性测试 "
              f"({report['mark_coverage']}) | "
              f"未分类文件: {report['unmarked_files']}")
        for category in CATEGORIES:
            print(f"  {category:<10} {report['categories'][category]}")
        print("过滤运行: pytest tests/ -m scenario -q")
    raise SystemExit(0)
