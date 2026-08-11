"""产品 CLI 规整命令：init / version / clean / completion。

- `pi-batch init`：创建用户级目录骨架（~/.config|.cache/.local/share/pbatch），
  打印目录布局与配置分层说明。
- `pi-batch version [--json]`：版本标识（pyproject.toml 单事实源）+ 运行环境。
- `pi-batch clean`：清空用户级缓存（~/.cache/pbatch），保留配置与密钥。
- `pi-batch completion bash|zsh`：输出 shell 补全脚本（注册表驱动）。
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Optional

try:
    import yaml
except ImportError:
    yaml = None

from . import user_dirs
from .tools import list_subcommands

# 产品名（与 pyproject 的 [project] name 一致；单事实源在 pyproject.toml）
PRODUCT = "pi-batch"


def _version_from_pyproject() -> str:
    """从 pyproject.toml 读版本（单事实源）；缺省回退 0.0.0-dev。"""
    candidates = [
        Path(__file__).resolve().parent.parent / "pyproject.toml",
        Path("pyproject.toml"),
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("version ="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return "0.0.0-dev"


def init_main(argv: Optional[list] = None) -> int:
    """`pi-batch init`：用户级目录骨架 + 布局说明。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py init",
        description="创建用户级工作目录（~/.config|.cache/.local/share/pbatch）")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    dirs = user_dirs.ensure_user_dirs()
    settings = user_dirs.user_settings_path()
    if not settings.exists():
        settings.write_text(
            "# pi-batch 用户配置（分层：CLI > 项目 pi-batch.yaml > 此处 > 默认）\n"
            "# 示例：\n"
            "# agent_bin: pi\n"
            "# session_mode: shared\n",
            encoding="utf-8",
        )
    if args.json:
        print(json.dumps({k: str(v) for k, v in dirs.items()}
                          + {"settings": str(settings)}, ensure_ascii=False,
                          indent=2))
    else:
        print(f"# {PRODUCT} 用户目录已就绪")
        for kind, path in dirs.items():
            print(f"  {kind:<8} {path}")
        print(f"  settings {user_dirs.user_settings_path()}")
        print()
        print("分层（高优先）：CLI 参数 > 项目 pi-batch.yaml > "
              "~/.config/pbatch/settings.yaml > 内置默认")
        print("项目相关状态（会话/记忆/被拒产物）保留在项目内 .pi-batch/ ——"
              "随仓库共享；用户级目录只放跨项目数据。")
        print("迁移设备密钥：旧项目内 devices/keys 请用 `pi-batch devices "
              "keymigrate` 导入。")
    raise SystemExit(0)


def version_main(argv: Optional[list] = None) -> int:
    """`pi-batch version [--json]`。"""
    parser = argparse.ArgumentParser(prog="pi-batch.py version",
                                     description="版本与环境信息")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    import platform
    report = {
        "product": PRODUCT,
        "version": _version_from_pyproject(),
        "python": platform.python_version(),
        "user_dirs": {k: str(v) for k, v in user_dirs.ensure_user_dirs().items()},
    }
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"{PRODUCT} {report['version']} (python {report['python']})")
        for kind, path in report["user_dirs"].items():
            print(f"  {kind:<8} {path}")
    raise SystemExit(0)


def clean_main(argv: Optional[list] = None) -> int:
    """`pi-batch clean [--all]`：清缓存（默认）；--all 连用户配置一起。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py clean",
        description="清空用户级缓存（保留配置与设备密钥）")
    parser.add_argument("--all", action="store_true",
                        help="连同 ~/.config/pbatch 与设备密钥一起清除")
    parser.add_argument("--yes", action="store_true", help="跳过确认")
    args = parser.parse_args(argv)
    cache = user_dirs.pbatch_cache_home()
    if args.all and not args.yes:
        reply = input(f"将删除 {cache} 及配置/密钥目录。确认? [y/N] ").strip().lower()
        if reply not in ("y", "yes"):
            print("clean: aborted")
            raise SystemExit(1)
    if cache.exists():
        shutil.rmtree(cache)
    if args.all:
        for path in (user_dirs.pbatch_config_home(),
                     user_dirs.pbatch_data_home()):
            if path.exists():
                shutil.rmtree(path)
    print(f"clean: {'all user dirs' if args.all else 'cache'} removed")
    raise SystemExit(0)


def _completion_script(shell: str) -> str:
    """注册表驱动补全（bash/zsh 用同一命令清单生成）。"""
    commands = sorted(list_subcommands())
    if shell == "zsh":
        return (
            "#compdef pi-batch\n"
            "_pi_batch() {\n"
            "  local -a cmds\n"
            f"  cmds=({' '.join(commands)})\n"
            "  _describe 'command' cmds\n"
            "}\n"
            "compdef _pi_batch pi-batch\n"
        )
    return (
        "_pi_batch_complete() {\n"
        "  local cur prev\n"
        "  cur=\"${COMP_WORDS[COMP_CWORD]}\"\n"
        "  prev=\"${COMP_WORDS[COMP_CWORD-1]}\"\n"
        "  local cmds=\"%s\"\n"
        "  COMPREPLY=( $(compgen -W \"$cmds\" -- \"$cur\") )\n"
        "}\n"
        "complete -F _pi_batch_complete pi-batch\n" % " ".join(commands)
    )


def completion_main(argv: Optional[list] = None) -> int:
    """`pi-batch completion bash|zsh` → 输出补全脚本（重定向到 rc 文件）。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py completion",
        description="生成 shell 补全（注册表驱动，命令清单自动同步）")
    parser.add_argument("shell", choices=("bash", "zsh"))
    args = parser.parse_args(argv)
    print(_completion_script(args.shell), end="")
    raise SystemExit(0)


def _doctor_checks() -> list:
    """环境诊断清单（每项: (name, ok, detail)）。"""
    from . import user_dirs, config
    checks = []
    checks.append(("安装", True, f"{PRODUCT} {_version_from_pyproject()}"))
    checks.append(("Python", True, __import__("platform").python_version()))
    for kind, path in user_dirs.ensure_user_dirs().items():
        checks.append((f"目录 {kind}", path.exists(), str(path)))
    settings = user_dirs.user_settings_path()
    checks.append(("用户配置", settings.exists() or True,
                   str(settings) if settings.exists() else "未创建（可选）"))
    project_cfg = config._find_batch_config()
    checks.append(("项目配置", project_cfg is not None,
                   "pi-batch.yaml 已加载" if project_cfg else "无（用内置默认）"))
    agent_bin = config.AGENT_BIN
    import shutil
    agent_ok = shutil.which(agent_bin) is not None
    checks.append((f"agent 二进制 {agent_bin}", agent_ok,
                   shutil.which(agent_bin) or "PATH 中未找到"))
    keys = user_dirs.keys_dir()
    key_count = len(list(keys.glob("*.json"))) if keys.exists() else 0
    # 信息性：无密钥是 serve 前的正常状态（注册时生成），不阻断。
    checks.append(("设备密钥", True,
                   f"{key_count} 个（{keys}）" if key_count else "无（serve 前生成）"))
    import os
    lock = Path(".pi-batch.lock")
    checks.append(("实例锁", not lock.exists(),
                   "空闲" if not lock.exists() else "另一实例运行中"))
    return checks


def doctor_main(argv: Optional[list] = None) -> int:
    """`pi-batch doctor [--json]`：环境诊断（安装/目录/配置/agent/密钥/锁）。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py doctor",
        description="环境诊断：安装、目录、配置分层、agent、密钥、锁")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    checks = _doctor_checks()
    if args.json:
        print(json.dumps([{"name": n, "ok": ok, "detail": d}
                          for n, ok, d in checks], ensure_ascii=False,
                         indent=2))
    else:
        for name, ok, detail in checks:
            print(f"  [{'OK' if ok else 'WARN'}] {name:<18} {detail}")
    failed = [n for n, ok, _ in checks if not ok]
    raise SystemExit(1 if failed else 0)
