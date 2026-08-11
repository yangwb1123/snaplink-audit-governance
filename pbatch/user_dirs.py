"""用户级目录规范（XDG 兼容）——产品 CLI 的 ~/.pbatch 工作目录。

分层（对齐 XDG Base Directory）：
- config: ~/.config/pbatch/   （用户配置 settings.yaml）
- cache:  ~/.cache/pbatch/    （可丢弃缓存：SSH 探测、规则缓存）
- data:   ~/.local/share/pbatch/ （持久数据：设备密钥、设备状态、全局日志）
- runtime: /tmp/pbatch-<uid>/ （运行时锁/套接字，无持久性）

兼容回退：XDG 变量未设置时按规范默认；显式环境变量
PBATCH_CONFIG_HOME / PBATCH_CACHE_HOME / PBATCH_DATA_HOME 优先。
另有统一 PBATCH_HOME（如 ~/.pbatch）——设置时三个目录都收拢到其下
（老式单目录习惯）。

原则：**项目相关状态留在项目内**（.pi-batch/：会话、memory 索引、被拒
产物——团队共享、随仓库走）；用户级目录只放**跨项目**数据（设备身份、
全局配置、可重建缓存）。
"""

from __future__ import annotations

import os
from pathlib import Path

# 环境变量（统一收拢用；未设置时走 XDG 规范）
PBATCH_HOME_ENV = "PBATCH_HOME"
XDG_CONFIG_HOME = "XDG_CONFIG_HOME"
XDG_CACHE_HOME = "XDG_CACHE_HOME"
XDG_DATA_HOME = "XDG_DATA_HOME"


def _home() -> Path:
    return Path(os.path.expanduser("~"))


def _xdg(name: str, fallback_dir: str) -> Path:
    value = os.environ.get(name, "")
    if value:
        return Path(value)
    return _home() / fallback_dir


def pbatch_config_home() -> Path:
    """~/.config/pbatch（或 PBATCH_HOME/.config 统一模式）。"""
    unified = os.environ.get(PBATCH_HOME_ENV, "")
    if unified:
        return Path(unified) / ".config"
    return _xdg(XDG_CONFIG_HOME, ".config") / "pbatch"


def pbatch_cache_home() -> Path:
    """~/.cache/pbatch（探测缓存等可丢弃数据）。"""
    unified = os.environ.get(PBATCH_HOME_ENV, "")
    if unified:
        return Path(unified) / ".cache"
    return _xdg(XDG_CACHE_HOME, ".cache") / "pbatch"


def pbatch_data_home() -> Path:
    """~/.local/share/pbatch（设备密钥/状态/全局日志）。"""
    unified = os.environ.get(PBATCH_HOME_ENV, "")
    if unified:
        return Path(unified) / ".local" / "share"
    return _xdg(XDG_DATA_HOME, ".local/share") / "pbatch"


def ensure_user_dirs() -> dict:
    """创建用户级目录骨架；返回 {config, cache, data} 路径。"""
    dirs = {
        "config": pbatch_config_home(),
        "cache": pbatch_cache_home(),
        "data": pbatch_data_home(),
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def user_settings_path() -> Path:
    """~/.config/pbatch/settings.yaml（用户级默认覆盖）。"""
    return pbatch_config_home() / "settings.yaml"


def keys_dir() -> Path:
    """设备身份密钥目录（用户级，跨项目复用）。"""
    return pbatch_data_home() / "keys"


def global_logs_dir() -> Path:
    """全局运行日志（无项目时的兜底落点）。"""
    return pbatch_data_home() / "logs"
