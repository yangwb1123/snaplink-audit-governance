"""Device-Aware Execution Fabric — SSH 客户端公共原语（无业务依赖）。

`SSH_OPTIONS` / `_valid_ssh_host` / `_build_ssh_argv` 同时被探测
（fabric_devices）与受控执行（fabric_exec）使用；独立成模块避免循环导入。
安全：BatchMode（无密码交互，fail closed）、目标名白名单（防选项注入）、
远程命令作为单个 argv 元素传递（不拼进本地 shell）。
"""

from __future__ import annotations

import re

# SSH 选项：BatchMode（无密码交互，fail closed）、连接/心跳超时。
SSH_OPTIONS = ("-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
               "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=2")


def _valid_ssh_host(name: str) -> bool:
    """SSH 目标名校验：仅字母数字._-，且不以 - 开头（防选项注入）。"""
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", name or ""))


def _build_ssh_argv(target: dict, remote_command: str) -> list:
    """ssh + 受控选项 + 目标 + 远程命令（远程命令已安全引用）。"""
    host = target.get("host_name", "")
    if not _valid_ssh_host(host):
        raise ValueError(f"invalid ssh host: {host!r}")
    argv = ["ssh", *SSH_OPTIONS]
    if target.get("config_path"):
        argv += ["-F", str(target["config_path"])]
    if target.get("user"):
        argv += ["-l", str(target["user"])]
    if target.get("port"):
        argv += ["-p", str(target["port"])]
    argv += [host, remote_command]
    return argv
