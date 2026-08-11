"""Device-Aware Execution Fabric — `pi-batch devices` CLI (read-only ops).

子命令：list / probe [NAME] / ssh-hosts / placement（运维只读层）+
run / schedule / task / migrate / cordon / drain（执行与调度层，实现见
pbatch/fabric_cmds.py）。全部不扫描网络、不自动安装（AADM-D §9）。
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Optional

from . import config
from .fabric import local_probe
from .fabric_desired import desired_main as _desired_main
from .fabric_cmds import (DISPATCH, _add_cordon_parser, _add_halt_parser,
                          _add_migrate_parser, _add_revoke_parser,
                          _add_run_parser, _add_schedule_parser,
                          _add_task_parser)
from .fabric_devices import (EFFECT_CLASSES, TaskPlacement,
                             device_inventory, evaluate_placement,
                             parse_ssh_config, remote_probe, resolve_device,
                             save_probe_state)


def _render_list(nodes: list) -> str:
    lines = ["# Device Inventory (INVENTORY mode)", ""]
    mode = config.DEVICE_FABRIC_MODE
    enabled = config.DEVICE_FABRIC_ENABLED
    lines.append(f"fabric: {'enabled' if enabled else 'disabled'} "
                 f"(mode={mode})")
    lines.append("")
    lines.append(f"{'DEVICE ID':<20} {'NAME':<24} {'OS':<10} "
                 f"{'ARCH':<10} STATUS")
    for node in nodes:
        caps = node.get("static_capabilities", {}) or {}
        state = node.get("dynamic_state", {}) or {}
        lines.append(f"{node['device_id']:<20} {node['name']:<24} "
                     f"{caps.get('os', '-'):<10} "
                     f"{caps.get('architecture', '-'):<10} "
                     f"{state.get('status', '-')}")
    lines.append("")
    lines.append("远程设备仅登记/缓存探测结果；probe <name> 可刷新（只读）。")
    return "\n".join(lines)


def _render_probe(report: dict) -> str:
    caps = report["static_capabilities"]
    dyn = report["dynamic_state"]
    lines = ["# Local Device Probe (P0)", ""]
    lines.append(f"device_id : {report['device_id']}")
    lines.append(f"name      : {report['name']}")
    lines.append(f"os/arch   : {caps['os']} {caps['os_release']} / "
                 f"{caps['architecture']}")
    lines.append(f"cpu/mem   : {caps['cpu_cores']} cores / "
                 f"{caps['total_memory_bytes'] // (1024**2)} MiB total / "
                 f"{dyn['available_memory_bytes'] // (1024**2)} MiB free")
    lines.append(f"load      : {dyn['load']}")
    lines.append(f"fingerprint: {report['capability_fingerprint']} "
                 f"(valid until {report['capability_valid_until']})")
    if report.get("runtimes"):
        present = ", ".join(sorted(t for t, ok in report["runtimes"].items() if ok))
        lines.append(f"runtimes  : {present or '(none found on PATH)'}")
    return "\n".join(lines)


def _render_remote_probe(report: dict) -> str:
    caps = report["static_capabilities"]
    state = report["dynamic_state"]
    lines = [f"# Remote Device Probe: {report['name']}", ""]
    lines.append(f"device_id : {report['device_id']}")
    lines.append(f"status    : {state['status']}")
    if caps:
        lines.append(f"os/arch   : {caps.get('os', '-')} "
                     f"{caps.get('os_release', '')} / "
                     f"{caps.get('architecture', '-')}")
        lines.append(f"cpu/mem   : {caps.get('cpu_cores', '-')} cores / "
                     f"{(caps.get('total_memory_bytes') or 0) // (1024**2)} MiB")
        present = ", ".join(sorted(t for t, ok in report.get("runtimes", {}).items() if ok))
        lines.append(f"runtimes  : {present or '(none)'}")
        lines.append(f"fingerprint: {report.get('capability_fingerprint', '')} "
                     f"(valid until {report.get('capability_valid_until', '-')})")
    else:
        lines.append(f"error     : {report.get('probe_error', '') or '(no output)'}")
        lines.append("提示：仅只读探测，未执行任何远程修改（探测与修复分离）。")
    return "\n".join(lines)


def _render_placement(placement: TaskPlacement, candidates: list) -> str:
    lines = ["# Placement Dry-run", ""]
    lines.append(f"约束: os={placement.required_os or '-'} "
                 f"arch={placement.required_architecture or '-'} "
                 f"min_memory={placement.min_memory_bytes // (1024**2)}MiB "
                 f"min_cpu={placement.min_cpu_cores} "
                 f"effect<={placement.max_effect_class} "
                 f"target={placement.target_required or '-'}")
    lines.append("")
    for item in candidates:
        mark = "yes" if item["feasible"] else "no "
        detail = "; ".join(item["reasons"]) if item["reasons"] else "-"
        warn = (" [warn: " + "; ".join(item["warnings"]) + "]"
                if item["warnings"] else "")
        lines.append(f"{item['device']:<24} {mark}  {detail}{warn}")
    feasible = [c for c in candidates if c["feasible"]]
    lines.append("")
    if feasible:
        fallback = ", ".join(c["device"] for c in feasible[1:]) or "-"
        lines.append(f"selected: {feasible[0]['device']}  "
                     f"(fallback: {fallback})")
    else:
        lines.append("无可行设备：约束过严或设备池为空")
    return "\n".join(lines)


def _cmd_probe_local(args) -> None:
    report = local_probe(depth="full" if args.full else "basic")
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(_render_probe(report))


def _cmd_probe_remote(args) -> None:
    target = resolve_device(args.name, ssh_config_path=args.ssh_config)
    if target is None:
        print(f"unknown device: {args.name}（静态配置或 ~/.ssh/config 中不存在）",
              file=sys.stderr)
        raise SystemExit(2)
    report = remote_probe(target)
    if report["dynamic_state"]["status"] == "online":
        save_probe_state(report)
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(_render_remote_probe(report))


def _cmd_list(args) -> None:
    nodes = device_inventory()
    if args.json:
        print(json.dumps(nodes, indent=2))
    else:
        print(_render_list(nodes))


def _clock_skew_report(nodes: list) -> list:
    """设备 last_seen 与本机时钟对比：偏差秒数 + 状态（fresh/stale）。"""
    import time as _time
    now = _time.time()
    report = []
    for node in nodes:
        last = node.get("dynamic_state", {}).get("last_seen_at", "")
        skew = None
        if last:
            try:
                skew = int(now - _time.mktime(_time.strptime(
                    last, "%Y-%m-%dT%H:%M:%S")))
            except (ValueError, OverflowError):
                skew = None
        status = ("online" if skew is not None and skew < 300
                  else "stale" if skew is not None else "unknown")
        report.append({"name": node.get("name", node.get("device_id", "?")),
                       "last_seen_at": last, "skew_seconds": skew,
                       "status": status})
    return report


def _add_placement_parser(sub) -> None:
    p_place = sub.add_parser("placement", help="设备放置 Dry-run")
    p_place.add_argument("--os", action="append", default=[],
                         help="要求的操作系统（可重复）")
    p_place.add_argument("--arch", action="append", default=[],
                         help="要求的架构（可重复）")
    p_place.add_argument("--min-memory-mb", type=int, default=0)
    p_place.add_argument("--min-cpu", type=int, default=0)
    p_place.add_argument("--capabilities", default="",
                         help="逗号分隔的能力/运行时要求（如 docker,python3）")
    p_place.add_argument("--effect-class", default="read_only",
                         choices=EFFECT_CLASSES)
    p_place.add_argument("--target", default="", help="必须使用指定设备")
    p_place.add_argument("--json", action="store_true")


def _add_clock_skew_parser(sub) -> None:
    p_clock = sub.add_parser(
        "clock-skew",
        help="时钟纪律：设备 last_seen 与本机时钟偏差（失联/漂移告警）")
    p_clock.add_argument("--json", action="store_true")

    p_key = sub.add_parser("keymigrate",
                          help="旧项目内设备密钥 → 用户级目录（跨项目复用）")
    p_key.add_argument("--from", dest="src", default="",
                       help="旧密钥目录（默认 .pi-batch/devices/keys）")


def _cmd_clock_skew(args) -> None:
    """设备 last_seen 与本机时钟对比：偏差秒数 + 状态（fresh/stale）。"""
    report = _clock_skew_report(device_inventory())
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    lines = ["# Device Clock Skew（对照本机时钟；>300s = stale）", ""]
    for entry in report:
        skew = entry["skew_seconds"]
        skew_text = f"{skew}s" if skew is not None else "-"
        lines.append(f"{entry['name']:<24} {skew_text:<12} "
                     f"{entry['status']}")
    print("\n".join(lines))


def _cmd_ssh_hosts(args) -> None:
    hosts = parse_ssh_config(args.ssh_config)
    if args.json:
        print(json.dumps(hosts, indent=2))
        return
    lines = ["# SSH Config Hosts (read-only)", ""]
    lines.append(f"{'ALIAS':<20} {'HOSTNAME':<24} {'USER':<12} PORT  PROXYJUMP")
    for host in hosts:
        lines.append(f"{host['alias']:<20} {host.get('host_name', ''):<24} "
                     f"{host.get('user', ''):<12} {host.get('port', '-'):<5} "
                     f"{host.get('proxyjump', '-')}")
    print("\n".join(lines))


def _cmd_placement(args) -> None:
    placement = TaskPlacement(
        required_os=args.os,
        required_architecture=args.arch,
        min_memory_bytes=args.min_memory_mb * 1024 * 1024,
        min_cpu_cores=args.min_cpu,
        required_capabilities=[c.strip() for c in args.capabilities.split(",")
                               if c.strip()],
        max_effect_class=args.effect_class,
        target_required=args.target,
    )
    candidates = evaluate_placement(placement)
    if args.json:
        print(json.dumps({"candidate_devices": candidates}, indent=2))
    else:
        print(_render_placement(placement, candidates))


def _add_desired_parser(sub) -> None:
    """Desired-State Controller 子命令（实现见 pbatch/fabric_desired.py）。"""
    p_desired = sub.add_parser(
        "desired", help="Desired-State Controller（声明期望→观察→纠偏）")
    p_desired.add_argument("command", choices=("apply", "status"))
    p_desired.add_argument("--file", required=True)
    p_desired.add_argument("--device", required=True)
    p_desired.add_argument("--control", default="http://127.0.0.1:8765")
    p_desired.add_argument("--state-dir", default="")
    p_desired.add_argument("--json", action="store_true")


def _build_devices_parser() -> argparse.ArgumentParser:
    """devices 子命令参数（运维只读层 + 执行/调度层）。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py devices",
        description="设备感知执行织网（INVENTORY/OBSERVE/EXECUTE 模式）")
    sub = parser.add_subparsers(dest="command", required=True)

    _add_desired_parser(sub)

    p_list = sub.add_parser("list", help="设备清单（本机 + 静态 + 缓存状态）")
    p_list.add_argument("--json", action="store_true")


    p_probe = sub.add_parser(
        "probe", help="探测：无 NAME = 本机 P0/P1；有 NAME = SSH 只读探测")
    p_probe.add_argument("name", nargs="?")
    p_probe.add_argument("--full", action="store_true",
                         help="含 P1 运行时存在性检查")
    p_probe.add_argument("--json", action="store_true")
    p_probe.add_argument("--ssh-config", default="")

    p_hosts = sub.add_parser("ssh-hosts",
                             help="列出 ~/.ssh/config 候选主机（只读解析）")
    p_hosts.add_argument("--ssh-config", default="")
    p_hosts.add_argument("--json", action="store_true")

    _add_clock_skew_parser(sub)

    _add_placement_parser(sub)

    # 执行与调度层（实现见 pbatch/fabric_cmds.py）
    _add_run_parser(sub)
    _add_schedule_parser(sub)
    _add_task_parser(sub)
    _add_migrate_parser(sub)
    _add_cordon_parser(sub, drain=False)
    _add_cordon_parser(sub, drain=True)
    _add_halt_parser(sub)
    _add_revoke_parser(sub)
    return parser


def devices_main(argv: Optional[list] = None) -> int:
    """`pi-batch devices`：运维只读 + 受控执行/调度。
    run/schedule 的 `--` 分隔符在解析前手工拆分（argparse REMAINDER 会
    吞掉 `--` 与后续选项，必须避免）。"""
    tokens = list(sys.argv[2:] if argv is None else argv)
    split_cmd = None
    if tokens and tokens[0] in ("run", "schedule"):
        for index, token in enumerate(tokens):
            if token == "--":
                split_cmd = tokens[index + 1:]
                tokens = tokens[:index]
                break
        else:
            # 没有 -- 分隔符：多余参数不参与解析，命令层给出用法提示
            split_cmd = []
            tokens = tokens[:2]
    args = _build_devices_parser().parse_args(tokens)
    if split_cmd is not None:
        args.cmd = split_cmd
    if args.command == "desired":
        _desired_main(sys.argv[2:])
    if args.command in DISPATCH:
        DISPATCH[args.command](args)
    elif args.command == "probe":
        if args.name:
            _cmd_probe_remote(args)
        else:
            _cmd_probe_local(args)
    elif args.command == "list":
        _cmd_list(args)
    elif args.command == "ssh-hosts":
        _cmd_ssh_hosts(args)
    elif args.command == "clock-skew":
        _cmd_clock_skew(args)
    elif args.command == "keymigrate":
        from .fabric_devices import keymigrate_main
        keymigrate_main([f"--from={args.src}"] if args.src else [])
        raise SystemExit(0)
    else:
        _cmd_placement(args)
    raise SystemExit(0)
