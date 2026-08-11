"""Device & attempt state machines (AADM-D §19).

设备状态机：UNREGISTERED → ENROLLED → PROBING → READY → RESERVED → BUSY
→ DRAINING → OFFLINE / STALE / QUARANTINED。执行尝试状态机：CREATED →
PLACED → RESERVED → STAGING → RUNNING → VERIFYING → SUCCEEDED / FAILED /
LOST → CLEANUP。LOST ≠ FAILED：失联不知道结果，处理方式取决于副作用类型。

非法迁移在进入执行前 fail closed；纯函数，可单测。
"""

from __future__ import annotations

DEVICE_TRANSITIONS = {
    "unregistered": {"enrolled"},
    "enrolled": {"probing", "ready"},
    "probing": {"ready", "offline"},
    "ready": {"reserved", "busy", "draining", "offline", "stale"},
    "reserved": {"busy", "ready", "draining"},
    "busy": {"ready", "draining", "offline"},
    "draining": {"ready", "offline", "stale"},
    "offline": {"enrolled", "ready", "stale"},
    "stale": {"ready", "offline", "quarantined"},
    "quarantined": {"offline"},
}

ATTEMPT_TRANSITIONS = {
    "created": {"placed", "failed"},
    "placed": {"reserved", "failed"},
    "reserved": {"staging", "running", "failed"},
    "staging": {"running", "failed"},
    "running": {"verifying", "failed", "lost"},
    "verifying": {"succeeded", "failed", "lost"},
    "succeeded": {"cleanup"},
    "failed": {"cleanup"},
    "lost": {"cleanup"},
    "cleanup": set(),
}

# LOST 与 FAILED 不能混为一谈：LOST 不知道实际结果。
LOST_POLICY = {
    "pure": "重新执行",
    "read_only": "其他节点重试",
    "sandboxed": "其他节点重试",
    "reversible": "确认状态后重试",
    "compensatable": "对账后重试或补偿",
    "irreversible": "暂停，等待状态确认（禁止自动重试）",
}


def transition(state_machine: dict, current: str, next_state: str) -> str:
    """状态迁移；非法迁移抛 ValueError（fail closed）。"""
    allowed = state_machine.get(current, set())
    if next_state not in allowed:
        raise ValueError(f"非法迁移 {current} → {next_state}"
                         f"（允许: {sorted(allowed) or '终态'}）")
    return next_state


def device_transition(current: str, next_state: str) -> str:
    return transition(DEVICE_TRANSITIONS, current, next_state)


def attempt_transition(current: str, next_state: str) -> str:
    return transition(ATTEMPT_TRANSITIONS, current, next_state)


def lost_policy(effect_class: str) -> str:
    """LOST 任务的处理方式由副作用类型决定（AADM-D §19/§29）。"""
    return LOST_POLICY.get(effect_class, "暂停，等待状态确认")
