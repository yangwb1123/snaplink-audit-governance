"""Device-Aware Execution Fabric — D4: 任务图调度器.

任务图（链 → DAG）的关键路径与就绪集计算、设备选择（可行集 → 反亲和 →
负载 → 数据局部性，AADM-D §13 字典序过滤的 D4 子集）、Cordon/Drain
感知（§31）。

与 fabric_devices.evaluate_placement 配合：先硬过滤（OS/架构/内存/能力/
副作用等级），再在可行集内选择。纯函数，无网络依赖，可单测。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .fabric_devices import TaskPlacement, evaluate_placement


@dataclass
class TaskNode:
    """任务图节点（AADM-D §7 TaskNode 的调度子集）。"""
    id: str
    goal: str
    argv: list
    depends_on: list = field(default_factory=list)
    estimate_seconds: float = 30.0
    mobility: str = "stateless"          # stateless/restartable/checkpointable/pinned
    effect_class: str = "read_only"
    affinity: list = field(default_factory=list)        # 尽量与这些设备共址
    anti_affinity: list = field(default_factory=list)   # 必须避开这些设备
    read_set: list = field(default_factory=list)        # 读取的对象（并行判定）
    write_set: list = field(default_factory=list)       # 修改的对象（并行判定）
    assumptions: list = field(default_factory=list)     # 执行前必须成立（契约）
    guarantees: list = field(default_factory=list)      # 执行后保证成立（契约）
    flexibility: str = "select"   # fixed=按明确方式执行 / select=已知方案选择
                                  # / generative=约束内创造（AADM-R §7）


def critical_path(nodes: list) -> float:
    """DAG 关键路径长度（最长依赖链估计耗时；§23 Priority 的 CriticalPath）。"""
    longest: dict = {}

    def depth(node: TaskNode) -> float:
        if node.id in longest:
            return longest[node.id]
        deps = [dep for dep in nodes if dep.id in node.depends_on]
        best = max((depth(dep) for dep in deps), default=0.0)
        longest[node.id] = best + max(float(node.estimate_seconds), 0.1)
        return longest[node.id]

    return max((depth(node) for node in nodes), default=0.0)


FLEXIBILITY_LEVELS = ("fixed", "select", "generative")


def flexibility_budget(nodes: list) -> dict:
    """主观能动性预算：fixed/select/generative 节点占比（AADM-R §7）。"""
    counts = {"fixed": 0, "select": 0, "generative": 0}
    for node in nodes:
        level = node.flexibility if node.flexibility in FLEXIBILITY_LEVELS \
            else "select"
        counts[level] += 1
    total = max(len(nodes), 1)
    return {"counts": counts,
            "generative_share": round(counts["generative"] / total, 2),
            "note": "图不把每一步写死：固定必要边界，开放可创造节点"}


def ready_nodes(nodes: list, done_ids: set) -> list:
    """依赖已满足的节点（拓扑就绪集，§10 波次调度第 1 步）。"""
    return [node for node in nodes
            if node.id not in done_ids
            and all(dep in done_ids for dep in node.depends_on)]


def _model_cached(name: str, model: str, devices: list) -> bool:
    """设备标签 model.<name>.cached=true → 权重已缓存（模型缓存感知）。"""
    for device in devices:
        if device.get("name") == name:
            labels = device.get("labels", {}) or {}
            return bool(labels.get(f"model.{model}.cached"))
    return False


def pick_device(placement: TaskPlacement, devices: list,
                cordoned: Optional[set] = None,
                busy: Optional[dict] = None,
                avoid: Optional[set] = None,
                prefer: Optional[set] = None,
                model: Optional[str] = None) -> Optional[str]:
    """可行设备中选择：Cordon/Drain 跳过 → 反亲和排除 → (负载, 局部性) 排序。

    返回设备名或 None（无可调度设备）。busy: 设备名 -> 当前任务数（负载）。
    prefer: 数据局部性优先的设备（模型/数据集已缓存，AADM-D §14）。"""
    blocked = set(cordoned or ())
    blocked |= set(avoid or ())
    preferred = set(prefer or ())
    loads = busy or {}
    candidates = []
    for item in evaluate_placement(placement, devices):
        if not item["feasible"] or item["device"] in blocked:
            continue
        name = item["device"]
        load = loads.get(name, 0)
        locality = 0.0 if name in preferred else 1.0
        if model is not None and _model_cached(name, model, devices):
            locality -= 0.5  # 模型已缓存 → 优先（权重/数据局部性）
        candidates.append((max(0.0, locality), load, name))
    if not candidates:
        return None
    candidates.sort()
    return candidates[0][2]


def devices_from_status(view: list) -> list:
    """把控制平面 GET /status 的设备视图转成 placement 可消费的设备列表。"""
    devices = []
    for item in view:
        caps = item.get("static_capabilities", {}) or {}
        state = {"status": "online" if item.get("online") else "offline"}
        devices.append({
            "device_id": item.get("device_id", ""),
            "name": item.get("name", ""),
            "static_capabilities": caps,
            "runtimes": item.get("runtimes", {}) or {},
            "labels": {},
            "dynamic_state": state,
            "trust": {"max_effect_class": "sandboxed"},
            "capability_valid_until": item.get("capability_valid_until", ""),
            "cordoned": item.get("cordoned", False),
        })
    return devices
