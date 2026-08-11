"""Node contracts (AADM-G §3): assume-guarantee composition.

每个节点声明 assumptions（执行前必须成立）/ guarantees（执行后保证成立）
/ invariants（不可破坏）/ forbidden_states / failure_semantics；组合时
检查**上游 guarantees ⊆ 下游 assumptions**。这种方式比"几个 Agent 商量
一下"可靠得多——契约不满足在调度前显式暴露，而不是执行中静默失败。

与 pbatch/fabric_sched.TaskNode 配合：TaskNode.assumptions/guarantees
直接参与检查。纯函数，可单测。
"""

from __future__ import annotations

from dataclasses import dataclass, field

FAILURE_SEMANTICS = ("atomic", "retryable", "compensatable", "best_effort",
                     "irreversible")


@dataclass
class NodeContract:
    """节点契约（独立于 TaskNode 的谓词视图，也可直接注解 TaskNode）。"""
    node_id: str
    assumptions: list = field(default_factory=list)      # 执行前成立
    guarantees: list = field(default_factory=list)       # 执行后保证
    invariants: list = field(default_factory=list)       # 全程不破坏
    forbidden_states: list = field(default_factory=list)
    failure_semantics: str = "atomic"
    depends_on: list = field(default_factory=list)


def compose_check(upstream: NodeContract, downstream: NodeContract) -> list:
    """上游 guarantees 是否满足下游 assumptions；返回未满足项。"""
    missing = [a for a in downstream.assumptions
               if a not in upstream.guarantees]
    return [{"downstream": downstream.node_id, "upstream": upstream.node_id,
             "assumption": a, "reason": "upstream guarantee missing"}
            for a in missing]


def _contract_of(node) -> NodeContract:
    """从 TaskNode 或 NodeContract 提取契约视图（鸭子类型）。"""
    if isinstance(node, NodeContract):
        return node
    return NodeContract(
        node_id=node.id, assumptions=list(node.assumptions),
        guarantees=list(node.guarantees),
        depends_on=list(node.depends_on))


def check_graph(nodes: list) -> list:
    """全图组合检查：每条依赖边做 compose_check，返回全部违规。"""
    by_id = {node.id: node for node in nodes}
    violations = []
    for node in nodes:
        contract = _contract_of(node)
        for dep_id in contract.depends_on:
            upstream = by_id.get(dep_id)
            if upstream is None:
                violations.append({"downstream": contract.node_id,
                                   "upstream": dep_id,
                                   "assumption": "",
                                   "reason": "unknown dependency"})
                continue
            violations += compose_check(_contract_of(upstream), contract)
    return violations


def validate_semantics(contract: NodeContract) -> list:
    """failure_semantics 合法性与 forbidden/invariants 基本一致性。"""
    problems = []
    if contract.failure_semantics not in FAILURE_SEMANTICS:
        problems.append(f"node '{contract.node_id}': invalid failure_semantics "
                        f"{contract.failure_semantics!r}")
    overlap = set(contract.forbidden_states) & set(contract.invariants)
    for state in sorted(overlap):
        problems.append(f"node '{contract.node_id}': '{state}' 同时是不变量"
                        "与禁止状态，契约自相矛盾")
    return problems
