"""Decision transaction state machine (AADM §3).

决策事务是 Agent 的最小闭环交互单元：触发 → 读取状态 → 理解目标 → 检查
约束 → 产生候选行动 → 选择行动 → 改变状态 → 获取证据 → 提交或回滚。

状态机：draft → proposed → authorized → executing → observed → verified
（终态），或任何非终态 → failed → rolled_back（终态）。非法迁移在进入
执行前 fail closed——"修改权限+创建文件+发通知+记审计"不是最小事务，
应拆分后经依赖组合（见 hypergraph）。

纯函数 + 轻量 dataclass，可单测。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# 合法迁移表（AADM §3 DT.status）。
TRANSITIONS = {
    "draft": {"proposed"},
    "proposed": {"authorized", "failed"},
    "authorized": {"executing", "failed"},
    "executing": {"observed", "failed"},
    "observed": {"verified", "failed"},
    "verified": set(),
    "failed": {"rolled_back"},
    "rolled_back": set(),
}

TERMINAL = {"verified", "rolled_back"}


@dataclass
class DecisionTransaction:
    """一个决策事务：单一目标、单一状态变化、可独立验证与补偿。"""
    id: str
    goal: str
    guard_atom_ids: list = field(default_factory=list)      # 执行前必须成立
    candidate_actions: list = field(default_factory=list)   # 候选行动
    selected_action: str = ""
    write_set: list = field(default_factory=list)           # 预期改变的状态
    expected_evidence: list = field(default_factory=list)   # 证明义务
    compensation: str = ""                                  # 失败回退
    status: str = "draft"


def can_transition(current: str, next_state: str) -> bool:
    return next_state in TRANSITIONS.get(current, set())


def transition(tx: DecisionTransaction, next_state: str) -> None:
    """状态迁移；非法迁移抛 ValueError（fail closed）。"""
    if next_state not in TRANSITIONS.get(tx.status, set()):
        raise ValueError(
            f"transaction {tx.id}: 非法迁移 {tx.status} → {next_state}"
            f"（允许: {sorted(TRANSITIONS.get(tx.status, set())) or '终态'}）")
    tx.status = next_state


def is_terminal(state: str) -> bool:
    return state in TERMINAL


def verify_transaction(tx: DecisionTransaction, evidence: dict) -> bool:
    """验证义务：所有 expected_evidence 必须在 evidence 中有支持。"""
    return all(item in evidence for item in tx.expected_evidence)


def validate_minimality(tx: DecisionTransaction) -> list:
    """最小事务六条标准（AADM §3）：单目标/单状态变化/单责任/可验证/
    可重试补偿。返回违规列表。"""
    violations = []
    if not tx.goal:
        violations.append("缺少单一主要目标")
    if len(tx.write_set) > 1:
        violations.append(f"write_set 含 {len(tx.write_set)} 项状态变化"
                          "——应拆分为多个事务经依赖组合")
    if not tx.expected_evidence:
        violations.append("缺少验证义务（expected_evidence）")
    if not tx.compensation and tx.write_set:
        violations.append("有状态变化但无补偿方案")
    return violations
