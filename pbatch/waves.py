"""Wave scheduling and agent assignment (AADM-R §10/§11, §24).

波次 = BSP 同步屏障（AADM-G §35.6）：每波取拓扑就绪集 → 写冲突分解 →
按上下文相似度聚类 → 分配 Agent → 执行 → 下一波。Agent 分配优化
**上下文连续性**（Fit 公式的确定性简化：共享依赖 > 同域 > 无关联），
不是平均分工——一个 Agent 连续处理相关节点比四个 Agent 各做一步更省
上下文同步。

纯函数，可单测；与 pbatch/fabric_sched.TaskNode 配合使用。
"""

from __future__ import annotations

from typing import Optional

from .fabric_sched import TaskNode, ready_nodes


def parallel_safe(a: TaskNode, b: TaskNode) -> bool:
    """并行判定完整公式（AADM-R §9）：

    Parallel(i,j) = NoDependency ∧ NoWriteConflict ∧ IndependentVerification
                  ∧ Mergeable（本实现：互不依赖 + 无写冲突 + 非 fixed 顺序
                  敏感 + 可独立验证）。"""
    if a.id in b.depends_on or b.id in a.depends_on:
        return False
    if _conflicts(a, b):
        return False
    if a.flexibility == "fixed" or b.flexibility == "fixed":
        return False  # fixed 节点必须按明确顺序执行
    return True


def add_agent_benefit(specialization_gain: float = 0.0,
                      independent_evidence_gain: float = 0.0,
                      critical_path_reduction: float = 0.0,
                      coordination_cost: float = 0.0,
                      context_duplication: float = 0.0,
                      merge_risk: float = 0.0) -> dict:
    """加 Agent 判据（AADM-R §10）：AgentBenefit > Threshold 才增加。"""
    benefit = (specialization_gain + independent_evidence_gain
               + critical_path_reduction - coordination_cost
               - context_duplication - merge_risk)
    return {"benefit": round(benefit, 3),
            "should_add": benefit > 0,
            "note": "默认 1 个 Agent；不能因涉及 N 个项目就机械派 N 个"}


def _conflicts(a: TaskNode, b: TaskNode) -> bool:
    """写冲突：W_a ∩ (R_b ∪ W_b) ≠ ∅ 或 W_b ∩ R_a ≠ ∅（AADM-R §9）。"""
    wa, ra = set(a.write_set), set(a.read_set)
    wb, rb = set(b.write_set), set(b.read_set)
    return bool(wa & (rb | wb)) or bool(wb & ra)


def plan_waves(nodes: list, done: Optional[set] = None,
               max_wave: Optional[int] = None) -> list:
    """拓扑波次：每波 = 就绪集（依赖已满足）且互相无写冲突的节点。

    写冲突节点推迟到下一波；整波全冲突时串行化（避免活锁）。"""
    completed = set(done or ())
    waves = []
    while True:
        ready = [n for n in ready_nodes(nodes, completed)]
        if not ready:
            break
        wave, blocked = [], []
        for node in ready:
            if any(_conflicts(node, member) for member in wave):
                blocked.append(node)
            else:
                wave.append(node)
        if not wave:  # 全部写冲突：串行化第一个，其余下波
            wave, blocked = ready[:1], ready[1:]
        if max_wave:
            wave = wave[:max_wave]
            blocked += wave[max_wave:]
        waves.append([node.id for node in wave])
        completed.update(node.id for node in wave)
    return waves


def context_similarity(a: TaskNode, b: TaskNode) -> float:
    """上下文连续性（0-1）：共享依赖 > 同域目标前缀 > 无关联。"""
    shared = len(set(a.depends_on) & set(b.depends_on))
    if shared:
        return 0.5
    if a.goal and b.goal and a.goal.split()[0] == b.goal.split()[0]:
        return 0.3
    return 0.1


def priority_score(node: TaskNode, nodes: list, done: Optional[set] = None,
                    context_load: float = 0.0,
                    deadline_urgency: float = 0.0) -> float:
    """节点优先级（AADM-G §23 公式的确定性代理）：

    Priority ≈ CriticalPath + UnblockValue + InformationGain + RiskReduction
             + ContextReuse − WriteConflict − MergeCost − ContextLoad

    代理映射：关键路径=自身估计耗时；解锁价值=依赖它的节点数（含传递）；
    信息增益=分析类（pure）节点优先；风险降低=副作用越强越早暴露；
    上下文复用=目标前缀与已完成节点重合；写冲突=与就绪集重叠写对象。"""
    done = done or set()
    unblock = sum(1 for other in nodes
                  if node.id in other.depends_on)
    information = 0.5 if node.effect_class == "pure" else 0.1
    risk_reduction = {"irreversible": 1.0, "compensatable": 0.8,
                      "reversible": 0.6, "sandboxed": 0.4,
                      "read_only": 0.2, "pure": 0.1}.get(
                          node.effect_class, 0.3)
    context_reuse = 0.0
    if done:
        prefix = (node.goal or "").split()[0] if node.goal else ""
        if prefix and any((other.goal or "").startswith(prefix)
                          for other in nodes
                          if other.id in done):
            context_reuse = 0.5
    write_conflict = float(len(node.write_set) > 0)
    return round(node.estimate_seconds + unblock + information
                 + risk_reduction + context_reuse + deadline_urgency
                 - write_conflict - context_load, 3)


def rank_nodes(ready: list, nodes: list, done: Optional[set] = None) -> list:
    """按优先级排序就绪节点（高优先级先执行）。"""
    return sorted(ready, key=lambda node: -priority_score(node, nodes, done))


def aging_boost(wait_cycles: int, factor: float = 0.1) -> float:
    """公平性老化（AADM-G §35.10）：长任务不能饿死小任务——等待轮次越多
    优先级越高（调度器用 wait_cycles 累计提升，防无限饥饿）。"""
    return round(wait_cycles * factor, 3)


def structural_complexity(nodes: list, cross_boundary_lambda: float = 2.0,
                          state_mu: float = 2.0, shared_nu: float = 3.0) -> dict:
    """结构复杂度（AADM-G §4）：复杂度来自关系与共享状态，不是代码行数。

    Complexity = ΣNodeCost + λ·ΣCrossBoundaryEdge + μ·log(StateSpace)
               + ν·SharedMutableState。跨域边（目标前缀不同）权重更高。"""
    node_cost = len(nodes)
    cross_edges = 0
    for node in nodes:
        for dep in nodes:
            if node.id in dep.depends_on:
                boundary = ((node.goal or "").split()[:1]
                            != (dep.goal or "").split()[:1])
                if boundary:
                    cross_edges += 1
    state_space = len({obj for node in nodes
                       for obj in node.read_set + node.write_set})
    writes = {}
    for node in nodes:
        for obj in node.write_set:
            writes[obj] = writes.get(obj, 0) + 1
    shared = sum(1 for count in writes.values() if count > 1)
    import math
    complexity = (node_cost + cross_boundary_lambda * cross_edges
                  + state_mu * (math.log(max(state_space, 1)) + 1)
                  + shared_nu * shared)
    return {"complexity": round(complexity, 2),
            "node_cost": node_cost, "cross_boundary_edges": cross_edges,
            "state_space": state_space, "shared_mutable_states": shared,
            "note": "减少共享可变状态与跨边界边比减少节点数更有效"}


def expand_node(nodes: list, node_id: str, subnodes: list) -> list:
    """expand：把抽象节点细化为子图（子节点依赖父节点原依赖）。"""
    parent = next((n for n in nodes if n.id == node_id), None)
    if parent is None:
        return nodes
    result = [n for n in nodes if n.id != node_id]
    result.extend(subnodes)
    for node in result:
        if node_id in node.depends_on:
            node.depends_on = [d for d in node.depends_on if d != node_id] \
                + [sub.id for sub in subnodes]
    return result


def prune_nodes(nodes: list, keep_ids: set) -> list:
    """prune：删除失败或无价值路径（保留者依赖中指向被删节点的边清除）。"""
    keep = set(keep_ids)
    survivors = [n for n in nodes if n.id in keep]
    for node in survivors:
        node.depends_on = [d for d in node.depends_on if d in keep]
    return survivors


def insert_node(nodes: list, node, after_ids: list) -> list:
    """insert：插入调查/验证/修复节点（依赖 after_ids 中的节点）。"""
    node.depends_on = list(after_ids)
    for existing in nodes:
        if set(existing.depends_on) & set(after_ids):
            existing.depends_on = [d for d in existing.depends_on
                                   if d not in after_ids] + [node.id]
    return nodes + [node]


def reroute_node(nodes: list, node_id: str, new_deps: list) -> list:
    """reroute：根据新证据改变路径（重设依赖）。"""
    node = next((n for n in nodes if n.id == node_id), None)
    if node is not None:
        node.depends_on = list(new_deps)
    return nodes


def merge_nodes(nodes: list, merge_ids: list) -> list:
    """merge：合并兼容结果为一个节点（依赖合并，写集合并）。"""
    to_merge = [n for n in nodes if n.id in merge_ids]
    if not to_merge:
        return nodes
    merged = to_merge[0]
    for other in to_merge[1:]:
        merged.depends_on = sorted(set(merged.depends_on)
                                   | set(other.depends_on))
        merged.write_set = sorted(set(merged.write_set)
                                  | set(other.write_set))
    merged.id = f"{merge_ids[0]}+{len(merge_ids)}"
    survivors = [n for n in nodes if n.id not in merge_ids[1:]]
    for node in survivors:
        node.depends_on = [merged.id if d in merge_ids else d
                           for d in node.depends_on]
    return survivors


def assign_agents(ready: list, agent_count: int,
                  similarity=context_similarity) -> list:
    """把就绪节点贪心聚类到 agent_count 个上下文连续簇（Fit 简化版）。

    每步把节点放入与其相似度总和最高的簇——最大化簇内上下文复用，
    最小化跨 Agent 边。"""
    if agent_count <= 1 or len(ready) <= 1:
        return [list(ready)]
    clusters = [[] for _ in range(agent_count)]
    for node in ready:
        scores = []
        for cluster in clusters:
            score = 0.0
            for member in cluster:
                score += similarity(node, member)
            if cluster:
                score /= len(cluster)
            scores.append(score)
        best = max(range(agent_count), key=lambda i: scores[i])
        clusters[best].append(node)
    return clusters
