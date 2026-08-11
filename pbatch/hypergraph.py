"""Typed property hypergraph (AADM §5): 类型化属性超图.

一个决策通常需要多个输入共同成立（用户具有审批权限 + 文件处于待审批状态
+ 审批单未被占用 + 意见合法 → 允许执行审批事务）——这不是单条边，而是
多个条件共同约束一个状态变化，因此用**超图**：relation.from 可以是多个
节点。

节点类型：atom（认知原子）/ transaction / capability / artifact / rule /
agent / evidence。关系类型：contains / depends_on / conflicts_with /
supports / refines / guards / realizes / verifies / compensates /
alternative_to / owned_by。

能力：
- 结构校验（端点存在/自环/空端点/类型合法）→ fail closed
- guards 求值：一个节点的所有 guard 前置必须同时成立
- 依赖环检测（活锁/死锁预防）
- 冲突显式暴露（conflicts_with，不静默忽略）
- 级联失效（沿 depends_on 传播，真值维护语义）

Usage:
    pi-batch graph validate --input graph.json
    pi-batch graph deps    --input graph.json --node approve
    pi-batch graph cycles  --input graph.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

NODE_TYPES = ("atom", "transaction", "capability", "artifact", "rule",
              "agent", "evidence")
RELATION_TYPES = ("contains", "depends_on", "conflicts_with", "supports",
                  "refines", "guards", "realizes", "verifies",
                  "compensates", "alternative_to", "owned_by")
# 需要方向语义的关系：其余视为无向/说明性。
DIRECTED = ("depends_on", "guards", "verifies", "realizes", "refines",
            "contains", "supports", "compensates", "owned_by")


@dataclass
class GraphNode:
    """超图节点：原子/事务/能力/产物/规则/Agent/证据。"""
    id: str
    type: str = "atom"
    payload: dict = field(default_factory=dict)
    version: int = 1

    def to_dict(self) -> dict:
        return {"id": self.id, "type": self.type,
                "payload": self.payload, "version": self.version}


@dataclass
class GraphRelation:
    """超边：from 可以是多个节点（多条件共同约束一个状态变化）。"""
    id: str
    from_ids: list
    to_ids: list
    type: str = "depends_on"
    condition: str = ""
    weight: float = 1.0
    confidence: float = 1.0

    def to_dict(self) -> dict:
        return {"id": self.id, "from": list(self.from_ids),
                "to": list(self.to_ids), "type": self.type,
                "condition": self.condition, "weight": self.weight,
                "confidence": self.confidence}


class Hypergraph:
    """类型化属性超图：节点 + 超边，结构校验与图算法。"""

    def __init__(self):
        self.nodes: dict = {}
        self.relations: list = []

    # -- 构建 --------------------------------------------------------------

    def add_node(self, node_id: str, node_type: str = "atom",
                 payload: Optional[dict] = None, version: int = 1) -> GraphNode:
        if node_type not in NODE_TYPES:
            raise ValueError(f"invalid node type {node_type!r} "
                             f"(must be one of {NODE_TYPES})")
        node = GraphNode(id=node_id, type=node_type,
                         payload=dict(payload or {}), version=version)
        self.nodes[node_id] = node
        return node

    def add_relation(self, from_ids: list, to_ids: list,
                     rel_type: str = "depends_on", condition: str = "",
                     weight: float = 1.0, confidence: float = 1.0,
                     rel_id: str = "") -> GraphRelation:
        if rel_type not in RELATION_TYPES:
            raise ValueError(f"invalid relation type {rel_type!r} "
                             f"(must be one of {RELATION_TYPES})")
        if not from_ids or not to_ids:
            raise ValueError("hyperedge needs at least one from and one to")
        relation = GraphRelation(
            id=rel_id or f"r{len(self.relations) + 1}",
            from_ids=list(from_ids), to_ids=list(to_ids),
            type=rel_type, condition=condition, weight=weight,
            confidence=confidence)
        self.relations.append(relation)
        return relation

    # -- 校验（fail closed） ------------------------------------------------

    def validate(self) -> list:
        """结构违规：未知端点 / 自环 / 空端点 / 类型不合法。"""
        violations = []
        for relation in self.relations:
            if relation.type not in RELATION_TYPES:
                violations.append(f"relation '{relation.id}': invalid type "
                                  f"{relation.type!r}")
            for endpoint in relation.from_ids + relation.to_ids:
                if endpoint not in self.nodes:
                    violations.append(
                        f"relation '{relation.id}': unknown endpoint "
                        f"'{endpoint}'")
            for node_id in relation.from_ids + relation.to_ids:
                if node_id in relation.from_ids and node_id in relation.to_ids:
                    violations.append(
                        f"relation '{relation.id}': self-loop on '{node_id}'")
            if not relation.from_ids or not relation.to_ids:
                violations.append(f"relation '{relation.id}': empty endpoints")
        return violations

    # -- 查询 --------------------------------------------------------------

    def dependencies(self, node_id: str) -> list:
        """depends_on 超边上的全部直接依赖（多输入共同成立）。"""
        deps = []
        for relation in self.relations:
            if relation.type == "depends_on" and node_id in relation.to_ids:
                deps.append({"relation": relation.id,
                             "from": list(relation.from_ids),
                             "condition": relation.condition})
        return deps

    def guards(self, node_id: str) -> list:
        """guards 关系：执行前必须成立的前置条件集合。"""
        return [relation for relation in self.relations
                if relation.type == "guards" and node_id in relation.to_ids]

    def guards_satisfied(self, node_id: str, satisfied: set) -> bool:
        """所有 guard 前置同时成立（超图语义：多条件共同约束）。"""
        guard_relations = self.guards(node_id)
        if not guard_relations:
            return True
        return all(set(relation.from_ids) <= set(satisfied)
                   for relation in guard_relations)

    def conflicts(self) -> list:
        """显式暴露冲突（不静默忽略）：conflicts_with 关系。"""
        return [{"relation": relation.id,
                 "between": list(relation.from_ids + relation.to_ids),
                 "condition": relation.condition}
                for relation in self.relations
                if relation.type == "conflicts_with"]

    def alternative_to(self, node_id: str) -> list:
        """替代方案（OR 语义）。"""
        return [relation.from_ids + relation.to_ids
                for relation in self.relations
                if relation.type == "alternative_to" and node_id in (
                    relation.from_ids + relation.to_ids)]

    # -- 图算法 ------------------------------------------------------------

    def depends_on_cycles(self) -> list:
        """依赖环检测（DFS；预防死锁/活锁）。返回每个环的节点序列。"""
        visiting, visited, stack = set(), set(), []
        cycles = []

        def visit(node_id: str) -> None:
            if node_id in visited:
                return
            if node_id in visiting:
                start = stack.index(node_id)
                cycles.append(list(stack[start:]) + [node_id])
                return
            visiting.add(node_id)
            stack.append(node_id)
            for relation in self.relations:
                if relation.type == "depends_on" and node_id in relation.from_ids:
                    for target in relation.to_ids:
                        if target in self.nodes:
                            visit(target)
            stack.pop()
            visiting.discard(node_id)
            visited.add(node_id)

        for node_id in self.nodes:
            visit(node_id)
        return cycles

    def cascade_invalidate(self, node_id: str) -> list:
        """前提失效 → 沿 depends_on 传播（真值维护 §20）。返回受影响集。"""
        affected = []
        pending = [node_id]
        seen = set()
        while pending:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            affected.append(current)
            for relation in self.relations:
                if (relation.type == "depends_on"
                        and current in relation.from_ids):
                    for target in relation.to_ids:
                        if target not in seen:
                            pending.append(target)
        return sorted(affected)

    # -- 序列化 ------------------------------------------------------------

    def to_dict(self) -> dict:
        return {"nodes": [node.to_dict() for node in self.nodes.values()],
                "relations": [r.to_dict() for r in self.relations]}

    @classmethod
    def from_dict(cls, data: dict) -> "Hypergraph":
        graph = cls()
        for node in data.get("nodes", []):
            graph.add_node(node["id"], node.get("type", "atom"),
                           node.get("payload", {}),
                           int(node.get("version", 1)))
        for relation in data.get("relations", []):
            graph.add_relation(relation.get("from", []),
                               relation.get("to", []),
                               relation.get("type", "depends_on"),
                               relation.get("condition", ""),
                               float(relation.get("weight", 1.0)),
                               float(relation.get("confidence", 1.0)),
                               rel_id=relation.get("id", ""))
        return graph


def _graph_cmd_validate(graph: Hypergraph, args) -> int:
    """结构校验（fail closed）：违规列表 + 退出码。"""
    violations = graph.validate()
    if args.json:
        print(json.dumps({"valid": not violations,
                          "violations": violations}, indent=2))
    else:
        if violations:
            print("GRAPH INVALID:")
            for violation in violations:
                print(f"  - {violation}")
        else:
            print(f"graph valid: {len(graph.nodes)} nodes, "
                  f"{len(graph.relations)} relations")
    raise SystemExit(1 if violations else 0)


def _graph_cmd_cycles(graph: Hypergraph, args) -> int:
    """依赖环检测（死锁/活锁预防）。"""
    cycles = graph.depends_on_cycles()
    print(json.dumps(cycles, indent=2) if args.json
          else ("no cycles" if not cycles
                else "cycles: " + "; ".join("→".join(c) for c in cycles)))
    raise SystemExit(1 if cycles else 0)


def _graph_cmd_deps(graph: Hypergraph, args) -> int:
    """节点依赖与 guards（超图多输入）。"""
    node_id = args.node or (next(iter(graph.nodes), ""))
    if node_id not in graph.nodes:
        print(f"unknown node {node_id!r}", file=sys.stderr)
        raise SystemExit(2)
    report = {"node": node_id, "dependencies": graph.dependencies(node_id),
              "guards": [{"relation": r.id, "from": r.from_ids,
                          "condition": r.condition}
                         for r in graph.guards(node_id)]}
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"# {node_id}")
        for dep in report["dependencies"]:
            print(f"  depends_on {dep['from']}"
                  + (f" when {dep['condition']}" if dep["condition"] else ""))
        for guard in report["guards"]:
            print(f"  guards: {guard['from']}"
                  + (f" when {guard['condition']}" if guard["condition"] else ""))
    raise SystemExit(0)


def _graph_cmd_summary(graph: Hypergraph, args) -> int:
    """节点/关系类型统计 + 冲突/环计数。"""
    from collections import Counter
    report = {"nodes": dict(Counter(n.type for n in graph.nodes.values())),
              "relations": dict(Counter(r.type for r in graph.relations)),
              "conflicts": len(graph.conflicts()),
              "cycles": len(graph.depends_on_cycles())}
    print(json.dumps(report, ensure_ascii=False, indent=2)
          if args.json else "\n".join(
              f"{key}: {value}" for key, value in report.items()))
    raise SystemExit(0)


def extract_module_graph(directory: str = "pbatch") -> Hypergraph:
    """从 Python 源码提取真实模块依赖（只统计模块级 import——函数内惰性
    导入不算边，避免把良性模式误报为循环）。构建超图并校验。"""
    graph = Hypergraph()
    root = Path(directory)
    modules = {}
    for path in sorted(root.glob("*.py")):
        if path.name.startswith("__"):
            continue
        node_id = path.stem
        modules[node_id] = path
        graph.add_node(node_id, "artifact",
                       payload={"file": path.name})
    for node_id, path in modules.items():
        in_def = False
        for line in path.read_text(encoding="utf-8",
                                   errors="replace").splitlines():
            stripped = line.strip()
            if stripped.startswith(("def ", "class ", "async def ")):
                in_def = True
                continue
            if in_def:
                continue
            match = re.match(r"^from \.(\w+) import", stripped)
            if match and match.group(1) in modules and match.group(1) != node_id:
                graph.add_relation([node_id], [match.group(1)], "depends_on")
    return graph


GRAPH_OPS = [
    ("expand", "抽象节点细化为子图", "hypergraph 节点分解；campaign 方向展开"),
    ("collapse", "已验证子图折叠成能力", "--reuse 复用已验证输出（fingerprint 匹配）"),
    ("replace", "更合适的实现替换节点", "pipeline 阶段替换 / meta 重新选角色"),
    ("prune", "删除失败或无价值路径", "失败签名拒绝 + racing 逐轮淘汰弱方案"),
    ("insert", "插入调查/验证/修复节点", "gate_fix 自动修复循环（sverp 沉淀）"),
    ("reroute", "根据新证据改变路径", "meta 每轮从产物清单重建上下文重新规划"),
    ("merge", "合并兼容结果", "aggregate 多上游证据合并"),
]


def _graph_cmd_extract(args) -> None:
    """从 Python 源码提取模块依赖图并报告（含环检测）。"""
    graph = extract_module_graph(args.input)
    violations = graph.validate()
    cycles = graph.depends_on_cycles()
    args._cycles = cycles
    report = {"nodes": len(graph.nodes),
              "relations": len(graph.relations),
              "module_level_cycles": [list(c) for c in cycles],
              "valid": not violations}
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    for key, value in report.items():
        if key != "module_level_cycles":
            print(f"{key}: {value}")
    print(f"cycles: {'; '.join('→'.join(c) for c in cycles)}"
          if cycles else "cycles: none")


def _print_graph_ops(args) -> None:
    """`graph ops`：七种动态图操作的确定性语义映射（AADM-R §8）。"""
    report = [{"op": op, "meaning": meaning, "implementation": impl}
              for op, meaning, impl in GRAPH_OPS]
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    print("# 动态图七操作 → 仓库实现（AADM-R §8）")
    for op, meaning, impl in GRAPH_OPS:
        print(f"{op:<10} {meaning} → {impl}")


def graph_main(argv: Optional[list] = None) -> int:
    """`pi-batch graph validate|deps|cycles|summary --input FILE`。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py graph",
        description="Typed property hypergraph (AADM §5)")
    sub = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (
            ("validate", "结构校验（端点/自环/类型，fail closed）"),
            ("deps", "列出节点依赖与 guards"),
            ("cycles", "依赖环检测"),
            ("summary", "节点/关系统计"),
            ("extract", "从 Python 源码提取模块依赖图（只统计模块级导入）")):
        p = sub.add_parser(name, help=help_text)
        p.add_argument("--input", required=True)
        p.add_argument("--node", default="")
        p.add_argument("--json", action="store_true")
    p_ops = sub.add_parser("ops", help="动态图七操作 → 仓库实现映射（AADM-R §8）")
    p_ops.add_argument("--json", action="store_true")

    args = parser.parse_args(argv)
    if args.command == "ops":
        _print_graph_ops(args)
        raise SystemExit(0)
    if args.command == "extract":
        _graph_cmd_extract(args)
        raise SystemExit(1 if args._cycles else 0)
    try:
        graph = Hypergraph.from_dict(
            json.loads(Path(args.input).read_text(encoding="utf-8")))
    except (OSError, ValueError, KeyError) as exc:
        print(f"invalid graph input: {exc}", file=sys.stderr)
        raise SystemExit(2)
    violations = graph.validate()
    command = {
        "validate": _graph_cmd_validate,
        "cycles": _graph_cmd_cycles,
        "deps": _graph_cmd_deps,
        "summary": _graph_cmd_summary,
    }.get(args.command)
    if command is None:
        print(f"unknown command {args.command!r}", file=sys.stderr)
        raise SystemExit(2)
    return command(graph, args)
