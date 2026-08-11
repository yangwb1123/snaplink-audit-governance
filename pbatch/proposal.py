"""Multi-agent proposals & combination benefit (AADM §25, AADM-G §12).

多 Agent 不应自由聊天后凭感觉合并：每个 Agent 提交标准化提案（findings/
assumptions/risks/affected_contracts/conflicts）；协调器按任务图处理依赖、
冲突、所有权与证据完整性。是否拆分由收益公式决定：

MultiAgentBenefit = ParallelBenefit + SpecializationBenefit
                  + IndependentReviewBenefit − CoordinationCost
                  − ContextSyncCost − MergeRisk

只有收益为正才拆分；否则一个 Agent 的多个内部视角更好。
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class AgentProposal:
    """一个 Agent 的标准化提案（AADM §25）。"""
    agent_id: str
    findings: list = field(default_factory=list)
    assumptions: list = field(default_factory=list)
    proposed_plan: str = ""
    affected_contracts: list = field(default_factory=list)
    risks: list = field(default_factory=list)
    required_evidence: list = field(default_factory=list)
    conflicts: list = field(default_factory=list)


def detect_conflicts(proposals: list) -> list:
    """提案冲突：同一契约的不同方案 + 相互矛盾的事实主张。"""
    conflicts = []
    contracts = {}
    for proposal in proposals:
        for contract in proposal.affected_contracts:
            contracts.setdefault(contract, []).append(proposal.agent_id)
    for contract, agents in contracts.items():
        if len(set(agents)) > 1:
            conflicts.append({
                "type": "contract_disagreement",
                "contract": contract,
                "agents": sorted(set(agents)),
                "condition": f"多 Agent 对契约 {contract} 提出不同方案",
            })
    for index, proposal in enumerate(proposals):
        for other in proposals[index + 1:]:
            shared_risks = (set(proposal.risks)
                            & set(other.risks))
            shared_findings = (set(proposal.findings)
                               & set(other.findings))
            if shared_findings and not shared_risks:
                conflicts.append({
                    "type": "finding_without_shared_risk",
                    "agents": [proposal.agent_id, other.agent_id],
                    "condition": "相同发现但风险认知不一致",
                })
    return conflicts


def merge_proposals(proposals: list) -> dict:
    """汇总：findings/risks 去重合并（不裁决，裁决交给 gate）。"""
    findings, risks, evidence = [], [], []
    seen_findings, seen_risks, seen_evidence = set(), set(), set()
    for proposal in proposals:
        for finding in proposal.findings:
            if finding not in seen_findings:
                seen_findings.add(finding)
                findings.append(finding)
        for risk in proposal.risks:
            if risk not in seen_risks:
                seen_risks.add(risk)
                risks.append(risk)
        for item in proposal.required_evidence:
            if item not in seen_evidence:
                seen_evidence.add(item)
                evidence.append(item)
    return {"findings": findings, "risks": risks,
            "required_evidence": evidence,
            "proposal_count": len(proposals)}


def agent_benefit(parallel_benefit: float = 0.0,
                  specialization: float = 0.0,
                  independent_review: float = 0.0,
                  coordination_cost: float = 0.0,
                  context_sync_cost: float = 0.0,
                  merge_risk: float = 0.0) -> dict:
    """MultiAgentBenefit 公式（>0 才拆分）。"""
    benefit = (parallel_benefit + specialization + independent_review
               - coordination_cost - context_sync_cost - merge_risk)
    return {"benefit": round(benefit, 3),
            "should_split": benefit > 0,
            "breakdown": {"parallel": parallel_benefit,
                          "specialization": specialization,
                          "independent_review": independent_review,
                          "coordination": -coordination_cost,
                          "context_sync": -context_sync_cost,
                          "merge_risk": -merge_risk}}


def proposal_main(argv: Optional[list] = None) -> int:
    """`pi-batch proposal --input FILE [--json]`：提案冲突检测 + 合并 +
    收益公式。"""
    parser = argparse.ArgumentParser(
        prog="pi-batch.py proposal",
        description="Multi-agent proposals & combination benefit (AADM §25)")
    parser.add_argument("--input", required=True,
                        help="JSON: {proposals: [{agent_id, findings, "
                             "assumptions, proposed_plan, "
                             "affected_contracts, risks, "
                             "required_evidence, conflicts}], benefit: {...}}")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        data = json.loads(__import__("pathlib").Path(
            args.input).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        raise SystemExit(2)
    proposals = [AgentProposal(**item) for item in data.get("proposals", [])]
    conflicts = detect_conflicts(proposals)
    merged = merge_proposals(proposals)
    benefit = agent_benefit(**(data.get("benefit", {}) or {}))
    report = {"conflicts": conflicts, "merged": merged,
              "benefit": benefit}
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"proposals: {len(proposals)} | conflicts: {len(conflicts)}")
        for conflict in conflicts:
            print(f"  ⚠ {conflict['type']}: {conflict['condition']}")
        print(f"benefit: {benefit['benefit']} → "
              f"{'拆分多 Agent' if benefit['should_split'] else '单 Agent 多视角'}")
    raise SystemExit(0)
