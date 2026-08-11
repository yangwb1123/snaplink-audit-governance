"""Multi-agent independence (AADM-G §12): diversity score.

三个相同模型、相同 Prompt、相同上下文的 Agent 很可能产生相同错误——
那不是三份独立证据，而是一个错误被复制三次。真正的独立性来自不同模型/
方法/数据/Oracle。

Diversity = ModelDiversity + MethodDiversity + DataDiversity + OracleDiversity
          − SharedFailureCorrelation

纯函数：agents 是 {model, method, data, oracle} 列表，返回 0~1 分数与
各维度明细。共享同一 model+method 的 Agent 对计入相关错误风险。
"""

from __future__ import annotations

import json
from typing import Optional


def _dimension_diversity(agents: list, key: str) -> float:
    """某维度成对差异占比：不同取值的 Agent 对 / 总对（相同=0，全异=1）。"""
    if len(agents) < 2:
        return 0.0
    values = [str(agent.get(key, "")) or "?" for agent in agents]
    differing = 0
    for i in range(len(values)):
        for j in range(i + 1, len(values)):
            if values[i] != values[j]:
                differing += 1
    pairs = len(values) * (len(values) - 1) / 2
    return round(differing / pairs, 3)


def shared_failure_correlation(agents: list) -> float:
    """共享同一 model 且同一 method 的 Agent 对比例（相关错误风险）。"""
    if len(agents) < 2:
        return 0.0
    correlated = 0
    for i in range(len(agents)):
        for j in range(i + 1, len(agents)):
            same_model = (agents[i].get("model") == agents[j].get("model"))
            same_method = (agents[i].get("method") == agents[j].get("method"))
            if same_model and same_method:
                correlated += 1
    pairs = len(agents) * (len(agents) - 1) / 2
    return round(correlated / pairs, 3)


def diversity_score(agents: list) -> dict:
    """多 Agent 独立性评分（0~1；AADM-G §12 公式）。"""
    if not agents:
        return {"score": 0.0, "note": "无 Agent 可评估"}
    model = _dimension_diversity(agents, "model")
    method = _dimension_diversity(agents, "method")
    data = _dimension_diversity(agents, "data")
    oracle = _dimension_diversity(agents, "oracle")
    correlation = shared_failure_correlation(agents)
    score = max(0.0, min(1.0, model + method + data + oracle
                         - correlation))
    return {
        "score": round(score, 3),
        "dimensions": {"model": model, "method": method,
                       "data": data, "oracle": oracle},
        "shared_failure_correlation": correlation,
        "adequate_independence": score >= 1.0,
        "note": "相同模型+方法的多 Agent 是复制错误，不是多份证据",
    }


def diversity_main(argv: Optional[list] = None) -> int:
    """`pi-batch diversity --json '[...]'`：评估多 Agent 独立性。"""
    import argparse
    parser = argparse.ArgumentParser(
        prog="pi-batch.py diversity",
        description="Multi-agent independence score (AADM-G §12)")
    parser.add_argument(
        "agents_json",
        help='JSON 列表：[{"model":"pi","method":"implement","data":"repo",'
             '"oracle":"tests"}, ...]')
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        agents = json.loads(args.agents_json)
    except ValueError as exc:
        print(f"invalid agents JSON: {exc}", file=sys.stderr)
        raise SystemExit(2)
    if not isinstance(agents, list):
        print("agents must be a JSON list", file=sys.stderr)
        raise SystemExit(2)
    report = diversity_score(agents)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"diversity score: {report['score']}"
              f"（{report['dimensions']}，correlation="
              f"{report['shared_failure_correlation']}）")
        print(report["note"])
    raise SystemExit(0 if report["score"] >= 0.5 else 1)


if __name__ == "__main__":
    import json as _json
    import sys as _sys
    _sys.exit(diversity_main(_sys.argv[1:]))
