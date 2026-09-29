# 提案与设计（历史输入，非现行规范）

这里是**设计输入**，不是当前规范。现行规范只有三类：根 [`README.md`](../../README.md)
（产品面）、[`adr/`](../adr/)（已决策）、[`api/**`](../../api/)（线面契约）。本目录里的文档
描述当时的约束与取舍，与代码冲突时以上述三类为准。

| 文档 | 规模 | 内容与状态 |
|---|---|---|
| [`implementation-b1-sink.md`](implementation-b1-sink.md) | 403 行 | B1（sink P0 基线批次：dev-token 关闭 / 冲突重试 / ledger / 自审计 / 读自审计 / gRPC 拓扑 / e2e / tenant 422）实施提案。**已实施** |
| [`audit-contract-batch-snaplink-audit-governance.md`](audit-contract-batch-snaplink-audit-governance.md) | 32 行 | 上述批次的入口指针（评估阶段产出） |
| [`bound-and-isolate-read-self-audit-trail-design.md`](bound-and-isolate-read-self-audit-trail-design.md) | 368 行 | 读路径自审计轨迹的隔离与有界化设计。**已实施**（读事实改写入 sidecar 轨迹） |
| [`bound-control-plane-snapshot-design.md`](bound-control-plane-snapshot-design.md) | 470 行 | 控制面快照有界化 + 内部 store 合并设计。**已实施** |
| [`ledgered-projection-pipeline.md`](ledgered-projection-pipeline.md) | 566 行 | 仅由 ledgered 事件驱动 ClickHouse 投影的管道设计。**已实施** |
| [`projector-ledgered-only-guard.md`](projector-ledgered-only-guard.md) | 337 行 | 投影端"只收 ledgered 事件"的守卫设计。**已实施** |
| [`projection-field-protection.md`](projection-field-protection.md) | 639 行 | 投影字段保护规则（敏感字段不得经投影泄漏）。**已实施** |
| [`vault-transit-signer-key-binding-and-context.md`](vault-transit-signer-key-binding-and-context.md) | 305 行 | Vault Transit 签名者的密钥名绑定与上下文。**已实施**（ADR-0010） |
| [`change-manifest-dlq-failure-tenant-id.md`](change-manifest-dlq-failure-tenant-id.md) | 96 行 | DLQ `Failure.tenant_id` 字段变更清单（跨仓协同输入） |
| [`compose-2026-017-audit-governance.md`](compose-2026-017-audit-governance.md) | 16 行 | COMPOSE-2026-017 组合变更在本仓的评估结论指针 |

跨仓的批次门禁与运行方式见 [`../campaigns/`](../campaigns/)；平台门禁矩阵快照见
[`../platform-gate-status.md`](../platform-gate-status.md)。
