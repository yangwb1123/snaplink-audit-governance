# 文档索引

按"要回答什么问题"组织，而不是按目录。

## 我想知道产品是什么、能做什么

- [`../README.md`](../README.md) — 职责边界、当前实现的功能面、核心原则
- [`ARCHITECTURE_PLAN.md`](ARCHITECTURE_PLAN.md) — 架构基线、CQRS 分工、产品边界与非目标
- [`GLOSSARY.md`](GLOSSARY.md) — 术语
- [`../api/openapi/openapi.yaml`](../api/openapi/openapi.yaml) · [`../api/asyncapi/asyncapi.yaml`](../api/asyncapi/asyncapi.yaml) · [`../api/proto/audit.proto`](../api/proto/audit.proto) — 三条线面契约（29 个 REST 路径 / 3 个 topic / 3 个 gRPC 方法）

## 我要改代码，怕破坏什么

- [`ENGINEERING_GATE.md`](ENGINEERING_GATE.md) — 强制门禁的阶段清单；失败行即阶段命令
- [`CONTRACT_SYNC.md`](CONTRACT_SYNC.md) — 契约面与运行时的同步规则（proto 三道防线等）
- [`adr/`](adr/) — 已决策且不再重开的问题（消息接入、编码、分段哈希链、投影路由、归档与密钥、租户键框架、开发认证、导出完整性、平台 token 过滤、跨进程一致性键）
- [`design/`](design/) · [`proposals/`](proposals/) — 设计与提案；`proposals/` 是历史决策输入，不是当前规范
- [`THREAT_MODEL.md`](THREAT_MODEL.md) — 威胁模型（含归档传输与留存门禁）

## 我要部署或值班

- [`DEPLOY_RUNBOOK.md`](DEPLOY_RUNBOOK.md) — WORM COMPLIANCE 门禁、cutover 分类、回滚语义、一致性键、全栈验证
- [`SLO_ALERTS.md`](SLO_ALERTS.md) — SLO、告警规则与错误预算
- [`BENCHMARKS.md`](BENCHMARKS.md) — 本机性能基线
- [`VALIDATION_PLAN.md`](VALIDATION_PLAN.md) — 验证计划
- [`outbox-writer-rollout.md`](outbox-writer-rollout.md) — 业务侧 Outbox writer 的落地与核对

## 我在查历史与状态

- [`release-notes.md`](release-notes.md) — 按日期的变更流水（追加式，最新在前）
- [`platform-gate-status.md`](platform-gate-status.md) — 跨仓 G1–G8 门禁矩阵的**日期快照**，含归属说明
- [`MAINTENANCE_REPORT.md`](MAINTENANCE_REPORT.md) — 早期 AI 维护轮次的执行报告
- [`campaigns/`](campaigns/) — 批次门禁矩阵与 campaign 运行方式

## 约定

- **当前规范**只有三类：README（产品）、`adr/`（决策）、`api/**`（线面契约）。其余是设计输入、
  历史记录或运营手册，冲突时以上述三类为准。
- 文档与代码同仓同 PR 更新；`python3 cli.py quality` 会校验契约面与 ADR 证据是否在位。
