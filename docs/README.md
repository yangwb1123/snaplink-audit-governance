# 文档索引

按"要回答什么问题"组织，而不是按目录。

## 我想知道产品是什么、能做什么

- [`../README.md`](../README.md) — 职责边界、当前实现的功能面、核心原则
- [`ARCHITECTURE_PLAN.md`](ARCHITECTURE_PLAN.md) — 架构基线、CQRS 分工、产品边界与非目标
- [`GLOSSARY.md`](GLOSSARY.md) — 术语
- [`../api/openapi/openapi.yaml`](../api/openapi/openapi.yaml) · [`../api/asyncapi/asyncapi.yaml`](../api/asyncapi/asyncapi.yaml) · [`../api/proto/audit.proto`](../api/proto/audit.proto) — 三条线面契约（33 个 REST 路径 / 41 个 REST 操作 / 4 个 topic / 3 个 gRPC 方法）

## 我要改代码，怕破坏什么

- [`ENGINEERING_GATE.md`](ENGINEERING_GATE.md) — 强制门禁的阶段清单；失败行即阶段命令
- [`CONTRACT_SYNC.md`](CONTRACT_SYNC.md) — 契约面与运行时的同步规则（proto 三道防线等）
- [`adr/`](adr/) — 已决策且不再重开的问题（消息接入、编码、分段哈希链、投影路由、归档与密钥、租户键框架、开发认证、导出完整性、平台 token 过滤、跨进程一致性键、发布确认与主动对账、当前目的地投影确认/流前缀/账本重建、固定查询成员集/授权游标/投影候选读取、导出队列预算/公平调度/锁外下载读取、HotFirst 原始追加意图/断点恢复、权限过滤的租户治理控制面摘要、文件数据描述符 I/O/私有创建权限、TLS/DLQ 文件信任边界、导出运行租约/心跳/过期 fencing、所有者取消/单后继关联重试/安全失败类别、密封对象检查点/终态提交续跑、导出访问的认证 Hold/锁外取证/放行复核、固定成员 selected 计划、逐页生成检查点、分页时间线锁外认证取证、Console 页/facets、操作摘要与操作回放的锁外认证读取、导出创建/新后继的认证 Hold 预检与准入复核、普通查询参考初页的认证前置、旧全量时间线与服务层聚合回放的认证读取、按坐标正文/回执身份绑定、回执精确索引与共享预算、既有 Hold 控制写的 CAS 重判与零证据重写、既有恢复审批/拒绝的控制 CAS 重判、文件自审计描述符关闭与夹具生命周期、导出执行器尾读保留与原子唤醒交接、既有恢复申请创建的控制增量提交、既有留存配置写的控制 CAS 重判、既有 Schema 注册的控制 CAS 与演进重判、既有来源更新的控制 CAS 与当前记录重判、既有来源创建的控制 CAS 与当前唯一性重判、既有恢复详情的控制面读取与单次预算、既有留存配置的控制面读取与单次预算）
- [`design/`](design/) · [`proposals/`](proposals/) — 设计与提案；每篇的规模与状态见 [`proposals/README.md`](proposals/README.md)（历史决策输入，不是当前规范）
- [`THREAT_MODEL.md`](THREAT_MODEL.md) — 威胁模型（含归档传输与留存门禁）
- [`SECURITY_SCAN_REVIEW.md`](SECURITY_SCAN_REVIEW.md) — 当前安全整改证据、完整剩余告警清单；不是发布通过证明

## 我要部署或值班

- [`DEPLOY_RUNBOOK.md`](DEPLOY_RUNBOOK.md) — WORM COMPLIANCE 门禁、cutover 分类、回滚语义、一致性键、全栈验证
- [`SLO_ALERTS.md`](SLO_ALERTS.md) — SLO、告警规则与错误预算
- [`BENCHMARKS.md`](BENCHMARKS.md) — 本机性能基线
- [`VALIDATION_PLAN.md`](VALIDATION_PLAN.md) — 验证计划
- [`outbox-writer-rollout.md`](outbox-writer-rollout.md) — 业务侧 Outbox writer 的落地与核对

## 我在查历史与状态

- [`release-notes/`](release-notes/) — 按月的变更流水（[`2026-09`](release-notes/2026-09.md) · [`2026-08`](release-notes/2026-08.md)，新月份在前；`docs/release-notes.md` 是指针）
- [`platform-gate-status.md`](platform-gate-status.md) — 跨仓 G1–G8 门禁矩阵的**日期快照**，含归属说明
- [`MAINTENANCE_REPORT.md`](MAINTENANCE_REPORT.md) — 早期 AI 维护轮次的执行报告
- [`campaigns/`](campaigns/) — 批次门禁矩阵与 campaign 运行方式

## 约定

- **当前规范**只有三类：README（产品）、`adr/`（决策）、`api/**`（线面契约）。其余是设计输入、
  历史记录或运营手册，冲突时以上述三类为准。
- 文档与代码同仓同 PR 更新；`python3 cli.py quality` 会校验契约面与 ADR 证据是否在位。
