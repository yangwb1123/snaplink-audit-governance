# 术语表

| 术语 | 定义 |
|---|---|
| **审计事件 (Event)** | 一条规范化的业务审计事实，含信封字段（event_id、tenant_id、source_system、actor、action、outcome 等）与版本化 payload |
| **event_id** | 调用方生成的全局唯一事件 ID；接入端幂等键之一 |
| **idempotency_key** | 来源系统幂等键，租户内唯一；重复使用关联到不同事件会冲突 |
| **tenant_id** | 租户标识；只由服务端从签名 Token 与来源注册解析，请求体值被忽略 |
| **source_system** | 事件来源系统；与签名 `client_id` 精确绑定（`allowed_client_ids` 白名单） |
| **operation_id** | 一次完整业务操作的关联 ID；跨事件关联与恢复的最小单位 |
| **causation_id** | 触发当前事件的上游事件 ID |
| **correlation_id** | 外部业务关联 ID（跨操作、跨系统） |
| **trace_id / span_id** | OpenTelemetry 技术链路标识；不替代业务审计事实 |
| **actor / targets** | 操作者身份快照（发生时部门/角色/岗位）与被操作用户、部门或资源 |
| **aggregate_type / aggregate_id / aggregate_version** | 领域聚合及其版本；聚合历史与回放的锚点 |
| **changed_fields** | 字段级变化摘要（before/after）；无副作用回放的状态来源 |
| **data_classification / retention_class** | 数据分类（PII/Secret/Financial/Health 等）与留存等级 |
| **Schema（event_schema）** | 版本化 payload 契约（required/allowed/encrypted/searchable 字段）；演进只增不减 |
| **Outbox（audit_outbox）** | 业务事务内写入的待投递事件表；与业务提交同事务 |
| **Relay（audit-outbox-relay）** | 消费 outbox 记录并投递到审计 API 的中继；成功 delivered、失败退避重试、超限/永久错误死信 |
| **accepted / ledgered / indexed / archived** | 事件生命周期状态：已确认接入 / 已进入规范化账本与哈希链 / 查询投影可见 / 完成 WORM 归档 |
| **Receipt** | 写入回执（event_id、租户、状态、流、序号、hash、duplicate/conflict 标记） |
| **Stream** | 分段哈希链的链作用域（tenant + aggregate/operation/source 派生）；维护单调 sequence 与 head_hash |
| **Segment** | 一段连续序列的事件包（默认 100），记录 first/last sequence、prev_hash、last_hash、Merkle Root 与签名 |
| **Checkpoint** | 对段 Merkle Root 的签名证据（参考实现 HMAC-SHA256，生产 KMS/HSM） |
| **Merkle Root** | 段内事件 hash 的默克尔树根；与清单摘要共同构成篡改可发现证据 |
| **WORM 归档** | 只写一次、不可删除/覆盖的合规存储（参考实现：O_EXCL 只读文件 + fsync） |
| **Legal Hold** | 按查询条件保全事件，阻止其进入可删除/归档窗口 |
| **Retention Policy** | 按租户 + retention_class 的 hot/warm/archive 天数策略；评估只归档、永不删除不可变账本 |
| **Export Job** | 异步 JSONL 导出任务（pending→running→completed/failed），租户鉴权下载 + 摘要 |
| **Restore** | 恢复流程：预览（无副作用状态重建）→ 申请（pending_approval）→ 审批（approved/rejected）；审批与业务执行是两个独立事实 |
| **Replay** | 基于 changed_fields 的无副作用状态重建；不调用任何外部副作用接口 |
| **VerifyIntegrity** | 逐流校验 prev_hash 链、事件 hash、段 Merkle Root 与签名 |
| **AdminAction** | 控制面管理操作的 append-only 自审计记录（谁、何时、做了什么、目标） |
| **IngestPrincipal** | 仅含服务端派生身份（client_id）的写入主体；传输层禁止用请求体填充 |
| **RLS** | PostgreSQL Row Level Security；会话变量 `app.tenant_id` 由应用在验证 Token 后设置 |
| **break-glass** | 平台跨租户运维的例外通道（独立权限、双人审批、有限时授权、产生新审计事件） |
