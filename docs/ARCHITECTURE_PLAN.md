# Snaplink Audit Governance 架构设计计划

状态：Draft  
日期：2026-08-04  
目标阶段：Architecture Baseline

## 1. 决策摘要

本项目建设为独立的多租户审计与治理产品，不嵌入 Snaplink
SSO Server 的核心进程。

推荐基线：

| 领域 | 默认技术 |
|---|---|
| 服务实现 | Go |
| 外部接口 | REST/JSON、批量 gRPC |
| 内部接口 | gRPC、Protobuf |
| 异步协议 | Kafka、Protobuf、AsyncAPI |
| 接入网关 | Envoy Gateway |
| 身份与租户 | OIDC/OAuth 2.0、Snaplink Adapter、服务间 mTLS |
| 控制面存储 | PostgreSQL HA、PgBouncer |
| 高吞吐事件总线 | Apache Kafka |
| 在线分析查询 | ClickHouse |
| 全文搜索 | OpenSearch，可选 |
| 热点缓存与限流 | Redis，可选，不保存审计事实 |
| 合规归档 | S3 Object Lock 或等价 WORM 存储 |
| 长任务编排 | Temporal，可选 |
| 运行平台 | Kubernetes，三可用区 |
| 可观测性 | OpenTelemetry、Prometheus、Grafana、Loki、Tempo |
| 密钥 | KMS、Vault 或 HSM |
| 发布 | Helm、Argo CD、Terraform 或 OpenTofu |

核心架构采用 CQRS 和事件驱动模型：

- 写模型负责接入、验证、排序、幂等、哈希链和不可变归档。
- 读模型负责操作时间线、筛选、统计、报表和全文检索。
- 查询投影允许最终一致，但不可变账本不得依赖查询投影。

## 2. 产品边界

### 2.1 本项目负责

- 多租户事件接入和租户级配额。
- 通用审计事件信封和 Schema 版本管理。
- operation_id、causation_id 和 trace_id 关联。
- 不可变账本、分段哈希链和签名检查点。
- 操作时间线、聚合状态重建和无副作用回放。
- 查询、统计、导出、留存、Legal Hold 和归档。
- 审计数据完整性验证。
- 可选的审批、恢复、补偿和执行记录。

### 2.2 Snaplink 负责

- 用户和服务认证。
- OAuth 2.0、OIDC、令牌和服务身份。
- 用户、客户端、租户身份及授权上下文。
- 登录、令牌、MFA 等身份安全事件。

Snaplink 的安全审计保持 fail-open，不允许审计后端故障阻断登录。
必须保证落账的业务审计事件由业务系统 Transactional Outbox 提交，
不能把 Snaplink 的 best-effort Sink 当作业务事务提交机制。

### 2.3 业务系统负责

- 领域数据和业务事务。
- 在事务内写入 Outbox。
- 提供稳定的 aggregate_type、aggregate_id 和 aggregate_version。
- 对 before/after 快照做数据分类、脱敏和最小化。
- 恢复时通过正常业务 API 执行补偿操作。

### 2.4 非目标

- 不重新实现认证系统。
- 不以纯 Event Sourcing 重写所有 ERP/OA 业务表。
- 不把 OpenTelemetry Trace 当作业务审计。
- 不承诺跨所有租户和所有事件的全局严格顺序。
- 不在第一阶段实现 BPMN 可视化设计器。
- 不允许回放重新调用支付、库存、ERP 等外部副作用接口。

## 3. 规划负载和 SLO

以下数值是架构及压测基线，不是未经测试的生产承诺。上线前必须用
真实事件大小、租户分布、查询模型和留存周期重新计算。

| 指标 | 初始基线 |
|---|---:|
| 活跃租户 | 10,000 |
| 峰值写入 | 50,000 events/s |
| 10 分钟突发目标 | 100,000 events/s |
| 平均规范化事件 | 2 KB |
| 单事件上限 | 256 KB，超过后使用加密对象引用 |
| 热查询数据 | 30 天 |
| 温数据 | 180 天，可配置 |
| 合规归档 | 1 至 7 年，按租户策略 |
| 接入可用性 | 99.99% |
| 查询可用性 | 99.95% |
| accepted 写入延迟 | 同区域 p99 小于 150 ms |
| ledgered 延迟 | p99 小于 2 秒 |
| 查询投影延迟 | p99 小于 5 秒 |
| 操作时间线查询 | 30 天范围 p99 小于 500 ms |
| 同区域 RPO | 已确认 accepted 事件为 0 |
| 跨区域 RPO | 默认小于等于 60 秒 |
| 跨区域 RTO | 默认小于等于 15 分钟 |

严格跨区域 RPO 0 需要同步跨地域复制，会显著增加写入延迟和成本，
必须作为独立产品等级验证，不能默认宣称。

基础容量估算：

    daily_raw_bytes =
      events_per_second
      × average_event_bytes
      × 86400

    provisioned_bytes =
      daily_raw_bytes
      × retention_days
      × replication_factor
      × index_amplification
      × safety_factor

规划时至少采用 1.5 的容量安全系数，并用真实压缩率替换估算值。

## 4. 架构原则

1. 租户上下文只能来自可信令牌、mTLS 服务身份或服务端解析器。
2. 所有写入至少一次投递，所有消费者必须幂等。
3. 接入、落账、投影和归档具有明确状态，不能用一个成功状态混淆。
4. 不可变事实与可重建索引分离。
5. 不建立全局串行哈希链。
6. 审批状态与业务执行状态分离。
7. 查询必须包含租户和时间边界。
8. 大租户可迁移到独立资源，但 API 和事件协议不变化。
9. 故障时优先背压和本地 Outbox，不允许静默丢弃。
10. 敏感数据在进入账本之前完成最小化和脱敏。

## 5. 逻辑架构

    ERP / OA / CRM / Snaplink / SDK
                    |
            Transactional Outbox
                    |
                    v
           Envoy / API Gateway
                    |
                    v
              audit-ingest
        authn / tenant / quota / schema
             idempotency / batching
                    |
                    v
              Kafka quorum
                    |
        +-----------+-------------+
        |           |             |
        v           v             v
    ledger       projector      export worker
    writer          |             |
        |           v             v
        |       ClickHouse      WORM archive
        |       OpenSearch
        v
    PostgreSQL
    control metadata / segment manifests
        |
        v
    query-api / integrity-api

管理控制台作为独立前端部署，通过 OIDC 登录后调用 control-plane 和
query-api，不由 Go 服务托管静态资源。

## 6. 服务拆分

第一阶段保持六个核心部署单元，避免过早拆成大量细粒度服务。

### 6.1 audit-control-plane

职责：

- 租户、Schema、来源系统和数据分类。
- 留存策略、Legal Hold、导出策略和配额。
- 租户密钥引用和数据驻留区域。
- 平台角色、租户角色和授权策略映射。

存储：PostgreSQL。

### 6.2 audit-ingest

职责：

- REST 和 gRPC 批量接入。
- 认证、租户解析、权限、限流和请求大小限制。
- Schema 校验、字段规范化和敏感字段策略检查。
- event_id 和 idempotency_key 基础校验。
- 写入 Kafka 并返回 accepted 回执。

该服务无状态，可水平扩展。

### 6.3 audit-ledger-writer

职责：

- 消费规范化事件。
- 幂等落账和单流序列分配。
- 生成事件哈希、PrevHash 和分段清单。
- 更新 stream checkpoint 和 operation head。
- 生成待归档段。

该服务按 Kafka partition 获取单写租约，不允许两个实例同时推进同一
ledger stream。

### 6.4 audit-projector

职责：

- 构建 ClickHouse 查询投影。
- 构建 operation timeline 和 aggregate history。
- 可选构建 OpenSearch 全文索引。
- 支持从账本或 Kafka 保留区重新构建。

投影失败不回滚账本，通过重试和 DLQ 修复。

### 6.5 audit-query-api

职责：

- 事件查询、操作时间线和状态回放。
- 按 actor、target、aggregate、operation、trace 和时间筛选。
- 强制租户边界、字段级脱敏和结果数量限制。
- 查询导出任务状态和完整性证明。

### 6.6 audit-governance-worker

职责：

- WORM 归档。
- 导出、留存、Legal Hold 和合规任务。
- 哈希链、对象、清单和签名检查点验证。
- 失败重试、对账和告警。

### 6.7 后续可选服务

- audit-workflow：审批和恢复流程。
- audit-notification：Webhook、邮件和消息通知。
- audit-policy：复杂 ABAC 或独立策略引擎。
- audit-connector：SIEM、数据湖和第三方系统连接器。

## 7. 事件契约

通用信封至少包含：

| 字段 | 含义 |
|---|---|
| event_id | 调用方生成的全局唯一事件 ID |
| tenant_id | 服务端解析并覆盖的租户 ID |
| source_system | 事件来源系统 |
| event_type | 稳定的领域事件类型 |
| schema_id/schema_version | 载荷 Schema |
| occurred_at | 业务事实发生时间 |
| received_at | 平台接收时间 |
| operation_id | 一次完整业务操作 |
| causation_id | 触发当前事件的上游事件 |
| correlation_id | 外部关联 ID |
| trace_id/span_id | 技术调用链 |
| actor | 操作者身份快照 |
| targets | 被操作用户、部门或资源 |
| aggregate_type/id/version | 领域聚合及版本 |
| workflow_instance_id | 可选审批流程 |
| execution_run_id | 可选执行尝试 |
| action/outcome/reason | 操作、结果和原因 |
| changed_fields | 字段级变化摘要 |
| payload_ref | 大载荷或敏感快照的加密对象引用 |
| data_classification | 数据分类 |
| retention_class | 留存等级 |
| idempotency_key | 来源系统幂等键 |
| stream_id/sequence | 账本流及序列 |
| prev_hash/hash | 链接和事件摘要 |
| server_version | 处理服务版本 |

约束：

- 原始密码、令牌、私钥和设备凭据禁止进入事件。
- actor 的部门、角色、岗位必须形成发生时快照。
- occurred_at 由来源提供，但 received_at 由平台生成。
- 平台保留来源时间偏差标志，不能依赖客户端时间排序安全事件。
- payload 使用版本化 Schema，不允许无约束任意 JSON。
- before/after 大快照默认写入加密对象，事件只保存摘要和引用。

## 8. 写入确认和一致性

事件生命周期：

    received
       |
       v
    accepted    Kafka 多副本确认
       |
       v
    ledgered    已进入规范化账本和哈希链
       |
       v
    indexed     在线查询投影可见
       |
       v
    archived    WORM 对象及签名清单完成

默认写入 API 在 accepted 后返回 202 和 event_id。强合规调用可请求
wait_for=ledgered，但必须设置有限超时，超时返回处理中状态而不是让
客户端盲目重发。

端到端语义：

- 业务服务使用 Transactional Outbox 保证业务提交与审计待发送记录
  同事务完成。
- Kafka 生产端开启 acks=all 和幂等生产。
- 消费端按 tenant_id、source_system、event_id 去重。
- 重试可能重复到达，因此不宣称网络端到端 exactly-once。
- event_id 已存在且规范化内容一致时返回原结果。
- event_id 已存在但内容不同视为安全冲突并产生告警。

## 9. 顺序和 Kafka 分区

默认不提供全局事件顺序，只保证 ordering_scope 内有序。

默认分区键：

    tenant_id + aggregate_type + aggregate_id

操作必须跨多个聚合时，可显式指定 operation_id 为 ordering key。

注意：

- 只按 tenant_id 分区会让大租户成为热点。
- 完全随机分区会破坏同一聚合顺序。
- 高流量租户可以配置更多逻辑 stream 或独立 topic。
- Kafka topic、partition 和 retention 调整必须经过容量 ADR。

建议 topic：

| Topic | 用途 |
|---|---|
| audit.events.accepted.v1 | 已验证的规范化事件 |
| audit.events.ledgered.v1 | 已进入账本的事件 |
| audit.events.projection.v1 | 投影任务 |
| audit.events.archive.v1 | 归档任务 |
| audit.events.dlq.v1 | 终止重试后的隔离事件 |
| audit.control.invalidate.v1 | Schema、租户和策略缓存失效 |

## 10. 不可变账本和哈希链

单一全局链会把所有写入串行化，因此采用分段多链：

- chain_scope 默认为 tenant_id + stream_id。
- 每个流维护单调 sequence、prev_hash 和 head_hash。
- 事件使用确定性编码后计算 SHA-256 摘要。
- 每个段记录 first_sequence、last_sequence、first_prev_hash、
  last_hash、事件数量和对象摘要。
- 定时对多个 segment root 构造 Merkle Root。
- checkpoint 由 KMS、Vault Transit 或 HSM 中的签名密钥签名。
- segment、manifest 和 checkpoint 写入 WORM 存储。
- 密钥只有签名权限，应用实例不能导出私钥。

哈希链提供篡改可发现性，WORM、密钥隔离、访问审计和外部检查点共同
组成完整证据链。不能仅凭数据库字段宣称数据绝对不可篡改。

## 11. 数据存储

### 11.1 PostgreSQL

保存控制面和强一致元数据：

- tenants
- sources
- event_schemas
- retention_policies
- legal_holds
- tenant_key_refs
- ledger_streams
- ledger_segments
- signed_checkpoints
- event_receipts
- operation_heads
- export_jobs
- replay_jobs

要求：

- 高可用主从或托管 Multi-AZ。
- PgBouncer 连接池。
- 应用层租户过滤加 PostgreSQL RLS 双重防护。
- 时间表使用 Range 分区，必要时叠加固定数量 Hash 子分区。
- 不采用一租户一分区，避免租户数量导致分区爆炸。
- schema migration 使用 expand/migrate/contract。

### 11.2 ClickHouse

保存在线查询投影：

- tenant_id 必须位于排序键前部。
- 建议按月份分区，按 tenant_id、occurred_at、event_id 排序。
- 高基数 Metadata 不直接全部展开为索引列。
- 常用稳定字段使用实体列，动态载荷保留受控 JSON。
- 使用分片和副本实现水平扩展和节点容错。
- Materialized View 只用于可重建聚合。

ClickHouse 不是事实源，表损坏或重建时从 ledgered 事件和归档恢复。

### 11.3 Object Storage

归档格式：

- canonical JSON Lines 或 Protobuf segment。
- Zstandard 压缩。
- 清单记录事件范围、Schema、对象摘要和链头。
- 分析副本可额外生成 Parquet，但 Parquet 不是唯一证据格式。

对象路径按 tenant、日期、stream 和 segment 组织。生产环境开启：

- Versioning。
- Object Lock 或等价 WORM。
- Legal Hold。
- 跨区域复制。
- 独立归档账号或项目。
- 禁止应用服务拥有缩短合规保留期的权限。

### 11.4 Redis

只用于：

- 短期限流计数。
- 允许丢失的查询缓存。
- 短期请求幂等加速。
- 租户策略缓存。

Redis 不保存账本、链头或唯一恢复来源。

### 11.5 OpenSearch

仅在存在全文搜索、模糊检索和复杂文本检索需求时引入。第一阶段优先
验证 ClickHouse 是否已经满足结构化查询，避免重复维护两个大型查询
集群。

## 12. API 规划

### 12.1 写入

| 接口 | 用途 |
|---|---|
| POST /api/v1/events | 单事件接入 |
| POST /api/v1/events:batch | 批量事件接入 |
| GET /api/v1/events/{eventId}/receipt | 查询处理状态 |
| gRPC Ingest/Write | 高吞吐单事件 |
| gRPC Ingest/WriteBatch | 高吞吐批量 |
| gRPC Ingest/WriteStream | 受控流式写入 |

### 12.2 查询

| 接口 | 用途 |
|---|---|
| GET /api/v1/events/{eventId} | 单事件 |
| GET /api/v1/events | 条件查询 |
| GET /api/v1/operations/{operationId} | 操作概要 |
| GET /api/v1/operations/{operationId}/timeline | 操作时间线 |
| GET /api/v1/operations/{operationId}/replay | 无副作用状态重建 |
| GET /api/v1/aggregates/{type}/{id}/timeline | 聚合历史 |

### 12.3 治理

| 接口 | 用途 |
|---|---|
| POST /api/v1/exports | 创建导出任务 |
| GET /api/v1/exports/{jobId} | 导出状态 |
| POST /api/v1/integrity/verify | 验证链段 |
| POST /api/v1/legal-holds | 创建保全 |
| POST /api/v1/restores/preview | 恢复预览，后续阶段 |
| POST /api/v1/restores | 执行补偿恢复，后续阶段 |

所有列表接口必须要求时间范围，设置最大页大小，使用游标分页，不支持
无界 offset 扫描。

## 13. 多租户隔离

### 13.1 身份与上下文

- 外部用户通过 Snaplink 或其他标准 OIDC Provider 登录。
- 用户和管理请求的 tenant_id 来自已验证 Token 和服务端授权关系。
- client_credentials 写入 Token 可不携带 tenant_id；接入服务按注册的
  `(client_id, source_system)` 唯一反查 tenant。签名 tenant claim 若存在只作
  收窄条件，零命中和跨租户多命中都失败关闭。
- 请求体中的 tenant_id 被忽略，不能参与租户解析或覆盖服务端上下文。
- audit 写入主体取自签名 Token 的 `client_id`；仅在缺少 `client_id` 时兼容
  `azp`，两者同时存在但不一致时拒绝，禁止以 `sub` 冒充客户端身份。
- 每个 source_system 维护精确的 `allowed_client_ids`。空列表只允许
  `client_id == source_system.id`；未知、停用和越权来源返回同一拒绝结果。
- 远程 JWKS 只接受 EdDSA/Ed25519、ES256/384/512、RS256、PS256，且在
  验签前匹配 kid、alg、kty、crv、use 和 key_ops。HS256 只允许独立的本机
  显式配置，不能与远程 JWKS 或本地非对称公钥共存。
- 内部服务使用 mTLS 和短期工作负载令牌。
- 禁止信任来自公网的 X-Tenant-ID。

### 13.2 授权角色

基础权限：

- audit:event:write
- audit:event:read
- audit:operation:read
- audit:export:create
- audit:integrity:verify
- audit:policy:read
- audit:policy:write
- audit:legal_hold:manage
- audit:platform:cross_tenant

平台运营人员默认不能读取租户明文事件。跨租户运维使用独立
break-glass 权限、双人审批、有限时授权并生成新的审计事件。

### 13.3 资源隔离

- 普通租户共享服务和分片，通过强制 tenant_id 隔离。
- 大租户可迁移到独立 Kafka topic、ClickHouse shard 或数据库。
- 每租户配置写入 QPS、突发量、存储和查询并发。
- 按租户监控消费延迟，防止 noisy neighbor。
- 每租户配置数据驻留区域和加密密钥。

## 14. 安全和隐私

- 全链路 TLS，内部 gRPC 使用 mTLS。
- 密钥、数据库密码和连接凭据由 Vault/KMS 管理。
- 每租户使用 envelope encryption：租户 DEK 由平台 KEK 包装。
- 敏感字段支持字段级加密和可搜索摘要。
- 禁止将密码、Token、私钥、银行卡完整号写入事件。
- payload Schema 标注 PII、Secret、Financial、Health 等分类。
- 日志、指标和 Trace 不得复制审计 payload。
- 导出文件使用短期下载凭据、独立加密和完整性摘要。
- 管理操作、策略变化、密钥变化、导出和 Legal Hold 全部自审计。
- 依赖锁定版本，生成 SBOM，执行镜像签名和漏洞扫描。

数据删除与不可变审计发生冲突时，优先采用数据最小化、主体标识令牌化、
密钥销毁和法定留存策略。具体处理必须由法务和合规要求形成 ADR，
不能由技术系统自行推断。

## 15. 高可用和扩展

### 15.1 Kubernetes

- 单地域跨三个可用区。
- 生产控制面至少三个节点或使用托管控制面。
- 无状态服务每个至少三个副本。
- PodDisruptionBudget、Topology Spread 和反亲和。
- ingest、worker、query 使用独立节点池。
- HPA 按 CPU、延迟和 QPS 扩展。
- KEDA 按 Kafka consumer lag 扩展 worker。
- readiness 反映依赖可用性和积压保护状态。

### 15.2 Kafka

- 至少三个 Broker，跨可用区放置副本。
- replication.factor=3。
- min.insync.replicas=2。
- producer acks=all，enable.idempotence=true。
- 禁止 unclean leader election。
- 容量预留 Broker 和磁盘故障余量。
- Kafka 维护或不可用时，业务服务 Outbox 保留事件并指数退避。

### 15.3 PostgreSQL

- 一个主节点和至少两个副本，至少一个同步副本。
- 自动故障切换必须使用 fencing，防止双主。
- 定期基础备份和持续 WAL 归档。
- 执行 PITR 恢复演练，而不仅检查备份文件存在。

### 15.4 ClickHouse

- 每个 shard 至少两个 replica。
- 查询入口感知节点健康。
- 复制延迟和 rejected inserts 触发告警。
- 重建投影是受支持的标准运维流程。

### 15.5 降级策略

| 故障 | 行为 |
|---|---|
| ClickHouse 不可用 | 写入继续，查询降级，积压投影 |
| OpenSearch 不可用 | 全文搜索降级，结构化查询继续 |
| WORM 暂时不可用 | 账本继续，archived 状态滞后并告警 |
| Kafka 不可用 | 接入背压；来源 Outbox 保留，不静默成功 |
| PostgreSQL 控制面只读 | 使用短期策略缓存接入，禁止策略变更 |
| KMS 不可用 | 不生成新签名检查点；不得伪造归档成功 |

## 16. 跨地域容灾

第一阶段采用 tenant home region 单主写入：

- 每个租户绑定一个 home region。
- 同一 ledger stream 同时只有一个地域可写。
- Kafka、对象存储和 PostgreSQL 备份异步复制到灾备地域。
- 故障切换通过租约和 fencing 阻止旧地域继续写。
- 恢复后校验 stream head 和最后一个签名 checkpoint。

第二阶段可提供多地域接入，但事件仍路由到租户 home region 落账。
只有在明确存在低延迟全球写入需求时，才设计按租户或 stream 分片的
Active-Active；禁止直接构建全局多主哈希链。

必须每季度执行：

- Kafka 恢复或替换演练。
- PostgreSQL PITR。
- ClickHouse 全量重建投影。
- WORM 归档验证。
- 整地域切换和回切。

## 17. 可观测性

统一传播：

- trace_id 和 span_id：技术链路。
- operation_id：业务操作。
- event_id：单条事实。
- tenant_id：日志中使用受控标识，禁止作为无界指标标签。

关键指标：

- ingest QPS、p50/p95/p99、429、503。
- Kafka produce latency、under-replicated partitions、consumer lag。
- accepted 到 ledgered、indexed、archived 延迟。
- 去重命中和 event_id 内容冲突。
- 每租户配额命中。
- ClickHouse 查询延迟、扫描行数和 rejected inserts。
- WORM 归档失败、积压和签名失败。
- 哈希链断裂和 checkpoint 验证失败。
- DLQ 数量和最老消息年龄。
- RPO、RTO、最近一次恢复演练结果。

指标禁止使用 user_id、event_id、operation_id 作为标签。详细标识只进入
受限日志和 Trace。

## 18. 发布和供应链

- 使用 trunk-based development 和短生命周期分支。
- Protobuf、OpenAPI、AsyncAPI 变化执行兼容性检查。
- 数据库 migration 前向兼容，先 expand 后 contract。
- 镜像使用最小运行时，非 root 用户。
- CI 生成 SBOM、签名镜像并执行依赖扫描。
- Argo CD 按环境 GitOps 发布。
- Canary 先验证 ingest 错误率和消费延迟，再扩大流量。
- Kafka consumer 升级确保新旧 Schema 可同时消费。
- 所有生产配置、topic 和 retention 由声明式代码管理。

## 19. 测试和工程门槛

### 19.1 单元和契约

- 事件规范化和确定性哈希测试。
- Schema 向前、向后兼容测试。
- 幂等和 event_id 冲突测试。
- 租户授权和数据过滤测试。
- OpenAPI、Protobuf、AsyncAPI 契约检查。

### 19.2 集成

- PostgreSQL、Kafka、ClickHouse 和对象存储真实容器测试。
- Outbox 到 ledgered 的端到端测试。
- 重复、乱序、延迟、DLQ 和重建投影测试。
- 哈希段、Merkle Root、签名和 WORM 清单验证。

### 19.3 安全

- 跨租户读取和写入负向测试。
- 未授权导出、Legal Hold 和 break-glass 测试。
- SSRF、注入、超大 payload、压缩炸弹和恶意 Schema 测试。
- 凭据和敏感数据泄漏扫描。

### 19.4 性能

- 稳态 50,000 events/s。
- 10 分钟 100,000 events/s 突发。
- 大租户热点和大量小租户混合模型。
- Kafka Broker、ClickHouse replica 和 PostgreSQL 主节点故障注入。
- 24 小时 soak test，监控积压、内存、磁盘和延迟漂移。

未经压测验证，不允许在产品文档宣称具体吞吐量。

## 20. 仓库结构规划

    snaplink-audit-governance/
    ├── README.md
    ├── AGENTS.md
    ├── go.mod
    ├── api/
    │   ├── openapi/
    │   ├── asyncapi/
    │   └── proto/
    ├── cmd/
    │   ├── audit-control-plane/
    │   ├── audit-ingest/
    │   ├── audit-ledger-writer/
    │   ├── audit-projector/
    │   ├── audit-query-api/
    │   └── audit-governance-worker/
    ├── internal/
    │   ├── tenant/
    │   ├── eventcontract/
    │   ├── ingest/
    │   ├── ledger/
    │   ├── operation/
    │   ├── projection/
    │   ├── retention/
    │   ├── integrity/
    │   └── export/
    ├── adapters/
    │   ├── snaplink/
    │   ├── kafka/
    │   ├── postgres/
    │   ├── clickhouse/
    │   ├── objectstore/
    │   └── opensearch/
    ├── migrations/
    ├── deploy/
    │   ├── helm/
    │   ├── argocd/
    │   └── terraform/
    ├── docs/
    │   ├── ARCHITECTURE_PLAN.md
    │   └── adr/
    └── test/
        ├── integration/
        ├── e2e/
        ├── performance/
        └── chaos/

目录是目标形态，第一阶段只在职责实际出现时创建，避免预先生成空包。

## 21. 分阶段实施

### Phase 0：契约和验证，2 至 3 周

- 确认负载、留存、数据驻留和 SLO。
- 定义事件信封、错误模型、Schema 规则和租户授权。
- 完成 Kafka、PostgreSQL、ClickHouse、WORM 技术验证。
- 编写 ADR 和最小性能基准。

退出条件：所有关键技术假设有基准或原型证据。

### Phase 1：最小可用账本，4 至 6 周

- audit-ingest。
- Transactional Outbox SDK。
- Kafka 接入和幂等消费。
- PostgreSQL 控制面。
- ledger writer、分段哈希链和完整性验证。
- 基础查询 API。

退出条件：事件可从业务事务进入可验证账本，重复投递不产生重复事实。

### Phase 2：高负载查询和归档，4 至 6 周

- ClickHouse 投影。
- operation timeline 和 aggregate history。
- WORM 归档、签名 checkpoint、导出和 Legal Hold。
- 租户配额、数据驻留和字段脱敏。
- 50,000 events/s 稳态压测。

退出条件：达到 SLO 基线，查询投影可完全重建。

### Phase 3：高可用和容灾，3 至 5 周

- 三可用区部署。
- 自动扩缩容、背压、故障注入。
- PostgreSQL PITR、Kafka 恢复、ClickHouse 重建。
- 跨区域复制和租户级切换。
- 24 小时 soak test。

退出条件：通过故障演练并记录实际 RPO/RTO。

### Phase 4：治理扩展

- Temporal 长任务。
- 恢复预览和补偿执行。
- 高风险恢复审批。
- OpenSearch 全文查询。
- SIEM 和数据湖连接器。
- 大租户独享部署。

## 22. 必须形成的 ADR

1. Kafka 与兼容实现的最终选型。
2. 事件编码、确定性序列化和 Schema 兼容规则。
3. chain_scope、segment 大小和 checkpoint 周期。
4. ClickHouse 分区、排序和租户路由策略。
5. WORM 提供商、合规模式和跨区域复制。
6. 租户密钥层级和密钥销毁策略。
7. 数据删除权与法定审计留存冲突处理。
8. home region、故障切换和 fencing。
9. accepted 与 ledgered 的 API 承诺。
10. 大租户独享资源的迁移门槛。

## 23. 主要风险

| 风险 | 控制措施 |
|---|---|
| 全局哈希链成为瓶颈 | 按租户和 stream 分链，签名聚合 checkpoint |
| Kafka 被误当长期账本 | WORM 归档和独立 manifest |
| ClickHouse 投影遗漏 | 对账、可重建投影、lag 告警 |
| 跨租户数据泄漏 | 服务端租户解析、RLS、负向测试、独立导出路径 |
| 大租户造成热点 | ordering key 分片、配额、专属 topic/shard |
| 任意 payload 泄漏敏感数据 | Schema 注册、分类、脱敏、大小限制 |
| 重试产生重复事实 | 生产者 event_id、消费者幂等、内容冲突检测 |
| 多地域双写破坏链 | tenant home region、租约、fencing |
| 技术栈过重 | 分阶段引入，OpenSearch 和 Temporal 默认不部署 |

## 24. 第一批交付物

- 产品范围和术语表。
- event-envelope-v1 Protobuf 和 JSON Schema。
- OpenAPI 和 AsyncAPI 草案。
- Tenant、Schema、Ledger Segment 数据模型。
- Kafka topic 和容量计划。
- 威胁模型。
- SLO、告警和错误预算。
- Phase 0 性能验证报告。
- ADR-0001 至 ADR-0005。

## 25. 架构验收条件

架构基线只有在以下条件全部满足后才能进入正式实现：

- 产品负责人确认范围、SLO、留存和租户等级。
- 安全负责人确认租户隔离和密钥模型。
- 合规负责人确认 WORM、Legal Hold 和删除策略。
- 运维负责人确认三可用区和容灾资源。
- Kafka、ClickHouse、PostgreSQL 和对象存储通过原型压测。
- event-envelope-v1 完成兼容性评审。
- accepted、ledgered、indexed、archived 状态语义不再含糊。
- 关键 ADR 已批准，未决项有明确负责人和截止时间。

## 参考资料

- Apache Kafka Design: https://kafka.apache.org/41/design/design/
- PostgreSQL Table Partitioning:
  https://www.postgresql.org/docs/current/ddl-partitioning.html
- Kubernetes Production Environment:
  https://kubernetes.io/docs/setup/production-environment/
- Amazon S3 Object Lock:
  https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html
- OpenTelemetry Context Propagation:
  https://opentelemetry.io/docs/concepts/context-propagation/
