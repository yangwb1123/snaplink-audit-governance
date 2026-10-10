# Snaplink Audit Governance

Snaplink Audit Governance 是面向多租户、多业务系统的审计与治理平台。
它提供统一事件接入、不可变审计账本、操作时间线、查询分析、合规导出、
留存策略、完整性校验，以及后续可选的审批、恢复和补偿能力。

本项目与 Snaplink 的职责边界：

- Snaplink 负责认证、授权、用户身份、租户上下文和身份安全事件。
- 本项目负责跨业务系统的审计事实、治理策略、操作关联和合规证据。
- ERP、OA、CRM 等业务系统保留自己的领域状态，通过 Outbox 或 SDK
  发送审计事件。
- OpenTelemetry 负责技术链路关联，不替代业务审计事实。

当前仓库包含可运行的参考实现；生产部署仍须按部署环境完成安全配置、外部依赖探测和容量验证。

## 设计文档

- [架构设计计划](docs/ARCHITECTURE_PLAN.md)
- [本机验证计划](docs/VALIDATION_PLAN.md)
- [SLO、告警与错误预算](docs/SLO_ALERTS.md)
- [本机性能基线](docs/BENCHMARKS.md)
- [威胁模型](docs/THREAT_MODEL.md)
- [Outbox writer inventory and rollout verification](docs/outbox-writer-rollout.md)
- [术语表](docs/GLOSSARY.md)
- [工程门禁](docs/ENGINEERING_GATE.md) — `python3 cli.py quality` 的阶段清单与设计约束
- [契约同步](docs/CONTRACT_SYNC.md) — OpenAPI / AsyncAPI / Proto 三条线的漂移防线
- [部署 / 回滚 runbook](docs/DEPLOY_RUNBOOK.md) — WORM COMPLIANCE 门禁、cutover、一致性键、全栈验证
- [平台批次联合核查报告（G1–G8 门禁矩阵快照）](docs/platform-gate-status.md)
- ADR：[0001 消息传输与接入](docs/adr/ADR-0001.md) · [0002 事件编码与 Schema 兼容](docs/adr/ADR-0002.md) · [0003 哈希链分段与检查点](docs/adr/ADR-0003.md) · [0004 查询投影与租户路由](docs/adr/ADR-0004.md) · [0005 合规归档与密钥](docs/adr/ADR-0005.md) · [0006 租户 ID 键框架校验](docs/adr/ADR-0006.md) · [0007 开发认证默认失败关闭](docs/adr/ADR-0007.md) · [0010 API/worker 签名与归档一致性键](docs/adr/ADR-0010.md) · [0011 发布确认与主动对账](docs/adr/ADR-0011.md) · [0012 投影确认与重建](docs/adr/ADR-0012.md) · [0013 固定查询与投影候选](docs/adr/ADR-0013.md) · [0014 导出队列预算与调度](docs/adr/ADR-0014.md) · [0015 HotFirst 断点恢复](docs/adr/ADR-0015.md)

## 当前实现

仓库现在包含一个可运行的 Go 单节点参考实现（构建至少使用 Go 1.26.9，CI 与 Docker builder 同步，避免旧标准库已知漏洞）：

- `go run ./cmd/audit-api` 启动 REST API。
- `go run ./cmd/audit-governance-worker -once` 执行一次留存/Legal Hold 评估。
- 聚合 checkpoint 每租户按上限保留（默认 1000 条，drop-oldest，`AUDIT_AGGREGATE_CHECKPOINT_HISTORY` 可覆盖，下限 1）；根与签名未变化的周期去重、空闲周期不写快照，`VerifyIntegrity` 聚合校验工作量由此有界。
- 导出 pending/running 队列按同一共享 control CAS 准入：全局最多 128、单租户 32，耗尽 429；全局 running 最多 2、单租户 1，API/worker 共用预算。有限 dispatcher 按租户轮转、租户内 oldest-first，不再为所有 pending 启 goroutine；API 启动/每秒重新唤醒，完成继续推进。Resume 返回观察 backlog 而非完成数。崩溃 running 会占并发，须 worker 恢复；租约/心跳/检查点、严格跨副本公平 SLA 和分块导出仍未实现，见 ADR-0014。
- 导出作业卡在 `running` 超过阈值（默认 24 小时，从 CAS 领取时记录的 `StartedAt` 起算，旧任务缺少有效开始时间时回退到 `CreatedAt`，避免久排队任务刚开始便被误判，`AUDIT_GOVERNANCE_STUCK_EXPORT_AGE`/`-stuck-export-age` 可覆盖；`0` = 默认）由 worker 在每次 pass 中失败并写入 `export.recovered` 自审计事实（状态变更与事实同一次原子保存；终态作业永不回退；空闲 pass 零写入）。
- 事件查询 `GET /api/v1/events` / `POST /api/v1/events:query` 使用 15 分钟固定成员集，count 在续页期间不变；迟到事件只出现在新查询。HMAC 游标绑定租户、主体、全部筛选、来源和页大小，过期/缺失/篡改返回 400，不能静默接回实时结果。最多 10000 个成员、4 MiB 元数据、每租户 16 个活跃分页集，耗尽 429。成员集独立保存、不含载荷或原始敏感筛选；PostgreSQL API 部署前须应用 `008_query_snapshot.sql`，缺表启动/readiness 失败关闭。响应不声明完整性；浏览器用 POST 传筛选/摘要并显示结果集有效期。单事件及续页按坐标取证，归档在 Store 锁外，整次读取共用操作 deadline 并失败关闭。参考初页和 v1 兼容仍有历史物化成本，见 ADR-0013。
- 操作时间线与聚合历史支持 `page_size`（默认 100、最大 1000）和作用域绑定的 `cursor`；分页响应的 `count` 是总匹配数，`next_cursor` 可安全续读，旧的 service 全量方法仍保留 10,000 条保护上限。
- 归档成功后事件正文从控制面热快照淘汰，仅保留 receipt/hash 链元数据；`GET /events`、`GET /events/{id}`、操作/聚合时间线、回放、导出和完整性校验会从 WORM 归档读回并校验身份与哈希。事件查询/续页、单事件与导出对象下载已按需锁外取证；其他参考读路径仍在存储操作闭包内持 `Store.mu` 回读。两者均与数据库语句共享同一个 `AUDIT_STORE_OPERATION_TIMEOUT` 上限：单次服务操作只派生一次回读上限，回读超时按既有的 `store_timeout` 可重试 503 失败关闭，而不会按 receipt 逐个重新计时。查询结果在淘汰前后保持一致。
- `go run ./cmd/audit-outbox-relay -once` 消费业务库 `audit_outbox` 待投递记录并写入审计 API；成功标记 delivered，失败按指数退避重试，超过上限或遇到客户端错误死信。
- `go run ./cmd/audit-kafka-dlq-replay -once` 恢复死信事件（从 accepted topic 按 key 找回原消息重发或经 API 重新接入，状态文件去重）。
- 默认本地状态保存到 `./data/state.json`，归档保存到 `./data/archive`。
- 文件状态首次打开时会把 v1 单快照迁移为 v2 热/冷布局：控制面仍在
  `state.json`，每个租户的热事件/流状态在 `tenants/`，receipt、segment、checkpoint
  追加到 `ledger/`，原文件保留为 `state.json.v1` 备份；v2 布局发现缺失热文件、冷文件或临时文件时拒绝启动。
- v2 文件写入优先走 `ReadTenant`/`UpdateTenant`，只重写当前租户热文档；冷 ledger 在启动时建立索引，写入采用 fsync 后追加。
- 文件 HotFirst 随 hot 持久化原始 `pending_ledger` 追加意图；API 启动/每秒轮转租户恢复，独立于 publisher，worker 封段/归档前也先恢复。锁外验证内容、连续前序、流头及原段/checkpoint 签名，版本/journal 重新核对后追加原记录，不重新分配序号或签名。未完成意图不是账本事实，阻止该租户覆盖；其他租户/纯管理写可继续。PostgreSQL v2 HotFirst 的 hot CAS 与 ledger 在同一事务，cold 失败回滚 hot。升级前无原 journal 的 hot-only 残留不推断为提交；摄入错误不能伪装 committed receipt。完整语义和回滚要求见 ADR-0015。
- 文件模式状态强制单写者：`store.Open` 在 `<path>.lock` 上持有进程生命周期排他 flock（flock 为 advisory 且依赖文件系统，NFS 上不可靠），同一 `-state` 路径的第二个实例（audit-api 或 audit-governance-worker）启动即报错退出；多实例部署必须使用 PostgreSQL 后端（乐观版本锁）。
- 设置 `AUDIT_POSTGRES_DSN`（或 `-postgres-dsn`）后，旧部署默认继续使用 PostgreSQL 单行表 `audit_state_snapshot`（迁移 `004_state_snapshot.sql`）；要启用 v2 热/冷分层，先停 API、应用 `migrations/006_hot_cold_split.sql`，再运行 `go run ./cmd/audit-pg-migrate -confirm MIGRATE`（或调用 `store.MigratePostgresSnapshot(db)`）完成带备份的显式切换。切换后 `audit_state_snapshot` 仅保存控制面，`audit_tenant` 保存每租户热文档，`audit_ledger` 保存追加式账本；新库在已建 006 表但尚无快照行时可直接以 v2 启动。未完成切换的旧行会失败关闭，不会被自动解释为 v2。两种 PostgreSQL 布局都支持多副本共享与乐观版本锁；`Store.Update` 内做有界 jitter 重试（3 次、5–25ms 指数退避 + 抖动，闭包在新鲜快照上重跑），重试耗尽返回 503 `snapshot_conflict`；`/readyz` 同时探测 store（PostgreSQL 不可达 → 503 `store_unavailable`）与归档目标。
- PostgreSQL 请求路径的全部语句（`internal/store`）都携带存储操作上下文：每个存储操作在 `Store.mu` 之前派生一次上下文，并由 `AUDIT_STORE_OPERATION_TIMEOUT`（默认 `store.DefaultOperationTimeout` = 10s）设定上限。超时会失败关闭为新哨兵 `store.ErrOperationDeadline`（同时匹配 `context.DeadlineExceeded`）：HTTP 复用既有可重试 503 类并新增 `code=store_timeout`，gRPC 复用 `codes.Unavailable`（固定文案，恒不外泄后端细节）；计数以无标签单调计数器 `audit_store_operation_deadline_exceeded_total` 暴露。ingest 提交按 F1 不变量在 `Service.Ingest` 处一次性与调用方取消解耦——取消前中止本次摄入且不写任何数据，已持久化的提交不会被回滚。连接池参数全部由运维显式配置且**不发明默认值**：未设置即不调用任何 setter（`database/sql` 默认，`MaxOpenConnections` 无上限），非法值启动即失败并指名变量；四个 PostgreSQL 二进制共用 `internal/runtimeconfig/postgres_pool.go`。审计-api 要求该超时**严格小于** 30s `WriteTimeout`，否则可重试的 503 写不出去。指标与取值门禁见 `docs/BENCHMARKS.md`，运维变量见 `docs/DEPLOY_RUNBOOK.md`。
- 接入、幂等、租户隔离、Schema 校验、分段哈希链、查询、操作回放、导出、Legal Hold、完整性验证和恢复申请已实现。
- 哈希链证据链分三层：事件 prev_hash 链 → 段 Merkle Root + 签名 checkpoint → 跨段聚合 Merkle（governance worker 周期生成，`VerifyIntegrity` 逐层验证）。
- 导出文件在归档前整体 AES-GCM 密封（独立加密），下载时解密，`job.Digest` 覆盖解密后内容。
- 设置 `AUDIT_OTLP_ENDPOINT`（如 `http://jaeger:4318`）后启用 OpenTelemetry tracing：HTTP 中间件提取/注入 W3C `traceparent`；server span 在路由匹配后按模式创建（`span name`/`http.route` 取自匹配到的 ServeMux pattern，如 `GET /api/v1/events/{eventID}`，不包含原始路径中的 ID 值，基数有界），未匹配任何路由的请求不产生 span、`X-Trace-ID` 回退为请求 ID；未配置时自动降级为 no-op tracer。
- 查询支持按 `operation_id`、`causation_id`、`correlation_id`、`trace_id` 等关联维度筛选（与操作时间线/聚合历史配合还原业务链路）。
- 控制面管理操作全部自审计：租户/来源/Schema/留存策略变更、导出、Legal Hold、恢复申请与审批与对应变更原子写入 append-only 审计轨迹；读路径同样自审计（`GET /api/v1/events`、`GET /api/v1/events/{id}` 追加 `audit.event.read`，导出下载追加 `audit.event.export`，追加失败读请求失败关闭），可通过 `GET /api/v1/admin/actions` 查询（租户 token 仅见本租户，平台 token 可跨租户）。读轨迹的物理保留由 `AUDIT_ADMIN_TRAIL_CAP`（默认 100000，`0` = 不限）压紧：超过上限即按「水位」压实——只保留最新的 `cap − ⌊cap/2⌋` 条，之后再追加 `⌊cap/2⌋` 条才触发下一次压实，因此压实代价按 append 均摊为 O(1)（文件后端不再每个读请求重写整份 JSONL；PostgreSQL 后端不再每个读请求在 `trailMu` 内跑一次 `DELETE … OFFSET` 扫描），轨迹行数始终在 `(cap/2, cap]` 内振荡。因此需要压实后仍有 `k` 条可查时，请把 `-admin-trail-cap` 设为 `2k`（默认即压实后约 5 万条）；多副本共享同一张 `admin_action_trail` 表时，每副本的计数只是全表行数的下界，峰值行数上界为 `cap − ⌊cap/2⌋ + (⌊cap/2⌋+1) × 副本数`。
- 恢复申请支持审批流程：`POST /api/v1/restores/{runId}/approve` 与 `reject` 记录审批事实（approval 与业务执行分离），状态机 `pending_approval → approved/rejected`。
- 业务系统可使用 `internal/outbox` SDK 在事务内写入 `audit_outbox`，再由 relay 投递（迁移 `003_outbox_relay.sql` 增加投递台账列）。
- relay 投递支持两种传输：HTTP（默认）与 Kafka（设置 `AUDIT_OUTBOX_KAFKA_BROKERS` 后写入 `audit.events.accepted.v1`，acks=all 同步生产）。HTTP 投递请求 `?wait_for=ledgered` 并**校验 API receipt**（`event_id` 匹配、状态 ∈ {ledgered/indexed/archived}、`ledgered_at` 非零、`hash` 非空）后才标记 delivered；`api_status`/`delivered_event_id` 存 API 返回的真实值（Kafka 投递为 NULL，不伪造）。`audit-kafka-consumer` 以手动 offset 提交消费该 topic 并接入审计 API，失败背压重试——同一条消息原地重试、不重新拉取（kafka-go 的 fetch 位置会越过已取出的消息，重新拉取会导致失败消息被静默跳过），单消息上限 8 次（可调 `AUDIT_KAFKA_MAX_ATTEMPTS`）——永久失败（4xx 除 401/429；401 属可恢复的凭证状态、重试而非死信）立即死信并发布 `Failure` 到 `audit.events.dlq.v1`（`AUDIT_KAFKA_DLQ_TOPIC`），不可解析消息记日志死信——验证了 AsyncAPI topic 契约与 Kafka 真实容器链路（compose `redpanda`）。自 2026-08-15 起：空 `AUDIT_OUTBOX_TOKEN` 启动即警告；401 类重试耗尽死信为 `error_code="unauthorized"`（独立计数 `audit_consumer_unauthorized_total` 与告警），区别于 API 故障。
- 配置 `AUDIT_LEDGERED_BROKERS`/`-ledgered-brokers` 后，API 在 ledger 事务提交后发布链式事件到 `audit.events.ledgered.v1`；待发布事件同时写入快照内的 `ledgered_outbox`，发布失败不影响账本成功但会由 API 重启恢复和周期冲刷，broker 成功后先持久确认，再删除队列记录。消息仍按 event_id 采用至少一次语义，投影端必须幂等。发布互斥门按**待发布事件**（`store.EventKey`）而非按进程串行，且**从不被等待**：请求路径若发现该事件已被占用（通常是后台冲刷正在发布它）就让出并计数 `audit_ledgered_publish_deferred_total`（无标签、单调，原子读取，抓取 `/metrics` 不取锁），durable 记录仍在 `ledgered_outbox` 中，下一轮冲刷补齐——因此后台冲刷整轮（快照读取 + 最多 100 次发布 + 100 次确认）不再 head-of-line 阻塞同步 ingest 响应。后台健康轮次间隔为 1–2 秒（含 jitter）；失败指数退避且间隔上限 30 秒，成功轮次重置，取消可即时中断等待。启动及周期轮次会主动对账 committed cold receipt：每轮一个租户、最多 100 条账本记录，租户轮转并定期从零重扫；无需客户端重试即可补建丢失意图。身份/流/序号/哈希绑定的持久确认独立保存于 `publications/` 或 PostgreSQL 的 `audit_publication_confirmation`（开启发布前须应用迁移 `007_publication_confirmation.sql`，启动/readiness 缺表失败关闭），不把逐事件确认历史堆入共享 control。确认已落盘、Outbox 清理失败时，下轮只清理不再发消息；确认未落盘允许至少一次重发。对账读取最新回执，归档取证在锁外、共享单轮 deadline，补发前验证重建内容及链哈希。旧历史无确认时为未知，可能重发；扫描位置不是提交/投影水位，`indexed/archived` 也不证明 Kafka/ClickHouse 确认。v1 PostgreSQL 兼容路径仍物化旧快照，规模化部署须先完成 v2 cutover；HotFirst 未提交断点由独立的原始追加意图恢复处理，不能混同 committed-cold 对账；旧无 journal 残留仍为未知。详见 ADR-0011、ADR-0015。
- `audit-kafka-dlq-replay`：DLQ 重放消费者——DLQ 记录只含失败元数据，原事件按 key 从 `audit.events.accepted.v1` 恢复并逐字节重发（或 `-api-url`/`-token` 改为经审计 API 重新接入）；已重放 event_id 持久化到 `-state`（`AUDIT_DLQ_REPLAY_STATE`，默认 `./data/dlq-replay-state.json`）使重扫幂等，accepted 主题每轮从头扫描以保证新 DLQ 记录能找到更早的原消息；`-once` 供调度器单轮执行，或按 `-interval` 常驻；瞬态失败下轮重试，API 永久拒绝（4xx 除 429）标记重放完成避免死循环（需人工处理）；`-metrics-listen` 暴露 `audit_dlq_*` 指标。自 2026-08-15 起：`error_code="unauthorized"` 记录默认被 auth-blocked（不重放、不标记、不提交，计 `audit_dlq_auth_blocked_total` + 积压 gauge `audit_dlq_auth_blocked`，配 `AuditDLQAuthBlockedBacklog` 告警）——凭证修复后由运营显式加 `-replay-auth-blocked` 排空并移除（事故处置 runbook 见 release-notes）。`audit-kafka-consumer` 与重放器均可暴露文本指标端点（`AUDIT_KAFKA_METRICS`/`AUDIT_DLQ_REPLAY_METRICS`），Prometheus 规则 `deploy/prometheus-rules.verify.yml` 对 DLQ 流量、积压、认证阻塞和重放失败告警（两个新告警依赖对应 /metrics 已挂载）。
- 外部基础设施接入（全部可选、本机容器可验证）：
  - `AUDIT_VAULT_ADDR` + `AUDIT_VAULT_TOKEN` + `AUDIT_VAULT_TRANSIT_KEY`：checkpoint 签名改用 Vault Transit 引擎（私钥不出 Vault，算法标记 `vault-transit:<key>`），未配置时默认 HMAC-SHA256；地址必须 `https://…`，或本机 loopback + 显式 `AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK=true`（非 loopback 明文 http 任何情况下启动/预检失败关闭；scheme 缺失、空 host、带路径/userinfo 的地址同样失败关闭且错误信息不回显凭据）；
  - `AUDIT_S3_ENDPOINT`/`AUDIT_S3_BUCKET`/`AUDIT_S3_ACCESS_KEY`/`AUDIT_S3_SECRET_KEY`：合规归档（事件/段清单/导出）写入 S3 兼容 Object Lock 桶（MinIO 验证：删除仅产生版本删除标记），默认本地只读目录；**自 2026-08-15（R-1）起归档就绪门禁强制要求桶默认留存为 COMPLIANCE 模式 + 正有效期（Days/Years > 0，如 365 天）**——Object Lock 已启用但无默认留存、或默认留存为 GOVERNANCE/零有效期的桶会失败关闭（`-check-config`/启动探针/`/readyz` 分别报 `no default retention`/`GOVERNANCE` 类错误并指明修复：`mc retention set --default compliance 365d <bucket>`）；已写对象的留存固定在写入时刻（继承当时的桶默认），不受后续配置变更影响；**每次写入还携带显式 COMPLIANCE 留存，时长由 `AUDIT_ARCHIVE_RETENTION_DAYS` 决定（正整数，如 365；上限 100 年；缺失/为零即配置错误，两个二进制 fail-closed）**——即使桶默认留存被移除/降级，写入窗口内的对象仍受保护，`StatusArchived` 回执与探测时机无关；`AUDIT_S3_USE_SSL=true` 启用 TLS，`https://` scheme 端点必须与之匹配否则失败关闭（绝不静默降级明文）；明文 http 仅允许既有本机/开发端点（`localhost`、loopback IP、`host.docker.internal`、`gateway.docker.internal`）以及验证栈精确端点 `minio:9000`（可带 `http://`，主机名不区分大小写），任意其他非 loopback 端点均失败关闭并提示设置 `AUDIT_S3_USE_SSL=true`，见 `docs/THREAT_MODEL.md` §3.8；
  - 两个二进制 `-check-config` 的 `check_config=ok` 逐腿报告 `transport_s3=`/`transport_vault=`（`tls`/`http`/`local`），格式串字节一致；
  - 两个二进制的 `-consistency-key` 是无网络、无归档探针的纯预部署模式，只输出非敏感的 `consistency_key=`；先运行 `python3 cli.py build`，再用相同的合并部署环境运行 `python3 cli.py consistency-check`（或 `python3 cli.py predeploy`）。该键是签名方案/Transit key 名称与归档桶或目录的**名称级一致性**信号，不替代完整 `-check-config`、传输门禁或密钥投递校验；HMAC 键的规范标识为 `HMAC-SHA256`，与运行时 `signer=hmac-sha256`/`HMAC-SHA256(dev-compatible)` 的展示文本差异是有意的；
- gRPC ingest 支持 `AUDIT_GRPC_TLS_CERT`/`AUDIT_GRPC_TLS_KEY` 与 `AUDIT_GRPC_TLS_CLIENT_CA` 原生 TLS/mTLS；生产可设置 `AUDIT_GRPC_REQUIRE_MTLS=true`（或 `-grpc-require-mtls`），此时 gRPC 监听器无论是否回环或配置明文 allowlist 都必须验证客户端证书。`AUDIT_ALLOW_INSECURE_GRPC_LISTEN=true` 仅保留给隔离验证栈。乐观锁重试耗尽（`ErrSnapshotConflict`，请求未入账）在 gRPC 上返回可重试的 `Unavailable`、在 `WriteBatch` 上标记 `rejection_code="unavailable"`，与 HTTP 的 503 `snapshot_conflict` 同一语义分类；客户端消息固定为 `state snapshot changed concurrently; retry the request`，后端细节只进服务端日志。
- 签名与加密密钥强制显式配置：`AUDIT_SIGNING_SECRET`（段/聚合 checkpoint HMAC 签名）与 `AUDIT_ENCRYPTION_KEY`（schema 加密字段、导出文件 AES-GCM）任一为空、等于公开默认值、两者相同，或任一配置值少于 32 个 UTF-8 字节（非开发模式）时，`audit-api`/`audit-governance-worker` 启动失败（退出码非零，错误信息指明需设置的变量）；本机开发须显式设置 `AUDIT_ALLOW_DEV_SECRETS=true`（或 `-allow-dev-secrets`，独立于 `-allow-dev-auth`）才恢复旧默认行为。长度门禁不等同于熵检查：不要用给弱口令补 padding 的方式满足要求，应使用充分随机性的两个独立值，例如分别运行两次 `openssl rand -base64 48`。本项目不自动轮换密钥或重加密既有数据。`-check-config` 不打开状态存储、不绑定监听器，供部署预检与 CI 使用（两个进程必须使用相同的两个值）：`audit-api` 的校验密钥、Vault/S3 与认证配置后退出（不发起网络）；`audit-governance-worker` 的还会对归档目的地做一次有界探测（5 秒超时；S3 配置会触网——桶必须存在且启用 Object Lock、versioning，且默认留存为 COMPLIANCE + 正有效期（R-1，2026-08-15），本地归档做可写性探测），失败时退出码 1 且不打印 `check_config=ok`。认证配置与启动同一规则：无 JWT 信任源（开发认证默认关闭）即失败，`-allow-dev-auth` 单独无法满足预检，开发认证白名单仅接受环境变量 `AUDIT_ALLOW_DEV_AUTH=true`（见 ADR-0007）。
  - `audit-projector`：消费 `audit.events.ledgered.v1` 同步写入 ClickHouse 新表 `audit_projection_events`（tenant/stream/sequence/event/hash/包摘要排序键、按月分区，启用插入/目录 fsync），强制关闭 async insert；不同变体保留并失败关闭，相同消息允许重复且逻辑统计按租户/event 去重。旧 `audit_events` 仅作兼容镜像，不作投影确认。`Confirmed` 校验当前目的地的精确链坐标，`Watermark` 仅表示单个租户/流无缺口且哈希相连的当前前缀，不是全租户完整证明或固定查询快照。`audit-projector rebuild -tenant <id>` 从已提交账本有界取证重建（无需 Kafka 保留期、锁外归档验证），`inspect -tenant <id> -stream <id>` 检查当前目的地；不改回执/Outbox，不触发业务执行、不自动删除数据，见 ADR-0012 与部署手册。查询 API 默认使用参考读路径；显式设置 `AUDIT_QUERY_CLICKHOUSE_DSN`/`-query-clickhouse-dsn` 后，事件检索用只读 ClickHouse 候选索引与逻辑去重 count，核对 committed receipts 后仍从 Ledger/WORM 取证，不直接输出投影载荷。失败/冲突不隐式降级；纳秒边界与 1900–2299 年契约保持。敏感摘要条件明确走账本，不把摘要发给 ClickHouse。完整性、全租户 Console facets、所有历史成本与容量验收仍独立，详见 ADR-0013。投影失败写入独立死信 topic `audit.events.projection.dlq.v1`（`-dlq-topic` / `AUDIT_KAFKA_DLQ_TOPIC` 可覆盖）：投影原像在 ledgered topic，而 `audit-kafka-dlq-replay` 只按 key 从 accepted topic 恢复原消息，因此它既不能解决也不能关闭投影失败——投影失败须从 ledgered topic 重驱或运行经验证的账本重建工具，`audit-kafka-dlq-replay` 不适用。把投影死信指向摄入 DLQ（或反过来把 consumer/replay 指向投影死信）会在启动时 fail closed。
- 导出任务支持状态轮询和租户鉴权的 JSONL 下载。
- API 契约位于 `api/openapi`、`api/asyncapi` 和 `api/proto`。
- 本机隔离依赖配置位于 `deploy/docker-compose.verify.yml`。

开发令牌仅用于本机验证，例如 `Bearer dev:demo:service`；需要显式模拟来源客户端时使用 `Bearer dev:demo:service:<client_id>`。开发认证自 2026-08-06 起默认关闭（`-allow-dev-auth` 默认 false，`AUDIT_ALLOW_DEV_AUTH=true` 显式启用，非法取值直接启动失败）；自 B1-1 收口起，`-allow-dev-auth` **单独（无 env 白名单）时真实启动也拒绝**——与 `-check-config` 同一规则（AC-3），仅环境变量 `AUDIT_ALLOW_DEV_AUTH=true` 是合法途径；`cli.py quality` 的 manifest 扫描同时拒绝非 verify 部署清单中出现 dev auth 开启项。生产环境必须保持关闭并接入 OIDC/JWT、外部 Kafka、PostgreSQL、ClickHouse、WORM 存储和 KMS/HSM。

事件写入还会把签名访问令牌中的 `client_id` 与来源系统绑定。兼容发行方可仅提供 `azp`，但 `client_id` 与 `azp` 同时存在时必须一致；`sub` 永不作为客户端身份。来源的 `allowed_client_ids` 是精确匹配列表；空列表安全默认只允许 `client_id == source.id`。

Snaplink 的 client_credentials Token 不投射通用 `tenant_id`。写入服务因此从服务端来源注册中按 `(client_id, source_system)` 唯一解析租户；请求体 `tenant_id` 永不参与解析——若携带则必须与解析出的租户一致（不一致 → 422 `tenant_mismatch` 拒绝入账，禁止静默重标，DS-08）。若 Token 自带签名 tenant claim，它只会收窄到该租户且仍需通过来源绑定。零命中或跨租户多命中都会失败关闭，因此同一个 client/source 组合不能跨租户复用。

生产 JWT/JWKS 验证仅允许 EdDSA（Ed25519）、ES256/384/512、RS256 和 PS256，并要求 JWK 的 `alg`、`kty`、`crv`、`use` 和可选 `key_ops` 一致。配置 `AUDIT_JWKS_URL` 时要求 HTTPS；本机 loopback HTTP 必须显式设置 `AUDIT_ALLOW_INSECURE_JWKS_LOOPBACK=true`。配置 JWKS 或本地 PEM 公钥时还必须设置非空的 `AUDIT_JWT_ISSUER` 与 `AUDIT_JWT_AUDIENCE`，并拒绝缺失或不匹配的 `iss`/`aud`；`-check-config` 与启动执行相同门禁。仅显式开发认证可启动未完成的非生产 asymmetric 配置，但真实 JWT 仍必须通过两项 claim 校验。本地 PEM 使用 `AUDIT_JWT_PUBLIC_KEY_PEM` 和固定的 `AUDIT_JWT_PUBLIC_KEY_ALG`（默认 RS256）。HS256 仅保留为隔离的本机模式，必须同时设置 `AUDIT_JWT_SECRET` 与 `AUDIT_ALLOW_LOCAL_HS256=true`，并且不能与 JWKS 或本地公钥共同配置。
- 三个客户端二进制（`audit-outbox-relay`、`audit-kafka-consumer`、`audit-kafka-dlq-replay` HTTP 模式）的 `-api-url`/`AUDIT_OUTBOX_API_URL` 自 2026-08-15 起有传输门禁（RFC 6750 §1，bearer token 不得明文过网）：`https://` 或 loopback（localhost/127.0.0.1/::1/host.docker.internal）放行；非 loopback 明文 `http://` 启动失败关闭，除非显式设置 `AUDIT_ALLOW_INSECURE_API_URL=true`（仅限本机验证栈，启动时打印醒目警告；compose 已内置该变量并标注 dev-only）。生产必须用 https。

升级已有环境时先应用 `migrations/002_source_identity_binding.sql`。文件状态会自动把缺失字段读为空列表。若已有写入客户端 ID 与来源 ID 不同，必须在切换写流量前预填数据库字段，或先启动新版本控制面并通过 `PUT /api/v1/sources/{sourceId}` 配置 `allowed_client_ids`；否则写入会以 403（gRPC `PermissionDenied`）失败关闭。原来仅配置 `AUDIT_JWT_SECRET` 的本机环境还需显式增加 `AUDIT_ALLOW_LOCAL_HS256=true`；生产环境应迁移到 Snaplink 的 JWKS。

运行测试：

```sh
go test ./...
```

工程质量检查统一入口（`python3 cli.py quality` 是 `AGENTS.md` 强制的改动门禁）：

```sh
python3 cli.py check       # gofmt、文件大小、go vet、单元测试
python3 cli.py quality     # 完整质量门禁：约 28 个阶段，含竞态与发布构建
python3 cli.py race        # race detector
python3 cli.py build       # 构建 API/worker 等发布二进制
python3 cli.py bench       # 本机基准（基准结果见 docs/BENCHMARKS.md）
python3 cli.py help
```

门禁的阶段清单、失败行如何按阶段单独重跑、以及门禁自身的接线约束，见
[docs/ENGINEERING_GATE.md](docs/ENGINEERING_GATE.md)。也可以使用等价的 `make check`、
`make quality`、`make test`、`make race` 和 `make build`。

## Web 管理端

仓库内的 [`web`](web/README.md) 是基于 Iris UI Svelte 5 的审计治理管理端，通过
Snaplink Hosted Login 完成 OIDC Authorization Code + PKCE 登录，并对接本服务的
事件查询、操作时间线、合规证据、Legal Hold、恢复审批和治理目录接口。

```sh
corepack pnpm install --dir web
make web-dev       # http://localhost:5178，默认代理 API 到 localhost:8089
make web-check     # 单测、类型检查、lint、生产构建
make web-stack-up  # Snaplink Hosted Login + PKCE + Audit API 完整本机联调
```

Snaplink 必须注册 public client `audit-governance-web`；完整回调、代理和容器部署配置见
[`web/README.md`](web/README.md)。

## 契约与运维文档

功能规格留在本文件；跨文件、跨发布、跨值班的信息各自成篇：

- [契约同步](docs/CONTRACT_SYNC.md)：`api/proto` 生成代码的三道防线、
  `make proto` 与漂移模拟、契约改动时的最小动作。
- [部署 / 回滚 runbook](docs/DEPLOY_RUNBOOK.md)：WORM COMPLIANCE 默认留存门禁（R-1）、
  既有桶 cutover 分类、回滚语义、跨进程一致性键、本机全栈验证脚本。
- [工程门禁](docs/ENGINEERING_GATE.md)：`python3 cli.py quality` 的 28 个阶段与设计约束。

完整的文档地图见 [`docs/README.md`](docs/README.md)。

## 回执与发布语义

| 现有状态/信号 | 表示什么 | 不表示什么 |
|---|---|---|
| `accepted` 响应 | 接入已接受；参考实现可能内部已完成后续阶段 | 异步链路的账本成功保证 |
| `ledgered` / `ledgered_at`、hash | 账本已提交、链坐标已分配 | Kafka、ClickHouse 或归档成功 |
| `indexed` / `indexed_at` | 当前参考读路径可用；归档失败也可能停留此状态 | ClickHouse 持久投影确认 |
| `archived` / `archived_at` | 事件及本轮已封段写入配置的归档，冷回执保留 | broker 确认；本地目录的正式 WORM 合规认证 |
| 持久发布确认 | broker 接受过该租户/事件的精确链坐标 | 投影成功、全历史完整或端到端 exactly-once |
| `duplicate` / `conflict` | 本次接入结果；同内容复用、异内容拒绝 | 业务执行回执或状态恢复 |

`wait_for` 沿用以上兼容状态，不将 `indexed` 伪装成尚未接入的投影确认。
恢复 `approved` 仍仅是治理决策，业务执行不在当前状态机内。

## 核心原则

- API-first，浏览器管理界面独立部署。
- 多租户隔离是所有读写路径的强制约束。
- 写入链路采用至少一次投递和端到端幂等，不依赖理想化的全局
  exactly-once。
- Kafka 是传输和重放层，不是长期合规归档。
- ClickHouse 是查询投影，不是不可变事实源。
- Redis 只用于缓存、限流和短期协调。
- 不建立全局单链，按租户和逻辑流建立分段哈希链并签名检查点。
- 审批完成与业务执行完成是两个独立事实。
