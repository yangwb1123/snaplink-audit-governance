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
- [术语表](docs/GLOSSARY.md)
- [平台批次联合核查报告（G1–G8 门禁矩阵）](docs/platform-gate-status.md)
- [ADR-0001 消息传输与接入](docs/adr/ADR-0001.md)
- [ADR-0002 事件编码与 Schema 兼容](docs/adr/ADR-0002.md)
- [ADR-0003 哈希链分段与检查点](docs/adr/ADR-0003.md)
- [ADR-0004 查询投影与租户路由](docs/adr/ADR-0004.md)
- [ADR-0005 合规归档与密钥](docs/adr/ADR-0005.md)
- [ADR-0006 租户 ID 键框架校验](docs/adr/ADR-0006.md)
- [ADR-0007 开发认证默认失败关闭](docs/adr/ADR-0007.md)

## 当前实现

仓库现在包含一个可运行的 Go 单节点参考实现：

- `go run ./cmd/audit-api` 启动 REST API。
- `go run ./cmd/audit-governance-worker -once` 执行一次留存/Legal Hold 评估。
- 聚合 checkpoint 每租户按上限保留（默认 1000 条，drop-oldest，`AUDIT_AGGREGATE_CHECKPOINT_HISTORY` 可覆盖，下限 1）；根与签名未变化的周期去重、空闲周期不写快照，`VerifyIntegrity` 聚合校验工作量由此有界。
- 导出作业卡在 `running` 超过阈值（默认 24 小时，从 `CreatedAt` 起算，`AUDIT_GOVERNANCE_STUCK_EXPORT_AGE`/`-stuck-export-age` 可覆盖；`0` = 默认）由 worker 在每次 pass 中失败并写入 `export.recovered` 自审计事实（状态变更与事实同一次原子保存；终态作业永不回退；空闲 pass 零写入）。
- 操作时间线与聚合历史支持 `page_size`（默认 100、最大 1000）和作用域绑定的 `cursor`；分页响应的 `count` 是总匹配数，`next_cursor` 可安全续读，旧的 service 全量方法仍保留 10,000 条保护上限。
- 归档成功后事件正文从控制面热快照淘汰，仅保留 receipt/hash 链元数据；`GET /events`、`GET /events/{id}`、操作/聚合时间线、回放、导出和完整性校验会从 WORM 归档读回并校验身份与哈希。查询结果在淘汰前后保持一致。
- `go run ./cmd/audit-outbox-relay -once` 消费业务库 `audit_outbox` 待投递记录并写入审计 API；成功标记 delivered，失败按指数退避重试，超过上限或遇到客户端错误死信。
- `go run ./cmd/audit-kafka-dlq-replay -once` 恢复死信事件（从 accepted topic 按 key 找回原消息重发或经 API 重新接入，状态文件去重）。
- 默认本地状态保存到 `./data/state.json`，归档保存到 `./data/archive`。
- 文件状态首次打开时会把 v1 单快照迁移为 v2 热/冷布局：控制面仍在
  `state.json`，每个租户的热事件/流状态在 `tenants/`，receipt、segment、checkpoint
  追加到 `ledger/`，原文件保留为 `state.json.v1` 备份；v2 布局发现缺失热文件、冷文件或临时文件时拒绝启动。
- v2 文件写入优先走 `ReadTenant`/`UpdateTenant`，只重写当前租户热文档；冷 ledger 在启动时建立索引，写入采用 fsync 后追加。
- 文件模式状态强制单写者：`store.Open` 在 `<path>.lock` 上持有进程生命周期排他 flock（flock 为 advisory 且依赖文件系统，NFS 上不可靠），同一 `-state` 路径的第二个实例（audit-api 或 audit-governance-worker）启动即报错退出；多实例部署必须使用 PostgreSQL 后端（乐观版本锁）。
- 设置 `AUDIT_POSTGRES_DSN`（或 `-postgres-dsn`）后，旧部署默认继续使用 PostgreSQL 单行表 `audit_state_snapshot`（迁移 `004_state_snapshot.sql`）；要启用 v2 热/冷分层，先停 API、应用 `migrations/006_hot_cold_split.sql`，再运行 `go run ./cmd/audit-pg-migrate -confirm MIGRATE`（或调用 `store.MigratePostgresSnapshot(db)`）完成带备份的显式切换。切换后 `audit_state_snapshot` 仅保存控制面，`audit_tenant` 保存每租户热文档，`audit_ledger` 保存追加式账本；新库在已建 006 表但尚无快照行时可直接以 v2 启动。未完成切换的旧行会失败关闭，不会被自动解释为 v2。两种 PostgreSQL 布局都支持多副本共享与乐观版本锁；`Store.Update` 内做有界 jitter 重试（3 次、5–25ms 指数退避 + 抖动，闭包在新鲜快照上重跑），重试耗尽返回 503 `snapshot_conflict`；`/readyz` 同时探测 store（PostgreSQL 不可达 → 503 `store_unavailable`）与归档目标。
- 接入、幂等、租户隔离、Schema 校验、分段哈希链、查询、操作回放、导出、Legal Hold、完整性验证和恢复申请已实现。
- 哈希链证据链分三层：事件 prev_hash 链 → 段 Merkle Root + 签名 checkpoint → 跨段聚合 Merkle（governance worker 周期生成，`VerifyIntegrity` 逐层验证）。
- 导出文件在归档前整体 AES-GCM 密封（独立加密），下载时解密，`job.Digest` 覆盖解密后内容。
- 设置 `AUDIT_OTLP_ENDPOINT`（如 `http://jaeger:4318`）后启用 OpenTelemetry tracing：HTTP 中间件提取/注入 W3C `traceparent`；server span 在路由匹配后按模式创建（`span name`/`http.route` 取自匹配到的 ServeMux pattern，如 `GET /api/v1/events/{eventID}`，不包含原始路径中的 ID 值，基数有界），未匹配任何路由的请求不产生 span、`X-Trace-ID` 回退为请求 ID；未配置时自动降级为 no-op tracer。
- 查询支持按 `operation_id`、`causation_id`、`correlation_id`、`trace_id` 等关联维度筛选（与操作时间线/聚合历史配合还原业务链路）。
- 控制面管理操作全部自审计：租户/来源/Schema/留存策略变更、导出、Legal Hold、恢复申请与审批与对应变更原子写入 append-only 审计轨迹；读路径同样自审计（`GET /api/v1/events`、`GET /api/v1/events/{id}` 追加 `audit.event.read`，导出下载追加 `audit.event.export`，追加失败读请求失败关闭），可通过 `GET /api/v1/admin/actions` 查询（租户 token 仅见本租户，平台 token 可跨租户）。
- 恢复申请支持审批流程：`POST /api/v1/restores/{runId}/approve` 与 `reject` 记录审批事实（approval 与业务执行分离），状态机 `pending_approval → approved/rejected`。
- 业务系统可使用 `internal/outbox` SDK 在事务内写入 `audit_outbox`，再由 relay 投递（迁移 `003_outbox_relay.sql` 增加投递台账列）。
- relay 投递支持两种传输：HTTP（默认）与 Kafka（设置 `AUDIT_OUTBOX_KAFKA_BROKERS` 后写入 `audit.events.accepted.v1`，acks=all 同步生产）。HTTP 投递请求 `?wait_for=ledgered` 并**校验 API receipt**（`event_id` 匹配、状态 ∈ {ledgered/indexed/archived}、`ledgered_at` 非零、`hash` 非空）后才标记 delivered；`api_status`/`delivered_event_id` 存 API 返回的真实值（Kafka 投递为 NULL，不伪造）。`audit-kafka-consumer` 以手动 offset 提交消费该 topic 并接入审计 API，失败背压重试——同一条消息原地重试、不重新拉取（kafka-go 的 fetch 位置会越过已取出的消息，重新拉取会导致失败消息被静默跳过），单消息上限 8 次（可调 `AUDIT_KAFKA_MAX_ATTEMPTS`）——永久失败（4xx 除 401/429；401 属可恢复的凭证状态、重试而非死信）立即死信并发布 `Failure` 到 `audit.events.dlq.v1`（`AUDIT_KAFKA_DLQ_TOPIC`），不可解析消息记日志死信——验证了 AsyncAPI topic 契约与 Kafka 真实容器链路（compose `redpanda`）。自 2026-08-15 起：空 `AUDIT_OUTBOX_TOKEN` 启动即警告；401 类重试耗尽死信为 `error_code="unauthorized"`（独立计数 `audit_consumer_unauthorized_total` 与告警），区别于 API 故障。
- 配置 `AUDIT_LEDGERED_BROKERS`/`-ledgered-brokers` 后，API 在 ledger 事务提交后发布链式事件到 `audit.events.ledgered.v1`；待发布事件同时写入快照内的 `ledgered_outbox`，发布失败不影响账本成功但会由 API 重启恢复和周期冲刷，成功后删除队列记录。消息仍按 event_id 采用至少一次语义，投影端必须幂等。
- `audit-kafka-dlq-replay`：DLQ 重放消费者——DLQ 记录只含失败元数据，原事件按 key 从 `audit.events.accepted.v1` 恢复并逐字节重发（或 `-api-url`/`-token` 改为经审计 API 重新接入）；已重放 event_id 持久化到 `-state`（`AUDIT_DLQ_REPLAY_STATE`，默认 `./data/dlq-replay-state.json`）使重扫幂等，accepted 主题每轮从头扫描以保证新 DLQ 记录能找到更早的原消息；`-once` 供调度器单轮执行，或按 `-interval` 常驻；瞬态失败下轮重试，API 永久拒绝（4xx 除 429）标记重放完成避免死循环（需人工处理）；`-metrics-listen` 暴露 `audit_dlq_*` 指标。自 2026-08-15 起：`error_code="unauthorized"` 记录默认被 auth-blocked（不重放、不标记、不提交，计 `audit_dlq_auth_blocked_total` + 积压 gauge `audit_dlq_auth_blocked`，配 `AuditDLQAuthBlockedBacklog` 告警）——凭证修复后由运营显式加 `-replay-auth-blocked` 排空并移除（事故处置 runbook 见 release-notes）。`audit-kafka-consumer` 与重放器均可暴露文本指标端点（`AUDIT_KAFKA_METRICS`/`AUDIT_DLQ_REPLAY_METRICS`），Prometheus 规则 `deploy/prometheus-rules.verify.yml` 对 DLQ 流量、积压、认证阻塞和重放失败告警（两个新告警依赖对应 /metrics 已挂载）。
- 外部基础设施接入（全部可选、本机容器可验证）：
  - `AUDIT_VAULT_ADDR` + `AUDIT_VAULT_TOKEN` + `AUDIT_VAULT_TRANSIT_KEY`：checkpoint 签名改用 Vault Transit 引擎（私钥不出 Vault，算法标记 `vault-transit:<key>`），未配置时默认 HMAC-SHA256；地址必须 `https://…`，或本机 loopback + 显式 `AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK=true`（非 loopback 明文 http 任何情况下启动/预检失败关闭；scheme 缺失、空 host、带路径/userinfo 的地址同样失败关闭且错误信息不回显凭据）；
  - `AUDIT_S3_ENDPOINT`/`AUDIT_S3_BUCKET`/`AUDIT_S3_ACCESS_KEY`/`AUDIT_S3_SECRET_KEY`：合规归档（事件/段清单/导出）写入 S3 兼容 Object Lock 桶（MinIO 验证：删除仅产生版本删除标记），默认本地只读目录；**自 2026-08-15（R-1）起归档就绪门禁强制要求桶默认留存为 COMPLIANCE 模式 + 正有效期（Days/Years > 0，如 365 天）**——Object Lock 已启用但无默认留存、或默认留存为 GOVERNANCE/零有效期的桶会失败关闭（`-check-config`/启动探针/`/readyz` 分别报 `no default retention`/`GOVERNANCE` 类错误并指明修复：`mc retention set --default compliance 365d <bucket>`）；已写对象的留存固定在写入时刻（继承当时的桶默认），不受后续配置变更影响；**每次写入还携带显式 COMPLIANCE 留存，时长由 `AUDIT_ARCHIVE_RETENTION_DAYS` 决定（正整数，如 365；上限 100 年；缺失/为零即配置错误，两个二进制 fail-closed）**——即使桶默认留存被移除/降级，写入窗口内的对象仍受保护，`StatusArchived` 回执与探测时机无关；`AUDIT_S3_USE_SSL=true` 启用 TLS，`https://` scheme 端点必须与之匹配否则失败关闭（绝不静默降级明文）；scheme 缺失的非 loopback 端点默认允许明文 http（本机开发兼容，残余风险：明文链路上静态密钥与归档证据可被 MITM 读取，由 `transport_s3=http` + CI 断言观察式强制，见 `docs/THREAT_MODEL.md` §3.7）；
  - 两个二进制 `-check-config` 的 `check_config=ok` 逐腿报告 `transport_s3=`/`transport_vault=`（`tls`/`http`/`local`），格式串字节一致；
- gRPC ingest 支持 `AUDIT_GRPC_TLS_CERT`/`AUDIT_GRPC_TLS_KEY` 与 `AUDIT_GRPC_TLS_CLIENT_CA` 原生 TLS/mTLS；生产可设置 `AUDIT_GRPC_REQUIRE_MTLS=true`（或 `-grpc-require-mtls`），此时 gRPC 监听器无论是否回环或配置明文 allowlist 都必须验证客户端证书。`AUDIT_ALLOW_INSECURE_GRPC_LISTEN=true` 仅保留给隔离验证栈。
- 签名与加密密钥强制显式配置：`AUDIT_SIGNING_SECRET`（段/聚合 checkpoint HMAC 签名）与 `AUDIT_ENCRYPTION_KEY`（schema 加密字段、导出文件 AES-GCM）任一为空、等于公开默认值或两者相同（非开发模式）时，`audit-api`/`audit-governance-worker` 启动失败（退出码非零，错误信息指明需设置的变量）；本机开发须显式设置 `AUDIT_ALLOW_DEV_SECRETS=true`（或 `-allow-dev-secrets`，独立于 `-allow-dev-auth`）才恢复旧默认行为。`-check-config` 不打开状态存储、不绑定监听器，供部署预检与 CI 使用（两个进程必须使用相同的两个值）：`audit-api` 的校验密钥、Vault/S3 与认证配置后退出（不发起网络）；`audit-governance-worker` 的还会对归档目的地做一次有界探测（5 秒超时；S3 配置会触网——桶必须存在且启用 Object Lock、versioning，且默认留存为 COMPLIANCE + 正有效期（R-1，2026-08-15），本地归档做可写性探测），失败时退出码 1 且不打印 `check_config=ok`。认证配置与启动同一规则：无 JWT 信任源（开发认证默认关闭）即失败，`-allow-dev-auth` 单独无法满足预检，开发认证白名单仅接受环境变量 `AUDIT_ALLOW_DEV_AUTH=true`（见 ADR-0007）。
  - `audit-projector`：消费 `audit.events.ledgered.v1` 写入 ClickHouse 查询投影（`ReplacingMergeTree` 按 event_id 去重、tenant 前缀排序键、按月分区），投影可重建、非事实源。
- 导出任务支持状态轮询和租户鉴权的 JSONL 下载。
- API 契约位于 `api/openapi`、`api/asyncapi` 和 `api/proto`。
- 本机隔离依赖配置位于 `deploy/docker-compose.verify.yml`。

开发令牌仅用于本机验证，例如 `Bearer dev:demo:service`；需要显式模拟来源客户端时使用 `Bearer dev:demo:service:<client_id>`。开发认证自 2026-08-06 起默认关闭（`-allow-dev-auth` 默认 false，`AUDIT_ALLOW_DEV_AUTH=true` 显式启用，非法取值直接启动失败）；自 B1-1 收口起，`-allow-dev-auth` **单独（无 env 白名单）时真实启动也拒绝**——与 `-check-config` 同一规则（AC-3），仅环境变量 `AUDIT_ALLOW_DEV_AUTH=true` 是合法途径；`cli.py quality` 的 manifest 扫描同时拒绝非 verify 部署清单中出现 dev auth 开启项。生产环境必须保持关闭并接入 OIDC/JWT、外部 Kafka、PostgreSQL、ClickHouse、WORM 存储和 KMS/HSM。

事件写入还会把签名访问令牌中的 `client_id` 与来源系统绑定。兼容发行方可仅提供 `azp`，但 `client_id` 与 `azp` 同时存在时必须一致；`sub` 永不作为客户端身份。来源的 `allowed_client_ids` 是精确匹配列表；空列表安全默认只允许 `client_id == source.id`。

Snaplink 的 client_credentials Token 不投射通用 `tenant_id`。写入服务因此从服务端来源注册中按 `(client_id, source_system)` 唯一解析租户；请求体 `tenant_id` 永不参与解析——若携带则必须与解析出的租户一致（不一致 → 422 `tenant_mismatch` 拒绝入账，禁止静默重标，DS-08）。若 Token 自带签名 tenant claim，它只会收窄到该租户且仍需通过来源绑定。零命中或跨租户多命中都会失败关闭，因此同一个 client/source 组合不能跨租户复用。

生产 JWT/JWKS 验证仅允许 EdDSA（Ed25519）、ES256/384/512、RS256 和 PS256，并要求 JWK 的 `alg`、`kty`、`crv`、`use` 和可选 `key_ops` 一致。配置 `AUDIT_JWKS_URL` 时要求 HTTPS；本机 loopback HTTP 必须显式设置 `AUDIT_ALLOW_INSECURE_JWKS_LOOPBACK=true`。本地 PEM 使用 `AUDIT_JWT_PUBLIC_KEY_PEM` 和固定的 `AUDIT_JWT_PUBLIC_KEY_ALG`（默认 RS256）。HS256 仅保留为隔离的本机模式，必须同时设置 `AUDIT_JWT_SECRET` 与 `AUDIT_ALLOW_LOCAL_HS256=true`，并且不能与 JWKS 或本地公钥共同配置。
- 三个客户端二进制（`audit-outbox-relay`、`audit-kafka-consumer`、`audit-kafka-dlq-replay` HTTP 模式）的 `-api-url`/`AUDIT_OUTBOX_API_URL` 自 2026-08-15 起有传输门禁（RFC 6750 §1，bearer token 不得明文过网）：`https://` 或 loopback（localhost/127.0.0.1/::1/host.docker.internal）放行；非 loopback 明文 `http://` 启动失败关闭，除非显式设置 `AUDIT_ALLOW_INSECURE_API_URL=true`（仅限本机验证栈，启动时打印醒目警告；compose 已内置该变量并标注 dev-only）。生产必须用 https。

升级已有环境时先应用 `migrations/002_source_identity_binding.sql`。文件状态会自动把缺失字段读为空列表。若已有写入客户端 ID 与来源 ID 不同，必须在切换写流量前预填数据库字段，或先启动新版本控制面并通过 `PUT /api/v1/sources/{sourceId}` 配置 `allowed_client_ids`；否则写入会以 403（gRPC `PermissionDenied`）失败关闭。原来仅配置 `AUDIT_JWT_SECRET` 的本机环境还需显式增加 `AUDIT_ALLOW_LOCAL_HS256=true`；生产环境应迁移到 Snaplink 的 JWKS。

运行测试：

```sh
go test ./...
```

工程质量检查统一入口：

```sh
python3 cli.py check       # gofmt、文件大小、go vet、单元测试
python3 cli.py quality     # 完整质量门禁：复杂度、架构、竞态、构建等
python3 cli.py race        # race detector
python3 cli.py bench       # 本机基准（基准结果见 docs/BENCHMARKS.md）
python3 cli.py accept      # 完整验收：check、race、构建、路由契约
python3 cli.py help
```

也可以使用等价的 `make check`、`make quality`、`make test`、`make race` 和 `make build`。

## Web 管理端

仓库内的 [`web`](web/README.md) 是基于 Iris UI React 的审计治理管理端，通过
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

### Proto 生成代码同步（api/proto）

`api/proto/audit.proto` 与检入的生成代码（`audit.pb.go`/`audit_grpc.pb.go`，即 gRPC
运行时实际编译的线面）由三道防线守护，全部挂在 `python3 cli.py quality` 中：

1. **描述符解析（零外部工具，始终执行）**：`checks/proto_sync.py` 直接解码
   `audit.pb.go` 内嵌的 `file_audit_proto_rawDesc`（纯标准库 wire walker），逐消息逐字段
   与 `audit.proto` 比对——`.proto` 新增字段但未重新生成会立即失败并点名该字段/消息。
2. **生成器版本钉（始终执行）**：`engineering.yaml` 的 `proto:` 块是唯一 pin 清单；
   生成头注释中的版本与 pin 不一致（或 `protoc-gen-go` pin 与 `go.mod` 的
   `google.golang.org/protobuf` 版本不一致）即失败。
3. **字节级重放（工具链存在时）**：`scripts/proto-gen.py --check` 用钉住的工具链在临时
   目录重新生成并与检入文件逐字节比对，注释/格式级漂移也能捕获；工具链缺失时显式打印
   跳过说明，描述符与版本检查仍强制执行（失败关闭，绝不静默通过）。

重新生成（首次运行会下载钉住的 protoc 到 `bin/protoc-<pin>/` 并用 `go install @<pin>`
安装两个生成器到被 gitignore 的 `bin/`，不触碰 `go.mod`/`go.sum`）：

```sh
make proto
```

CI 无差异断言（本地可复现，`make proto` 在未漂移树上幂等）：

```sh
make proto && test -z "$(git status --porcelain api/proto)"
```

漂移模拟（在 `git archive HEAD` 副本中注入字段、跑完整门禁、恢复后再跑一次，绝不改动
工作区）：

```sh
bash scripts/drift-simulate.sh
```

门禁级漂移测试的等价命令：`AUDIT_DRIFT_SIM=1 python3 -m unittest checks.test_proto_sync`
（默认跳过以避免完整门禁耗时翻倍）。

全栈容器验证（outbox → relay → Kafka → 账本 + ClickHouse 投影 + MinIO WORM 归档 +
DLQ 重放 + gRPC 入站，2026-08-07 真实容器全链路通过）：

```sh
bash test/e2e/fullstack.sh
```

B1-7 真实 IdP token 注入（G1 模式，**自包含**）：设置 `AUDIT_IDP_CLIENT_ID`/
`AUDIT_IDP_CLIENT_SECRET` 两个变量即可——compose 栈自带 `audit-idp` 容器
（从兄弟仓库 snaplink 构建，配置 `deploy/idp.verify.yaml`），e2e 自动铸造带
`tenant_id`/`scope` claims 的 JWT 并把栈切换到 **dev auth 关闭 + JWKS 验证**
（dev token 负向断言 401；JWKS 经 host-gateway 别名访问容器 IdP；issuer 校验）。
验证栈同时发送 RFC 8707 `resource=audit-governance`，并在 Audit API 强制校验同名
audience；可选 `AUDIT_IDP_SCOPE`/`AUDIT_IDP_RESOURCE`/`AUDIT_IDP_TOKEN_URL` 覆盖。未配置时过渡期使用 dev
token 并告警。两种模式均实测通过（2026-08-07）。

### COMPLIANCE 默认留存 cutover 验证（R-1 部署配套，2026-08-15）

fullstack.sh 在启动任何应用之前自举归档桶并**自动化 cutover 验证**：用 worker
二进制（宿主 `go build`，5 秒有界探针）对一次性 `worm-audit-cutover` 桶依次断言

| 桶状态 | `-check-config` 期望 |
|---|---|
| 无默认留存（`--with-lock` + versioning 后） | 退出码 1 + `no default retention`（R-3a），不打印 `check_config=ok` |
| `mc retention set --default governance 365d` | 退出码 1 + `GOVERNANCE`（R-3b），不打印 `check_config=ok` |
| `mc retention set --default compliance 365d` | 退出码 0 + `check_config=ok` |

随后给真实 `worm-audit` 桶设置 COMPLIANCE 365d 默认留存（幂等覆盖）并正向复核，
然后才启动 worker——否则 worker 启动即 fatal 进入重启退避。scratch 桶从不写入
数据、无留存对象，`mc rb --force` 可安全重建（带留存对象的 WORM 桶无法强制删除）。
本镜像内置 mc 的 retention 子命令使用**位置参数模式**（`mc retention set
--default compliance 365d`，而非 `--compliance`），已在 pinned 镜像上实测。

### 部署 / 回滚 runbook（R-1 COMPLIANCE 默认留存门禁）

**部署（新环境）**：

1. 创建/校验归档桶：`mc mb --with-lock <alias>/worm-audit` + `mc version enable`
   + `mc retention set --default compliance 365d <alias>/worm-audit`。
2. 预检（不打开状态存储、不绑定监听器；S3 仅做 5 秒有界探针）：
   `AUDIT_SIGNING_SECRET=… AUDIT_ENCRYPTION_KEY=… AUDIT_S3_ENDPOINT=…
   AUDIT_S3_BUCKET=worm-audit AUDIT_S3_ACCESS_KEY=… AUDIT_S3_SECRET_KEY=…
   ./bin/audit-governance-worker -check-config` → 退出码 0 且输出
   `check_config=ok … archive=s3`。
3. 部署新二进制（API + worker 同发布），观察 worker 启动日志
   `archive_ready=ok`；`/readyz` 返回 200。

**部署（既有桶 cutover）**：对每个现存归档桶先跑
`./bin/audit-governance-worker -check-config` 分类：

- 报 `no default retention`（R-3a）或 `GOVERNANCE`（R-3b）→ 桶处于可删除
  状态，先修复再升级：`mc retention set --default compliance 365d <bucket>`，
  重跑 `-check-config` 到 `check_config=ok`。
- 已是 COMPLIANCE + 正有效期 → 直接升级，行为不变。

已写对象不受影响（留存固定在写入时刻）；门禁只保护新写入。

**回滚**：

1. 重新部署旧二进制（`git revert` 或部署上一版本镜像）。旧二进制接受所有既有
   桶配置（pre-gate 行为），包括 GOVERNANCE/无默认留存——因此回滚总是成功。
2. 桶上已设置的 COMPLIANCE 默认留存**无需撤销**：它是合法配置，旧二进制同样
   接受（默认留存只约束新对象，不删除/改写任何数据）。
3. 若回滚发生在 cutover 之后且新二进制已写入对象：对象留存继承自写入时刻的
   桶默认，不会因回滚解除；无需清理。
4. fullstack.sh 的 cutover 负向断言依赖 R-1 门禁（pre-gate 二进制三类桶全通过）
   ——回滚到 pre-gate 后 CI 不应再跑该 e2e 的负向断言，或与门禁代码一起回滚。

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
