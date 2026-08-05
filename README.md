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

当前阶段为架构设计期，尚未承诺任何生产功能或兼容性。

## 设计文档

- [架构设计计划](docs/ARCHITECTURE_PLAN.md)
- [本机验证计划](docs/VALIDATION_PLAN.md)
- [本机性能基线](docs/BENCHMARKS.md)
- [威胁模型](docs/THREAT_MODEL.md)
- [术语表](docs/GLOSSARY.md)
- [ADR-0001 消息传输与接入](docs/adr/ADR-0001.md)
- [ADR-0002 事件编码与 Schema 兼容](docs/adr/ADR-0002.md)
- [ADR-0003 哈希链分段与检查点](docs/adr/ADR-0003.md)
- [ADR-0004 查询投影与租户路由](docs/adr/ADR-0004.md)
- [ADR-0005 合规归档与密钥](docs/adr/ADR-0005.md)

## 当前实现

仓库现在包含一个可运行的 Go 单节点参考实现：

- `go run ./cmd/audit-api` 启动 REST API。
- `go run ./cmd/audit-governance-worker -once` 执行一次留存/Legal Hold 评估。
- `go run ./cmd/audit-outbox-relay -once` 消费业务库 `audit_outbox` 待投递记录并写入审计 API；成功标记 delivered，失败按指数退避重试，超过上限或遇到客户端错误死信。
- 默认本地状态保存到 `./data/state.json`，归档保存到 `./data/archive`。
- 设置 `AUDIT_POSTGRES_DSN`（或 `-postgres-dsn`）后，控制面状态快照保存在 PostgreSQL 单行表 `audit_state_snapshot`（迁移 `004_state_snapshot.sql`），支持多副本共享；乐观版本锁防止丢失更新。
- 接入、幂等、租户隔离、Schema 校验、分段哈希链、查询、操作回放、导出、Legal Hold、完整性验证和恢复申请已实现。
- 设置 `AUDIT_OTLP_ENDPOINT`（如 `http://jaeger:4318`）后启用 OpenTelemetry tracing：HTTP 中间件提取/注入 W3C `traceparent`、为每个请求创建 server span 并导出到 Jaeger；未配置时自动降级为 no-op tracer。
- 查询支持按 `operation_id`、`causation_id`、`correlation_id`、`trace_id` 等关联维度筛选（与操作时间线/聚合历史配合还原业务链路）。
- 控制面管理操作全部自审计：租户/来源/Schema/留存策略变更、导出、Legal Hold、恢复申请与审批与对应变更原子写入 append-only 审计轨迹，可通过 `GET /api/v1/admin/actions` 查询（租户 token 仅见本租户，平台 token 可跨租户）。
- 恢复申请支持审批流程：`POST /api/v1/restores/{runId}/approve` 与 `reject` 记录审批事实（approval 与业务执行分离），状态机 `pending_approval → approved/rejected`。
- 业务系统可使用 `internal/outbox` SDK 在事务内写入 `audit_outbox`，再由 relay 投递（迁移 `003_outbox_relay.sql` 增加投递台账列）。
- 导出任务支持状态轮询和租户鉴权的 JSONL 下载。
- API 契约位于 `api/openapi`、`api/asyncapi` 和 `api/proto`。
- 本机隔离依赖配置位于 `deploy/docker-compose.verify.yml`。

开发令牌仅用于本机验证，例如 `Bearer dev:demo:service`；需要显式模拟来源客户端时使用 `Bearer dev:demo:service:<client_id>`。生产环境必须关闭开发认证并接入 OIDC/JWT、外部 Kafka、PostgreSQL、ClickHouse、WORM 存储和 KMS/HSM。

事件写入还会把签名访问令牌中的 `client_id` 与来源系统绑定。兼容发行方可仅提供 `azp`，但 `client_id` 与 `azp` 同时存在时必须一致；`sub` 永不作为客户端身份。来源的 `allowed_client_ids` 是精确匹配列表；空列表安全默认只允许 `client_id == source.id`。

Snaplink 的 client_credentials Token 不投射通用 `tenant_id`。写入服务因此从服务端来源注册中按 `(client_id, source_system)` 唯一解析租户；请求体 tenant 永远不会参与解析。若 Token 自带签名 tenant claim，它只会收窄到该租户且仍需通过来源绑定。零命中或跨租户多命中都会失败关闭，因此同一个 client/source 组合不能跨租户复用。

生产 JWT/JWKS 验证仅允许 EdDSA（Ed25519）、ES256/384/512、RS256 和 PS256，并要求 JWK 的 `alg`、`kty`、`crv`、`use` 和可选 `key_ops` 一致。配置 `AUDIT_JWKS_URL` 时要求 HTTPS；本机 loopback HTTP 必须显式设置 `AUDIT_ALLOW_INSECURE_JWKS_LOOPBACK=true`。本地 PEM 使用 `AUDIT_JWT_PUBLIC_KEY_PEM` 和固定的 `AUDIT_JWT_PUBLIC_KEY_ALG`（默认 RS256）。HS256 仅保留为隔离的本机模式，必须同时设置 `AUDIT_JWT_SECRET` 与 `AUDIT_ALLOW_LOCAL_HS256=true`，并且不能与 JWKS 或本地公钥共同配置。

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
