# 本机验证计划

本计划针对当前仓库的单节点参考实现，不能替代三可用区、跨地域和正式 WORM 合规验收。

## 本机基线

- Zorin OS 18.1，Linux 7.0，x86_64。
- AMD Ryzen AI Max+ 395，16 核 32 线程。
- 内存 124GiB，无 Swap；盘剩余约 1.5TiB。
- Docker 29.7.0、Compose 5.3.1、Go 1.26.5。
- Kubernetes v1.28.2，单节点 control-plane，`local-path` RWO 存储。
- 已有 PostgreSQL、Redis、NATS、MinIO、Jaeger、Prometheus、Grafana 和 OIDC 测试服务。
- 当前没有 Kafka、ClickHouse、OpenSearch、Temporal 的现成实例。

## 运行方式

### 仅运行参考 API

```sh
go run ./cmd/audit-api \
  -state /tmp/snaplink-audit-state.json \
  -archive /tmp/snaplink-audit-archive \
  -listen :8089
```

开发令牌格式为 `dev:<tenant>:<role>[:<client_id>]`，例如：

```text
Authorization: Bearer dev:demo:service
Authorization: Bearer dev:demo:service:snaplink-commerce
Authorization: Bearer dev:demo:tenant-auditor
```

省略第四段时，本机开发客户端 ID 默认为 tenant，因而只可写入同名来源
（或显式允许该客户端的来源）。该认证方式仅用于本机验证，生产环境必须
关闭并配置 OIDC/JWT 验证。

与 Snaplink 联调时使用 HTTPS `AUDIT_JWKS_URL`、匹配的
`AUDIT_JWT_ISSUER`/`AUDIT_JWT_AUDIENCE`，并将 `AUDIT_ALLOW_DEV_AUTH` 设为
false（自 2026-08-06 起默认即为 false：开发认证默认失败关闭，只有显式
`AUDIT_ALLOW_DEV_AUTH=true` 才启用）。允许的签名算法为 EdDSA/Ed25519、ES256/384/512、RS256、PS256。
仅本机需要测试 HS256 时，必须同时设置 `AUDIT_JWT_SECRET` 和
`AUDIT_ALLOW_LOCAL_HS256=true`，且不得设置 JWKS 或 PEM 公钥。自
2026-08-15 起 `AUDIT_JWT_SECRET` 必须至少 32 字节（256 位，与 HS256
密钥尺寸一致；低于 32 字节在启动与 `-check-config` 预检均失败关闭，
错误为 `local HS256 JWT secret must be at least 32 bytes`），且必须与
`AUDIT_SIGNING_SECRET`/`AUDIT_ENCRYPTION_KEY` 不同（同一字符串不得既
伪造令牌又签名校验点或解密受保护字段）。

client_credentials Token 可不含 tenant_id。验证前应为每租户注册来源及
`allowed_client_ids`，确保 `(client_id, source_system)` 只匹配一个租户；
测试请求体伪造 tenant_id 不改变回执租户，并验证跨租户重复绑定会被拒绝。

### 启动隔离依赖

```sh
docker compose -f deploy/docker-compose.verify.yml config
docker compose -f deploy/docker-compose.verify.yml up -d
```

Compose 使用 `19000+` 端口和独立网络/数据卷，不应复用本机已有服务的数据。

### 签名与加密密钥

checkpoint 签名密钥 `AUDIT_SIGNING_SECRET` 与加密密钥
`AUDIT_ENCRYPTION_KEY`（schema 加密字段、导出文件）必须显式配置为两个
不同的强随机值（例如分别运行两次 `openssl rand -base64 48`）。非开发模式
下每个配置值还必须至少有 32 个 UTF-8 字节。任一为空、等于公开默认值
（`development-signing-key-change-me` / `development-encryption-key-change-me`）、
两者相同或长度不足时，两个二进制启动失败（退出码非零，错误信息指明需设置的变量）。
这是长度门禁而非熵证明，不能通过给弱口令补 padding 规避。长度不足的既有部署
需要操作员保留旧密钥并执行解密/重加密及签名/checkpoint 迁移，本项目不自动轮换
或重加密。仅本机开发须显式设置 `AUDIT_ALLOW_DEV_SECRETS=true`（或
`-allow-dev-secrets`，独立于 `-allow-dev-auth`）才恢复旧默认行为。

部署预检（不打开状态存储、不绑定监听器；API 的 `-check-config` 不发起网络，
worker 的会做一次有界归档目的地探测——S3 触网且要求桶已启用 Object Lock、
versioning，且默认留存为 **COMPLIANCE + 正有效期**（R-1，2026-08-15：无默认
留存/GOVERNANCE/零有效期桶均失败关闭，错误分别含 `no default retention`/
`GOVERNANCE` 并指明 `mc retention set --default compliance 365d` 修复）；S3
归档还强制配置 `AUDIT_ARCHIVE_RETENTION_DAYS`（正整数，缺失/为零即配置错误，
每次 Put 携带显式 COMPLIANCE 留存，F1）；本地
归档做可写性探测，失败退出码 1；预检与启动使用相同的认证校验规则）：

```sh
AUDIT_SIGNING_SECRET=... AUDIT_ENCRYPTION_KEY=... \
AUDIT_JWT_SECRET=... AUDIT_ALLOW_LOCAL_HS256=true ./bin/audit-api -check-config
AUDIT_SIGNING_SECRET=... AUDIT_ENCRYPTION_KEY=... ./bin/audit-governance-worker -check-config
```

退出码 0 且输出不含 `=well-known-default` 警告即通过；API 与 worker
必须使用相同的两个值（共用同一条证据链，AC-3）。自 2026-08-06 起预检
同时校验认证配置：没有任何 JWT 信任源（密钥/PEM/JWKS，开发认证默认
关闭）的配置会失败；`-allow-dev-auth` 单独无法满足预检（仅环境变量
`AUDIT_ALLOW_DEV_AUTH=true` 可白名单开发认证，失败时输出
`auth=dev_auth_flag_not_allowlisted` 指明原因）；本机开发栈须在运行时
环境（而非仅 CI 预检步骤）显式设置 `AUDIT_ALLOW_DEV_AUTH=true`。自
2026-08-15 起 API 的 `check_config=ok` 行在 `encryption_key_length` 后
新增 `jwt_secret_length` 字段（无 JWT 信任源时为 0，仅输出长度、不输出
密钥值）。

跨进程一致性预检使用独立的无网络模式，避免把 S3 可用性误报为一致性
失败：构建两个二进制后，以同一份合并部署环境运行
`./bin/audit-api -consistency-key` 与
`./bin/audit-governance-worker -consistency-key`，或运行
`python3 cli.py consistency-check`。该命令要求两边都成功输出且仅输出一个
`consistency_key=`，值不一致或缺失均以非零退出；它不能替代下面的完整
`-check-config`（worker 的归档 WORM 探针仍需单独执行）。`consistency_key`
只表达方案和归档名称级一致性，不包含密钥、token、凭据或 endpoint 主机。

## 验证顺序

1. Schema、规范化 JSON、哈希和游标单元测试。
2. 事件接入、`event_id` 幂等、内容冲突和租户隔离。
3. 账本 sequence、`prev_hash`、Merkle Root、checkpoint 和归档文件。
4. 操作时间线、聚合历史、无副作用回放。
5. 导出任务、Legal Hold、恢复预览和补偿恢复申请。
6. Jaeger/Prometheus 接入、敏感字段日志扫描和 API 负向测试。
7. 单节点逐级压测，并单独记录本机上限。

## 外部基础设施验证状态（2026-08-05 更新）

以下基础设施已在本机容器完成真实链路验证：

- **PostgreSQL**：控制面快照后端（乐观锁）、outbox 表（relay 台账）。
- **Redpanda（Kafka 兼容）**：outbox → relay(Kafka) → topic → consumer/audit-api
  全链路；手动 offset 提交、失败背压、不可解析消息死信（unparsable_message
  DLQ 记录 + 计数）。
- **DLQ 重放（2026-08-07 真实链路闭环）**：未注册 schema 事件 → 422 permanent
  → `audit.events.dlq.v1` Failure；注册 schema 后 `audit-kafka-dlq-replay -once`
  按 key 从 accepted topic 恢复原消息重发 → consumer 重新接入 → 事件入账，
  状态文件持久化；`-once` 使用独立 consumer group（避免与常驻实例 rebalance
  竞争），每阶段独立 drain 窗口。
- **ClickHouse**：`audit-projector` 消费 ledgered topic 写入查询投影表
  （ReplacingMergeTree、tenant 前缀排序键、按月分区），投影可 SQL 查询。
- **MinIO（Object Lock）**：事件/段清单/导出写入 `--with-lock` 桶；
  桶默认留存为 **COMPLIANCE 365d**（R-1 门禁必需，fullstack.sh 自举：
  `mc retention set --default compliance 365d`），删除仅产生版本删除标记，
  对象不可物理删除。
- **COMPLIANCE cutover 验证矩阵（2026-08-15，fullstack.sh 内自动化）**：对
  一次性 scratch 桶（无数据、可 `mc rb --force` 重建）以 worker
  `-check-config` 断言三态——无默认留存 → 退出码 1 + `no default retention`
  （R-3a）；`--default governance 365d` → 退出码 1 + `GOVERNANCE`（R-3b）；
  `--default compliance 365d` → 退出码 0 + `check_config=ok`。真实
  `worm-audit` 桶随后设为 COMPLIANCE 365d 并正向复核。cutover 负向断言仅在
  R-1 门禁进入部署二进制后成立（pre-gate 二进制三类桶全通过）。
- **Vault（Transit）**：checkpoint 签名改走 Transit 引擎（`AUDIT_VAULT_*`），
  私钥不出 Vault；协议层由单测覆盖，真实 Vault 容器可另行启动。
- **Jaeger/Prometheus**：OTLP 导出已验证；指标端点接 Prometheus。

门控集成测试：`AUDIT_TEST_POSTGRES_DSN`、`AUDIT_TEST_CLICKHOUSE_DSN` 设置后
运行 `go test ./internal/store/ ./internal/outbox/ ./internal/projection/`。

## 不在本机宣称通过的项目

- Kafka 三物理节点和跨可用区副本故障。
- PostgreSQL fencing、PITR 和跨地域 RPO/RTO。
- ClickHouse shard/replica 高可用。
- 正式 S3 Object Lock、KMS/Vault/HSM 的合规证明。
- 50,000 events/s 稳态、100,000 events/s 突发生产 SLO。

这些项目必须在隔离的多节点环境中完成故障演练和容量报告。
