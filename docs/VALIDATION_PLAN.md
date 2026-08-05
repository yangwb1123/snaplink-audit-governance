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
false。允许的签名算法为 EdDSA/Ed25519、ES256/384/512、RS256、PS256。
仅本机需要测试 HS256 时，必须同时设置 `AUDIT_JWT_SECRET` 和
`AUDIT_ALLOW_LOCAL_HS256=true`，且不得设置 JWKS 或 PEM 公钥。

client_credentials Token 可不含 tenant_id。验证前应为每租户注册来源及
`allowed_client_ids`，确保 `(client_id, source_system)` 只匹配一个租户；
测试请求体伪造 tenant_id 不改变回执租户，并验证跨租户重复绑定会被拒绝。

### 启动隔离依赖

```sh
docker compose -f deploy/docker-compose.verify.yml config
docker compose -f deploy/docker-compose.verify.yml up -d
```

Compose 使用 `19000+` 端口和独立网络/数据卷，不应复用本机已有服务的数据。

## 验证顺序

1. Schema、规范化 JSON、哈希和游标单元测试。
2. 事件接入、`event_id` 幂等、内容冲突和租户隔离。
3. 账本 sequence、`prev_hash`、Merkle Root、checkpoint 和归档文件。
4. 操作时间线、聚合历史、无副作用回放。
5. 导出任务、Legal Hold、恢复预览和补偿恢复申请。
6. Jaeger/Prometheus 接入、敏感字段日志扫描和 API 负向测试。
7. 单节点逐级压测，并单独记录本机上限。

## 不在本机宣称通过的项目

- Kafka 三物理节点和跨可用区副本故障。
- PostgreSQL fencing、PITR 和跨地域 RPO/RTO。
- ClickHouse shard/replica 高可用。
- 正式 S3 Object Lock、KMS/Vault/HSM 的合规证明。
- 50,000 events/s 稳态、100,000 events/s 突发生产 SLO。

这些项目必须在隔离的多节点环境中完成故障演练和容量报告。
