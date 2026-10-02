# 部署 / 回滚 runbook

本文件是运营文档：WORM 归档桶的 COMPLIANCE 留存门禁（R-1）、cutover 验证、部署与回滚
步骤，以及本机全栈验证脚本。产品边界与功能面见根 [`README.md`](../README.md)。

## 归档就绪门禁（R-1，2026-08-15 起）

两个二进制在配置了 S3 归档时**启动即失败关闭**，除非：

- 桶默认留存为 **COMPLIANCE 模式且有效期为正**（`mc retention set --default compliance 365d`）。
  仅有 Object Lock 但无默认留存、默认留存为 `GOVERNANCE`、或零有效期，一律拒绝；
- 每次写入还携带**显式 COMPLIANCE 留存**，时长由 `AUDIT_ARCHIVE_RETENTION_DAYS` 决定
  （正整数，如 365，上限 100 年；缺失或为零即配置错误）。

已写对象的留存固定在写入时刻（继承当时的桶默认），不受后续配置变更影响；第二次门禁
（每次写入的显式留存）保证即使桶默认被移除或降级，写入窗口内的对象仍受保护。

预检（不打开状态存储、不绑定监听器；S3 只做 5 秒有界探针）：

```sh
AUDIT_SIGNING_SECRET=… AUDIT_ENCRYPTION_KEY=… \
AUDIT_S3_ENDPOINT=… AUDIT_S3_BUCKET=worm-audit \
AUDIT_S3_ACCESS_KEY=… AUDIT_S3_SECRET_KEY=… \
./bin/audit-governance-worker -check-config
# 期望：退出码 0 且输出 check_config=ok … archive=s3
```

失败时退出码 1 且**不打印** `check_config=ok`，错误信息会指出是 `no default retention`
还是 `GOVERNANCE` 类问题。`/readyz` 与启动探针复用同一判定。

传输门禁：`https://` scheme 的端点必须配 `AUDIT_S3_USE_SSL=true`；明文 http 只允许
本机/开发端点（`localhost`、loopback IP、`host.docker.internal`、`gateway.docker.internal`）
以及验证栈的 `minio:9000`；其余非 loopback 明文端点启动即失败并提示设置
`AUDIT_S3_USE_SSL=true`（见 `THREAT_MODEL.md` §3.8）。

## 部署（新环境）

1. 创建/校验归档桶：

   ```sh
   mc mb --with-lock <alias>/worm-audit
   mc version enable <alias>/worm-audit
   mc retention set --default compliance 365d <alias>/worm-audit
   ```

2. 预检（命令见上），退出码 0。
3. 部署新二进制（API + worker 同一发布），观察 worker 启动日志 `archive_ready=ok`，
   `/readyz` 返回 200。

## 部署（既有桶 cutover）

对每个现存归档桶先跑 `-check-config` 分类：

- 报 `no default retention`（R-3a）或 `GOVERNANCE`（R-3b）→ 桶处于可删除状态，**先修复再升级**：
  `mc retention set --default compliance 365d <bucket>`，重跑 `-check-config` 到 `check_config=ok`。
- 已是 COMPLIANCE + 正有效期 → 直接升级，行为不变。

已写对象不受影响（留存固定在写入时刻）；门禁只保护新写入。

## 回滚

1. 重新部署旧二进制（`git revert` 或部署上一版本镜像）。旧二进制接受所有既有桶配置
   （pre-gate 行为），包括 GOVERNANCE / 无默认留存——因此回滚总是成功。
2. 桶上已设置的 COMPLIANCE 默认留存**无需撤销**：它是合法配置，旧二进制同样接受
   （默认留存只约束新对象，不删除/改写任何数据）。
3. 若回滚发生在 cutover 之后且新二进制已写入对象：对象留存继承自写入时刻的桶默认，
   不会因回滚解除；无需清理。
4. `test/e2e/fullstack.sh` 的 cutover 负向断言依赖 R-1 门禁（pre-gate 二进制三类桶全通过）
   ——回滚到 pre-gate 后 CI 不应再跑该 e2e 的负向断言，或与门禁代码一起回滚。

## 一致性键（跨进程一致性）

API 与 worker 必须对同一份证据签名和写入同一处归档。合并部署环境后运行：

```sh
python3 cli.py build            # 先构建两个二进制
python3 cli.py consistency-check # 比较纯一致性键（无网络、无归档探针）
# 或一步到位：python3 cli.py predeploy
```

输出一致即表示签名方案/Transit key 名称与归档桶（或目录）**名称级**一致。这不替代
`-check-config`、传输门禁或密钥投递校验。

## 本机全栈验证

```sh
bash test/e2e/fullstack.sh   # outbox → relay → Kafka → 账本 + ClickHouse 投影 +
                            # MinIO WORM 归档 + DLQ 重放 + gRPC 入站
```

该脚本在启动任何应用之前自举归档桶并**自动化 cutover 验证**，对一次性
`worm-audit-cutover` 桶依次断言：

| 桶状态 | `-check-config` 期望 |
|---|---|
| 无默认留存（`--with-lock` + versioning 后） | 退出码 1 + `no default retention`，不打印 `check_config=ok` |
| `mc retention set --default governance 365d` | 退出码 1 + `GOVERNANCE`，不打印 `check_config=ok` |
| `mc retention set --default compliance 365d` | 退出码 0 + `check_config=ok` |

随后给真实 `worm-audit` 桶设置 COMPLIANCE 365d 默认留存（幂等覆盖）并正向复核，然后才
启动 worker——否则 worker 启动即 fatal 进入重启退避。scratch 桶从不写入数据、无留存
对象，`mc rb --force` 可安全重建（带留存对象的 WORM 桶无法强制删除）。

真实 IdP 令牌注入（G1 模式，自包含）：设置 `AUDIT_IDP_CLIENT_ID` /
`AUDIT_IDP_CLIENT_SECRET` 两个变量即可——compose 栈自带 `audit-idp` 容器，e2e 自动
铸造带 `tenant_id`/`scope` claims 的 JWT 并把栈切换到 **dev auth 关闭 + JWKS 验证**
（dev token 负向断言 401；issuer 校验；RFC 8707 `resource=audit-governance` audience
强制校验）。未配置时过渡期使用 dev token 并告警。

其他 e2e：

```sh
make web-stack-up          # Snaplink Hosted Login + PKCE + Audit API 联调
make web-auth-e2e
make e2e-tenant-scope
make e2e-projector-permanent-dlq
```

本机隔离依赖栈定义在 `deploy/docker-compose.verify.yml`。

## Kafka topic 预创建（DLQ 分离后）

两个死信 topic 的恢复语义不同，必须分别预创建，并给对应服务主体授权（生产 broker
关闭 auto-create 时，topic 缺失会让 `Consumer.deadLetter` 发布失败、offset 不提交，
投影管道停滞——停滞而非丢数）：

```sh
rpk topic create audit.events.dlq.v1 --partitions 1 --replicas 1               # 摄入 DLQ
rpk topic create audit.events.projection.dlq.v1 --partitions 1 --replicas 1     # 投影 DLQ
```

发布顺序：**先建 topic 与 ACL，再滚动 `audit-projector`**。`audit-projector` 默认
`-dlq-topic=audit.events.projection.dlq.v1`（`AUDIT_KAFKA_DLQ_TOPIC` 可覆盖）；滚动
后其启动日志会打印 `dlq_topic="audit.events.projection.dlq.v1"`，据此确认生效。
`audit-kafka-dlq-replay` 保持默认摄入 DLQ 与 accepted topic，无需改动。

投影 DLQ 的记录**不能**用 `audit-kafka-dlq-replay` 恢复：它的原像在
`audit.events.ledgered.v1`，而该工具只按 key 从 accepted topic 恢复原消息，喂入投影
死信只会得到无意义的 accepted 重投或永久 unresolvable 关闭。恢复动作是从 ledgered
topic 重新驱动投影；把 `-dlq-topic` 指向投影 DLQ（或把 consumer/replay 指向摄入
DLQ 以外的对端地址）会在启动时 fail closed 且不建立任何 Kafka 连接。
