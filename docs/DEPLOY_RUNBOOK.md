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

## 投影检查与账本重建

升级 projector 会创建 `audit_projection_events` 新表并验证变体保留排序键、设置 fsync，
强制同步 insert；旧 `audit_events` 仍写入兼容镜像，但不充当确认。旧历史在新表缺失时是
未知，不能根据 broker 确认或 `indexed/archived` 补造已投影状态。需要 DDL/ALTER 权限，
表设置不兼容时失败关闭；副本/分片/备份与容灾需另行验收，不能把本机 fsync 当作跨区保证。

使用与 API 相同的密钥、S3/Vault 环境（凭证勿放命令行、日志或 shell tracing）：

```sh
# AUDIT_POSTGRES_DSN / AUDIT_CLICKHOUSE_DSN / AUDIT_SIGNING_SECRET /
# AUDIT_ENCRYPTION_KEY 由安全环境注入；归档配置与 API 一致。
./bin/audit-projector rebuild -tenant tenant-a -page-size 100 -sweeps 2
./bin/audit-projector inspect -tenant tenant-a -stream stream-id
# 可选检查一个已知账本坐标：
./bin/audit-projector inspect -tenant tenant-a -stream stream-id \
  -event event-id -sequence 10 -hash ledger-hash
```

- 文件后端重建前停 API/worker，`-state` 指向现存账本；遵守单写者锁，不用另一个路径
  创建空账本冒充重建。PostgreSQL 重建可在线，但 v1 历史仍物化旧快照，规模部署先 cutover。
- 工具直接写投影，不发布 Kafka、不重置组偏移、不改回执/Outbox/审批，不调用业务 API。
  缺失/坏证据不写入并非零退出；目的地中断不推进页位置，重新从零运行安全。
- 写入数是操作数，回执版本/重复 sweep 可能重复；逻辑统计按租户/事件去重。
- 两次有限 sweep 不是完整提交快照：写入可持续，较小 SQL ID 也可能晚提交。
  日志明确 `completeness=not_asserted`。迁移验收须源端静默/一致性核对，逐流比较
  `sequence/hash` 与账本头，不能只看最大序号、Outbox 清空或重建命令退出 0。
- 检查缺号返回前缀和 `gap=true`，变体/断链失败；目的地被删除后不会继承旧确认。
  工具不设置 TTL、不删表、不缩短 COMPLIANCE；回滚旧 projector 可忽略新表，但不应
  将历史旧表计数视作新版确认。完整设计见 ADR-0012。

## 固定查询与 ClickHouse 候选读取

v1/v2 PostgreSQL **API 升级前应用 `008_query_snapshot.sql`**。启动与 readiness 探测
独立成员集表；不创建缺表，不退回进程内状态。全栈验证脚本已应用 008。
文件模式成员集位于 split root 的 `query-snapshots/`，独立于 control/publications/WORM。
有效期 15 分钟、最多 10000 个成员/4 MiB、每租户 16 个活跃分页集；创建时回收过期
运行元数据，不删除证据。丢失成员集或轮换签名键只使旧游标失效，重新查询即可。

默认查询使用参考读路径。需要 ClickHouse 候选索引时，通过安全环境注入
`AUDIT_QUERY_CLICKHOUSE_DSN`（不要传带密码的命令行参数），先用 projector 初始化/重建
新表、核对当前目的地与账本头，再启用 API。API 角色只须 SELECT（含 schema 探测），
不替 projector 执行 DDL/ALTER；启动/readiness 依赖失败关闭，响应不会暗中改用另一个
实时来源。已保存成员集续页从账本取证，不依赖目的地继续可达。

查询 count 为该固定结果集的逻辑身份数，不是投影完整性水位。敏感摘要条件显式走
账本，不发送 ClickHouse；浏览器用 `POST /api/v1/events:query`，URL 仅含 tenant selector。
旧 GET 保留兼容，不建议通过 URL 发送摘要。已有 Console facets/时间线保留原读语义，
不是本成员集的异步统计快照。参考初页仍扫描历史，v1 续页仍物化旧快照；容量部署
先做 v2 cutover，并独立验收 ClickHouse 聚合/排序与逐身份 ledger 核对成本。

只允许回收运行成员集；不因该功能修改 ClickHouse TTL、归档保留期、Hold 或账本。
完整语义见 [ADR-0013](adr/ADR-0013.md)。

## Ledgered 发布升级与主动对账

配置 `AUDIT_LEDGERED_BROKERS` 的 PostgreSQL 部署（v1/v2 都适用）必须**先应用**
`migrations/007_publication_confirmation.sql`，再升级 API。API 启动和 `/readyz` 探测
独立确认表，缺失时失败关闭；`-check-config` 无网络，不能证明已应用迁移。
验证脚本 fullstack/consumer-tenant-scope 已按此顺序应用 007。

- 确认表及文件模式 `publications/` 与状态一起备份，不含事件载荷。
- broker 成功 → 确认落盘 → 清理 Outbox；确认失败保留意图，清理失败只重试清理。
- 升级前没有确认的历史事件可能被至少一次补发，先核对 projector/消费者租户与 event_id 幂等，
  评估升级重驱流量；不得依据 `indexed/archived` 或空 Outbox 填造已确认记录。
- 后台启动即执行对账；每轮一个租户、最多 100 条 cold 记录，完成后从零重扫。
  v1 PostgreSQL 仍物化旧快照，要有界历史 IO 须先按 README 完成迁移 006 的 v2 cutover。
- `audit_ledgered_reconcile_scanned_total` 是扫描工作量，不是投影水位；
  `audit_ledgered_reconcile_evidence_failures_total` 提示缺失/损坏/无法认证的证据或确认元数据；
  `audit_ledgered_reconcile_errors_total` 提示未完成的轮次（存储/取消/超时）。
  修复源对象、Schema/密钥或确认元数据前不强制发布坏证据；后续 sweep 会重新检查。
- 确认已落盘而清理失败的恢复轮次返回发布数 0，这是正常 cleanup-only，不是停止修复。
  修复/成功计数是操作数，不是不同事件数；Outbox 清空也不证明 ClickHouse 完整。
- 回滚保留新增表/文件（旧版本忽略）；丢失交付元数据会重复投递，不会撤销账本事实。
  只写 hot、尚未提交 cold 的断点用下述独立 journal 恢复，不将发布对账解释为提交。

设计决策见 [ADR-0011](adr/ADR-0011.md)。

## HotFirst 热冷断点恢复

文件 HotFirst 的 `pending_ledger` 随租户热文档持久化，保留原 receipt/segment/checkpoint
记录；它不是 committed ledger truth。API 不依赖 publisher，启动立即、每秒轮转一个
租户恢复；worker 封段/归档前先恢复。内容、精确 Schema/密钥、前序/流头与原签名验证
失败时保留 journal、阻止该租户覆盖，其他租户/纯控制面操作可继续；不要抹掉标记、
改写序号、重新签名或将正文复制到新账本。恢复后只完成 cold 提交，欠发布意图另由
committed-cold 对账收敛，不证明 Kafka/ClickHouse 完整。

`audit_hot_commit_recovered_records_total` 是原记录追加操作数；
`audit_hot_commit_recovery_failures_total` / `AuditHotCommitRecoveryFailures` 独立于 publisher。
完整备份同一份 control、tenants、ledger 和 journal，并保留同一签名/加密环境。回滚
旧版本前停写、用新版本完成 journal 恢复并核对 cold，旧版本忽略 marker 不是安全回滚。
升级前无 journal 的历史 hot-only 残留保持未知，需人工核对原计划，不能拼造 receipt。
PostgreSQL v2 不产生文件 journal：hot CAS 与 ledger 追加在同一事务，cold 失败整体回滚，
control/Outbox 仍单独对账。v1 路径与全历史成本限制未变，见 ADR-0015。

## 导出队列容量与故障收敛

同一共享 control 中 pending/running 最多全局 128、租户 32；创建 CAS 耗尽 429，
不创建作业或 export.created。执行领取另有限额：全局 2、租户 1，API/worker 共用，
不通过加副本乘大并发。API 启动及每秒唤醒有界 dispatcher；租户轮转、租户内
oldest-first；恢复 API 返回 backlog，不表示已领取/完成。

running 崩溃残留会占配额，必须运行同密钥/归档配置的 governance worker；既有
started_at/24h 默认卡死恢复规则不变。先检查任务真实运行状态，不改终态、放宽预算
或缩短 COMPLIANCE 来恢复吞吐。尚无租约/心跳/检查点或可续跑的分块导出；整块
物化/密封的内存、共享 control 历史成本及真实 PG 容量须另验收。详见 ADR-0014。
下载对象读取已移到锁外，共用一次 Store deadline；仍在所有认证/Hold/自审计通过
后才输出明文，不能将该改造表述为认证前输出的流式大文件下载。

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

## 存储操作超时与连接池（PostgreSQL 控制面）

请求路径上的每一条 PostgreSQL 语句都在进程级 `Store.mu` 之下执行，因此每条语句都
必须带上下文，并受**存储操作截止时间**约束：语句无法被取消时，一个黑洞连接会同时
冻结本进程内所有租户的摄入与读取。四个 PostgreSQL 二进制（`audit-api`、
`audit-governance-worker`、`audit-outbox-relay`、`audit-pg-migrate`）共用同一份解析与
应用实现（`internal/runtimeconfig/postgres_pool.go`），因此同一套变量在四个进程上
含义一致。

| 变量 | 取值 | 未设置时的行为（发布默认） | 回滚 |
|---|---|---|---|
| `AUDIT_STORE_OPERATION_TIMEOUT` | Go duration，如 `10s`、`2m`；audit-api 下必须**严格小于** 30s HTTP `WriteTimeout` | `store.DefaultOperationTimeout` = 10s | 取消设置 |
| `AUDIT_POSTGRES_MAX_OPEN_CONNS` | 正整数 | **不调用任何 setter**，`database/sql` 默认（无上限） | 取消设置 |
| `AUDIT_POSTGRES_MAX_IDLE_CONNS` | 正整数 | 不调用 setter（默认保留 2 条） | 取消设置 |
| `AUDIT_POSTGRES_CONN_MAX_LIFETIME` | Go duration | 不调用 setter（连接不回收） | 取消设置 |
| `AUDIT_POSTGRES_CONN_MAX_IDLE_TIME` | Go duration | 不调用 setter | 取消设置 |

要点：

- **不发明默认值。** 未设置 = 完全不设置，与本次变更前的 `database/sql` 行为逐字节
  一致；`/metrics` 的 `audit_store_pg_pool_max_open 0` 就是"无运维上限"，不是被隐藏
  的数字。取值依据与决策门禁见 `docs/BENCHMARKS.md`「PostgreSQL control-plane pool
  decision gate」。
- **失败关闭。** 任一变量非法（无法解析、非正整数、非正 duration）时进程启动即退出并
  指名变量；`AUDIT_STORE_OPERATION_TIMEOUT ≥ 30s` 在 `audit-api` 启动时同样拒绝——
  否则可重试的 `503 store_timeout` 还没写出去连接就已经断了。
- **观测。** `curl -s :19089/metrics | grep '^audit_store_pg_pool'` 输出 5 条无标签行，
  外加 `audit_store_operation_deadline_exceeded_total`。该计数器非零即表示有语句在
  截止时间处被中断（结果未知，按幂等语义重试，HTTP 返回 `503 store_timeout`，
  gRPC 返回 `codes.Unavailable`）。
- **升级/回滚顺序。** 上线：新二进制 + 变量全部不设置（连接池不变、截止时间 10s）。
  回滚：回滚二进制；旧版本忽略全部五个变量，取消设置即可恢复到变更前的连接池。

**`audit-pg-migrate` 的两处行为变化**（均有意的）：原先硬编码的
`SetConnMaxLifetime(10m)` 已移除（一次性命令，等价物为 `AUDIT_POSTGRES_CONN_MAX_LIFETIME=10m`）；
原先无界的 `db.Ping()` 改为带 10s 上限的探测，黑洞 DSN 会让命令失败而不是永久挂起。
大规模切换可临时调高 `AUDIT_STORE_OPERATION_TIMEOUT`。

## SIGTERM 下的有界退出序列（gRPC drain）

`audit-api` 收到 `SIGTERM`/`SIGINT` 后按固定顺序退出，每一段都有界，因此进程
**一定会**走到 `st.Close()`——释放文件后端的 `<state>.lock` 独占 flock 与
PostgreSQL 连接池。退出序列（`cmd/audit-api/main.go`）：

| 顺序 | 阶段 | 上界 | 失败表现 | 日志 |
|---|---|---|---|---|
| 1 | HTTP 连接排空（`server.Shutdown`） | `shutdownTimeout` = 10s | 错误按旧行为丢弃 | — |
| 2 | tracer flush（**自己的** context） | `shutdownTimeout` = 10s | 采集端不可达 | `tracing_shutdown=failed` |
| 3 | health 置 `NOT_SERVING`（FR-2.3） | 无需上界 | — | — |
| 4 | gRPC 摄入面 drain（优雅优先） | `grpcDrainBound(st)` = `AUDIT_STORE_OPERATION_TIMEOUT`（默认 10s） | 空闲流永不返回 → 强制停止 | `grpc_drain=drained` / `grpc_drain=force_stopped` |
| 5 | `st.Close()` | 文件后端瞬时；PG 后端 ≤ `op` | — | — |

- **最坏预算。** `2·shutdownTimeout + 2·op`（PG：第 5 步的 `db.Close()` 等待在途
  语句）；发布默认下 `op` = 10s，即 **40s**，启动门禁允许的最大值（`op` 趋近 30s）下
  **< 80s**。文件后端为 `2·shutdownTimeout + op`，默认 30s。两者都是有限的：
  释放状态锁**不再需要外部 `SIGKILL`**。
- **drain 窗口是推导值，不是新常量。** 第 4 步的窗口恰好等于一次存储操作预算，
  因此被 detach 的在途提交（`context.WithoutCancel`）在强制停止之前已经享有同一个
  上界；`AUDIT_STORE_OPERATION_TIMEOUT` 仍是唯一的旋钮，启动时
  `runtimeconfig.ValidateStoreOperationTimeoutBelow` 保证它 `< 30s` HTTP `WriteTimeout`。
  **调小它同时缩短整个退出预算**（例如 `5s` → PG 约 30s），但也会同步收紧请求路径上的
  存储上界，二者一起改。
- **`grpc_drain` 的两值语义。** `drained` = 优雅排空；`force_stopped` = 到达窗口上界、
  已调用 `grpcServer.Stop()`（连接关闭、handler context 取消）。**未配置 gRPC 监听时
  这一行不打印**（没有 drain 可报告），不要把缺行当成失败。每条退出最多一行，
  取值域封闭为两值，不带 tenant / client / event / route / URL 标签。
- **强制停止是安全的（at-least-once）。** 摄入提交与 RPC context 解耦，`fileBackend.Save`
  是原子的；被 pool 关闭打断的 PG 语句落在 `store.ErrOperationDeadline` 的“结果未知、
  按 `event_id` 幂等重试”类别里。客户端看到 `Unavailable`/`Canceled` 时按 `event_id`
  重试即可收敛到**恰好一份** receipt。
- **回滚。** 回滚二进制即恢复旧行为（无界 drain）；无数据迁移、无配置迁移，也没有
  新增变量。
