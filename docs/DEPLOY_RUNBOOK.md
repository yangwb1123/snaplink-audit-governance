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

## 文件数据路径与权限升级（ADR-0017）

Linux 文件后端的数据路径逐组件拒绝 symlink，文件须为 regular/单链接 inode；
截断前验证，不打开 FIFO/设备作数据。新文件 0600、新目录 0700；旧 0640/0750
不自动 chmod。原子写在同一 directory fd 下 O_EXCL 创建 `.tmp`、fsync、renameat
并同步祖先；未知 temp 原样保留，不能删掉换成功。非 Linux 文件数据失败关闭。

升级前停写，核对真实目录/挂载、服务 UID、ACL、硬/软链接、未知 temp 和 journal。
文件 API/worker 仍不能共占同一路径；数据路径不依赖 symlink。备份身份须能显式读取
0600/0700，同步备份 control、tenants、ledger、journal、publications 与密钥环境。
本版本不自动遍历权限、更换账本或重新签名；旧组读的潜在泄露需单独处置。

回滚前以当前版本完成已知 journal 恢复及 cold 核对，保存未知残留及原证据；旧版本
忽略 journal、创建组读文件或截断 `.tmp` 都不是安全回滚。正常读仍可能可用而未知
控制面 `.tmp` 阻止覆盖，运维须检查，而不是盲目重试。描述符防 symlink 重定向，
不是特权主机/mount 管理或正式 WORM 的防篡改证明。TLS/DLQ 独立策略见 ADR-0018。

## TLS secret mounts 与 DLQ state 升级（ADR-0018）

- Linux gRPC cert/key/client CA 每文件最多 4 MiB，要求 regular inode，实际读取同样
  有界且 FIFO 不阻塞。配置父目录作为受信 root，允许 `..data/...` 等 root 内相对
  symlink，拒绝 root 外/绝对叶链接；父路径和挂载/CA 仍需运维授权。上线前核对
  secret UID/ACL、大小和真实目标，需要跨目录就配置真实父目录，不能放宽 mTLS。
  文件逐个读取、仅启动/预检加载，不是跨文件快照或热轮换；轮换后重启验证。
- DLQ 可变 state/lock 使用严格 NOFOLLOW 数据策略（不是 TLS 策略）。唯一 O_EXCL
  0600 temp 及 renameat/cleanup 共用捕获 parent，actual fd fsync 后发布；保留未知
  temp，不遍历 chmod。旧 v1/v2 和显式 legacy tenant scope 不变；共用 state 的
  DLQ writers 继续依赖本地 POSIX flock union，不能与 ledger 单写者混淆。
- 升级/回滚前停本轮 DLQ writers，核对链接、owner/ACL、原 durable marks/union 和
  legacy scope，备份原 state；不清空 state、不忽略旧 mark、不自动处理未知残留。
  关闭/同步失败可能已经落盘，结果未知；保留 pending/offset，允许幂等重试。
  非 Linux 文件 loader 失败关闭；仍无全历史 load/conversion 内存容量保证。

## 租户治理状态观察

`GET /api/v1/governance/status` 与 Web 概览展示当前保留的控制面元数据，不限 24 小时；
已有导出创建、操作读取、Hold 管理、策略读取权限分别开放对应分区，至少需要其中一个。
平台凭证必须选择单个 tenant_id；tenantless Console 能力不能读取此接口。未授权分区
省略，不可解释为零；成功及错误均 `Cache-Control: no-store`，自审计失败不返回摘要。
无需新增数据库迁移，API/Web 一起升级即可；旧 API 无此接口时不可补造成功计数。

- 排队上限 32、运行上限 1 是本租户的固定共享 CAS 上限，不是可用余量；仍可能因
  其他租户占满全局预算而无法创建/领取。不得根据本租户计数修改任务状态或扩大预算。
- 运行数不是实际工作进程/租约数，完成不代表可绕过 Hold 下载；未知状态单独计数。
- 待我决策排除本人和缺少申请人的旧记录，但最终审批仍重新授权和原子职责分离。
  已批准仅是治理决策；业务执行未接线，不以此判断恢复成功。
- Hold 数量不证明对象版本保全或保护的事件数量；留存只是存储的策略配置。
  不判断对象 COMPLIANCE 到期、不执行删除、不缩短既有留存。
- observed_at/source=control/completeness=not_asserted 不证明发布、投影或全租户证据完整。
  摘要不探测 broker/ClickHouse/WORM；就绪、证据校验、完整性验收仍使用独立门禁。
- 只使用 ReadControl；PostgreSQL v1 仍可能物化旧快照，v2 共享 control 的终态历史
  也没有被本接口限额回收。生产查询成本/容量须另行验收，详见 ADR-0016。

## 导出队列容量与故障收敛

同一共享 control 中 pending/running 最多全局 128、租户 32；创建 CAS 耗尽 429，
不创建作业或 export.created。执行领取另有限额：全局 2、租户 1，API/worker 共用，
不通过加副本乘大并发。API 启动及每秒唤醒有界 dispatcher；租户轮转、租户内
oldest-first；恢复 API 返回 backlog，不表示已领取/完成。

领取原子保存私有 2 分钟租约、每 30 秒 control-only CAS 心跳；API 队列启动/每秒
派发前和 governance worker 收敛无检查点的过期租约；合法 selected 计划或 sealed 检查点保留 running，
由有限 dispatcher 在原 lease 到期后换新 owner 续跑已持久阶段（生成页或 sealed 终态提交）。恢复失败该队列 pass
不派发，畸形租约/检查点失败关闭；失败状态/元数据清理与 export.recovered 同一次
保存，终态不重新排队。仅无
租约的旧 running 保留 started_at/CreatedAt、默认 24h 的严格年龄恢复。续期失败
取消执行，迟到 Put 可能已提交无引用密封对象，但失主不能发布路径或删除 WORM。
预算是有效持久 claim 上限，不是跨主机强杀或物理 I/O 上限；先核对真实运行和未知
远端结果，不改终态、清空租约、放宽预算或缩短 COMPLIANCE 来恢复吞吐。

升级/回滚不得混跑旧/新 API/worker：旧版本忽略 owner fencing。先停止新领取、停写
并核对/排空原执行；running=0 不证明外部 Put 已结束。control 连同 export_leases、
export_checkpoints、export_plans 必须按既有 ledger/hot/journal/发布确认/密钥流程备份；
租约不是事实源或进度证明，计划绑定已完成的选择阶段，sealed 检查点绑定密封阶段。
进程壁钟需同步，监测心跳写放大、control CAS 冲突与归档延迟；配置一致性键不证明
二进制支持租约。API/worker 仍须同密钥/归档配置，文件数据仍单写者。

导出所有者可通过 POST /api/v1/exports/{jobId}/cancel 撤销 pending/合法 leased
running；未知 requester、其他主体（含平台主体）或旧无 lease running 不可代办。
状态/撤销 lease/清输出引用与 export.cancelled 同一 CAS，重复取消零写入。取消释放
持久预算，但最多在后续心跳观察撤销，不证明外部 Put 停止，不删除无引用 WORM。

POST /api/v1/exports/{jobId}/retry 为本人已结束 failed/cancelled 创建关联新任务，原
终态不变；清旧 cursor、重新读取/自审计/Hold 检查及共享队列准入。每个原任务最多
一个直接后继，重复请求返回同一后继（也可能已结束）；再次尝试须指定失败后继。
新任务不延续旧固定成员集。创建/重试共享各自一次服务 deadline，Hold 预检捕获
独立快照后锁外取证，不阻塞租约续期/所有者取消；历史物化成本仍在。
操作只返回安全 control 摘要；Hold 不阻止所有者撤销，
也不能通过操作响应读取证据。499/超时/5xx 后先读列表 view=mine 核对，不能推断
回滚；幂等重复不是批量自动重试策略。failure_code 是固定类别、旧记录可缺失，
不是物理停止或成功重试回执。治理计数 cancelled 不计作 unknown/failed。

API/worker/Web 配套升级；即使旧版本支持 lease，也不得混跑会丢弃 retry_of/
failure_code/export_checkpoints 的旧 control writer。备份保留完整原任务、后继、leases
和活跃检查点；不要清理
关联历史、改 requester、回退终态或删 WORM 来重试。回滚须停写核对目标版本字段
保存能力及远端未知结果，不以 running=0 或一致性键证明安全，详见 ADR-0020。

### Put 后密封检查点 / 终态提交续跑（ADR-0021，有限子切片）

- `export_checkpoints` 是私有 control 元数据，version=1/seal_version=2；任务/申请人/
  retry_of/原筛选及创建/启动时间使用独立用途 HMAC 绑定，另认证原对象引用、密封
  字节/JSONL 摘要、计数与检查点时间。没有载荷、密文、原始筛选副本或 owner；不得
  将该 map、HMAC、对象字节放入 DTO/诊断日志/URL。既有 10000 匹配身份预算在执行
  选择时也复核，不截断输出；选择 Get 现在锁外，仍有历史物化和整块 AEAD 内存。
- Put 成功后，当前未过期 owner 以同一 CAS 保存 checkpoint + export.checkpointed。
  未知 Save 可能已提交；不猜成功、不删对象。检查点提交前崩溃仍失败收敛，未知
  WORM 对象/临时文件不可作为恢复计划。只恢复 sealed 阶段，不从短期查询 cursor
  或最大 sequence 推断成员完整；也不自动重试已结束任务。
- 原 running lease 到期后，接管 CAS 保留 started_at、换新随机 owner 并写
  export.resumed；继续占原 running 配额，仍须全局≤2/租户≤1。不回退 pending、
  不抢健康 owner、不放宽预算。API 启动/周期 dispatcher 和 worker Resume 均可唤醒；
  Resume 返回 pending + 合法到期检查点的观察 backlog，不是完成数。恢复计数只统计
  原有失败收敛，不把保留的检查点算作成功执行。
- 续跑仅读原密封对象，重新认证精确密封字节/v2 tenant-job AEAD/JSONL 摘要和计数、
  最新 committed receipt 坐标；不会重新选择输出、重新密封或 Put。当前 Hold 检查
  和内部 governance-worker 读自审计失败关闭；一次验证共用一个 Store deadline。
  Hold 集在锁外检查后若变化，终态 CAS 拒绝完成，留待后续重新检查。检查点终态的
  Hold 证据从 committed receipts 锚定，在原查询/Hold 谓词之前验证身份、精确 Schema、
  重建内容和链；敏感条件从认证原值按条件自身的 bound/legacy 格式推导，不信任派生
  搜索索引的存在、格式或值。缺失/坏正文/不可认证 Schema 保留原检查点，不当作空
  选择或删除运行元数据；先核对并修复原证据/密钥/目录，再等待正常过期接管重查，
  不手改 lease/终态换成功。无候选/不相交 Hold 仍不读源对象；当前迟到证据和最低
  匹配 Hold ID 语义不变，但不会改变原输出成员。这不是其他 Hold 路径或历史成本
  的整体改造。源 Get 阻塞不持 Store.mu，仍共用终态验证 deadline，所有者取消/续期
  可继续；取消/换主后的晚验证不能发布路径。
- Get/Store/取消/超时/读自审计失败保留 running 检查点，停止本次 heartbeat 后等待
  下一次租约接管。坏格式/摘要/AEAD/账本坐标原子 failed + export.checkpoint_rejected；
  当前 Hold 阻断原子失败并写 export.blocked；完成与一次 export.completed 同 CAS。
  完成/失败/所有者取消移除已知活跃检查点和 lease，不删事实/WORM。未知版本、任务
  漂移或坏 MAC 失败关闭并保留现场，禁止手改元数据、清 lease 或重置终态换成功。
- API/worker 配套停写升级；即使旧版本支持租约，也不能让不认识检查点的旧 writer
  重写 control 或把它失败清理。备份完整任务/后继/leases/checkpoints 与既有证据、
  journal、发布确认和原密钥。回滚须核对目标版本保存能力与远端未知结果；密钥轮换、
  版本级对象 Hold、正式 WORM/合规和容量验收仍独立，不以一致性键或本机测试代替。

### 生成前固定成员计划 / selected 阶段续跑（ADR-0023，v1 兼容路径）

新单页任务和已保留的 v1 计划继续以下路径；新多页 v2 计划另见下节。

- 私有 control export_plans（version=1/format_version=1），不含载荷/密文/原始
  筛选副本/搜索摘要/owner；仅有任务 HMAC、有序账本坐标/发生时间、精确 Schema
  ID/version/定义 HMAC、时间与签名。10000 身份/4 MiB 编码，不扩大共享 running
  2/tenant1 或队列128/32；没有查询 cursor 的15分钟到期。计划不是新账本事实。
- 生成前先认证 committed receipts/精确 Schema/内容/链再选择，不信任搜索 sidecar。
  内部读自审计后，当前未过期 owner 将计划与 export.planned 同 CAS 保存。Save
  未知立即停止，不猜提交或 Put。原 lease 到期后，合法计划留 running 供有限
  dispatcher 换新 owner，保留 started_at，不复活终态，不用较新查询替代计划。
- 恢复只读原成员坐标并认证最新 receipt/WORM、内容/链及 Schema 定义；当前 Hold
  仍认证原查询的当前证据，可能包含迟到事实但不改输出成员。源 Get 锁外，一次
  selected 验证 deadline 覆盖取证/Hold/读自审计/计划复核。证据缺失/不可认证、
  Schema 漂移、自审计或 Store 失败保留计划，核对原证据/密钥/目录后等正常接管，
  或由已知所有者取消；不清 metadata、改 owner 或重选一批换成功。
- Put 前复核计划/当前 Schema/owner；sealed Save 再核对生成 JSONL 的有序身份/
  版本，复核计划/Schema 并同 CAS 消费 selected，活跃阶段互斥。Put 后 sealed
  保存前崩溃可能留下无引用对象，新 owner 仅按已知计划生成不同 attempt 对象；
  不重放未知旧 key，不推断回滚/一次 Put，不删除 WORM。取消/终态只清已知合法
  活跃计划，保留原阶段事实；retry 新建任务且不继承原计划。
- API/worker/所有 control writer 配套停写升级，备份完整 plans/checkpoints/leases/
  任务关联与既有证据/journal/确认/密钥。旧 sealed-only writer 也不得混跑；回滚
  须核对字段保存与未知远端结果，不能以一致性键、running=0 或启动成功作证明。
  沿用文件单写者/descriptor-relative 权限与 UTC 时钟，不改认证/超时/保留期。

### 逐页生成进度（ADR-0024，新多页 v2 计划）

- 新选择超过 100 成员使用 version=2/format_version=1，每页最多 100 个固定成员。
  `chunks` 只含连续页范围、独立绑定的原密封对象引用/摘要/时间，不含对象字节、
  原始筛选/搜索摘要或 owner。最多 10000 成员/100 页，4 MiB **总**计划元数据内预留
  每页 1 KiB 引用空间，共享队列128/32与running2/1不变，无新增 TTL/并行执行器。
- 每页认证原坐标/精确 Schema/内容/链、重查当前认证 Hold（包括迟到证据）、失败
  关闭读自审计。锁外源读、Hold/audit、页 Put/Save 共用本页的一次原 operation
  deadline；不按对象重启、不放宽原超时。Put 前和进度 CAS 复核 owner/计划/Schema/
  Hold 集，页引用与一次 export.progressed 同 CAS。未知 Save 停止当前执行，实际
  可能已提交；正常到期新 owner 只按耐久前缀继续，不人工改租约换成功。
- 接管先认证引用的已生成页、最新 ledger 坐标和 Schema，只生成剩余页；缺失/不可用/
  损坏的已提交页保留原计划，**不重读源事件来生成替代页**。修复原对象/密钥/目录后
  等待正常接管，或所有者取消；未知版本/错绑元数据保留现场。当前 Hold 匹配失败
  并清合法活跃元数据，但原页/事实不删；详情/下载仍有各自权威门禁。
- 未提交/迟到页 Put 可留下无引用 `export-chunks/` 对象；新 owner 对未提交页用不同
  attempt key，不猜旧 key、不宣称回滚或仅 Put 一次。所有页提交后才合并/整体密封
  为现有最终对象，sealed Save 原子消费计划/进度，随后原 sealed finalizer 仍复核
  当前 Hold/读自审计与终态 fencing。完成/取消不删除任何页或最终 WORM 对象。
- API/worker/全部 control writer 配套停写升级；旧 v1-plan writer 也不能混跑/丢字段。
  备份完整任务/关联/leases/plans（含 chunks）/checkpoints、页对象及既有证据/journal/
  确认/原密钥。回滚先停领取/写入，核对格式保存能力与未知远端结果；禁止清理页对象、
  owner/终态元数据或缩 COMPLIANCE 换成功。文件单写者、时钟同步要求不变。

M3 检查点整项仍 Partial：已有多页 JSONL 生成进度，最终合并/整体 AEAD/Put 仍可重做，
尚无流式认证大文件下载；首次选择/当前 Hold 历史扫描、最终合并/密封内存、共享
control 成本及真实 PG 容量须另验收。详见 ADR-0014/0019/0020/0021/0023/0024。
下载对象读取已移到锁外，共用一次 Store deadline；仍在认证/Hold/自审计通过后才
输出明文，不能将该改造表述为认证前输出的流式大文件下载。

### 导出详情 / 下载放行的当前 Hold（ADR-0022）

详情与下载放行现在复用 receipt 锚定、精确 Schema/内容/链认证的 Hold 判定，再评估
原查询/Hold；敏感条件从认证原值按条件的 bound/legacy 格式推导，不以搜索 sidecar
的存在/格式/值判断未命中。缺失/损坏证据拒绝访问，不改终态或删除对象。无候选、
时间不相交仍不读源对象；历史深快照/扫描成本和其他 Hold 路径并未整体消除。

源 Get 与认证在锁外；详情、下载放行各有一次既有 Store 操作 deadline，后者包含
拒绝/允许自审计和 control 复核，不按对象/追加重启。慢 Get 时续期/所有者取消仍可
继续。允许前重读 control，活跃 Hold 集/任务选择变化返回 409，要求刷新，不自动
重发；正常向前的作业进展不阻断详情轮询，下载及已捕获终态要求完整 job 不变。
下载自审计前后均复核，独立 trail 已落盘的尝试事实不撤回，也不证明输出过明文或
接收方已收到字节。匹配 Hold 后即使随后释放，本次仍保守拒绝，新的调用重新检查。

HTTP 详情/下载成功及全部错误均 no-store；认证、AEAD/摘要、当前 Hold 与自审计
通过前不输出明文。最终 control 复核是本次访问的放行观察点，不是全链路事务或
对已经开始响应的撤回；整体认证下载不变，没有增加分块下载、生成检查点、执行
权限、迁移或新运行元数据。继续沿用前述版本配套、备份与回滚限制，不清历史换成功。

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

### governance worker 启动期间的终止

正常启动在 openStore 之前注册 SIGINT/SIGTERM，避免已经取得文件锁、尚未完成归档
预检时落入 OS 默认终止。启动 Ready 使用同一进程取消上下文和原 5 秒上限；收到
终止信号后取消预检，不开始治理 pass，正常关闭 store。无信号取消的 Ready 失败
仍是启动错误、非零退出；没有绕过 Object Lock/versioning/COMPLIANCE 检查。其他
存储打开/操作、每对象和整 pass 上限不变，不据此承诺所有启动 I/O 可被物理强杀。

锁存在/可观察只证明排他所有权，不能用作就绪标记。验证分别在真实 startup bucket
HEAD、归档 object HEAD 和 flock 边界同步；前置 v2 测试数据在计时前准备，启动护栏
和 SIGTERM 后退出上限仍各 10 秒。API drain 夹具的 1 秒 store/drain 配置及退出上限
不变；准备接入仅对固定存储超时使用同一完整事件进行幂等核对，其他错误立即失败，
成功流不继承准备 TTL。此准备策略不是生产超时放宽或运行终态重试。

无 SQL/control/归档格式迁移或新变量；回滚会恢复 worker 的启动信号窗口，不能靠
延迟发信号、清锁文件、另起空账本或修改导出 owner/终态掩盖。API/worker 仍须遵守
前述版本配套与完整备份限制。
