# 审计治理产品化实施清单

状态：实施中。此文件追踪规划，不替代 README、ADR 和 api/** 的现行规范。

## 顺序与完成定义

每批先落实最小可用闭环，再扩展容量；只有代码、契约、回归测试和完整门禁均通过才勾选。
不把原有热冷分层、冷回执索引、mTLS、幂等、CAS 领取或 DLQ 处理重新算作新增交付。
任何跨仓、外部设施、业务执行或正式合规验收尚未验证的内容必须保持未完成。

## M1：边界与前端基础

- [x] 证据区按子功能权限开放，保留服务端最终授权。
- [x] 身份/租户变化隔离请求、弹窗、预演和轮询；陈旧响应不能覆盖新上下文。
- [x] 统一错误分类、请求 ID、空状态与确认交互。
- [x] 事件字段 before/after 差异展示（不在浏览器解密）。
- [x] 证据页拆分为导出、恢复、Legal Hold、完整性子页。
- [x] 查询 count 返回总匹配数，与时间线口径一致。
- [ ] 完整权限/回执状态/恢复/留存语义矩阵及剩余边界回归校准。

## M2：可信读链路

- [x] 已提交 cold 事实的启动/周期意图修复与持久发布确认（批次 2，完整门禁通过；整栈另行验收）。
- [x] 新版文件 HotFirst 原始追加 journal 的无人重试恢复、PG v2 原子热冷提交（批次 6；旧无原计划残留不推断，不混同发布对账）。
- [x] 发布重试退避/jitter、失败可观测，保持至少一次语义。
- [x] 当前目的地投影确认、单流连续哈希前缀及账本重建工具（批次 3；全租户完整性需另行核对）。
- [x] 事件检索可选 ClickHouse 候选索引及逻辑去重 count，返回证据核对 Ledger/WORM（批次 4）。
- [ ] 其余 ClickHouse facets/统计接线、全租户完整性核对与生产容量验收。
- [x] 事件查询固定成员集、主体/租户/筛选/来源绑定游标、迟到事件回归（批次 4；不宣称全局事务快照）。
- [x] 单事件/事件续页的按需锁外归档取证、共享 deadline 和证据验证（批次 4）。
- [ ] 参考初页、timeline/Console、导出等其余路径的全历史扫描与锁内长 I/O 改造。

## M3：治理作业工作台与容量

- [x] 导出/恢复分页列表、我的申请/待决策视图。
- [x] 前端任务列表/详情，刷新后可找回；Hold 不可被列表绕过。
- [x] 持久真实 started_at，恢复按运行时间判断，兼容旧任务，避免误伤久排队任务。
- [ ] 租户作用域的治理状态摘要。
- [x] 导出有界 dispatcher/租户轮转/租户内 oldest-first，API 启动与周期唤醒（批次 5；非跨副本严格公平 SLA）。
- [ ] 导出租约/心跳/检查点及可续跑执行。
- [x] Legal Hold 拒绝提供兼容 forbidden 的机器可读原因和租户内 Hold ID。
- [ ] 重试创建关联新任务、受控取消及其余结构化失败/阻断原因。
- [ ] 分块密封/校验和大文件下载，不输出未经认证的明文。
- [x] 同一共享 control CAS 的导出队列及运行并发预算（批次 5；真实 PG 容量另验收）。
- [ ] 其余跨副本租户预算与活跃 gRPC 流容量准入。

## M4：长期合规

- [ ] 热冷一致的留存评估、版本化策略及对象保全协调。
- [ ] 对象版本级 Hold、多 Hold 释放协调及保留期延长。
- [ ] 显式 Key ID、多版本历史解密/验签与租户密钥层级。
- [ ] 自审计历史归档及完整性证明。
- [ ] 经评审的生命周期执行；禁止默认删除不可变账本或缩短 COMPLIANCE 留存。

## M5：安全恢复

- [ ] 持久计划绑定聚合/业务版本/证据摘要/有效期。
- [ ] 申请、读取、决策权限拆分及 Snaplink scope 联调。
- [ ] 业务系统正常 API 执行及独立执行回执协议试点。
- [ ] 版本漂移拒绝、执行幂等与失败闭环；审批不等于执行成功。

## 前端与发布验证

- [ ] 查询/任务/事件安全深链接，目录模块渐进拆分。
- [x] 主应用浏览器测试：真实 PKCE/JWT、权限、跨租户陈旧响应、导出下载与 Hold 阻断、双主体审批。
- [ ] 兄弟仓库 Hosted Login UI 和完整 Compose 链路浏览器验收。
- [x] 当前已交付代码通过 `python3 cli.py quality`、`make web-check` 和相关定向/浏览器回归。
- [ ] 后续每批新增能力继续通过上述门禁及对应集成验收。
- [ ] 外部设施容量、容灾与正式合规验收（独立发布条件，不以本机测试替代）。

## 批次记录

### 批次 1：治理工作台基础、读口径与异步隔离（已验证）

- 后端：`internal/service/governance_list.go`、`internal/httpapi/handlers_jobs.go`；
  exports/restores 列表及 HMAC 游标绑定租户/主体/类型/筛选；只读 control，导出摘要不含证据字段。
- 前端：`web/src/features/` 下四个证据功能，任务分页/详情与白名单深链接，确认交互、
  代次隔离及旧 token 401 保护；修复浏览器原生 fetch 接收者和首帧草稿清空问题。
- 校准：事件 count 为游标过滤前总匹配数；Hold 拒绝返回 reason/hold_id；
  同时验证 ID/access token subject 一致，既有服务端授权规则未放宽。
- 运维：`cmd/audit-api/ledgered_retry.go` 正向 jitter、指数退避至最多 30 秒、成功重置及可取消等待。
- 作业：领取时保存 started_at，恢复阈值不再从排队创建时间起算；旧数据保留兼容兜底。
- 验证：完整 quality（含 vet、单元、race、生产构建）通过；完整复验命令为
  `GOMAXPROCS=2 GOFLAGS="$(go env GOFLAGS) -p=1" python3 cli.py quality`，未去掉测试或降低阈值。
  Web 检查通过（68 个测试、Svelte/TS、lint、build）；3 个 Chrome 浏览器回归通过。
  可选 gosec/govulncheck 未安装，未声称外部扫描通过。

### 批次 2：发布确认与主动对账（本机完整门禁通过）

- `internal/store/publication.go`、迁移 007：租户/事件/流/序号/哈希绑定的确认，
  存于独立 SQL 索引表或寻址文件；不把确认历史永久堆入 control，不包含载荷或搜索摘要。
  broker 成功先确认、后清理；确认可见但祖先 fsync 失败时必须先修复持久边界。
- `internal/service/ledgered_reconcile.go`、`cmd/audit-api/ledgered_reconcile.go`：
  启动/周期轮转租户、有界 cold page、固定有限扫描终点；完成后从零重扫，避免持续写入
  或 SQL ID 非提交序造成永久遗漏。扫描位置不是查询快照、提交水位或投影确认。
- 读取最新回执、锁外有界归档取证，补发前重建内容摘要并验证链；损坏/缺失证据不能补发。
  确认落盘但清理失败只做 cleanup，不再调用 broker；未落盘则允许至少一次重发。
- README、ADR-0011、OpenAPI/AsyncAPI 描述同步；补无标签扫描/证据失败/失败轮次指标和告警。
  `indexed/archived` 不再被解释成 broker/ClickHouse 确认，兼容回执状态未修改。
- 已通过定向回归、真实 PostgreSQL race（跨实例确认/冲突、分页、有界移动尾部、迟到提交重扫）
  及 `make web-check`（68 个测试、类型/lint/build）。完整 `python3 cli.py quality` 通过，
  包含 Go `-race -count=1`、vet/build、契约/隔离/复杂度及 Python 156 个测试（1 项可选跳过）。
  上轮 race 命中了既有 50ms 摄入准备时序假设，改为 Put 边界精确取消和独立的 deadline
  继承断言后完成全量复验；保留生产超时、原有真实超时测试、race 和工程阈值。
  gosec/govulncheck 未安装，未宣称外部安全扫描通过。

### 批次 3：当前目的地投影确认/流前缀/账本重建（完整门禁通过）

- `internal/projection/confirmed.go`：新表保留坐标/内容变体，同步写与 fsync；
  当前目的地精确确认、逻辑去重计数、单流连续哈希前缀；缺号不能跳跃，断链/变体失败关闭。
- `internal/service/projection_rebuild.go`：已提交 cold 分页、最新回执、锁外归档取证，
  重建内容与链校验；失败不推进页，重复写安全。回执版本/重扫可能重复，计数是操作数。
- `audit-projector rebuild/inspect`：单租户、现存账本、与 API 相同密钥/归档环境，
  不重置 Kafka、不改事实/审批/Outbox、不自动删表。扫描结束明确不声称投影完整。
- 真实 ClickHouse 24.8 原生协议及全 projection 包 race 已通过，覆盖重复、租户隔离、
  迟到事件补缺口、合并后冲突、async 设置与新表持久设置；源码定向回归已覆盖
  重建的篡改拒绝、锁外 I/O、失败进度及无副作用。完整 `python3 cli.py quality` 已通过，
  包含 Go 全量 `-race -count=1`（1673.7s）、vet/build、Python 156 个测试（1 项可选跳过）
  及所有工程/契约/隔离门禁；未削减测试或阈值。新增 inspect/rebuild 无 Web 改动。
  文件重建也使用既有操作 timeout；gosec/govulncheck 未安装，未声明安全扫描通过。
- README、ADR-0012 与部署手册已同步；查询 API 未改接 ClickHouse，固定快照、
  敏感条件查询、全租户摘要和生产容量仍未完成，不把基础设施交付冒充整个 M2。

### 批次 4：固定结果集/POST 查询/ClickHouse 候选读（完整门禁通过）

- 独立成员集存储与迁移 008：15 分钟、10000 个成员/4 MiB、租户 16 个活跃分页集；
  HMAC 绑定主体/租户/筛选/来源/页大小，不保存载荷或原始敏感条件，不污染 control/ledger。
  迟到事件只进入新查询；成员集/游标过期、丢失、篡改不静默退回实时续页。PG 跨副本准入。
- `POST /api/v1/events:query` 复用服务端 scope 与读自审计；Web 只在 body 发送筛选/摘要，
  显示观察/到期时间与不声明完整性；Query/详情代次隔离及取消，过期游标要求显式重新查询。
- 可选只读 `AUDIT_QUERY_CLICKHOUSE_DSN`：普通筛选参数化、tenant 内身份去重及变体拒绝；
  count 为成员数，全部候选核对 committed receipt，返回页依旧从 Ledger/WORM 认证取证。
  敏感摘要条件明确走 ledger，投影故障不隐式降级。启动/readiness 探测依赖与新表结构。
- 单事件/续页锁外按需归档，共用 Store 操作 deadline，缺/坏证据不冒充空页；保留自审计
  fail-closed。真实 ClickHouse 揭示原生 time.Time 参数的秒精度/2106 年饱和问题，改用
  显式 RFC3339→DateTime64(3) 插入/绑定，按 envelope 精确时间兼容旧物理列；覆盖纳秒边界、
  2299 年上界及冲突合并。不能将候选/成员上限冒充历史扫描容量已解决。
- 完整 `python3 cli.py quality` 通过：Go 全量 `-race -count=1`（710.7s）、普通测试（654.4s）、
  vet/build、全部工程/契约/隔离门禁，Python 156 测试/1 可选跳过；未降低阈值或取消 race。
  完整 `make web-check` 通过：22 文件/70 测试、Svelte/TS/lint/build。真实 PostgreSQL 17
  跨副本成员集准入 race、ClickHouse 24.8 全 projection 包 native/race、锁外取证回归通过。
  本批未声称整栈/浏览器或生产容量已通过；gosec/govulncheck 未安装，无安全扫描通过声明。
- README、OpenAPI、ADR-0013 与部署手册同步；REST 为 30 路径/38 操作。
  其余 timeline/Console/facets、参考初页/导出历史成本及全租户完整性仍未完成。

### 批次 5：导出队列预算/有限公平调度/锁外下载（完整门禁通过）

- `internal/service/export_scheduler.go`：pending/running 全局 128、租户 32 的创建 CAS
  准入；running 全局 2、租户 1 的领取 CAS，共享 control 的 API/worker 共用配额。
  不再为每个 pending 作业启 goroutine；tenant 轮转、租户内 oldest-first，本地预约与
  durable running 去重。既有终态不回退、不删除；API 启动立即及每秒唤醒。
- 容量 CAS 竞争失败仍为 pending，依靠周期/worker 重试而非 hot-spin；Resume 返回
  观察 backlog，不是完成数。回归以实际 Put 边界同步，覆盖 shared Store 的多 Service
  并发和任务最终收敛；不冒充真实多 PG 副本容量/公平性验收。
- `ReadExportObject` 只锁内复制 tenant/job-bound control 元数据，Get 移到锁外；
  WithOperation 共用一次 deadline，既有 503/取消/缺失分类、AEAD/摘要认证、Hold 和
  读自审计仍 fail-closed。Get 阻塞时 control 写可完成；不是认证前输出的分块流下载。
- 扩展定向 race 3 轮通过（24.463s），明确实际执行 scheduler/admission/下载/恢复测试；
  早期正则只匹配精确短测试名的日志不作为新调度器验收。一次完整 race 命中既有
  ingest-cost 耗时阈值；该测试/生产代码/阈值未修改，独立连续 race 10 轮通过
  （41.273s）后完整复验成功。最终 `python3 cli.py quality` 为 QUALITY PASS：
  全量 race 828.6s、普通测试（缓存复用）3.2s、vet/build、所有工程/契约/隔离门禁和
  Python 156 测试/1 可选跳过。未移除 race、降低阈值或以定向测试代替完整门禁。
- 本批无 Web 源码/契约路径变更；仍为 30 REST 路径/38 操作。README、ADR-0014、
  部署手册已同步；租约/心跳/检查点、关联重试/取消、整块物化/密封内存及其他预算
  仍未完成。未新增整栈、浏览器、生产容量或正式合规通过记录；安全工具仍未安装。

### 批次 6：HotFirst 原始追加意图/无人重试恢复（完整门禁通过）

- 文件 HotFirst 随 hot 保存原 receipt/segment/checkpoint journal；恢复锁外验证内容、
  连续前序、流头/pending hashes 和原签名，重新核对 version/journal 后追加原记录。
  不重新分配序号、签名或推断无 journal 的历史残留。未完成租户不能被覆盖，其他
  租户/管理写可继续；API 不依赖 publisher，启动/每秒轮转恢复，worker 封段前先恢复。
- PostgreSQL v2 HotFirst 的 hot CAS 与 ledger 同一事务，cold 失败整体回滚；既有
  committed-cold 发布对账独立。摄入错误不能假造 durable receipt；缺回执的热正文
  不进入证据查询/选择或归档。旧无计划残留不作为 clean duplicate，也不延续其热流头。
- 保留完整性校验的既有 invalid verdict/自审计契约：检查损坏的 committed 正文后
  返回 valid=false，而非将诊断接口错误改成 409。定向 race 覆盖重启、原封段、后续
  封段及未封前缀、取消、坏签名/载荷/流头、持久污染、管理写保护和自动循环。
- 安全工具安装于独立临时路径；首次 govulncheck 报告 14 个可达漏洞，升级 Go 最低
  版本与 Docker builder 为 1.26.9、gRPC 1.83.2、x/net 0.60.0、x/text 0.42.0 及关联
  x/* 后，可达/导入包漏洞为 0。未调用的 openpgp 模块级警告没有修复版本；gosec
  仍报告 62 条待逐条评审的静态问题，security-scan 仍失败，不宣称安全发布已通过。
- 最终完整 `python3 cli.py quality` 为 QUALITY PASS：普通 Go 测试 553.8s、全量
  race 1026.0s、vet/build、Python 156/1 可选跳过及全部工程/契约/隔离门禁；未降低
  阈值或移除 race。扩展恢复/legacy head/完整性定向 race 3 轮通过（service 115.455s、
  HTTP 1.790s、API 1.383s）；真实 PostgreSQL 17 cold 失败回滚/跨租户 CAS/控制 delta
  race 3 轮通过（4.164s）。本轮 PG/network 临时容器已清理，只清理自己的资源。
  此前一轮发现证据选择提前拒绝损坏正文破坏完整性 verdict，已修正后完整复验，
  不能把失败日志当作通过。无 Web 源码/契约路径变更，未重跑 Web/浏览器；本批不
  声称整栈、容量、正式 WORM 或业务执行验收。

### 未完成与当前阻塞

- `make web-stack-up` 两轮均在容器 API 连接 PostgreSQL 超时处失败，尚未完成
  PostgreSQL→Kafka→ClickHouse/WORM 完整栈验收。批次 2 使用独立 Compose project 与新卷
  重建了最新镜像、应用 007，API 重启及一次性容器仍失败；同网部分容器 TCP 可达，
  API 一次性容器的 nc 也超时。批次 6 新建隔离 PG/network，用原 batch2 API 镜像
  的 uid=100 执行 DNS/route/nc 探测成功，仅说明这次网络诊断可达；不是新 API 或
  完整 Compose 验收，旧整栈阻塞仍未关闭。未改主机防火墙、生产认证或超时来换取通过。
  本轮仅停止/移除自己创建的临时服务，保留卷；未触碰无关服务。修复了验证脚本的 fixture-secret 放行、
  secondary mint 后来源 client 恢复及 sourced token 避免打印；不能把脚本修改当作整栈通过。
- 浏览器回归使用本机真实 JWT API、真实 Snaplink IdP 和主应用；授权步骤由 password-provider
  自动化适配器驱动，不包含 Flutter Hosted Login UI 交互，也不证明生产容量或 WORM 合规。
- 批次 2–6 本机完整门禁已通过；随后推进其余查询/统计与治理作业容量；
  PostgreSQL v1 兼容对账仍物化旧快照，容量部署须完成 v2 cutover。
  broker 迁移/重建不能根据旧确认声称新目的地已完整。
- 安全工具已安装并运行：可达漏洞已修复，但 gosec 静态评审/整改仍未完成，安全发布
  门禁失败；不能继续把“工具未安装”作为当前状态，也不以局部漏洞扫描通过替代整套扫描。
- 治理状态接口、租约/心跳/检查点、重试/取消、分块导出、其余跨副本预算，
  M4 合规生命周期和 M5 版本绑定业务执行均仍未实现；不将审批表述为恢复执行成功。
