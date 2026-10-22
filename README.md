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
- [安全扫描整改记录](docs/SECURITY_SCAN_REVIEW.md) — 已整改项、剩余告警与发布阻塞，不以零可达漏洞代替扫描通过
- [Outbox writer inventory and rollout verification](docs/outbox-writer-rollout.md)
- [术语表](docs/GLOSSARY.md)
- [工程门禁](docs/ENGINEERING_GATE.md) — `python3 cli.py quality` 的阶段清单与设计约束
- [契约同步](docs/CONTRACT_SYNC.md) — OpenAPI / AsyncAPI / Proto 三条线的漂移防线
- [部署 / 回滚 runbook](docs/DEPLOY_RUNBOOK.md) — WORM COMPLIANCE 门禁、cutover、一致性键、全栈验证
- [平台批次联合核查报告（G1–G8 门禁矩阵快照）](docs/platform-gate-status.md)
- [Aero 平台联调](docs/PLATFORM_INTEGRATION.md) — Snaplink 身份 + Aero ID/IM/Vault 来源 + Console 兼容读的本机端到端验证
- ADR：[0001 消息传输与接入](docs/adr/ADR-0001.md) · [0002 事件编码与 Schema 兼容](docs/adr/ADR-0002.md) · [0003 哈希链分段与检查点](docs/adr/ADR-0003.md) · [0004 查询投影与租户路由](docs/adr/ADR-0004.md) · [0005 合规归档与密钥](docs/adr/ADR-0005.md) · [0006 租户 ID 键框架校验](docs/adr/ADR-0006.md) · [0007 开发认证默认失败关闭](docs/adr/ADR-0007.md) · [0010 API/worker 签名与归档一致性键](docs/adr/ADR-0010.md) · [0011 发布确认与主动对账](docs/adr/ADR-0011.md) · [0012 投影确认与重建](docs/adr/ADR-0012.md) · [0013 固定查询与投影候选](docs/adr/ADR-0013.md) · [0014 导出队列预算与调度](docs/adr/ADR-0014.md) · [0015 HotFirst 断点恢复](docs/adr/ADR-0015.md) · [0016 租户治理摘要](docs/adr/ADR-0016.md) · [0017 文件数据安全 I/O](docs/adr/ADR-0017.md) · [0018 TLS/DLQ 文件边界](docs/adr/ADR-0018.md) · [0019 导出租约与执行 fencing](docs/adr/ADR-0019.md) · [0020 导出取消与关联重试](docs/adr/ADR-0020.md) · [0021 密封对象检查点与终态续跑](docs/adr/ADR-0021.md) · [0022 导出访问的认证 Hold 与锁外复核](docs/adr/ADR-0022.md) · [0023 固定成员计划与 selected 阶段续跑](docs/adr/ADR-0023.md) · [0024 逐页生成检查点](docs/adr/ADR-0024.md) · [0025 分页时间线的锁外认证取证](docs/adr/ADR-0025.md) · [0026 Console 页与 facets 的锁外认证读取](docs/adr/ADR-0026.md) · [0027 操作摘要的锁外认证读取](docs/adr/ADR-0027.md) · [0028 操作回放的锁外认证取证](docs/adr/ADR-0028.md) · [0029 导出创建与新后继的认证 Hold 预检](docs/adr/ADR-0029.md) · [0030 普通查询参考初页的认证前置](docs/adr/ADR-0030.md) · [0031 旧全量时间线与聚合回放的认证读取](docs/adr/ADR-0031.md) · [0032 按坐标读取的回执与精确 Schema 正文绑定](docs/adr/ADR-0032.md) · [0033 回执元数据的身份绑定、索引与共享预算](docs/adr/ADR-0033.md) · [0034 既有 Hold 控制写的 CAS 重判与零证据重写](docs/adr/ADR-0034.md) · [0035 既有恢复决策的控制 CAS 重判](docs/adr/ADR-0035.md) · [0036 文件自审计描述符关闭与夹具生命周期](docs/adr/ADR-0036.md) · [0037 导出执行器尾读保留与原子唤醒交接](docs/adr/ADR-0037.md) · [0038 既有恢复申请创建的控制增量提交](docs/adr/ADR-0038.md) · [0039 既有留存配置写的控制 CAS 重判](docs/adr/ADR-0039.md) · [0040 既有 Schema 注册的控制 CAS 与演进重判](docs/adr/ADR-0040.md) · [0041 既有来源更新的控制 CAS 与当前记录重判](docs/adr/ADR-0041.md) · [0042 既有来源创建的控制 CAS 与当前唯一性重判](docs/adr/ADR-0042.md) · [0043 既有恢复详情的控制面读取与单次预算](docs/adr/ADR-0043.md) · [0044 既有留存配置的控制面读取与单次预算](docs/adr/ADR-0044.md) · [0045 跨租户单事件读取的锁外归档认证与单次预算](docs/adr/ADR-0045.md)

## 快速开始（隔离 demo）

不依赖 PostgreSQL、Kafka、ClickHouse、IdP 或兄弟仓库即可运行核心链路：

```sh
make demo-up
make demo-seed
make demo-prove
make demo-down
```

`core` 使用单 API 进程、真实文件状态和本地归档（内置导出队列）。切换到既有
PostgreSQL + MinIO + 治理 worker 的 `advanced` profile：

```sh
DEMO_PROFILE=advanced make demo-up
DEMO_PROFILE=advanced make demo-seed
DEMO_PROFILE=advanced make demo-prove
DEMO_PROFILE=advanced make demo-down
```

可用 `DEMO_PROJECT` 隔离并行项目、`DEMO_API_PORT` 指定 loopback 端口；所有端口仅绑定
`127.0.0.1`，`demo-down` 默认保留卷且只操作当前项目。开发认证关闭，使用显式本地
HS256 模式；随机密钥和运行文件位于仓库外的私有目录，不输出凭据。完整断言、篡改
对照和 advanced 冒烟说明见 [docs/DEMO.md](docs/DEMO.md)。

## 当前实现

仓库现在包含一个可运行的 Go 单节点参考实现（构建至少使用 Go 1.26.10，CI 与 Docker builder 同步，避免旧标准库已知漏洞）：

- `go run ./cmd/audit-api` 启动 REST API。
- `go run ./cmd/audit-governance-worker -once` 执行一次留存/Legal Hold 评估。正常启动在打开状态存储前注册 SIGINT/SIGTERM；启动归档 Ready 同时受信号取消与原 5 秒上限约束，取消启动不进入治理 pass，关闭存储并释放文件锁。无取消的就绪失败仍非零退出；持锁不等于启动就绪，不变更导出状态、租约、检查点或保留期。
- 聚合 checkpoint 每租户按上限保留（默认 1000 条，drop-oldest，`AUDIT_AGGREGATE_CHECKPOINT_HISTORY` 可覆盖，下限 1）；根与签名未变化的周期去重、空闲周期不写快照，`VerifyIntegrity` 聚合校验工作量由此有界。
- `GET /api/v1/governance/status` 返回租户控制面治理摘要，Web 概览独立于事件读取权限展示：导出作业、恢复决策、Hold 数量和留存配置按既有权限分别开放；未授权分区省略，不冒充零计数。只读 control 元数据并失败关闭追加 `governance.status.read`，读取与自审计共享一次 deadline；不扫描账本或请求 WORM/投影。审批不是执行，导出完成不绕过 Hold，配置不是生命周期或 COMPLIANCE 证明；`source=control`、`completeness=not_asserted`，详见 ADR-0016。旧 v1 读取与共享 control 历史成本仍待改造。
- 导出 pending/running 队列按同一共享 control CAS 准入：全局最多 128、单租户 32，耗尽 429；全局 running 最多 2、单租户 1，API/worker 共用预算。有限 dispatcher 按租户轮转、租户内 oldest-first，不再为所有 pending 启 goroutine；API 启动/每秒重新唤醒，完成继续推进。Resume 返回观察 backlog 而非完成数。领取同一次 CAS 保存私有 2 分钟租约，执行器每 30 秒续期；过期/失主/续期失败取消执行，终态须精确未过期 owner fencing。API 队列启动/每秒和 worker 原子失败无检查点的过期任务并追加一次 `export.recovered`；合法 selected 计划或 sealed 检查点留给有限 dispatcher 接管，不重新排队终态。迟到 Put 可留下无引用 sealed WORM 对象，但不能发布路径或删除对象；预算限制持久 claim，不承诺强杀远端 I/O。见 ADR-0014/0019/0021/0023/0024；新多页任务已有逐页生成进度，严格跨副本公平 SLA 和分块流式下载仍未实现。
- 导出生成前的 **持久固定成员计划 / selected 阶段续跑**（ADR-0023）：私有 control 计划绑定原任务/申请人/lineage/完整查询及首次 started_at，只含有序账本身份/坐标/发生时间与精确 Schema ID/version/定义 HMAC，不复制载荷、密文、原始筛选、搜索摘要或 owner。最多 10000 成员/4 MiB，仍占共享 running 2/tenant 1、队列 128/32；不使用查询 cursor 的 15 分钟 TTL。首次选择先认证 committed receipt/Schema/内容/链再过滤，不信任派生搜索 sidecar；内部读自审计后，当前未过期 owner 将计划与一次 export.planned 同 CAS 保存。崩溃后仅合法到期 running 换新 owner，保留 started_at、跳过选择；迟到事件不增加输出成员，当前 Hold 仍核对原查询的当前认证证据。保留的 v1 计划与新单页任务仍整块生成；新多页任务使用下述 v2 逐页进度。锁外源读、当前 Hold、自审计及阶段复核共享各次既有 deadline；失败保留计划，不用新选择替代。Put 前及 sealed 保存 CAS 复核计划/最新 Schema/owner，后者原子消费计划/进度；终态或取消只清合法活跃元数据，不删事实/WORM。Put 后 sealed 保存前崩溃可能留下无引用对象，新 owner 按已知计划生成不同 attempt 对象，不重放未知旧 key、不宣称仅 Put 一次。M3 整项仍 Partial；生成计划、sealed 终态续跑、关联重试、心跳和业务执行各不相同。
- 新增 **逐页生成检查点**（ADR-0024，M3 有限子切片）：新选择超过 100 个成员时保存 version=2/format_version=1 计划，每页最多 100 个原成员。真实生成、剥离搜索 sidecar、独立 AEAD 密封并 Put 后，精确未过期 owner 将连续页引用/范围/密封和 JSONL 摘要与一次 export.progressed 同 CAS 保存；不把对象字节放入 control。页 AEAD 独立绑定 tenant/job/完整固定成员与 Schema/格式/页范围/对象，页 key 使用不透明 owner 哈希；总元数据仍限 4 MiB（内含每页 1 KiB 预留），共享预算不变。接管先认证原已提交页及最新账本坐标/Schema，只生成剩余页；坏/缺页保留计划，绝不重读源事件替代已提交页。每页锁外源读、当前认证 Hold（包括迟到事实）、读自审计、Put/Save 共用一次原服务 deadline；Put 前和进度 CAS 复核 owner/计划/Schema/Hold 集，慢页允许续期/取消。未知 Save 停止本执行，以实际耐久前缀恢复；迟到/未提交页 Put 可留无引用对象，新 owner 用不同 attempt key，不删 WORM、不猜回滚。所有页提交后合并为现有整块 export:v2，再走 sealed 检查点/当前 Hold/自审计/终态 fencing；最终整块密封/Put 尚无更细字节检查点。首次选择/当前 Hold 历史成本、最终合并/密封内存仍在；不是恒定内存、分块流式下载、严格公平 SLA 或业务恢复执行。
- 导出新增 **Put 后密封对象检查点 / 终态提交续跑**（ADR-0021，M3 有限子切片）：原事件先按精确 Schema、内容和 committed receipt/链坐标认证，执行选择的 Get 移到锁外；最多沿用 10000 个匹配身份的查询预算。Put 成功后，当前未过期 owner 将私有 version=1/seal_version=2 检查点与 `export.checkpointed` 同 CAS 保存。检查点仅含任务 HMAC 绑定、原对象引用、密封/JSONL 摘要、计数与时间，不复制载荷、密文或原始筛选，也不使用短期查询 cursor 续跑。崩溃后仅合法到期 running 以新 owner 接管，保留 started_at 和共享 2/1 配额；读取原对象、认证精确密封字节/v2 tenant-job AEAD/JSONL 摘要/计数及最新账本坐标，重查当前 Hold 并失败关闭自审计，终态 CAS 同时核对 Hold 集没有变化。恢复不重新选择输出、不重新密封或 Put。检查点终态的当前 Hold 复查先从 committed receipts 锚定证据，按精确 Schema 重建认证内容/链，再评估原查询与 Hold；敏感条件从认证原值按条件自身的 bound/legacy 格式重算，不信任派生搜索摘要的存在、格式或值。缺失/损坏/版本不可认证的 Hold 证据保留检查点并拒绝完成；无候选/时间不相交仍不读源对象。该认证判定随后按 ADR-0022 复用于详情/下载放行，创建/重试预检另见 ADR-0029；首次选择、当前 Hold 与其余读路径的历史成本仍未整体改造。完成/失败/取消同 CAS 清活跃检查点，事实和 WORM 不删。Get/Store/读自审计失败保留检查点，坏证据失败并记录固定事实，晚 owner 不能发布路径。selected/sealed 阶段均未提交前崩溃仍按租约失败；sealed 本身只续跑终态提交，逐页生成另见 ADR-0024，大文件流式下载和业务执行仍未实现，M3 整项仍为 Partial。
- 导出详情与下载放行复用 receipt 锚定、精确 Schema/内容/链认证的当前 Hold 判定，再评估原查询和 Hold；敏感条件从认证原值按条件自身格式推导，不信任派生搜索索引。深快照捕获后 Get/认证锁外，详情与下载放行各共享一次既有服务 deadline（后者含自审计），慢源读不阻塞续期/取消。成功前只读复核当前租户活跃 Hold 集与任务选择，变化返回 409 要求刷新；详情可观察同查询正常向前的较新状态，下载及已捕获终态必须原样。自审计前后均复核下载放行，事实可能记录已拒绝输出的尝试，不证明字节交付。HTTP 成功/错误全部 no-store，认证/Hold/自审计之前不输出明文。仍有历史物化/扫描，不是分块生成续跑、流式大文件下载或其他 Hold 路径整体改造，见 ADR-0022。
- 导出创建与关联重试**创建新后继**的 Hold 预检复用 receipt 锚定、精确 Schema/重建内容/链认证，先认证再评估原查询与 Hold，敏感条件从认证原值按自身 bound/legacy 格式重算；派生搜索 sidecar 不能隐藏真正被 Hold 覆盖的证据。本租户 receipt key/body 错绑、坏/缺证据失败关闭，hot-only/journal 不是事实。原 QueryEvents/一次读自审计后捕获独立深快照，Get/认证锁外，直到拒绝事实或新任务 Save 共享该 create/retry 的一次原 deadline。允许时仅返回请求内活跃 Hold 集 HMAC，新任务显式使用 control-only CAS，在每次冲突重试的新鲜 control 上重新核对原 Hold 绑定与原队列 128/32；不把预先物化的整快照覆盖回去，也不替换未变的租户 hot/cold 文件。被 Hold 拒绝时的独立拒绝事实同样只更新新鲜 control。集合变化返回 409、零创建/成功事实，要求显式刷新，不暗中重选或重新计时。无候选/时间不相交不增加 Hold 源 Get，零匹配仍可创建；其他租户 Hold/无关策略不制造冲突或被旧快照覆盖。已存在后继仍只返幂等安全摘要、不重读证据；原终态不回退，owner/lease/阶段与后续 Hold/下载门禁不变。HTTP 创建与 retry 全响应 no-store；未知 Save 不当回滚或成功，不新增创建请求幂等承诺。不是跨后端原子证据/业务版本计划，初页认证后续按 ADR-0030 演进，全历史物化/扫描仍未消除，预检见 ADR-0029，M3 仍 Partial。
- 导出所有者可用 `POST /api/v1/exports/{jobId}/cancel` 原子取消 pending/合法 leased running，并写 `export.cancelled`；同主体重复取消零写入。沿用导出权限，平台凭证也须显式租户且不能代替其他申请人；未知申请人、旧无围栏 running 和其他终态失败关闭。取消撤销运行权，不证明远端停止或回滚、不删除 WORM。`/retry` 仅对本人已结束 failed/cancelled 创建带 `retry_of` 的新任务，原终态不变、清除旧 cursor、重做读自审计/Hold/共享配额；同一 control CAS 每个原任务最多一个直接后继，重复请求返回同一摘要，需要再试须指定后继。操作响应只含安全摘要、均 no-store，详情/下载 Hold 门禁不变。可选 `failure_code` 为固定失败类别，不透传诊断；治理摘要单列 cancelled。Web 列表/详情提供本人确认操作和关联，未知提交先刷新、不盲重发；见 ADR-0020。关联重试不继承原计划/检查点或执行业务恢复，旧 control writer 不得丢弃关联/计划/检查点字段。
- 仅无租约的旧导出 `running` 使用原年龄恢复：默认 24 小时，从有效 `StartedAt` 起算，否则回退 `CreatedAt`，严格超过阈值才由 worker 失败并原子写 `export.recovered`（`AUDIT_GOVERNANCE_STUCK_EXPORT_AGE`/`-stuck-export-age` 可覆盖；`0` = 默认）。健康租约不因旧年龄阈值判死，空闲恢复零写入；部署需同步时钟、停写核对旧执行，不混跑忽略租约的旧 API/worker，不人工清空租约换成功，详见 ADR-0019。
- 事件查询 `GET /api/v1/events` / `POST /api/v1/events:query` 使用 15 分钟固定成员集，count 在续页期间不变；迟到事件只出现在新查询。HMAC 游标绑定租户、主体、全部筛选、来源和页大小，过期/缺失/篡改返回 400，不能静默接回实时结果。最多 10000 个成员、4 MiB 元数据、每租户 16 个活跃分页集，耗尽 429。成员集独立保存、不含载荷或原始敏感筛选；PostgreSQL API 部署前须应用 `008_query_snapshot.sql`，缺表启动/readiness 失败关闭。响应不声明完整性；浏览器用 POST 传筛选/摘要并显示结果集有效期。单事件及续页按坐标取证，归档在 Store 锁外，整次读取共用操作 deadline 并失败关闭。普通 ledger-reference 初次选择复用 Console 的认证扫描：捕获独立深副本后，锁外核对 committed receipt/map 身份、精确 Schema/重建内容及链，再评估所有筛选；坏/缺证据不能藏为空匹配或部分成员。敏感条件从认证原值按自身 bound/legacy 格式推导，不信任派生 sidecar；因此加密 searchable 字段的新初次查询也支持双格式互查，返回保护字段不替换为明文。仍保留 10000 匹配上限、不限制 Console，旧成员集/续页不重选；捕获至成员 Save/一次读自审计共享原 deadline，追加后取消也零结果，不撤销未知已提交事实/运行元数据。参考初页和 v1 兼容仍有历史物化/认证扫描成本，见 ADR-0013/0030。
- 单事件、固定成员续页及投影实际输出页的按坐标取证进一步核对请求 tenant/event 与 receipt **正文**绑定（包括不携带链坐标的单事件），错绑在任何源 Get 前失败关闭；精确 Schema map key 还须与其正文 tenant/id/version 一致，再认证原内容/链。缺失/错绑不取最新版本替代，停用的精确历史定义仍可读，保护字段不输出内部重建明文。单事件在读自审计追加后也复核原 context，已取消/超时零事件，不撤回已耐久的尝试事实或声称字节交付；取证/一次读事实仍共享原 deadline，Get/认证锁外，不回写旧捕获覆盖并发 control。原成员/游标/count/来源/预算不变，续页仅读本页原坐标、不重选或重扫前缀；HTTP 原租户/权限、no-store 与摘要剥离保持。旧跨租户 helper 和历史物化/扫描、完整语义矩阵与容量仍未整体完成，见 ADR-0032；不是导出新阶段或业务执行，M3仍Partial。
- 回执 `GET /api/v1/events/{eventId}/receipt` 使用租户精确冷索引读取元数据（旧 v1 仍物化旧快照），核对请求 tenant/event 与回执正文身份，传播底层读取错误；错绑固定 409、零输出/成功读事实。读取和原一次失败关闭自审计共享原 deadline，捕获后和追加后取消均零回执，不撤销耐久尝试事实。HTTP 成功/全部错误均 no-store，原签名租户/平台显式选择不变。不 Get 源对象或依赖 Schema、不重新解释状态/时间/坐标，不把元数据当正文/全链认证、投影确认或下载授权；不写回旧快照覆盖并发 control。租户 hot/control 克隆、旧兼容及其余历史成本仍在，完整语义矩阵/M3仍Partial，见 ADR-0033。
- 操作时间线与聚合历史支持 `page_size`（默认 100、最大 1000）和作用域绑定的 `cursor`；分页响应的 `count` 是本次总匹配数。两个分页入口先捕获独立深快照，再在 `Store.mu` 外按 committed receipt、精确 Schema 身份/版本、重建内容及链哈希认证，之后才评估 operation/aggregate 条件、排序与分页；坏/缺证据不能过滤隐藏或返回部分页。捕获、Get/认证、分页和一次失败关闭读自审计共用该服务调用的一次原 deadline，不阻塞并发租约续期/取消，也不保存旧快照覆盖 control。仍为 live 分页，后续调用的 count 可随迟到/新提交变化，不是事件查询的固定成员集或全历史完整性证明。历史物化/扫描成本仍在；旧 service 全量方法保留 10,000 条保护上限，其后续锁外认证读取见 ADR-0031；分页决策见 ADR-0025，操作摘要另见 ADR-0027。
- 操作摘要 `GET /api/v1/operations/{operationId}` 从独立深快照的 committed receipts 锚定，锁外认证精确 Schema 身份/版本、重建内容与链后才评估 operation 条件、计数、时间和 outcomes；坏/缺证据不能藏为正常空摘要/404 或部分统计。捕获、Get/认证、聚合和一次失败关闭读自审计共享该服务调用的一次原 deadline，不保存旧快照覆盖并发终态、租约、Hold 或策略。保留本次匹配数、最早/最晚发生时间、排序去重 outcomes 与原 tenant/权限；HTTP 成功和错误均 no-store。仍是 live 观察、非固定成员集或完整性证明，后续迟到/提交可改变摘要；不套用旧全量时间线的 10000 条保护上限，也不把该兼容口径当容量保证。全历史物化/认证扫描与恢复整体流程仍未整体改造，见 ADR-0027；旧全量/aggregate replay 的有限认证改造另见 ADR-0031。
- 操作回放 `GET /api/v1/operations/{operationId}/replay` 复用 receipt 锚定、精确 Schema 身份/版本、重建内容与链 hash 的认证源，在深快照捕获后锁外认证，再选择/排序/本地折叠 ChangedFields；坏/缺 committed 证据不能隐藏或输出部分 state。捕获至一次失败关闭读自审计共享原服务 deadline，不写回旧快照覆盖并发终态/租约/Hold/control，HTTP 全部 no-store。保留最多 10000 个匹配事件、occurred_at/sequence/event_id 升序、after=nil 删除及最后折叠事件的 last_sequence（不是最大序号、水位或执行回执）；仍为 live 观察，无业务调用或完整性保证。已有恢复预演/创建的空 actor 委托继承此证据读取，保留零读事实与原独立控制提交/审批语义，不将整个恢复流程冒充共享预算、版本绑定或业务执行。全历史物化/扫描和 M3 其余项仍待改造，见 ADR-0028；旧全量/aggregate replay 的后续认证读取见 ADR-0031。
- 旧全量 `OperationTimeline` / `AggregateTimeline` 与服务层 `ReplayAggregate` 复用 receipt 锚定的认证源，独立深快照后锁外核对精确 Schema、重建内容和链，再筛选/排序/本地折叠；捕获至一次原读自审计共享原 deadline，失败或追加后取消零结果，不回写旧快照覆盖并发 control。保留最多 10000 个匹配事件（超限拒绝、不截断）、operation 的发生时间/序号/ID 顺序、aggregate 的版本/ID 顺序及空 actor 零事实策略。聚合回放仍 after=nil 删除，last_sequence 为最后折叠事件而非最大序号；无业务副作用，当前没有生产调用或 HTTP 路由，不新增执行入口。仍是 live 观察和全历史物化/认证扫描，不是固定成员/完整性或容量保证，见 ADR-0031；原分页时间线/操作回放不重复计功，M3 仍 Partial。
- Console 事件页与 facets 从独立深快照的 committed receipts 锚定，锁外认证精确 Schema 身份/版本、重建内容和链后才过滤/排序/统计；坏/缺证据不能藏为正常空结果或返回部分统计，hot-only/journal 不是事实。捕获、Get/认证、分页/聚合与一次失败关闭读自审计共用该服务调用的一次原 deadline，慢 Get 不阻塞同 Store 续期/取消，也不写回旧快照。服务层敏感条件从认证原值按条件自身 bound/legacy 格式推导，不信任搜索 sidecar；返回保护字段不解密替换，HTTP 仍递归剥离摘要，成功/错误均 no-store。保留原 tenant claim 与已批准 all-tenant Console 例外，不新增敏感筛选 URL。**Console 仍为 newest-first live 分页，count 沿用 cursor 后匹配数**（首页为全部），不是普通事件固定成员集或分页时间线的总 count；facets 忽略分页，total 为全部匹配、providers 仍空。历史物化/认证扫描、ClickHouse facets/全租户完整性和旧跨租户详情 helper 成本未解决，见 ADR-0026。
- 归档成功后事件正文从控制面热快照淘汰，仅保留 receipt/hash 链元数据；`GET /events`、`GET /events/{id}`、操作/聚合时间线、回放、导出和完整性校验会从 WORM 归档读回并校验身份与哈希。事件查询/续页、单事件、导出创建/重试预检、执行选择/密封检查点验证、详情/下载放行的当前 Hold 与对象下载已锁外取证；创建/重试各共享一次服务操作 deadline，预检仍有完整历史快照成本；两个分页时间线另按 ADR-0025、Console 事件页/facets 按 ADR-0026、operation summary 按 ADR-0027、operation replay 及其已有内部恢复委托的证据读取按 ADR-0028、旧全量 timeline 与 aggregate replay 按 ADR-0031 锁外认证；旧跨租户单事件兼容 helper 按 ADR-0045 锁外认证。以上读路径均与数据库语句共享同一个 `AUDIT_STORE_OPERATION_TIMEOUT` 上限：单次服务操作只派生一次回读上限，回读超时按既有的 `store_timeout` 可重试 503 失败关闭，而不会按 receipt 逐个重新计时。查询结果在淘汰前后保持一致。
- `go run ./cmd/audit-outbox-relay -once` 消费业务库 `audit_outbox` 待投递记录并写入审计 API；成功标记 delivered，失败按指数退避重试，超过上限或遇到客户端错误死信。
- `go run ./cmd/audit-kafka-dlq-replay -once` 恢复死信事件（从 accepted topic 按 key 找回原消息重发或经 API 重新接入，状态文件去重）。状态追加/修复/锁的关闭失败返回错误，不把未知结果记为内存成功，允许幂等重试；不产生业务恢复副作用。
- Kafka consumer/replay 的独立指标服务仅注册 `GET /metrics`、不继承进程默认 mux；header/read/write/idle 上限为 5s/10s/10s/60s，header 最大 16 KiB。部署仍须限制监控网络，超时不等于鉴权或连接准入。
- 聚合签名拒绝负版本/计数及非正 checkpoint 序号；合法 v2 签名输入不变，不需要重签名。已提交坏证据仍保留 `valid=false` 与自审计诊断。S3 保留期仍为 1–36500 天，转换前就地检查，不改变已有对象 COMPLIANCE。
- 默认本地状态保存到 `./data/state.json`，归档保存到 `./data/archive`。
- 文件状态首次打开时会把 v1 单快照迁移为 v2 热/冷布局：控制面仍在
  `state.json`，每个租户的热事件/流状态在 `tenants/`，receipt、segment、checkpoint
  追加到 `ledger/`，原文件保留为 `state.json.v1` 备份；v2 布局发现缺失热文件、冷文件或临时文件时拒绝启动。
- v2 文件写入优先走 `ReadTenant`/`UpdateTenant`，只重写当前租户热文档；冷 ledger 在启动时建立索引，写入采用 fsync 后追加。
- 文件 HotFirst 随 hot 持久化原始 `pending_ledger` 追加意图；API 启动/每秒轮转租户恢复，独立于 publisher，worker 封段/归档前也先恢复。锁外验证内容、连续前序、流头及原段/checkpoint 签名，版本/journal 重新核对后追加原记录，不重新分配序号或签名。未完成意图不是账本事实，阻止该租户覆盖；其他租户/纯管理写可继续。PostgreSQL v2 HotFirst 的 hot CAS 与 ledger 在同一事务，cold 失败回滚 hot。升级前无原 journal 的 hot-only 残留不推断为提交；摄入错误不能伪装 committed receipt。完整语义和回滚要求见 ADR-0015。
- Linux 文件数据按目录描述符逐组件拒绝符号链接，验证 regular/单链接 inode 后才读写或截断；新文件 0600、新目录 0700。原子写使用同一父目录 fd 的 O_EXCL 临时文件、fsync、renameat 和祖先同步，不覆盖/删除未知 `.tmp`。原子写与 ledger append 的实际捕获父目录只同步一次，再按原顺序同步全部所需祖先；仍复核父路径安全及 inode 一致，目录替换/同步失败不能报告耐久成功。不缓存“以前同步过”来跳过本次边界，不省略文件 fsync 或新目录链。旧文件读取不自动 chmod；新原子重写得到 0600，备份身份和部署 UID/ACL 必须核对。查询元数据回收限定捕获目录，只清过期元数据。数据目录须为真实受控目录/挂载，不能据此宣称防特权主机篡改或正式 WORM 合规；非 Linux 文件数据失败关闭。见 ADR-0017；TLS 运维 secret mounts 与 DLQ 的独立策略见 ADR-0018。
- 文件读自审计的持久追加描述符在 `Store.Close` 中与原 trail mutex 串行关闭：已在途追加先完成/报错，再关闭 trail，最后释放 flock；两种关闭错误均保留，重复 Close 返回原结果且不重复消费 fd。关闭后的 trail 读/追加失败关闭、不使用旧 fd 或重新打开，必要读自审计不能被旧句柄绕过。关闭不改事实、fsync、WORM或未知 temp；调用方仍须先停止并 join 工作，这不是通用 Store 全方法关闭 fencing 或远端强停。常用 service 文件夹具显式 cleanup 原 trail/flock，不依赖 GC、保留全部真实文件 I/O/断言与原超时。此局部资源修复不证明历史累计超时/原 running 唯一根因，见 ADR-0036。
- 导出本地 reservation 保留至执行（含心跳 stop/join）及原最后一次有界 control 读取都结束，再同一 scheduler mutex 内释放并登记有限后继 wake；终态可见不再被误当作完整执行器退出。读取仍锁外、原次数/deadline不变，错误/取消/保留运行阶段不热重试；共享 running/lease/阶段/当前Hold/自审计与终态不回退保持。原真实文件顺序回归在已有 cleanup 护栏内另观察完整退出，再关闭存储，不延长原5秒终态等待或10m包上限。调用方仍须先停止队列/外部waker，这不是通用关闭/强停协议，也不证明历史累计超时/running唯一根因；见 ADR-0037。
- 文件模式状态强制单写者：`store.Open` 在 `<path>.lock` 上持有进程生命周期排他 flock（flock 为 advisory 且依赖文件系统，NFS 上不可靠），同一 `-state` 路径的第二个实例（audit-api 或 audit-governance-worker）启动即报错退出；多实例部署必须使用 PostgreSQL 后端（乐观版本锁）。
- 设置 `AUDIT_POSTGRES_DSN`（或 `-postgres-dsn`）后，旧部署默认继续使用 PostgreSQL 单行表 `audit_state_snapshot`（迁移 `004_state_snapshot.sql`）；要启用 v2 热/冷分层，先停 API、应用 `migrations/006_hot_cold_split.sql`，再运行 `go run ./cmd/audit-pg-migrate -confirm MIGRATE`（或调用 `store.MigratePostgresSnapshot(db)`）完成带备份的显式切换。切换后 `audit_state_snapshot` 仅保存控制面，`audit_tenant` 保存每租户热文档，`audit_ledger` 保存追加式账本；新库在已建 006 表但尚无快照行时可直接以 v2 启动。未完成切换的旧行会失败关闭，不会被自动解释为 v2。两种 PostgreSQL 布局都支持多副本共享与乐观版本锁；`Store.Update` 内做有界 jitter 重试（3 次、5–25ms 指数退避 + 抖动，闭包在新鲜快照上重跑），重试耗尽返回 503 `snapshot_conflict`；`/readyz` 同时探测 store（PostgreSQL 不可达 → 503 `store_unavailable`）与归档目标。
- PostgreSQL 请求路径的全部语句（`internal/store`）都携带存储操作上下文：每个存储操作在 `Store.mu` 之前派生一次上下文，并由 `AUDIT_STORE_OPERATION_TIMEOUT`（默认 `store.DefaultOperationTimeout` = 10s）设定上限。超时会失败关闭为新哨兵 `store.ErrOperationDeadline`（同时匹配 `context.DeadlineExceeded`）：HTTP 复用既有可重试 503 类并新增 `code=store_timeout`，gRPC 复用 `codes.Unavailable`（固定文案，恒不外泄后端细节）；计数以无标签单调计数器 `audit_store_operation_deadline_exceeded_total` 暴露。ingest 提交按 F1 不变量在 `Service.Ingest` 处一次性与调用方取消解耦——取消前中止本次摄入且不写任何数据，已持久化的提交不会被回滚。连接池参数全部由运维显式配置且**不发明默认值**：未设置即不调用任何 setter（`database/sql` 默认，`MaxOpenConnections` 无上限），非法值启动即失败并指名变量；四个 PostgreSQL 二进制共用 `internal/runtimeconfig/postgres_pool.go`。审计-api 要求该超时**严格小于** 30s `WriteTimeout`，否则可重试的 503 写不出去。指标与取值门禁见 `docs/BENCHMARKS.md`，运维变量见 `docs/DEPLOY_RUNBOOK.md`。
- 接入、幂等、租户隔离、Schema 校验、分段哈希链、查询、操作回放、导出、Legal Hold、完整性验证和恢复申请已实现。
- 哈希链证据链分三层：事件 prev_hash 链 → 段 Merkle Root + 签名 checkpoint → 跨段聚合 Merkle（governance worker 周期生成，`VerifyIntegrity` 逐层验证）。
- 导出文件在归档前整体 AES-GCM 密封（独立加密），下载时解密，`job.Digest` 覆盖解密后内容。
- 设置 `AUDIT_OTLP_ENDPOINT`（如 `http://jaeger:4318`）后启用 OpenTelemetry tracing：HTTP 中间件提取/注入 W3C `traceparent`；server span 在路由匹配后按模式创建（`span name`/`http.route` 取自匹配到的 ServeMux pattern，如 `GET /api/v1/events/{eventID}`，不包含原始路径中的 ID 值，基数有界），未匹配任何路由的请求不产生 span、`X-Trace-ID` 回退为请求 ID；未配置时自动降级为 no-op tracer。
- 查询支持按 `operation_id`、`causation_id`、`correlation_id`、`trace_id` 等关联维度筛选（与操作时间线/聚合历史配合还原业务链路）。
- 控制面管理操作全部自审计：租户/来源/Schema/留存策略变更、导出、Legal Hold、恢复申请与审批与对应变更原子写入 append-only 审计轨迹；读路径同样自审计（`GET /api/v1/events`、`GET /api/v1/events/{id}` 追加 `audit.event.read`，导出下载追加 `audit.event.export`，追加失败读请求失败关闭），可通过 `GET /api/v1/admin/actions` 查询（租户 token 仅见本租户，平台 token 可跨租户）。读轨迹的物理保留由 `AUDIT_ADMIN_TRAIL_CAP`（默认 100000，`0` = 不限）压紧：超过上限即按「水位」压实——只保留最新的 `cap − ⌊cap/2⌋` 条，之后再追加 `⌊cap/2⌋` 条才触发下一次压实，因此压实代价按 append 均摊为 O(1)（文件后端不再每个读请求重写整份 JSONL；PostgreSQL 后端不再每个读请求在 `trailMu` 内跑一次 `DELETE … OFFSET` 扫描），轨迹行数始终在 `(cap/2, cap]` 内振荡。因此需要压实后仍有 `k` 条可查时，请把 `-admin-trail-cap` 设为 `2k`（默认即压实后约 5 万条）；多副本共享同一张 `admin_action_trail` 表时，每副本的计数只是全表行数的下界，峰值行数上界为 `cap − ⌊cap/2⌋ + (⌊cap/2⌋+1) × 副本数`。
- 恢复申请支持审批流程：`POST /api/v1/restores/{runId}/approve` 与 `reject` 记录审批事实（approval 与业务执行分离），状态机 `pending_approval → approved/rejected`。两条既有决策只更新新鲜 control；实际 Save 冲突后重判当前租户/记录、pending 状态与职责分离，保持 404→409→403 优先级，首个耐久决定不被覆盖，重复决定仍 409/零写入。变更与一次原事实同 CAS，不替换 v2 hot/cold 证据，不覆盖并发导出终态、lease/阶段、Hold或策略，不 Get/Put WORM。原一次决策 operation 预算/有限重试不变；失败或未知 Save 返回零成功对象，已耐久决定/事实不撤销，须只读核对、不盲重发。此为局部写放大/并发覆盖修复，不证明旧 service 累计超时或 running 唯一根因闭环；此决策当时未改创建/读取，后续创建提交另见 ADR-0038、详情读取另见 ADR-0043；不新增 scopes、版本计划或业务执行，见 ADR-0035。
- 既有 `CreateRestore` 的原认证回放之后，创建提交显式使用 control-only CAS：实际 Save 冲突只把原新请求与一次 `restore.created` 合入新鲜 control，不重写 v2 hot/cold、不覆盖当前导出取消/终态、租约续期、selected/页/sealed 阶段、其他恢复决定、Hold或策略。原 ID/申请人/时间生成一次，不因提交重试再回放；原空actor零读事实、10000匹配上限及回放/提交的分离预算保持，提交本身不 Get/Put WORM。失败/未知 Save 零成功对象，已耐久事实不撤销、不自动重建请求；不是请求幂等、证据/业务版本计划或执行回执。详情读取后续见 ADR-0043，通用Store.Update/v1及历史扫描成本仍在；局部文件写放大与真实PG控制覆盖修复不证明旧累计超时/running唯一根因闭环，M3仍Partial，见 ADR-0038。
- 既有 `GetRestore` 详情只观察 control 中的恢复申请/决定，v2 不再物化无关租户 hot/cold 历史、不受无关证据表锁或坏冷记录牵连；不 Get/Put WORM、不 Save、不新增读事实，原恢复元数据静默读取策略保持。读取及捕获后取消检查共用一次原 operation deadline，Load 失败、超时或取消零返回对象；返回时间指针独占，原租户/404、状态/时间/诊断不重解释，每次请求重新观察当前 control。它不是证据认证、版本计划、执行回执或重试授权；回放/必要事件读自审计及当前 Hold、lease/阶段 fencing、终态不回退与共享预算不变。v1仍物化旧快照、共享control历史/其余扫描仍在，局部减少物化/锁等待不证明旧 service 累计10m或原 running 唯一根因、长期稳定性或容量，M3仍Partial，见 ADR-0043。
- 既有 `SetRetentionPolicy` 只将原配置与一次 `retention_policy.set` 合入新鲜 control；实际 Save 冲突后重查当前租户，缺失仍404/零提交，不以旧整快照恢复已取消导出、旧lease/阶段或覆盖当前Hold、恢复决定与另一租户策略。v2不替换hot/cold，配置本身零WORM Get/Put、零独立读事实；原一次deadline/有限重试/校验/权限保持，相同值的再次设置仍各保存一次原事实，不新增版本或幂等。失败/未知Save返回错误，已耐久事实不撤销、先只读核对，不猜回滚/盲重发。仅配置写，不执行生命周期、释放Hold或缩短对象COMPLIANCE；配置读取后续另见ADR-0044；评估、通用Update/v1及历史成本仍在，局部稳定性修复不是旧累计超时/running唯一根因或容量保证，M3仍Partial，见 ADR-0039。
- 既有 `GetRetentionPolicy` 只读当前control，v2不物化无关hot/cold或查询证据表，不Get/Put WORM、不Save/追加事实，保留原配置元数据静默读策略。一次原operation预算覆盖读取及捕获后取消检查；失败、超时或取消零配置，不缓存/重试Load。原tenant key/缺配置404、字段/天数/类别与独占返回保持，不新增当前tenant注册检查或默认配置。`EvaluateRetention`仍按原分离预算读取证据与当前Hold，不将配置观察当留存评估、可删除/COMPLIANCE证明或业务执行。HTTP权限/错误/缓存、必要读自审计、lease/阶段fencing、终态与共享预算不变。真实文件分配与独立PG证据锁依赖的局部修复不是旧累计超时/running唯一根因或容量保证；v1、共享control及其余历史成本仍在，M3仍Partial，见ADR-0044。
- 既有 `RegisterSchema` 只将原新定义与一次 `schema.created` 合入新鲜 control；实际 Save 冲突后重判当前租户、同租户/Schema最高版本、重复key与最近前序的原 required/allowed 兼容规则，不能丢掉并发定义或按旧前序放行。保留缺租户404→回退400→重复409→兼容拒绝400的原顺序、前向跳版本、原字段/上限/默认处理和合法历史精确版本读取。v2不重写hot/cold，不覆盖当前导出取消/终态、续期、selected/页/sealed、恢复决定、Hold或策略；注册本身零WORM I/O/独立读事实。原一次deadline/有限重试不变，失败/未知Save报错、不撤销已耐久定义/事实，先只读核对、不盲重发；重复注册不变为幂等成功。ListSchemas/通用Update/v1及历史成本仍在，局部写放大与真实CAS修复不是旧累计超时/running唯一根因或容量保证，M3仍Partial，见 ADR-0040；不新增版本计划、scope或业务执行。
- 既有 `UpdateSource` 只将原来源替换和一次 `source.updated` 合入新鲜 control；实际 Save 冲突后重查当前 tenant/source（缺失仍404），CreatedAt 取当前记录，不写旧快照覆盖导出取消/终态、健康续期、selected/页/sealed、恢复决定、Hold、策略或邻居来源。v2不替换hot/cold，更新本身零WORM I/O/独立读事实；原allow-list排序/校验、空列表仅同ID客户端、Active=false停用和signed client/tenant解析不变，同值再次设置仍各一次原事实。原一次deadline/有限重试保持，失败/未知Save返回错误与零成功对象，不撤销已耐久配置/事实，须先只读核对、不盲重发。HTTP错误包/权限无线面变化，不新增条件更新/请求幂等或身份能力；CreateTenant/通用Update/v1及历史成本仍在，局部文件写放大/真实PG控制覆盖修复不是旧累计超时/running唯一根因或容量保证，M3仍Partial，见ADR-0041。
- 既有 `AddSource` 只将原新来源和一次 `source.created` 合入新鲜 control；实际 Save 冲突后重查当前 tenant 与同租户/source key 唯一性，保留缺租户404→重复409顺序，不能覆盖或重新启用首个注册。v2不替换hot/cold，不覆盖当前导出取消/终态、续期、selected/页/sealed、恢复决定、当前Hold、策略或邻居来源；创建本身零WORM I/O/独立读事实。原校验/allow-list排序、空列表仅同ID客户端、创建Active默认true、缺省CreatedAt在提交前只生成一次及跨租户歧义失败关闭保持；停用仍走UpdateSource。原一次deadline/有限重试不变，失败/未知Save返回错误，不撤销已耐久注册/事实，先只读核对、不盲重发；重复仍409，不新增幂等或身份协议。CreateTenant/通用Update/v1及全历史成本仍在，少写无关文件不是旧累计超时/running唯一根因、容量或长期稳定性证明，M3仍Partial，见ADR-0042。
- 业务系统可使用 `internal/outbox` SDK 在事务内写入 `audit_outbox`，再由 relay 投递（迁移 `003_outbox_relay.sql` 增加投递台账列）。
- relay 投递支持两种传输：HTTP（默认）与 Kafka（设置 `AUDIT_OUTBOX_KAFKA_BROKERS` 后写入 `audit.events.accepted.v1`，acks=all 同步生产）。HTTP 投递请求 `?wait_for=ledgered` 并**校验 API receipt**（`event_id` 匹配、状态 ∈ {ledgered/indexed/archived}、`ledgered_at` 非零、`hash` 非空）后才标记 delivered；`api_status`/`delivered_event_id` 存 API 返回的真实值（Kafka 投递为 NULL，不伪造）。`audit-kafka-consumer` 以手动 offset 提交消费该 topic 并接入审计 API，失败背压重试——同一条消息原地重试、不重新拉取（kafka-go 的 fetch 位置会越过已取出的消息，重新拉取会导致失败消息被静默跳过），单消息上限 8 次（可调 `AUDIT_KAFKA_MAX_ATTEMPTS`）——永久失败（4xx 除 401/429；401 属可恢复的凭证状态、重试而非死信）立即死信并发布 `Failure` 到 `audit.events.dlq.v1`（`AUDIT_KAFKA_DLQ_TOPIC`），不可解析消息记日志死信——验证了 AsyncAPI topic 契约与 Kafka 真实容器链路（compose `redpanda`）。自 2026-08-15 起：空 `AUDIT_OUTBOX_TOKEN` 启动即警告；401 类重试耗尽死信为 `error_code="unauthorized"`（独立计数 `audit_consumer_unauthorized_total` 与告警），区别于 API 故障。
- 配置 `AUDIT_LEDGERED_BROKERS`/`-ledgered-brokers` 后，API 在 ledger 事务提交后发布链式事件到 `audit.events.ledgered.v1`；待发布事件同时写入快照内的 `ledgered_outbox`，发布失败不影响账本成功但会由 API 重启恢复和周期冲刷，broker 成功后先持久确认，再删除队列记录。消息仍按 event_id 采用至少一次语义，投影端必须幂等。发布互斥门按**待发布事件**（`store.EventKey`）而非按进程串行，且**从不被等待**：请求路径若发现该事件已被占用（通常是后台冲刷正在发布它）就让出并计数 `audit_ledgered_publish_deferred_total`（无标签、单调，原子读取，抓取 `/metrics` 不取锁），durable 记录仍在 `ledgered_outbox` 中，下一轮冲刷补齐——因此后台冲刷整轮（快照读取 + 最多 100 次发布 + 100 次确认）不再 head-of-line 阻塞同步 ingest 响应。后台健康轮次间隔为 1–2 秒（含 jitter）；失败指数退避且间隔上限 30 秒，成功轮次重置，取消可即时中断等待。启动及周期轮次会主动对账 committed cold receipt：每轮一个租户、最多 100 条账本记录，租户轮转并定期从零重扫；无需客户端重试即可补建丢失意图。身份/流/序号/哈希绑定的持久确认独立保存于 `publications/` 或 PostgreSQL 的 `audit_publication_confirmation`（开启发布前须应用迁移 `007_publication_confirmation.sql`，启动/readiness 缺表失败关闭），不把逐事件确认历史堆入共享 control。确认已落盘、Outbox 清理失败时，下轮只清理不再发消息；确认未落盘允许至少一次重发。对账读取最新回执，归档取证在锁外、共享单轮 deadline，补发前验证重建内容及链哈希。旧历史无确认时为未知，可能重发；扫描位置不是提交/投影水位，`indexed/archived` 也不证明 Kafka/ClickHouse 确认。v1 PostgreSQL 兼容路径仍物化旧快照，规模化部署须先完成 v2 cutover；HotFirst 未提交断点由独立的原始追加意图恢复处理，不能混同 committed-cold 对账；旧无 journal 残留仍为未知。详见 ADR-0011、ADR-0015。
- `audit-kafka-dlq-replay`：DLQ 重放消费者——DLQ 记录只含失败元数据，原事件按 key 从 `audit.events.accepted.v1` 恢复并逐字节重发（或 `-api-url`/`-token` 改为经审计 API 重新接入）；已重放 event_id 持久化到 `-state`（`AUDIT_DLQ_REPLAY_STATE`，默认 `./data/dlq-replay-state.json`）使重扫幂等，accepted 主题每轮从头扫描以保证新 DLQ 记录能找到更早的原消息；`-once` 供调度器单轮执行，或按 `-interval` 常驻；瞬态失败下轮重试，API 永久拒绝（4xx 除 429）标记重放完成避免死循环（需人工处理）；`-metrics-listen` 暴露 `audit_dlq_*` 指标。自 2026-08-15 起：`error_code="unauthorized"` 记录默认被 auth-blocked（不重放、不标记、不提交，计 `audit_dlq_auth_blocked_total` + 积压 gauge `audit_dlq_auth_blocked`，配 `AuditDLQAuthBlockedBacklog` 告警）——凭证修复后由运营显式加 `-replay-auth-blocked` 排空并移除（事故处置 runbook 见 release-notes）。`audit-kafka-consumer` 与重放器均可暴露文本指标端点（`AUDIT_KAFKA_METRICS`/`AUDIT_DLQ_REPLAY_METRICS`），Prometheus 规则 `deploy/prometheus-rules.verify.yml` 对 DLQ 流量、积压、认证阻塞和重放失败告警（两个新告警依赖对应 /metrics 已挂载）。
- DLQ 可变 state/lock 按 ADR-0017 的 descriptor-relative 边界拒绝祖先/叶 symlink、special 和多链接 inode；独占唯一 temp、actual fd fsync、renameat/cleanup 共用 pinned parent，未知 temp 原样保留。保留旧 v1/v2、legacy tenant 显式证明、flock 多进程 union、有界等待和 O(1) append；任何关闭/同步失败不更新成功内存 mark 或提交 offset，结果未知可幂等重试，不视作磁盘回滚。见 ADR-0018，旧全历史 load/conversion 的成本仍在。
- 外部基础设施接入（全部可选、本机容器可验证）：
  - `AUDIT_VAULT_ADDR` + `AUDIT_VAULT_TOKEN` + `AUDIT_VAULT_TRANSIT_KEY`：checkpoint 签名改用 Vault Transit 引擎（私钥不出 Vault，算法标记 `vault-transit:<key>`），未配置时默认 HMAC-SHA256；地址必须 `https://…`，或本机 loopback + 显式 `AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK=true`（非 loopback 明文 http 任何情况下启动/预检失败关闭；scheme 缺失、空 host、带路径/userinfo 的地址同样失败关闭且错误信息不回显凭据）；
  - `AUDIT_S3_ENDPOINT`/`AUDIT_S3_BUCKET`/`AUDIT_S3_ACCESS_KEY`/`AUDIT_S3_SECRET_KEY`：合规归档（事件/段清单/导出）写入 S3 兼容 Object Lock 桶（MinIO 验证：删除仅产生版本删除标记），默认本地只读目录；**自 2026-08-15（R-1）起归档就绪门禁强制要求桶默认留存为 COMPLIANCE 模式 + 正有效期（Days/Years > 0，如 365 天）**——Object Lock 已启用但无默认留存、或默认留存为 GOVERNANCE/零有效期的桶会失败关闭（`-check-config`/启动探针/`/readyz` 分别报 `no default retention`/`GOVERNANCE` 类错误并指明修复：`mc retention set --default compliance 365d <bucket>`）；已写对象的留存固定在写入时刻（继承当时的桶默认），不受后续配置变更影响；**每次写入还携带显式 COMPLIANCE 留存，时长由 `AUDIT_ARCHIVE_RETENTION_DAYS` 决定（正整数，如 365；上限 100 年；缺失/为零即配置错误，两个二进制 fail-closed）**——即使桶默认留存被移除/降级，写入窗口内的对象仍受保护，`StatusArchived` 回执与探测时机无关；`AUDIT_S3_USE_SSL=true` 启用 TLS，`https://` scheme 端点必须与之匹配否则失败关闭（绝不静默降级明文）；明文 http 仅允许既有本机/开发端点（`localhost`、loopback IP、`host.docker.internal`、`gateway.docker.internal`）以及验证栈精确端点 `minio:9000`（可带 `http://`，主机名不区分大小写），任意其他非 loopback 端点均失败关闭并提示设置 `AUDIT_S3_USE_SSL=true`，见 `docs/THREAT_MODEL.md` §3.8；
  - 两个二进制 `-check-config` 的 `check_config=ok` 逐腿报告 `transport_s3=`/`transport_vault=`（`tls`/`http`/`local`），格式串字节一致；
  - 两个二进制的 `-consistency-key` 是无网络、无归档探针的纯预部署模式，只输出非敏感的 `consistency_key=`；先运行 `python3 cli.py build`，再用相同的合并部署环境运行 `python3 cli.py consistency-check`（或 `python3 cli.py predeploy`）。该键是签名方案/Transit key 名称与归档桶或目录的**名称级一致性**信号，不替代完整 `-check-config`、传输门禁或密钥投递校验；HMAC 键的规范标识为 `HMAC-SHA256`，与运行时 `signer=hmac-sha256`/`HMAC-SHA256(dev-compatible)` 的展示文本差异是有意的；
- gRPC ingest 支持 `AUDIT_GRPC_TLS_CERT`/`AUDIT_GRPC_TLS_KEY` 与 `AUDIT_GRPC_TLS_CLIENT_CA` 原生 TLS/mTLS；Linux 启动/预检对三类 PEM 分别限制 4 MiB/regular inode/实际读取上限，捕获配置父目录作受信 root，允许 root 内 projected secret 相对 symlink，拒绝叶链接逃逸和阻塞 FIFO。父路径/挂载/所有权仍由运维授权，不作热加载或跨文件原子快照，非 Linux 失败关闭，见 ADR-0018；生产可设置 `AUDIT_GRPC_REQUIRE_MTLS=true`（或 `-grpc-require-mtls`），此时 gRPC 监听器无论是否回环或配置明文 allowlist 都必须验证客户端证书。`AUDIT_ALLOW_INSECURE_GRPC_LISTEN=true` 仅保留给隔离验证栈。乐观锁重试耗尽（`ErrSnapshotConflict`，请求未入账）在 gRPC 上返回可重试的 `Unavailable`、在 `WriteBatch` 上标记 `rejection_code="unavailable"`，与 HTTP 的 503 `snapshot_conflict` 同一语义分类；客户端消息固定为 `state snapshot changed concurrently; retry the request`，后端细节只进服务端日志。
- gRPC `WriteStream` 在首条消息读取前验证签名 claims 和写权限；actor roles/payload 不能扩权。客户端 `Send` 返回 nil 仅表示已缓冲，服务端提前拒绝时也可返回 `io.EOF`，均不是成功回执；仍须用 `Recv` 获取最终状态。已认证但无写权限时必须为 `PermissionDenied`、无成功回执及事件/receipt/Outbox 写入，见 ADR-0001 与 [API 客户端说明](api/README.md)。
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

文件热冷容量回归仍测真实文件的 retained state/archive 字节，保留原样本数和斜率阈值；
仅归档夹具准备按互斥 key 分为至多 16 个真实 FileStore writer，完整 Put/fsync、全部退出后，
用新 handle 逐对象 Get 校验原字节再测量，并与串行小样本对照。不替换为内存、虚拟大小
或吞吐指标，也不证明崩溃耐久性/生产容量。调度及读语义的内存夹具同样不能代替既有
真实文件重启、归档 I/O 与独立 PostgreSQL pools 回归；完整门禁和独立外验分别记录。
API drain 回归先经真实认证 gRPC 完成原事件的文件/归档提交并释放锁，再启动被测真实
二进制；它须对同一原请求认证并返回完全相同的 duplicate 回执，才建立空闲流并发送
SIGTERM。保留 1 秒测试 store/drain 配置、原退出上限、长寿命 JWT 与无空闲流对照，
不把新事件 fsync 准备耗时加入 drain SLA；新的在途提交/未知结果回归仍独立保留。
同理，跨进程锁夹具先准备真实域/目录，仍从实际 flock 和当前 holder PID 同步；gRPC
新文件域将相同三个真实管理 API 生成的目录/事实合入一次目标 Update，实际摄入仍用
原文件后端，注入故障夹具保留原 Save 序列，另做文件重启/逻辑等价对照。HTTP 归档
超时回归在实际摄入耐久、关闭准备服务并重开同一文件后才启用原 150ms 读预算。

摄入归档的 5 秒是每对象 Put 上限，不是整次 Ingest（含前后文件提交）的完成 SLA。
相应回归从真实 Put 进入到 context Done 计时，保留原容差、deadline、错误与回执断言；
真实第二次 ledger append 暂停的反例验证，提交变慢不能被算成 Put 变慢。归档失败仍须
耐久记录 indexed、不得伪报 archived；包总文件 I/O 成本与完整门禁仍须独立收敛。
发布门回归的后台 flush 由当前测试持有可取消 context，正常路径显式释放 broker 并等待
真实确认/退出，早退或 Fatal 的 cleanup 也取消并按原 3 秒护栏 join；不能让上一个夹具的
发布/后续文件写混入下一个测试。此生命周期修复与导出准入的 control-only 写放大/CAS
修复不提高原 5 秒导出等待、10 分钟包超时或生产 deadline，也不证明旧 running/累计
耗时的唯一根因已闭环。
新摄入的强停回归另在真实 hot 文件提交后的 cold append 边界暂停，确认提交已进入而
非仅把预检堵在 mutex 前；原锁夹具的一次真实全文件 Save 保留在准备期，并核对逻辑
证据/事实不变。仍用原 10 秒 store、200ms 在途观察、50ms drain 和 15 秒收敛护栏，
后者一次预算覆盖实际 detached handler 退出与回执观察；RPC EOF 不当作 handler 完成。
原新提交/全文件与 ledger fsync、无强停对照、两条唯一回执及同内容 duplicate 重试断言
保持，cleanup join 自有 handler/listener。此项不同于既有空闲流进程的 duplicate 准备，
不把任何 Sync 栈当作死锁或宿主唯一归因。

既有 Legal Hold 创建/释放显式只更新新鲜 control：实际 PG Save 冲突后重查租户/ID及
当前释放决定，不以旧整快照覆盖较新的导出终态、lease、阶段元数据、其他 Hold或策略；
文件 v2 不替换未变的 hot/cold 证据。已释放返回首个 actor/time，零 Save/零重复事实，
真正变更与原一次自审计仍同 CAS，原 operation deadline/有限重试不变。未知 Save 零
成功对象，不撤销实际耐久事实、不盲重发创建；当前 Hold/下载及 owner/阶段 fencing、
共享预算继续独立权威。此为可复现的控制写放大/并发覆盖修复，不增加对象版本 Hold、
生命周期或业务执行，也不证明旧累计超时/原 running 唯一根因闭环。v1及其他兼容 writer、
历史物化成本仍在，见 ADR-0034；完整自验、独立验收与安全发布分别记录。

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
