# API 契约与 gRPC 流客户端语义

线面契约分别位于 [OpenAPI](openapi/openapi.yaml)、[AsyncAPI](asyncapi/asyncapi.yaml)
和 [Proto](proto/audit.proto)。本说明不增加字段、方法、topic、scope 或权限。

## `Ingest.WriteStream` 的发送结果不是接入回执

- 服务端在首条消息读取前认证并检查 `audit:event:write`，随后每条消息仍重新验证。
  来源/租户绑定来自签名 claims 和服务端注册；actor roles、payload 不能扩权。
- 按 gRPC-Go [`ClientStream.SendMsg`](https://pkg.go.dev/google.golang.org/grpc#ClientStream)
  语义，`Send` 返回 nil 仅表示消息进入传输缓冲，不证明服务端已读取或入账。
  服务端提前结束流时，`Send` 可以返回 `io.EOF`；实际 RPC 状态仍须从 `Recv` 获取。
  不得把其他发送错误一概当成正常拒绝，也不得把 nil/EOF 当作成功回执。
- 已认证但没有写权限的流，最终必须是 `PermissionDenied`，没有成功回执，也不持久化
  事件、receipt 或 LedgeredOutbox；权限拒绝发生在应用读取消息之前。真实流回归覆盖
  服务端拒绝先于 Send、Send 先缓冲及二者并发的顺序，均保持同一授权结论。

这是既有传输语义澄清，不是放宽认证、JWT 有效期、超时或客户端自动重试授权。
回执的 ledger/broker/投影边界仍以根 [README](../README.md) 为准；决策见
[ADR-0001](../docs/adr/ADR-0001.md)，生成代码同步规则见
[契约同步](../docs/CONTRACT_SYNC.md)。

## 准备接入、结果未知与退出

`Unavailable` 的固定存储超时分类不证明未入账；在已有授权与重试策略内核对/幂等
重试时，必须复用原 event_id、idempotency_key 和完整原始内容（包括 occurred_at），
不能重新生成时间戳或换事件身份。实际回执仍是结果依据，`Send` 的 nil/EOF 都不是。
这不是自动重试所有 Unavailable、权限失败或外部业务写入的授权。

进程持有文件锁不等于服务就绪。正常 governance worker 在取得锁前注册终止信号，
启动归档 Ready 继承取消及原 5 秒上限；取消启动不进入治理 pass，关闭存储释放锁。
API 既有有界 gRPC drain、接入幂等、权限与生产超时不变；无新增协议字段或方法。
文件 Store.Close 与原 trail mutex 串行，等待在途读自审计追加退出后先关闭持久 trail fd、
再释放 flock；两条关闭错误均保留、descriptor仅消费一次，重复关闭返回原结果。关闭后
trail 读/追加失败关闭，不让旧 writer 在锁释放后继续追加或重新打开，也不能借此输出
回执等要求自审计的结果。已耐久事实不撤销，不重写证据/未知temp或删除WORM；调用方
仍须先停止并join服务工作，这不是通用Store全方法关闭fencing或新shutdown协议。常用
文件夹具显式cleanup且保留原真实I/O/断言/超时；不是旧累计超时/running唯一根因证明。
见[ADR-0036](../docs/adr/ADR-0036.md)，OpenAPI/AsyncAPI/Proto无线面变更。

导出本地活跃 reservation 另保留至原最后一次有界 control 读取结束（在执行及心跳
stop/join之后），再同一 scheduler mutex 内释放并登记原有限后继 wake；终态可见或
心跳停止都不是完整执行器退出。Store读取仍锁外、次数与原deadline不变，失败/取消/
仍运行不热重试，原共享预算、lease/阶段fencing、当前Hold和下载自审计保持。已停止
队列/外部waker后才能以完整idle观察关闭资源；不是新增通用关闭/远端强停协议或业务
执行回执。原文件顺序回归保留全部真实I/O/断言及5秒终态等待，cleanup另核对完整退出。
见[ADR-0037](../docs/adr/ADR-0037.md)；无线面/格式变更，不证明旧累计超时/running唯一根因。

工程回归的内存调度/认证夹具只隔离无关准备 I/O，不证明文件耐久性或独立 PG 一致性。
文件容量夹具仍逐个真实 Put/fsync、以新 handle Get 原字节并测相同 retained 文件大小，
仅以有界互斥 key 分区准备；不能据此推导接入吞吐、放宽 deadline 或更改上述回执语义。
文件重启/实际 I/O 与独立 PG pools 回归、完整 quality 和后续独立验收仍分别要求。
API drain 进程回归先通过真实认证 gRPC 完成同一原事件的文件/归档准备并关闭原 writer，
被测真实二进制仍须认证原请求、返回精确同 hash/stream/status 的 duplicate 回执，
才保留原长寿命空闲流并测 SIGTERM；1 秒测试 store/drain 和原退出上限不变。
这不替代新摄入的在途提交/未知结果回归，也不改变生产 timeout、认证或幂等契约。
跨进程锁仍以实际 flock/当前 holder PID 同步，准备的真实控制域不作就绪信号。gRPC
文件夹具仅批合原管理 API 产生的初始化目录/三条事实，后续仍逐次真实文件摄入并
验证重启；故障/CAS 夹具原 Save 序列不变。HTTP 归档超时回归则在真实摄入完成并
重开同一文件后启用原 150ms 读预算，不把准备 fsync 计为源 Get 超时。
新摄入强停测试另在实际 hot 文件提交后的 cold append 处同步，不把 mutex 前的预检
等待冒充“提交已进入”。旧锁夹具的一次真实全文件 Save 仍在准备期完成并核对逻辑
等价；实际摄入 fsync、无强停对照和精确唯一回执/同内容 duplicate 断言不减。原
10 秒 store、200ms 在途观察、50ms drain 与 15 秒收敛上限保持，最后一次预算覆盖
真实 detached handler 退出和只读观察，EOF 不是 handler 完成。自有 handler/listener
退出须 join，不修改生产超时、认证或扩大结果未知时的外部重试授权。

文件原子写与 ledger append 复用一次实际捕获父目录 fsync，再同步全部原需祖先，
不重复同步同一父目录；仍复核安全父路径/inode，保留文件 fsync、未知 temp、rename
后同步失败的结果未知语义。测试 hook 不能替代实际父目录同步。此成本修复不改变
回执、导出阶段、权限、租约/Hold/自审计、预算或任何 deadline，也不证明原 running
失败唯一根因/生产容量已解决；完整命令与独立验收仍分开记录，无新增线面字段。
摄入归档原 5 秒上限约束的是每对象 Put，不是包括前后文件提交的整次 Ingest SLA。
测试在真实 Put 进入/context Done 边界计量，保留原容差、精确 deadline、错误和未归档
回执断言；实际第二次 ledger append 暂停/文件重启验证最终仍仅 indexed。未放宽
生产存储/归档超时或导出预算，未将未知提交当回滚，也不解决累计包耗时风险。

## 导出 selected / generating / sealed 阶段

生成前的私有固定成员计划绑定原账本坐标、发生时间及精确 Schema 定义，不受查询
cursor TTL 或迟到成员影响。新选择超过 100 成员使用 v2 计划，每页最多 100 成员；
真实 JSONL 页密封/Put 后，页引用与 export.progressed 同 CAS 提交。接管认证复用已
提交页，只生成剩余页；坏/缺页保留原计划，不重新生成已提交页。每页当前认证 Hold
（含迟到事实）、读自审计、锁外 I/O 与 Save 共享一次既有 deadline；Put 前/进度 CAS
复核 owner、原计划/Schema 与 Hold 集，沿用共享预算。原 lease 必须到期才可换主，
保留 started_at，终态不回退。v1 历史计划和新单页任务仍整块生成。

所有页提交后仍合并/整体密封为现有最终对象，再走 sealed 终态提交和独立下载门禁。
不暴露计划/页引用/owner/部分输出；不是恒定内存、流式大文件下载或业务执行。
未知/迟到 Put 可留无引用不可变对象，新 owner 只为未提交页使用不同 attempt key，
不能猜旧 key、删除 WORM 或宣称一次 Put。retry 是新任务、新选择，不继承计划/页。
详见 [ADR-0023](../docs/adr/ADR-0023.md)、[ADR-0024](../docs/adr/ADR-0024.md) 和
OpenAPI；没有新路径、公开字段、scope、topic 或 Proto 方法，AsyncAPI/Proto 不变。

## 导出创建与新后继的当前 Hold 预检

创建导出/创建重试后继在原 QueryEvents 和失败关闭读自审计后，捕获独立深快照，复用
receipt 锚定、精确 Schema/重建内容/链认证，先认证再评估原查询与 Hold。敏感条件从
认证原值按自身 bound/legacy 格式推导，不信任派生 sidecar；本租户 receipt key/body
错绑、坏/缺证据失败关闭，不藏为空选择。无候选/时间不相交不增加额外 Hold Get，
原查询自身仍可能取证；零匹配可创建，最低匹配 Hold ID/半开窗与租户隔离保持。

新任务显式走现有 control-only CAS，每次实际 Save 冲突后在新鲜 control 上重跑原 Hold
绑定/共享队列检查与创建事实，不能把先前物化的整快照写回覆盖当前取消、租约、Hold、
预算或策略，也不替换未变的租户 hot/cold 文件。独立 Hold 拒绝事实同样只更新新鲜
control。新任务同一次共享 control CAS 复核请求内捕获的本租户活跃 Hold 集 HMAC；变化返回
409、零新任务/创建事实，须显式刷新重试，不隐式重选或重启 deadline。原查询/读事实、
锁外 Hold Get/认证、拒绝事实或新任务 Save 共享该 create/retry 的一次既有预算。无关
策略/其他租户 Hold 不被旧快照覆盖或制造冲突；原 128/32、2/1 预算与后续 lease/阶段
fencing、当前 Hold/下载自审计均不变。既有后继的重复命令仍仅返 metadata-only 摘要，
不新建/重新读证据；原终态不回退，未知 Save 不能当回滚或自动重发创建的依据。

HTTP 创建和既有 retry 均在认证前设置 no-store，覆盖所有响应；403 仍为 forbidden /
reason=legal_hold / 租户内 hold_id，内部错误固定脱敏。筛选/摘要仅走既有 POST body，
不新增 URL、字段、scope、持久模型/迁移或密封格式，AsyncAPI/Proto 不变。原 QueryEvents
初页当时未在该 Hold 切片重写，后续认证演进见下文 ADR-0030；历史物化/扫描与最终整块导出仍在，不是原子证据/业务版本
计划、下载授权或业务执行。见 [ADR-0029](../docs/adr/ADR-0029.md)，M3仍Partial。

## 既有 Legal Hold 创建与释放的控制提交

两条既有写操作只合入新鲜 control，不替换 v2 hot/cold 证据、不请求 WORM。实际 Save
冲突后重新检查创建 tenant/ID 或当前释放记录，保留并发导出终态、lease、阶段、其他
Hold和策略；不能用旧整快照恢复 running 权限。变更与原一次 Hold 事实同 CAS，已释放
返回首个 actor/time、零 Save/零重复事实。原权限/租户与404/409、一次operation预算/
有限退避保持；Save失败/未知返回零成功对象，已耐久事实不撤销，未知创建先只读核对，
不新增创建幂等或自动重发授权。当前导出认证/Hold/下载自审计及owner/阶段fencing和
共享预算不变，无对象版本保全、生命周期/业务执行或COMPLIANCE变更。v1及其余兼容
writer成本仍在；见 [ADR-0034](../docs/adr/ADR-0034.md)。无线面/SQL/密封格式变更，
AsyncAPI/Proto不变；局部减少写入不是历史累计超时/running唯一根因或容量保证。

## 既有恢复审批与拒绝的控制提交

`POST /restores/{runId}/approve` 和 `/reject` 只更新新鲜 control。实际 Save 冲突后重新
核对当前 tenant/记录、pending 状态及请求者/决策者分离；保留404→409→403优先级，
首个耐久决定 actor/time 不被覆盖，重复决定仍409、零Save/零重复事实（不是幂等成功）。
决策与原一次事实同CAS，不替换v2 hot/cold证据，不覆盖并发导出终态、lease、计划/页/
检查点、当前Hold或策略，不Get/Put WORM、不执行任何业务调用。决策仍使用一次原
operation预算/有限重试；失败或未知Save返回零成功对象，不撤销已耐久决定/事实。
结果未知先只读核对，不新增自动重发授权。原权限、租户选择及线面错误分类不变。

上述决策切片当时不改CreateRestore/GetRestore；后续创建提交见下文ADR-0038、详情
读取见ADR-0043，空actor读取策略与创建的分离预算保持。此决策切片
不提供原子证据/业务版本计划、正式scopes或执行回执。无线面/SQL/密封格式变更，
AsyncAPI/Proto不变。局部减少写入不证明旧累计超时/running唯一根因或生产容量，
M3仍Partial，M5未交付；见[ADR-0035](../docs/adr/ADR-0035.md)。

## 既有恢复申请创建的控制提交

`POST /restores` 的原认证回放后，只将本次原新pending_approval请求和一次restore.created
合入新鲜control；真实Save冲突后不保存旧物化快照，不覆盖并发恢复决定、导出取消/
终态、lease续期、selected/生成页/sealed阶段、当前Hold或策略，也不替换v2 hot/cold。
原ID/申请人/创建时间在调用内一次生成，提交冲突不重新回放或创建第二条记录。

原字段验证、tenant/权限、错误与空actor零读事实策略不变；前置回放仍认证committed
receipt/精确Schema/原内容和链，保留10000匹配上限。回放与提交仍是原独立操作预算，
提交重试只复用其一次原deadline/有限退避；不宣称整个创建是单预算或原子证据/业务版本
计划。提交本身不Get/Put WORM或调用业务API，审批仍不是执行。失败/未知Save零成功
对象，不撤销已耐久请求/事实、不授权自动重新创建；先只读核对未知结果。

无新线面字段/路径/scope、SQL/持久/密封格式，AsyncAPI/Proto不变；不新增请求级幂等、
正式恢复scopes或业务执行回执。详情读取后续见ADR-0043，通用Store.Update/v1及原认证扫描成本仍在，
M3仍Partial；局部写放大/控制覆盖修复不是旧累计超时/running唯一根因或容量保证。
见[ADR-0038](../docs/adr/ADR-0038.md)，完整自验与随后独立验收分别留证。

## 既有恢复详情的控制面读取

`GET /restores/{runId}` 仅观察当前 control 中的申请/决定元数据。v2 使用 ReadControl，
不物化无关 hot/cold 历史，不查询证据表或请求 WORM；不 Save、不追加事实，原恢复
元数据静默读取策略保持。一次原 operation deadline 覆盖读取与捕获后取消检查；Load
失败、超时或取消返回零对象，不输出先前捕获值，不重试 Load 或恢复写操作。每次请求
重新读取当前 control，返回副本含独占时间指针；原租户隔离/404、状态/时间/字段不重解释。

无新字段/路径/scope、SQL/格式/迁移，原HTTP授权/错误包及AsyncAPI/Proto不变。本切片
不改变HTTP缓存策略，不宣称恢复详情已完成全部M1边界。v1仍物化旧快照，共享control
及其余历史成本仍在；这不是证据认证、完整性、持久版本计划、业务执行回执或自动重发
授权，不改变回放/必要读自审计或当前Hold/lease/阶段fencing/共享预算。局部物化/锁等待
修复不证明旧累计超时/running唯一根因或容量，M3仍Partial；见
[ADR-0043](../docs/adr/ADR-0043.md)，完整自验与独立验收另留证，安全扫描非零阻塞发布。

## 既有留存配置的控制提交

`PUT /policies/retention` 只将原配置与一次 retention_policy.set 合入新鲜 control；实际
Save 冲突后重查当前租户，缺失仍404/零提交，不覆盖并发导出取消/终态、lease续期、
selected/页/sealed阶段、恢复决定、当前Hold或另一租户策略。v2不替换hot/cold，配置
本身不Get/Put WORM或增加独立读事实。原校验/权限/错误/事实编码、一次operation
预算与有限退避保持；同值再次设置仍各保存一次事实，不新增no-op、策略版本或请求
幂等。失败/未知Save返回错误，已耐久配置/事实不撤销，须只读核对、不盲重发。

这只是既有配置写，不执行生命周期/业务调用、不释放Hold或缩短对象COMPLIANCE。
配置读取后续另见ADR-0044；评估、通用Store.Update/v1和历史成本未整体改造；无线面
字段/路径/scope、SQL/持久/密封格式变更，AsyncAPI/Proto不变。不是旧累计超时/running唯一根因或容量保证，
M3仍Partial，M4/M5未交付；见[ADR-0039](../docs/adr/ADR-0039.md)，完整自验与随后
独立验收分别留证。

## 既有留存配置的控制面读取

`GET /policies/retention`只观察当前control中的配置，v2不物化无关hot/cold、不查询
证据表或Get/Put WORM。读取与捕获后取消检查共享一次原operation deadline；Load
失败、超时或取消零配置，不缓存或重试Load。原tenant key、缺配置404与空scope拒绝、
字段/天数/类别及标量返回独占保持，不新增当前tenant注册检查或默认配置。不Save/
追加事实，保留配置元数据静默读策略；原HTTP权限/租户选择/错误包/缓存行为不变。

EvaluateRetention仍独立读取证据/当前Hold、保持原分离预算与评估语义；不是把它改为
纯控制面统计，也不是可删除、完整性、COMPLIANCE或业务执行证明。必要事件读事实、
当前Hold、lease/阶段fencing、终态不回退与共享预算不变。无新字段/路径/scope、SQL/
迁移/格式，AsyncAPI/Proto不变；v1、共享control及其余历史成本仍在，M3仍Partial。
局部物化/锁依赖修复不证明旧累计超时/running唯一根因、容量或长期稳定性；见
[ADR-0044](../docs/adr/ADR-0044.md)。完整自验/独立验收另留证，安全非零阻塞发布。

## 既有 Schema 注册的控制提交

`POST /schemas` 只将原新定义与一次 schema.created 合入新鲜control；每次真实Save
冲突后重判当前租户、同租户/Schema最高版本、重复key与最近前序的原required/allowed
兼容规则，不能覆盖并发定义或按旧前序放行。原缺租户404→回退400→重复409→兼容
拒绝400顺序、前向跳版本、字段/key framing/资源上限与时间/Active默认处理保持。
合法历史精确版本仍用于摄入/认证，不变为只允许最新版本。

v2不替换hot/cold，不写旧快照覆盖当前导出取消/终态、lease续期、selected/页/sealed、
恢复决定、Hold或策略；注册本身零WORM Get/Put、零独立读事实。原一次deadline/有限
冲突重试不变，失败/未知Save报错、不撤销已耐久定义/事实，先只读核对、不盲重发；
重复注册仍冲突，不新增请求幂等。当前Hold/必要读自审计、共享预算与owner/阶段fencing
继续独立权威，无业务调用、重签名或留存变更。

无线面字段/路径/scope、SQL/持久/密封格式变更，AsyncAPI/Proto不变。ListSchemas、
通用Store.Update/v1及历史物化成本仍在；不是旧累计超时/running唯一根因或容量保证，
M3仍Partial，M4/M5未交付。见[ADR-0040](../docs/adr/ADR-0040.md)，完整自验与随后
独立工程/安全验收分别记录，扫描非零仍阻塞发布。

## 既有来源更新的控制提交

`PUT /sources/{sourceId}` 只把原来源替换和一次 source.updated 合入新鲜 control。每次
实际 Save 冲突后重查当前 tenant/source，缺失仍404/零成功保存，CreatedAt 取当前
存量记录；不把旧整快照写回覆盖并发导出取消/终态、续期、selected/页/sealed、恢复
决定、当前 Hold、策略或邻居来源。v2不替换hot/cold，更新本身零WORM I/O/独立读事实。

原字段/key framing/资源上限、allow-list精确值校验和排序、空列表仅同ID客户端、
Active=false停用及signed client/tenant解析不变。同值再次更新仍各一次原事实，不
新增条件更新或请求幂等。原一次deadline/有限重试保持；失败/未知Save返回错误与
零成功对象，不撤销已耐久配置/事实，先只读核对、不盲重发。HTTP原本即在失败时仅
返回错误包，原权限、scope、错误分类及成功响应不变，不增加身份发行/业务执行能力。

CreateTenant、通用Update/v1与历史物化成本仍在，无新字段/路径/scope、SQL/持久/密封
格式，AsyncAPI/Proto不变。局部成本/真实控制覆盖修复不是旧累计超时/running唯一
根因或容量保证，M3仍Partial；见[ADR-0041](../docs/adr/ADR-0041.md)。完整自验、随后
独立验收与安全发布分别记录，扫描非零仍阻塞发布。

## 既有来源创建的控制提交

`POST /sources` 只将原新来源与一次 source.created 合入新鲜 control；实际Save冲突后
重查当前tenant与同tenant/source key唯一性，保持缺租户404优先于重复409，首个注册
及其事实不被覆盖或被败方重新启用。v2不替换hot/cold，不把旧物化快照覆盖当前导出
取消/终态、健康续期、selected/页/sealed、恢复决定、当前Hold、策略或邻居来源。
创建本身零WORM I/O/独立读事实，当前Hold、必要读自审计、共享预算与fencing不变。

原字段/key framing/资源上限与allow-list精确校验/排序保持，空列表仅同ID客户端；
创建Active=false仍默认true，停用仍走UpdateSource。缺省CreatedAt在提交前生成一次，
提供的时间保持；signed client/tenant解析及跨租户歧义失败关闭不变，不新增全局注册
唯一性或身份发行。原一次deadline/有限重试保持，失败/未知Save返回错误，不撤销
已耐久注册/事实，先只读核对、不盲重发。重复仍409，不新增请求幂等或线面字段。

CreateTenant/通用Update/v1及历史物化成本仍在，无新路径/scope、SQL/持久/密封格式，
AsyncAPI/Proto不变。局部写放大/真实CAS修复不是旧累计超时/running唯一根因、容量或
长期稳定性证明，M3仍Partial；见[ADR-0042](../docs/adr/ADR-0042.md)。完整自验、随后
独立验收与安全发布分别记录，扫描非零仍阻塞发布。

## 普通固定成员查询的 reference 初次选择

`GET /events` 与 `POST /events:query` 的 ledger-reference 初次选择复用 Console 的
receipt 锚定认证扫描：独立深快照后锁外核对 map/正文身份、精确 Schema 身份/版本、
重建内容与链，再评估所有筛选。坏/缺 committed 证据不能隐藏为空匹配或部分页；
hot-only/journal 不是事实，其他租户不取证，停用历史精确 Schema 仍可读。

敏感条件从认证原值按条件自身 bound/legacy 格式推导，不信任 sidecar 的存在/值/格式；
新 reference 初次查询的加密 searchable 字段因而也支持双格式互查，保持精确数字与
bound tenant/field 隔离，不把内部重建明文替换进响应保护字段。旧 helper 的同格式
限制不是新查询的授权依据；HTTP 仍剥离 sidecar，敏感条件只走已有 POST body。

保留原 10000 匹配身份/4 MiB/tenant16/15 分钟及 HMAC scope/source/subject/filter/
page-size 绑定，不给 Console 新增此上限。旧保存成员集不重选，续页只读本页坐标，
迟到事实只进入新查询；ClickHouse 候选不新增 fallback，敏感筛选仍强制 ledger。
捕获、Get/认证/选择、成员 Save 与一次失败关闭读事实共享原 deadline，追加后取消
也零结果。未知已提交事实/运行元数据不当回滚或字节交付证明；不写旧快照覆盖并发
终态/lease/Hold/control，导出后续当前 Hold/CAS/fencing/共享预算仍独立权威。

原租户/权限/no-store/DTO/错误不变，无新路径、scope、字段、持久模型、SQL/密封
格式；AsyncAPI/Proto 不变。历史物化/认证扫描、旧兼容路径和最终整块导出仍在，
不是恒定内存、容量/完整性证明或业务执行。见 [ADR-0030](../docs/adr/ADR-0030.md)，
M3仍Partial，完整门禁与随后独立验收分别留证。

## 单事件与固定成员/投影输出页的按坐标取证

单事件、已保存固定成员续页及投影实际输出页先核对请求 tenant/event 与 receipt 正文
身份，不能只信 lookup key 或另一有效内容/链；错绑在源 Get 前返回固定 conflict、零
读成功事实/输出。随后在锁外按精确 Schema key 核对定义正文 tenant/id/version，再认证
内容/链；缺失/错绑不取最新版本替代，停用历史定义仍可读，保护字段不输出重建明文。

原固定成员/count/游标/来源/TTL/预算不变，续页只取本页坐标，不重选或重扫已服务前缀；
投影故障不 fallback。捕获/Get/认证/一次读事实共享原 deadline，单事件追加成功后也
复核 context，取消/超时零事件；耐久但未知/随后取消的尝试事实不撤销，不证明交付。
使用自有捕获目录，不覆盖并发终态/lease/Hold/control；后续请求重新观察目录变化。

HTTP canonical/compat 详情仍是 scoped read，平台须显式 tenant、签名 claim 权威，不
获得 Console all-tenant 例外；原 no-store、固定安全错误与递归摘要剥离保持。无新字段、
路径/scope、SQL/持久模型/密封格式，AsyncAPI/Proto 不变。跨租户单事件helper的锁外归档认证另见
[ADR-0045](../docs/adr/ADR-0045.md)；其余历史成本/完整语义矩阵与容量仍未完成，M3仍Partial；
不重交导出阶段或业务执行。见[ADR-0032](../docs/adr/ADR-0032.md)，完整工程与随后独立验收分别留证。

## 回执元数据读取

`GET /events/{eventId}/receipt` 核对可信 tenant/event 与 committed receipt 正文身份，
错绑固定409、零回执/成功读事实；底层读取失败不冒充不存在或成功。v2 使用现有租户
精确冷索引，旧v1仍物化快照，hot/control克隆成本仍在。读取至原一次失败关闭读事实
共享原deadline，捕获后/追加后取消均零输出，耐久尝试事实不撤销、不证明字节交付。

全部HTTP响应no-store，原read权限、签名tenant和平台显式选择保持，无all-tenant
详情例外。原元数据/状态不升级、不猜测；不Get源对象、不依赖Schema，也不声称正文/
全链认证、broker/ClickHouse确认、当前归档可用、完整性、业务执行或下载授权。不保存
旧捕获覆盖并发control，无新线面字段/迁移，AsyncAPI/Proto及导出门禁不变。详见
[ADR-0033](../docs/adr/ADR-0033.md)，完整语义矩阵/M3仍Partial，独立验收另留证。

## 分页操作/聚合时间线

两个分页服务入口在独立深快照捕获后锁外认证 committed receipt、精确 Schema 身份/
版本、重建内容与链，再评估 operation/aggregate 条件。捕获、源 Get/认证、分页和一次
失败关闭读自审计共享该服务调用的一次原 deadline；坏/缺证据、取消或追加失败不返回
部分 items/count/cursor，不以最新 Schema 替代缺失精确版本。并发 control 写不被旧
快照覆盖，HTTP 的租户/权限与搜索摘要剥离不变。

仍是 live 分页：count 是本次总匹配数，下一页可因迟到/新提交而变化，不是固定成员集
或完整性证明。全历史物化/认证扫描与兼容 helper 成本仍在；操作摘要及旧全量服务方法的
锁外认证分别见下文。不新增路径、scope、DTO、持久元数据或迁移，不改导出 Hold/fencing/预算。见
[ADR-0025](../docs/adr/ADR-0025.md) 与 OpenAPI；AsyncAPI/Proto 不变。

## Console 事件页与 facets

两个既有兼容查询在独立深快照捕获后，锁外认证 committed receipt/map 身份、精确
Schema/重建内容和链，再进行任何筛选、排序或聚合；坏/缺证据不成为正常空结果或
部分统计。捕获、Get/认证、分页/聚合与一次失败关闭读自审计共享该服务调用的一次
原 deadline，不写回旧快照覆盖并发终态/control。敏感条件只在既有服务层从认证原值
按条件自身 bound/legacy 格式推导，不信任派生 sidecar，不新增敏感条件 URL；响应
保留保护格式，事件递归剥离搜索摘要。HTTP 成功与全部错误 no-store，内部诊断脱敏。

既有 tenant claim 与批准的 all-tenant Console 例外不变，不能用于其他 scoped API。
Console 仍为 newest-first live 页，**count 保留 cursor 后匹配数**（首页全部）；不同
于普通固定成员事件查询及分页时间线总 count。facets 忽略分页输入、统计全部筛选
匹配，providers 无可信对应字段而保持空。空查询仍为空数组/空 map，不是 404。
历史物化/认证扫描、ClickHouse facets、完整性及旧读路径成本仍未整体改造，无新字段、
路径/scope、持久模型或迁移；见 [ADR-0026](../docs/adr/ADR-0026.md)，AsyncAPI/Proto 不变。

## 操作摘要

`GET /api/v1/operations/{operationId}` 复用分页时间线的 receipt 锚定、精确 Schema 身份/
版本、重建内容与链认证源，在独立深快照捕获后锁外取证。认证先于 operation 条件和
计数/最早最晚时间/排序去重 outcomes 聚合；缺失或损坏的 committed 证据拒绝整个
摘要，不能藏为正常 404 或部分统计，hot-only/journal 不是事实。捕获、Get/认证、聚合
和一次失败关闭读自审计共享该服务调用的一次原 deadline；取消/追加失败无部分结果，
已提交但结果未知的尝试事实不撤销，也不是字节交付证明。不写回旧快照覆盖并发 control。

原 tenant claim、平台显式选择和 audit:operation:read 权限不变；HTTP 全部响应 no-store。
保留 live 匹配数、最早/最晚发生时间、排序去重 outcomes；新调用可见迟到/新提交，不是
固定成员集或全历史完整性证明。不新增旧全量时间线的 10000 条截断/拒绝上限；原有
服务操作 deadline 仍权威，这不是生产容量保证。全历史物化/认证扫描和 restore 整体
成本仍未改造，旧全量服务方法的后续认证读取见 ADR-0031，审批仍不是执行。见 [ADR-0027](../docs/adr/ADR-0027.md)；
无新字段、路径/scope、持久模型或迁移，AsyncAPI/Proto 以及导出 Hold/fencing/预算不变。

## 操作回放

`GET /api/v1/operations/{operationId}/replay` 从独立深快照的 committed receipts 锚定，
在锁外认证 map/正文身份、精确 Schema 身份/版本、重建内容（包括 changed_fields）与
链 hash 后才选择、排序和本地折叠；坏/缺证据不能过滤隐藏或产生部分 state/count/
sequence。捕获至原一次失败关闭读自审计共享该 replay 服务调用的原 deadline，不写回
旧快照覆盖并发 control。HTTP 在认证前设置 no-store，原租户/权限和固定错误保持。

保留最多10000匹配事件、occurred_at/sequence/event_id升序、after=null删除键；
last_sequence 是最后折叠事件的序号，不是最大值、水位或业务执行回执。仍为 live
观察，不是固定成员、完整性证明或容量保证。内部空 actor 继续零读事实；已有恢复
预演/创建委托继承认证源，但其原权限、独立创建提交和审批不是执行的语义不变。坏
源不生成预演/新 pending run；不把整个创建调用说成一次预算或原子证据/业务版本计划。

旧全量服务方法的后续认证读取见下文；恢复整体流程及全历史物化/扫描成本仍未整体
改造，M3仍Partial。见 [ADR-0028](../docs/adr/ADR-0028.md)；无新公开字段、路径/scope、持久
模型或迁移，AsyncAPI/Proto、导出阶段/当前Hold/自审计/fencing/共享预算均不变。

## 旧全量时间线与服务层聚合回放

`OperationTimeline` / `AggregateTimeline` / `ReplayAggregate` 三个旧服务方法现在复用
receipt/map、精确 Schema 身份/版本、重建内容与链认证源，独立深快照后锁外认证，
再选择/排序/本地折叠；坏/缺 committed 证据不能过滤隐藏或输出部分结果。捕获至一次
原读自审计共享原 deadline，追加后取消也零结果；未知尝试事实不撤销，不证明交付。
原 target/detail 与空 actor 零事实策略保持，不回写旧快照覆盖并发终态/lease/Hold/control。

保留10000个匹配事件的拒绝上限、不截断；operation 按发生时间/序号/ID，aggregate
按版本/ID 升序。聚合回放 after=null 删除，last_sequence 是最后折叠事件而非最大值。
返回独占副本、保护 payload 不解密替换；仍 live 观察与全历史物化/认证扫描，不是固定
成员、完整性或容量保证。HTTP 时间线仍调用分页方法；ReplayAggregate 当前无生产调用
或 HTTP 路由，不能把此服务加固说成新增 API 或业务执行。见
[ADR-0031](../docs/adr/ADR-0031.md)。无新路径/scope/DTO/持久格式/迁移，AsyncAPI/Proto、
导出阶段/当前Hold/自审计/fencing/共享预算均不变，M3仍Partial；独立工程验收另行留证。
