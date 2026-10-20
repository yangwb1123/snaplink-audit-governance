# 审计治理产品化实施清单

状态：实施中。此文件追踪规划，不替代 README、ADR 和 api/** 的现行规范。

## 顺序与完成定义

每批先落实最小可用闭环，再扩展容量；只有代码、契约、回归测试和完整门禁均通过才勾选。
不把原有热冷分层、冷回执索引、mTLS、幂等、CAS 领取或 DLQ 处理重新算作新增交付。
任何跨仓、外部设施、业务执行或正式合规验收尚未验证的内容必须保持未完成。

## 本窗口第一优先：稳定性复核（20261018T053223Z/0001 已独立工程验收；整体仍 Partial）

- 保留旧 0014 自验 quality=1、独立 quality=1（普通测试通过、service uncached race 累计 10m 超时），以及同源码 final-verification quality=0 的两类真实结果。失败计数继承 1，不能用新窗口或最后一次绿清空历史；瞬时 fsync 栈不是死锁或宿主磁盘唯一归因。
- 本轮原样全 service race/JSON 诊断通过，只是基线观察，不是稳定性闭环或全量 quality 替代。新增确定性回归复现导出创建/拒绝重写未变文件，以及真实 PG Save 冲突后旧整快照覆盖当前 Hold、队列或取消/租约/策略；改用现有 control-only CAS，保留原查询/预算/事实和阶段 fencing。
- 发布夹具正常路径显式释放/等待 flush，早退 cleanup 取消并按原护栏 join，避免后台发布/文件写跨越测试边界。修复前失败与后三轮专项/PG race 分开保留在本轮 component；不是重交启动、域初始化或 fsync 补丁。
- 首次完整自验 quality=1：普通 service 包通过，但 cmd/audit-api 强停新摄入测试的后续读取耗尽原 10s 存储预算，race/build 尚未执行，失败原样保留。新增实际 cold-append 观察先确定性复现旧 mutex 门只堵预检；随后把该门移至真实 hot 提交后的 cold append，原门的一次全文件 Save 保留在准备期并补三文件/逻辑等价对照。200ms 在途观察、50ms drain、10s store、15s 收敛和全部唯一回执/同内容 retry 断言不变；实际 handler 与读取共享原收敛护栏、cleanup join。此为新暴露的在途提交边界修复，不重交既有空闲流/启动/fixture/fsync补丁，修后专项三轮 race 通过，仍须修后完整门禁。
- 0001 独立 verification.json 已于 2026-10-18 07:11:59 UTC 完成：quality=0（736.460s，service uncached race=393.001s）、Web/diff=0、security=1，验收期间源码不变；0002开工858文件/HEAD/暂存内容与其source-after逐一相同，零未知源码变更。该次真实通过不抹掉首次自验失败、旧0014两次超时或其同源码final-verification通过，不重复交付原修复。
- 完整 quality、Web、安全扫描与 controller 独立验收分别留证；未据此扩产品范围。仍未确定性复现旧导出 running 现象或证明累计耗时所有根因消失，M1/M2/M3 总项保持 Partial，安全发布保持阻塞，M4/M5 不扩展。

### 0002：既有 Hold 控制写的有界稳定性修复（ADR-0034，已独立工程验收；安全发布阻塞）

- 只修复CreateLegalHold/ReleaseLegalHold的整快照写放大与实际PG Save冲突后旧control覆盖，不交付新Hold/生命周期/业务协议。修前真实文件回归替换未变的四个hot/cold文件，实际独立PG pools的六种Save冲突三轮均失败，包含原取消/lease被复活及首个释放决定被覆盖；失败日志原样保留。
- 创建显式UpdateControl，实际冲突后重查租户/ID；释放UpdateControlChecked，实际冲突后重查当前scope/释放决定，首个actor/time保持，已释放零Save/零重复事实。原变更与一次事实同CAS、一次原预算/有限重试不变，未知Save不返回成功对象、不推断回滚，不替换v2证据文件、不读写WORM。
- 回归保留原真实文件/归档、未知temp、字节/版本/inode与重启、实际Save未知/失败/取消/deadline及原预算；真实PG行锁/pg_blocking_pids同步Save，核对并发终态/lease/Hold/策略与晚owner零发布。新增与相关服务/HTTP及回执专项三轮race、真实PG三轮分别留证；不以专项代替完整门禁。ADR-0033只校准0001已独立验收的已有有限子项，不重交回执代码。
- README/ADR/api同步；本轮完整自验与随后独立quality/Web/security/diff分别记录在当前round证据，不自行授予发布或独立通过。通用Store.Update、v1及其余兼容writer/历史成本仍未整体改造；局部可归因修复不等于旧累计超时/running唯一根因闭环，M3仍Partial，M4/M5不扩展。

### 0003：既有恢复审批/拒绝的控制提交稳定性（ADR-0035；已独立工程验收，安全发布阻塞）

- 开工862文件/HEAD/暂存内容与0002 source-after逐一相同，零未知源码漂移。0002独立verification.json于2026-10-18 08:08:07 UTC完成：quality=0（601.423s，service uncached race=341.478s）、Web/diff=0、security=1，期间源码不变；仅校准已有验收，不重交Hold/回执/旧fixture或fsync补丁。
- 原样全service race/JSON诊断保留逐测试与原导出阶段；这是基线，不是稳定性闭环。只修既有ApproveRestore/RejectRestore的整快照写放大与真实PG Save冲突后覆盖。修前文件决策各替换四个未变hot/cold；PG独立pools复现先提交决定被覆盖、取消/lease/selected或sealed被复活。自身夹具接线/空map表示失败与真实控制错误、修后专项分别留证，不把表示差异当丢证据。
- 决策显式UpdateControl，实际冲突后重判当前tenant/记录、pending及职责分离，404→409→403不变；首个actor/time保持，重复决定仍409/零Save/零重复事实。决策与原一次事实同CAS，不改v2证据、不Get/Put WORM、不覆盖当前终态/lease/阶段/Hold/策略。原一次决策预算/有限重试保持，失败或未知Save零成功对象，不撤销已耐久事实、不盲重发。
- 真实文件/选中计划/归档/未知temp/重启/当前Hold，实际Save失败/未知/取消/deadline、真实PG行锁同步冲突/版本/晚owner，以及已有恢复/HTTP/回执/Hold/准入三轮race分别回归。完整quality/Web/security与随后独立验收以本轮实际证据为准，不由本清单自行授予passed或发布。
- 只改既有决策提交，不改CreateRestore/GetRestore兼容writer、原空actor策略或整次创建预算，不新增M5 scopes/版本计划/业务执行回执。仍未确定性复现旧running或证明累计耗时所有根因消失；M1/M2/M3总项仍Partial，M4/M5/整栈/容量/正式合规及安全发布继续独立待办。

### 0004：文件自审计描述符退出与常用夹具生命周期（ADR-0036；已独立工程验收，不扩产品范围）

- 开工866文件/HEAD/暂存内容与0003收尾清单逐一相同，零未知源码漂移。0003独立verification.json于2026-10-18 09:09:17 UTC完成：quality=0（614.179s，service uncached race=348.518s）、Web/diff=0、security=1，期间源码不变；只校准原真实验收，不重交恢复决定/Hold/回执补丁。
- 原样全service race/JSON/逐测试和原导出阶段作为基线，不能替代全量门禁。确定性真实文件回归复现Close只关flock、未关持久trail，旧writer可在successor取得锁后追加，以及两个常用夹具早退后仍保留实际flock/trail fd；引用保留到子测试结束后，不让GC掩盖缺失的显式teardown。
- Close与原trail mutex串行，先等待在途追加、关闭trail再关闭flock；保留两条错误、各fd一次消费，重复返回原结果。关闭后trail读/追加失败关闭，回执自审计失败零输出，已耐久事实不撤销、不猜未知结果成功。常用service/admin-trail文件夹具注册cleanup，晚注册的后台/IO cleanup先退出，不依赖GC。原实际I/O/fsync、所有文件/未知temp/重启、lease/阶段/终态/Hold/共享预算及全部等待/deadline保持。
- 此切片不是通用Store全方法关闭fencing、任意后台任务收集或远端物理停止；调用方仍须先停止并join工作。修前失败、修后三轮race/回执真实文件/独立PG pools、全service诊断及完整quality/Web/security与随后独立验收分别留证，以rounds/0004真实命令为准，不由清单预授passed或发布。
- 仍未确定性复现旧running或证明累计耗时所有根因消失；不能把后续全量绿、句柄GC回收或局部关闭修复当作唯一根因闭环。通用Store.Update/其余writer、CreateRestore/GetRestore及历史成本未改，M1/M2/M3总项仍Partial，M4/M5/整栈/容量/正式合规与安全发布继续独立待办。

### 0005：导出执行器尾部读取的本地保留与原子唤醒交接（ADR-0037；已独立工程验收，不扩产品范围）

- 开工869文件/HEAD/暂存内容与0004收尾清单逐一相同，零未知源码漂移。0004独立verification.json于2026-10-18 10:11:00 UTC完成：quality=0（594.586s，service uncached race=336.027s）、Web/diff=0、security=1，期间源码不变；只校准真实验收，不重交原描述符/Hold/恢复决定/回执修复。
- 原样全service race/JSON/逐测试与原导出阶段作为基线，不替代完整quality。修前六种结局在实际最终Backend.Load暂停时均为active=0/dispatching=false：执行器先注销却仍有尾读/未登记wake，旧idle观察可能提前关闭资源。局部确定性复现不是历史累计10m超时/原running唯一根因证明，两份旧0014相反结果均保留。
- reservation保留至runExport（含原heartbeat stop/join）和原最后一次有界ReadControl都结束，再同一exportMu内删除并登记原有限后继wake；不持该mutex读取Store/归档，不新增读取/等待、不热重试失败或保留运行阶段。原共享128/32、running2/1、lease/阶段、当前Hold、自审计与终态不回退保持，不改持久/密封格式或业务协议。
- 新矩阵另覆盖Put明确失败，核对锁外尾读、完整idle、有限wake、错误/取消/实际deadline和原终态/阶段/事实不变。真实PG17独立pools/实际FileStore源及导出在终态后占住本测试专用单连接池，由真实WaitCount证明SQL尾读在途，验证另pool当前Hold不被覆盖且仍阻断下载。单连接池仅测试同步，不改生产池默认值。原TestExportMatchesQueryEventsOrder另在cleanup按既有2秒护栏观察完整退出再关文件，保留原4事件/全部顺序成员与认证/I/O断言、5秒终态等待及10m包上限，不重交旧fixture/fsync补丁。
- 首次完整quality=1：普通测试通过，race的service为326.947s，新增取消场景0.01s时观察active=1/dispatching=true。CancelExport原来就会独立wake，夹具只等了取消前pass，不能把合法并发当假空闲。修正在实际Put仍暂停时核对取消generation恰增1并等该有限pass退出，再进入原尾读oracle；原断言、2秒观察护栏、5秒终态等待/10m包上限与生产行为不放宽。保留失败及其源码绑定，修后重新完整quality，不无改动碰绿。
- README/ADR/api同步，回执真实文件/HTTP与独立PG再复核但不重复交付。专项、完整quality/Web/security与随后controller独立验收分别绑定源码留证，以rounds/0005真实命令为准；本清单不预授passed或发布。仍无历史running确定性复现、全部累计耗时根因/长期稳定性闭环或通用Service关闭；M1/M2/M3整体Partial，M4/M5/整栈/Hosted Login/容量/容灾/正式合规与安全发布保持独立待办。

### 0006：既有恢复申请创建的控制增量提交（ADR-0038；已独立工程验收，不扩产品范围）

- 开工872文件/HEAD/暂存内容与0005收尾清单逐一相同，零未知源码漂移。0005独立verification.json于2026-10-18 11:23:44 UTC完成：quality=0（602.079s，service uncached race=351.264s）、Web/diff=0、security=1，期间源码不变；仅校准真实独立验收，不重交尾读/描述符/Hold/决定/回执补丁。
- 原样全service race/JSON/逐测试及原导出阶段先作基线，不能替代全量或证明历史running根因。只改CreateRestore的原认证回放后控制提交：修前成功/未知temp失败均替换四个未变hot/cold文件；真实PG17独立pools、实际Save冲突在selected/生成页/sealed三阶段覆盖取消/续期、另一恢复决定、Hold与策略，取消后晚owner可失去原fencing保护。修前失败保留，不把瞬时fsync栈归为死锁/宿主磁盘唯一根因。
- 创建显式UpdateControl，只将原新请求与一次restore.created合入新鲜control，不覆盖当前终态/lease/阶段/决定/Hold/策略、不重写v2证据；原ID/申请人/时间一次生成，提交重试不重复回放。原10000匹配上限、空actor零读事实及回放/提交的分离预算保持，提交一次deadline/有限重试不变；失败/未知Save零成功对象，不撤销已耐久事实、不自动重新创建，不新增请求幂等或业务执行。
- 新夹具起初误用仅control的leaseSnapshot计算冷回执数，造成修后PG源Get期望为零的真实失败；修正为实际完整Snapshot，同时加强前后完整证据核对，而非降低断言。原控制覆盖/文件替换失败与该接线错误分开留证；真实文件/归档/未知temp/重启/当前Hold、九种Save结果/预算，以及三阶段×取消/续期实际PG竞争另验。首次完整quality/Web/diff自验通过、安全1之后，收尾进一步加强新续期夹具：实际推进原共享测试时钟、在竞争前后检查owner运行权并保持原请求创建时间，不能只写未来心跳但仍用旧观察时钟。生产代码未变，前一版本通过保留，修改后重新完整quality及相关专项，不用旧绿冒充新源码验收。既有回执真实文件/HTTP/独立PG与导出退出/共享预算回归只复核、不重复交付。
- README/ADR/api同步；专项、全service诊断、完整quality/Web/security与随后独立验收分别绑定本轮真实源码，以rounds/0006命令结果为准，本清单不预授passed或发布。GetRestore/通用Store.Update/v1/全历史及锁内物化成本仍在，未确定性复现旧running或证明累计耗时所有根因消失；M1/M2/M3整体Partial，M4/M5/整栈/Hosted Login/容量/容灾/正式合规与安全发布保持独立待办。

### 0007：既有留存配置写的控制 CAS 重判（ADR-0039；已独立工程验收，不扩产品范围）

- 开工876文件/HEAD/暂存内容与0006收尾清单逐一相同，零未知源码漂移。0006独立verification.json于2026-10-18 12:41:39 UTC完成：quality=0（583.553s，service uncached race=340.802s）、Web/diff=0、security=1，期间源码不变；仅校准真实验收，不重交恢复创建/尾读/描述符/Hold/回执补丁。
- 原样全service race/JSON/逐测试及原导出阶段先作基线，不替代全量或证明历史running根因。只修SetRetentionPolicy的控制配置写：修前成功和未知temp失败均替换四个未变hot/cold文件、推进hot版本；真实PG17独立pools实际Save冲突在selected/生成页/sealed三阶段覆盖取消/健康续期、恢复决定、当前Hold与另一租户策略，租户已不存在也被旧快照恢复。修前失败原样留证，不把fsync栈归为死锁或宿主磁盘唯一原因。
- 配置显式UpdateControl，真实冲突后重查当前租户，只合入原配置与一次retention_policy.set，不覆盖当前终态/lease/阶段/决定/Hold/邻居策略或重写v2证据。原一次deadline/有限退避/校验/权限/事实编码保持，失败/未知Save报错、不撤销已耐久事实或盲重发；相同值再次设置仍各一次事实，不新增no-op、请求幂等或版本策略。仅配置，不执行生命周期/业务调用、不释放Hold或缩短对象COMPLIANCE。
- 真实文件/归档/原selected计划/当前Hold/未知temp/重启，九种实际Save结果与共享预算，三阶段×取消/健康续期以及当前租户消失均回归；续期实际推进共享观察时钟、核对owner运行权。新专项修后三轮race与真实PG三轮分别留证；已有回执真实文件/HTTP/独立PG及执行器退出/共享预算仅复核、不重复交付。README/ADR/api同步，无新路径/字段/scope/SQL或持久/密封格式。
- 完整quality/Web/security与随后独立验收分别绑定本轮稳定源码，以rounds/0007实际命令为准，本清单不预授passed或发布。GetRetentionPolicy/EvaluateRetention/通用Store.Update/v1及全历史/锁内物化成本未整体改造，未确定性复现旧running或证明累计耗时所有根因消失；M1/M2/M3整体Partial，M4/M5/整栈/Hosted Login/容量/容灾/正式合规及安全发布仍独立待办。

### 0008：既有 Schema 注册的控制 CAS 与演进重判（ADR-0040；已独立工程验收，不扩产品范围）

- 开工880文件/HEAD/暂存内容与0007收尾清单逐一相同，零未知源码漂移；raw index字节已刷新但暂存内容未变，不reset/restage掩盖。0007独立verification.json于2026-10-18 13:40:26 UTC完成：quality=0（639.623s，service uncached race=321.351s）、Web/diff=0、security=1，期间源码不变；仅校准真实验收，不重交留存/Hold/恢复/尾读/描述符/回执补丁。旧0014两类真实相反结果及其源码绑定继续保留。
- 原样全service race/JSON/逐测试与原导出阶段先作基线，不代替全量或证明历史running根因。只修RegisterSchema的既有提交：修前真实两租户文件成功/未知temp失败均替换四个未变hot/cold inode并改变hot版本；真实PG17独立pools实际Save冲突在selected/生成页/sealed三阶段覆盖取消/健康续期、恢复决定、当前Hold及邻居策略。并发更高/重复版本、较新前序required/allowed变化与租户消失也未重判。原失败日志保留，不把瞬时fsync栈当死锁或宿主磁盘唯一原因。
- 注册显式UpdateControl，实际冲突后重跑原当前租户/最高版本/重复key/最近前序兼容校验，只合入原定义与一次schema.created，不覆盖并发定义/控制状态或重写v2证据。原404→回退400→重复409→兼容400顺序、前向跳版本、精确历史版本、字段/上限/默认处理、一次deadline/有限退避保持；失败/未知Save报错、不撤销已耐久事实或盲重发，重复注册不变为幂等成功。注册本身零WORM I/O/独立读事实，不新增scopes、版本计划或业务执行。
- 新真实文件/原归档与selected计划/当前Hold/未知temp/重启，九种实际Backend.Save结果/一次预算，以及三阶段×取消/健康续期和五种演进重判场景分别回归。续期实际推进共享观察时钟并核对运行权；自有goroutine早退也取消/join，晚owner零控制发布。修后三轮race与真实PG三轮通过，未改新断言换绿。已有回执真实文件/HTTP/独立PG、执行器退出及共享预算仅复核、不重复交付；最终专项/全service诊断/完整门禁以当前轮实际证据为准。
- 首次修后全service诊断exit1，包456.220s、非10m超时；两条原Ingest计数测试分别观察累计2/1而期望单次1/0，原因是Schema初始化现在也经过同一真实control故障seam。完整quality尚未启动，不能冒称全量门禁失败或通过。仅在两条测试捕获真实初始化后的基数，保留Ingest增量精确1/0、原三次初始化API/Save故障序列、I/O/事实/超时断言，不清零累计counter。新增真实文件护栏确认初始化/armed注册失败/disarmed成功依次累计1/2/3、失败零定义/事实、两份hot/cold不改写。原失败绑定保留，修后三轮专项通过，随后重新完整诊断与quality，不以专项洗白。
- README/ADR/api同步，无新字段/路径/scope、SQL/持久/密封格式。完整quality/Web/security及随后controller独立验收分别绑定稳定源码，见rounds/0008/component和随后verification.json，本清单不预授passed或发布。少写四个无关文件不是整包wall下降、历史累计超时/running唯一根因或容量SLO；ListSchemas、通用Store.Update/v1和全历史/锁内物化成本仍在。M1/M2/M3总项仍Partial，M4/M5业务协议、整栈/Hosted Login、容量/容灾/正式合规和安全发布仍独立待办。

### 0010：既有来源更新的控制 CAS 与当前记录重判（ADR-0041；已独立工程验收，安全发布阻塞）

- 0011开工888文件/HEAD/暂存内容与0010 source-after逐一相同，零未知源码漂移。0010独立verification.json于2026-10-18 16:18:32 UTC完成：quality=0（751.226s，service uncached race=404.680s）、Web/diff=0、security=1，期间源码不变；只校准已有真实验收，不重交来源更新或其他修复。

- 开工884文件/HEAD/暂存内容与0008独立验收后、0009前后清单逐一相同，零未知源码副作用。0008独立verification.json于2026-10-18 15:08:22 UTC完成：quality=0（665.832s）、Web/diff=0、security=1，期间源码不变。0009真实runner-result=1（15.143s、fetch failed），没有verification.json、没有代码/契约进展或外部写入；不盲重放，不把此次provider失败当作service累计耗时或旧running根因。旧0014自验/独立超时及同源码收尾通过均保留，不重复交付回执/Schema或已有fixture/fsync/尾读修复。
- 原样全service race/JSON/逐测试和原导出阶段先作基线，不替代全量或证明历史running根因。只修UpdateSource的既有替换：修前启用/禁用及未知control temp失败都替换四个未变hot/cold inode、改变hot版本；真实PG17独立pools实际Save冲突在selected/生成页/sealed三阶段覆盖并发取消/健康续期、恢复决定、当前Hold、策略和邻居来源。缺tenant/source未重判、同key当前CreatedAt被旧值覆盖，Save失败/未知仍暴露未确认来源对象；这些确定性失败与源码绑定保留，不归为死锁/宿主磁盘唯一原因。
- 更新显式UpdateControl，实际冲突后重查当前tenant/source并沿用其CreatedAt，只合入原来源替换与一次source.updated，不覆盖当前终态/lease/阶段/决定/Hold/策略/邻居来源或重写v2证据。原allow-list精确值校验/排序、空列表仅同ID客户端、Active=false停用、signed client/tenant解析与同值各一次事实保持；不新增身份能力、条件更新或请求幂等。原一次deadline/有限重试不变，失败/未知Save返回错误与零成功对象，不撤销已耐久配置/事实，先只读核对、不盲重发。
- 新真实文件/原归档/认证selected计划/当前Hold/未知temp/重启和精确冷回执、九种实际Backend.Save结果/一次预算、原绑定/停用/切片独占，以及三阶段×取消/健康续期与当前记录三场景分别回归。续期推进共享观察时钟并核对实际运行权，自有goroutine早退也取消/join；新专项与真实PG修后三轮race通过，不改新断言换绿。既有回执服务/HTTP/真实文件/独立PG与执行器退出/共享预算另复核、不重复计功。
- README/ADR/api同步，无新字段/路径/scope、SQL/持久/密封格式；未修改Web、认证、生产deadline、原5秒等待/10m包超时、门禁或扫描规则。完整quality/Web/security及随后独立验收分别绑定稳定源码，以rounds/0010/component和随后真实verification为准，本清单不预授passed或发布。AddSource/通用Update/v1及全历史/锁内物化仍在，少写四个文件不是整包wall下降、历史累计耗时/running唯一根因或容量保证；M1/M2/M3总项仍Partial，M4/M5协议、整栈/Hosted Login、容量/容灾/正式合规与安全发布继续独立待办。

### 0011：既有来源创建的控制 CAS 与当前唯一性重判（ADR-0042；已独立工程验收，不扩产品范围）

- 0012开工892文件/HEAD/暂存内容与0011收尾清单逐项相同，零未知源码漂移。0011独立verification.json于2026-10-18 17:16:09 UTC完成：quality=0（640.427s，service uncached race=391.081s）、Web/diff=0、安全=1，期间源码不变；仅校准已验收状态，不重交来源/回执/fixture/fsync等原代码。旧0014失败及同源码收尾通过均保留。

- 完整核对本run交接/期限、0010真实命令/修前有界失败与源码/副作用证据；保留旧0014自验和独立累计10m失败、同源码收尾完整通过两类真实结果。原样全service race/JSON逐测试与原导出阶段先作基线，不以一次通过或瞬时fsync栈推导死锁、宿主磁盘唯一归因、容量或历史running根因闭环。
- 只修AddSource既有提交。修前缺省/提供创建时间×成功/未知control temp拒绝都替换四个未变hot/cold inode、改变hot字节/版本；真实PG17独立pools的实际Save冲突三轮覆盖并发取消/健康续期、selected/真实页/sealed、恢复决定、当前Hold、策略和邻居来源。同ID首个注册被覆盖/重新启用、当前租户缺失仍按旧观察成功也真实复现；失败及源码绑定原样保留，不重交ADR-0041的UpdateSource。
- 创建显式UpdateControl，实际冲突后重查当前tenant及同租户/source key唯一性，保持404→409优先级，首个注册/事实不被败方覆盖或激活；只合入原新来源与一次source.created，v2不替换hot/cold、不覆盖当前控制权。原allow-list校验/排序、空列表仅同ID客户端、创建Active默认true、缺省CreatedAt在提交前一次生成/提供时间保持、signed client/tenant解析与跨租户歧义失败关闭不变；不新增全局唯一性、身份发行或请求幂等。原一次deadline/有限重试保持，失败/未知Save报错、不撤销已耐久注册/事实，先只读核对、不盲重发，创建本身零WORM I/O/独立读事实。
- 新真实文件/原归档、认证selected计划/当前Hold/未知temp/重启/精确冷回执，九种实际Backend.Save结果/单预算，三阶段×取消/健康续期及同ID/缺租户/同时重复的真实PG竞争分别回归。续期推进共享观察时钟并核对运行权，同时验证原来源缺省时间未被重试重生；自有goroutine早退也取消/join。修后三轮专项与真实PG三轮通过。旧Schema故障护栏只将真实初始化累计计数纳入新增Source control写：Source/Schema各一次，2→3(armed失败)→4(disarmed成功)，三个原管理API/实际Save故障序列与Ingest精确增量不变，不清零counter、不批合初始化或删I/O/断言。已有回执服务/HTTP/真实文件/独立PG与原导出退出/共享预算继续复核、不重复计功。
- README/ADR/api同步，无新字段/路径/scope、SQL/持久/密封格式或迁移，未改Web、认证、生产deadline、原5秒等待/10m包超时、门禁/扫描规则。最终全service诊断、完整quality/Web/security与随后独立验收分别绑定稳定源码，以rounds/0011/component及随后真实verification为准，本清单不预授passed或发布。CreateTenant/通用Update/v1、共享control历史与全历史/锁内物化成本仍在；少写四个文件不是整包wall下降或全部累计耗时/历史running根因证明。M1/M2/M3总项仍Partial，M4/M5协议、整栈/Hosted Login、容量/容灾/正式合规与安全发布继续独立待办。

### 0012：既有恢复详情的控制面读取与单次预算（ADR-0043；已独立工程验收，不扩产品范围）

- 0013开工896文件/HEAD/暂存内容与0012 source-after逐项相同，零未知源码漂移。0012独立verification.json于2026-10-18 18:20:29 UTC完成：quality=0（586.290s，service uncached race=334.232s）、Web/diff=0、安全=1，期间源码不变；只校准真实验收，不重交恢复详情/回执/fixture/fsync等代码，安全发布仍阻塞。
- 原样全service race/JSON基线通过，仍只是观察；保留旧0014两类相反真实结果，不以瞬时fsync栈/最后一次绿归因宿主磁盘、死锁或唯一根因。只改GetRestore既有详情，未新增M1/M2/M3产品能力。
- 修前真实文件相同control、1→32个额外hot事件的同一读取分配由478增至4520；真实PG17独立pools实际锁住两张证据表时详情耗尽原caller预算，无关坏冷记录也拒绝元数据观察。Load带有效快照但迟取消/超时仍输出亦确定性复现。初版新夹具将额外样本自动归档而误当hot、用全局ListAdminActions检查已损坏control、热内存int与重启json.Number表示差异的失败均单列保留；校准实际准备/只读trail/精确数字后，原生产源码仍复现真正问题，不改新断言或原门禁换绿。
- GetRestore只读现有ReadControl，v2不物化hot/cold、不查询证据表或Get/Put WORM；读取与捕获后取消检查共享原一次deadline，失败/超时/取消零对象，不重试Load或写操作。原tenant/404、历史状态/字段/时间及独占副本保持，不Save、不增加读事实，原恢复元数据静默策略/必要事件读自审计不变。真实PG可观察另一pool当前决定，坏目标回执/实际control故障仍失败；真实文件/未知temp/重启/当前Hold、原完整证据与零写断言保持。
- README/ADR/api同步，无新字段/路径/scope、SQL/迁移、topic/Proto或持久/密封格式；未改HTTP/Web/缓存、认证、生产deadline、5秒导出等待、10m包上限或扫描规则。已有回执真实文件/HTTP/独立PG及原导出退出/共享预算只复核不计新交付；专项、全service诊断、完整quality/Web/security与随后独立验收分别绑定本轮源码，以rounds/0012实际证据为准，不由清单预授passed或发布。
- v1/共享control、其余历史物化/锁内成本与CreateTenant/通用Update仍在。分配/依赖下降不是整包wall下降、历史累计超时/running唯一根因、容量或长期稳定性证明，M1/M2/M3总项仍Partial，M4/M5协议、整栈/Hosted Login/容量/容灾/正式合规及安全发布继续独立待办。

### 0013：既有留存配置的控制面读取与单次预算（ADR-0044；局部稳定性切片，不扩产品范围）

- 原样全service race/JSON基线通过只是观察；旧0014自验/独立累计10m失败与同源码收尾通过均保留，不以最后一次绿或瞬时fsync栈推导死锁/宿主磁盘唯一归因。先核对0012真实命令、修前有界失败、源码/资源清理证据与当前清单，不盲重放旧外部写入。
- 只修GetRetentionPolicy既有配置读取。修前真实文件相同control、1→32个额外hot事件的同一读取分配509→4561；真实PG17独立pools实际证据表锁使其耗尽原caller预算，无关坏冷记录也拒绝配置观察。实际Load带有效快照但同时取消/超时仍输出亦确定性复现；修前命令与源码绑定原样保留。
- 显式ReadControl，v2不物化hot/cold、不查询证据表或Get/Put WORM；读取与捕获后撤回检查共享一次原deadline，失败/超时/取消零配置、不缓存/重试Load。不Save/追加事实，原静默配置读策略、tenant key/404/空scope拒绝、字段/天数/类别与独占返回保持，不新增当前tenant注册检查或默认配置。EvaluateRetention仍独立读原证据/当前Hold并保留原分离预算，不把配置当生命周期、可删除/COMPLIANCE或业务执行证明。
- 新真实文件/归档/认证selected计划/当前Hold、未知control temp、control及四份hot/cold字节/inode/版本、完整事实/重启和实际Load预算分别回归；额外样本仍逐次真实文件Ingest/Save/append/fsync，不转内存/文件系统。PG真实两张证据表锁/坏冷/control故障、另一pool当前配置、推进共享时钟的健康续期、owner取消/晚写fencing及精确版本/完整证据另验。修后三轮race与真实PG三轮通过，未改新断言换绿；已有回执真实文件/HTTP/独立PG、原导出顺序/完整退出/阶段和共享预算仅复核、不重复计功。
- README/ADR/api同步，无新字段/路径/scope、SQL/迁移、topic/Proto或持久/密封格式；未改HTTP/Web/缓存、认证、生产deadline、原5秒等待/10m包上限或扫描规则。专项、最终全service诊断、完整quality/Web/security与随后独立验收分别绑定本轮源码，以rounds/0013真实命令为准，不由清单预授passed或发布。
- v1、共享control历史、EvaluateRetention/通用Update及其余全历史/锁内成本仍在。局部分配/锁依赖改善不是整包wall下降、历史累计超时/running唯一根因、容量或长期稳定性闭环；M1/M2/M3整体仍Partial，M4/M5业务协议、整栈/Hosted Login/容量/容灾/正式合规及安全发布继续独立待办。

### 0014：跨租户单事件读取的锁外归档认证（ADR-0045；局部稳定性切片，不扩产品范围）

- 窗口外有界修复会话（用户明确“先修复”），不重开24h、不复用已过期的窗口门禁；保留0013/0014/0015失败与全部旧证据，安全发布仍阻塞。
- 只修旧跨租户单事件兼容helper：原实现持Store.mu在Read闭包内做Archive.Get，慢WORM会阻塞所有租户的无关控制写；读取与读自审计各自计预算。新增暂停Archive.Get的确定性锁oracle在修前2s失败，修后并发UpdateControl可完成。
- 改为一次WithOperation内先捕获热事件副本/归档receipt身份，锁外做一次archive预算内的身份/内容/链认证，唯一匹配后以事件tenant追加一次audit.event.read；零匹配404、跨租户歧义conflict、失败/撤回零事件零成功读事实，多receipt共用一次archive上限。
- 无新路径/scope/字段、SQL/持久/密封格式；热/归档独占副本、归档淘汰前后一致与授权边界保持。定向race×3、相关owned/console/self-audit回归、完整quality/Web/diff由本轮独立验收绑定同一源码；安全扫描1继续阻塞。

## M1：边界与前端基础

- [x] 证据区按子功能权限开放，保留服务端最终授权。
- [x] 身份/租户变化隔离请求、弹窗、预演和轮询；陈旧响应不能覆盖新上下文。
- [x] 统一错误分类、请求 ID、空状态与确认交互。
- [x] 事件字段 before/after 差异展示（不在浏览器解密）。
- [x] 证据页拆分为导出、恢复、Legal Hold、完整性子页。
- [x] 查询 count 返回总匹配数，与时间线口径一致。
- [ ] 完整权限/回执状态/恢复/留存语义矩阵及剩余边界回归校准。
  - [x] 回执元数据的请求/正文身份、精确冷索引、读取至原自审计单次预算及HTTP全部no-store（旧run20261017T002957Z/0014，ADR-0033；其自验与首次独立quality均真实失败，同源码随后final-verification完整quality真实通过，不能互相抹消。新run0001复核原回执专项/真实文件重启/独立PG pools，修后完整自验及2026-10-18 07:11:59 UTC独立quality/Web/diff均0、安全1；0002开工858文件/HEAD/暂存内容与source-after相同，仅校准已有有限工程验收、不重交代码。原状态/时间/坐标不重解释，零源Get/Schema依赖，不把元数据当全链认证、投影确认或下载授权；v1物化及hot/control成本、完整矩阵仍在，M3仍Partial）。

## M2：可信读链路

- [x] 已提交 cold 事实的启动/周期意图修复与持久发布确认（批次 2，完整门禁通过；整栈另行验收）。
- [x] 新版文件 HotFirst 原始追加 journal 的无人重试恢复、PG v2 原子热冷提交（批次 6；旧无原计划残留不推断，不混同发布对账）。
- [x] 发布重试退避/jitter、失败可观测，保持至少一次语义。
- [x] 当前目的地投影确认、单流连续哈希前缀及账本重建工具（批次 3；全租户完整性需另行核对）。
- [x] 事件检索可选 ClickHouse 候选索引及逻辑去重 count，返回证据核对 Ledger/WORM（批次 4）。
- [ ] 其余 ClickHouse facets/统计接线、全租户完整性核对与生产容量验收。
- [x] 事件查询固定成员集、主体/租户/筛选/来源绑定游标、迟到事件回归（批次 4；不宣称全局事务快照）。
- [x] 单事件/事件续页的按需锁外归档取证、共享 deadline 和证据验证（批次 4）。
- [ ] 参考初页、timeline/Console、导出等其余路径的全历史扫描与锁内长 I/O 改造（执行选择/检查点终态及详情/下载放行的当前 Hold 已锁外取证；后者 runner 0004 已通过独立 quality/Web/diff，安全扫描仍失败；不代表消除历史物化或其余 Hold 成本）。
  - [x] 分页 operation/aggregate 时间线的锁外认证取证与单次共享 deadline（本 run 20261017T002957Z/0001，ADR-0025；独立 verification.json 于 2026-10-17 01:57:17 UTC 记录 quality/Web/diff exit0、security exit1，验收期间源码不变；0002 开工 791 文件/HEAD/index 完全一致，只校准真实验收，不重新计功。旧全量/summary/replay 与全历史成本仍未改造）。
  - [x] Console 事件页/facets 的锁外认证与单次共享 deadline（本 run 0002，ADR-0026；独立 verification.json 于 2026-10-17 04:15:40 UTC 完成 quality/Web/diff exit0、安全 exit1，验收期间源码不变；0003 开工 801 文件/HEAD/index 相同。其 final-quality、-02、-03 自验均真实失败，不能用报告洗白；以随后独立实际命令校准此有限子项，不重新计功。保持原 all-tenant/live count，历史物化/扫描、ClickHouse facets/完整性及旧兼容 helper 未完成）。
  - [x] 操作摘要的锁外认证与单次共享 deadline（本 run 0003，ADR-0027；独立 verification.json 于 2026-10-17 05:48:42 UTC 完成 quality/Web/diff exit0、安全 exit1，期间源码不变。0004–0006 仅有下述五处测试夹具变化；0006 停机后新独立检查的 capacity 超时不改写 0003 当时通过，也不能当作当前全部源码通过。本项只校准既有工程交付、不重复计功；旧全量/replay/restore 和全历史成本仍未改造，M3 整体 Partial）。

  - [x] 操作回放的锁外认证与单次共享 deadline（本 run 0009，ADR-0028；独立 verification.json 于 2026-10-17 18:44:59 UTC 完成 quality/Web/diff exit0、安全 exit1，期间源码不变；0010 开工824文件/HEAD/index与其 source-after 完全相同。只校准既有工程验收，不重新计功。含已有内部恢复委托的原证据读取，不改原空 actor 策略/创建与审批；旧全量/aggregate replay、恢复整体流程及全历史成本仍未完成）。
  - [x] 导出创建/新后继的认证 Hold 预检与当前集合 CAS 准入复核（本 run 0010，ADR-0029；独立 verification.json 于 2026-10-17 19:45:29 UTC 完成 quality/Web/diff exit0、安全 exit1，期间源码不变；0011 开工831文件/HEAD/index与其 source-after 逐一相同。仅校准已有验收，不重复计功。先认证后谓词，不信任搜索 sidecar；新任务同原共享 control CAS 复核请求内当前活跃 Hold 集，变化409/零创建，既有后继仍仅幂等安全摘要。单次原 deadline/原读事实、锁外 Get、终态/lease/阶段 fencing 与预算不变；历史物化/认证扫描及原 QueryEvents 初页自身当时未整体改造，M3仍Partial）。
  - [x] 普通事件查询 reference 初次选择的认证前置/可信敏感条件（本 run 0011，ADR-0030；独立 verification.json 于2026-10-17 20:36:46 UTC完成quality/Web/diff exit0、安全exit1，期间源码不变；0012开工837文件/HEAD/index与其source-after逐一相同。只校准已有有限子项，不重复计功。复用既有 Console 认证扫描，receipt/map/精确 Schema/内容/链认证先于所有筛选，敏感条件由认证原值按条件格式重算；加密 searchable 新初页支持 bound/legacy 互查而不输出重建明文，旧 helper 限制仍保留直接回归。原固定成员/续页/投影/10000匹配上限/原 deadline/读事实不变，Console 不新增上限；历史物化/认证扫描仍在，不重交原锁外 Get/固定成员或导出阶段，M3仍Partial）。
  - [x] 旧全量时间线/服务层聚合回放的锁外认证与单次共享deadline（本run0012，ADR-0031；独立verification.json于2026-10-17 21:31:21 UTC完成quality/Web/diff exit0、安全exit1，期间源码不变；0013开工842文件/HEAD/index与source-after逐一相同。仅校准既有有限子项，不重复计功。OperationTimeline/AggregateTimeline/ReplayAggregate保留10000匹配上限、原顺序/单次读事实/空actor兼容；没有新增HTTP路由、业务调用或导出阶段。历史物化/认证扫描、旧跨租户详情helper/其余成本仍在，M3仍Partial）。
  - [x] 单事件/固定成员续页/投影实际页的按坐标回执身份与精确Schema正文绑定（本run0013，ADR-0032；独立verification.json于2026-10-17 22:39:37 UTC记录quality/Web/diff exit0、安全exit1，期间源码不变；0014开工848文件/HEAD/index与其source-after逐一相同。只校准已有有限子项、不重复计功。已保留修复前确定性失败、新/相关服务与HTTP三轮race、真实文件重启及独立PG pools三轮回归；请求tenant/event与receipt正文错绑在Get前拒绝，精确Schema正文tenant/id/version在内容/链认证前核对；单事件读事实耐久后取消也零输出、不撤销尝试事实。原成员/count/游标/来源/预算/锁外读取/一次deadline不变，不重选或重扫前缀；旧跨租户helper与其余历史成本、完整语义矩阵未完成，M3仍Partial）。

## M3：治理作业工作台与容量

- [x] 导出/恢复分页列表、我的申请/待决策视图。
- [x] 前端任务列表/详情，刷新后可找回；Hold 不可被列表绕过。
- [x] 持久真实 started_at，恢复按运行时间判断，兼容旧任务，避免误伤久排队任务。
- [x] 租户作用域、权限过滤的控制面治理状态摘要与 Web 概览（批次 7；不声明证据/执行完整性）。
- [x] 导出有界 dispatcher/租户轮转/租户内 oldest-first，API 启动与周期唤醒（批次 5；非跨副本严格公平 SLA）。
- [x] 导出租约/心跳、过期执行 fencing（批次 11；真实 PG 跨实例回归及完整门禁通过，不交付检查点续跑）。
- [ ] 检查点及可续跑执行（Partial；新多页任务逐页生成切片见下，最终整体合并/密封和流式下载仍未全部改造；不能重选整块计作进度）。
  - [x] Put 后密封对象检查点/终态提交续跑（runner 0001 实施；0001 独立未启动与 0002 独立 quality 失败历史保留。0003 有界修复后，独立 quality/Web/diff 退出 0，验收源码未变化；安全扫描仍退出 1，未交付生成阶段续跑）。
  - [x] 检查点终态当前 Hold 的认证前置/派生索引污染拒绝及源 Get 边界回归（runner 0002 实施；0003 独立完整 quality/Web 已通过，0004 开工 750 文件哈希一致。只校准实际验收状态，不重新计功或扩展执行阶段）。
  - [x] 导出详情/下载放行复用认证 Hold、锁外源 Get、共享 deadline、放行前后 control 复核及 HTTP no-store（runner 0004 实施，独立 quality/Web/diff 于 08:09:24 UTC 验收完成、源码未变；安全扫描退出 1，安全发布仍阻塞，不新增生成阶段）。
  - [x] 生成前持久固定成员计划 / selected 阶段续跑（旧 run 0007，ADR-0023；独立 verification.json 于 2026-10-16 09:42:03 UTC 完成 quality/Web/diff exit0、安全 exit1，验收期间源码未变，新 run 基线哈希相同；只校准旧状态，不计本轮新增；它仍是继续整块生成）。
  - [x] 新多页任务的逐页生成检查点（旧 run 20261016T091326Z/0001，ADR-0024；保留其独立 quality exit1：API drain 准备超时、worker 未达对象 HEAD、持锁启动期 SIGTERM；旧 0002 已修复并完整自验 quality exit0，但 fetch failed/exit1、无独立 verification，不能冒充验收。本 run 20261017T002957Z/0001 先原样完整复验，再由独立 controller 于 2026-10-17 01:57:17 UTC 完成 quality/Web/diff exit0、安全 exit1，验收期间源码不变；0002 开工 791 文件/HEAD/index 完全相同，故仅校准有限逐页切片的工程验收，不重交租约/心跳/selected/sealed。v2 计划保存真实页 Put 后的连续认证引用，只生成未提交页，最终仍整体合并/密封，M3 整体 Partial、发布阻塞）。
- [x] Legal Hold 拒绝提供兼容 forbidden 的机器可读原因和租户内 Hold ID。
- [x] 所有者关联新任务重试、受控取消与基础安全失败分类（批次 12；完整工程/Web 与真实 PG 专项通过，不代表安全发布）。
- [ ] 其余细分结构化失败/阻断原因。
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
- [x] 批次 1–12 已交付基线通过 `python3 cli.py quality`、`make web-check` 和相关定向/浏览器回归；不包含随后 runner 的独立验收完成声明。
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

### 批次 7：权限过滤的租户治理摘要（完整工程/Web 门禁通过）

- `GET /api/v1/governance/status` 只读 control 并失败关闭记录独立读事实；按现有最小
  权限省略不可见分区，租户/主体来自认证边界，平台显式选择，不扩展 Console 例外。
- 仅运行元数据计数、排队创建时间、固定租户上限和留存配置；不输出证据/筛选/原因，
  不探测发布/投影/WORM。actionable 排除本人/未知申请人，不替代最终职责分离。
- Web 概览独立接入，Token/权限/租户代次隔离，失败不冒充零计数；清楚区分审批与
  执行、导出完成与 Hold 门禁、策略配置与 COMPLIANCE。
- 最终完整 `python3 cli.py quality` 为 QUALITY PASS：普通 Go 测试 41.3s（其他包缓存
  复用）、全量 race 596.3s、vet/build、Python 156/1 可选跳过及所有工程/契约/隔离门禁。
  `make web-check` 通过：23 文件/82 测试、Svelte/TS/lint/build。定向租户/权限/摘要/读
  自审计/取消 race 3 轮通过；补齐既有路由穷举清单后完整复验，初轮失败不作为成功。
  未降低阈值或移除 race，无本批真实 PG/浏览器/整栈/生产容量新增验收。
- 当前源码再次运行 security-scan 退出 1：可达/导入包漏洞 0，保留未调用模块级警告；
  gosec 131 文件/32053 行仍有 62 项待评审，未新增静态 finding，不声明安全发布通过。
- README、OpenAPI、ADR-0016、部署手册同步；REST 增为 31 路径/39 操作，无新迁移。

### 批次 8：指标入口/数值边界/DLQ 关闭错误整改（工程门禁通过，安全发布仍阻塞）

- consumer/replay 的指标服务改为私有 mux，header/read/write/idle 超时有界，
  不继承 default mux 的其他 handler；TCP 回归在准备完不完整请求后启动读计时，
  保留生产 5s header 上限。指标网络限制、认证和连接准入仍由部署/后续容量治理负责。
- 就地验证保留期、聚合签名和投影的数值转换；合法聚合 v2 编码不变，黄金摘要
  覆盖 MaxInt64。gRPC 使用已限量批次的协议索引类型，e2e schema version 拨号前校验。
  坏的已提交聚合仍 `valid=false` 并自审计，未改变事实或重新签名。
- DLQ 状态追加/修复/短写/锁的关闭错误合并保留，真实 flock 边界注入关闭失败；
  descriptor 只关闭一次，未知结果不能成为内存成功 mark，允许幂等重试，保持租户隔离。
- 新边界及相关定向 race 3 轮通过；最终完整 quality 退出 0、QUALITY PASS：普通
  607.4s、全量 race 700.0s、vet/build、Python 156/1 可选跳过及全部门禁。
  完整 web-check 通过（23 文件/82 测试、Svelte/TS/lint/build），本批未改 Web 源码。
- 安全重扫退出 1：可达/导入包漏洞 0、1 项未调用模块警告；gosec 62→36 项，
  新增 0、移除 26（G115 12/G112 2/G104 12），nosec=0，未改扫描规则/豁免或生成代码。
  全部剩余告警登记于 `docs/SECURITY_SCAN_REVIEW.md`，不把工程门禁冒充安全发布通过。
- 无新迁移或业务 API、仍为 31 REST 路径/39 操作；没有新增真实组件、浏览器、整栈、
  生产容量或正式合规验收。租约/检查点、M4/M5 和整个规划仍未完成。

### 批次 9：文件数据描述符 I/O/私有创建权限（工程门禁通过，安全发布仍阻塞）

- Linux 文件数据逐组件 O_NOFOLLOW/openat，验证 regular/单链接 inode 后才读写或
  截断；FIFO/设备/目录、硬/软链接拒绝。原子创建、renameat 与清理共用 parent fd，
  固定 temp O_EXCL，未知残留不截断/删除。新文件 0600/目录 0700，旧账本不自动 chmod。
- 控制/热/冷/发布确认/固定成员集/独占锁/自审计接入；格式、账本、签名、journal、
  单写者与操作 deadline 不变。查询 GC 限捕获目录，读取始终有 4 MiB 上限。
  README、ADR-0017 与部署手册记录备份 UID/ACL、真实目录/挂载、残留保留和回滚约束。
- 定向安全边界/文件/查询/发布/自审计/journal race 3 轮、完整 fsutil/archive race 通过；
  最终完整 quality 退出 0、QUALITY PASS：普通 479.6s、全量 race 761.0s、vet/build、
  Python 156/1 可选跳过与全部门禁。Web 复验通过（23 文件/82 测试、类型/lint/build）。
- 最终 security-scan 退出 1：可达/导入包漏洞 0、1 项未调用模块警告；gosec 36→12，
  新增 0、移除 24（G304 18/G302 6），nosec=0，规则/豁免/生成代码未修改。
  剩余 TLS/DLQ 路径、环境变量名/jitter、protobuf unsafe 告警保持安全发布阻塞。
- 无 SQL/业务 API 迁移，仍为 31 REST 路径/39 操作；文件的真实目录/私有权限是部署
  行为变更，不能当作无约束热升级。本批未新增真实组件、浏览器/整栈、容量、容灾或
  正式合规验收，不把描述符防护当作特权主机/mount 防篡改或正式 WORM 证明。

### 批次 10：TLS/DLQ 文件信任边界（工程门禁通过，安全发布仍阻塞）

- Linux gRPC cert/key/client CA 受限 root 内读取、每文件 4 MiB、regular/实际读取
  上限及 non-blocking 打开；允许配置 parent 内 projected secret 相对链接，拒绝
  叶 root escape/绝对链接/loop/FIFO，不放宽 TLS/mTLS，不提供热加载/跨文件快照。
- DLQ state 的 load/classify/legacy/rebuild/append/repair/lock 接入严格数据 fd 策略；
  唯一 O_EXCL 0600 temp、actual fd fsync、renameat/cleanup 共用 parent，未知残留
  保留。原 JSONL/tenant identity、flock union、有界等待、O(1) append 与错误未知
  结果的幂等/offset 契约不变，不声明全历史 load/conversion 已常量内存化。
- 新 fd/temp/TLS/replay 边界、完整 replay 及 TLS/plaintext socket 定向 race 3 轮
  通过，使用真实 parent/symlink/content-sync 边界，不放宽生产超时。
- 最终完整 quality 退出 0、QUALITY PASS：普通 390.5s、全量 race 1108.5s、vet/build、
  Python 156/1 可选跳过与全部门禁；Web 复验 23 文件/82 测试、类型/lint/build 通过。
- 最终 security-scan 退出 1：可达/导入包漏洞 0，1 项未调用模块警告；gosec 12→7，
  新增 0、移除 G304 5，139 文件/32545 行/nosec=0。G101 2/G404 1/G103 4 仍保留，
  用途/常量/生成器 runtime 核对不是豁免或正式发布批准，不手改生成代码/扫描规则。
- ADR-0018、README、部署手册与扫描记录同步受信 root/挂载/UID/ACL、4 MiB 与
  回滚约束。无 API/SQL/JSONL 迁移，31 REST 路径/39 操作不变；无新增真实组件、
  浏览器/整栈、Hosted Login UI、生产容量、容灾或合规验收，整体规划仍未完成。

### 批次 11：导出运行租约/心跳/过期 fencing（完整工程/Web 门禁通过）

- 私有版本 1 control 元数据绑定 owner/job/tenant；领取与 StartedAt/running 同一次
  共享 CAS，2 分钟租约/30 秒心跳。仅无租约旧任务保留 24h 年龄恢复，健康慢任务
  不被旧年龄误伤。API 队列启动/每秒及 worker 原子失败过期任务并自审计，终态不
  重新排队；公共 DTO/摘要不暴露 owner，心跳不作为进度或证据完整性。
- Store control checked 使用同一 operation deadline、false 无保存；深克隆/重启和
  tenant control delta 保留租约。终态必须当前未过期 claim；迟到 Put 可留下无引用
  sealed 对象，但不能发布路径/覆盖终态/删除 WORM。共享预算仍为 128/32、2/1，
  限制的是持久 claim，不声明跨主机强杀或物理 I/O 上限，文件账本仍单写者。
- 新边界及相关回归三轮 race 通过，涵盖实际 Save/Put、续期失败取消、错绑/版本/
  时间/坐标污染、到期、终态一次性、旧恢复兼容与队列到期释放预算。真实 PostgreSQL
  17 独立连接池三轮 race 通过，覆盖同任务竞争、全局/tenant 限额、续期跨实例可见
  和过期 fencing；不是生产容量或容灾验收，临时容器仅本轮清理。
- 首轮因遗漏 snapshotJSONCopy 租约字段失败，修正后导出回归通过；随后边界测试
  修正取消后 checked 保存问题，未降低阈值/认证/生产超时。最终 map 身份校准前
  主动停止的质量轮次不作成功记录；首次完整普通测试失败于旧 worker 取消恢复仍
  预期成功，改为错误断言并新增无保存/无恢复事实测试，worker pass race 3 轮通过。
  最终完整 quality 退出 0、QUALITY PASS：普通 Go 测试（含缓存）8.8s、全量 race
  365.3s、vet/build、Python 156/1 可选跳过及全部工程/契约/隔离门禁。未减测或降低
  阈值，保留前述失败/停止日志。Web 无源码变化，完整复验 23 文件/82 测试与
  Svelte/TS/lint/build 通过。最终日志为 `/tmp/audit-productization-batch11-quality-final.log`、
  `...-web-check-final.log`（均 .exit=0）。
- 最终 security-scan 仍退出 1：可达/导入包漏洞 0、1 未调用模块警告，143 文件/
  32789 行/nosec=0、同一剩余 7 项（G101 2/G404 1/G103 4），无新增/豁免/消音。
  ADR-0019、README、runbook 已记录时钟、私有 control 备份、禁止新旧执行器混跑
  与回滚核对外部未知结果。无 API/SQL 迁移，31 REST 路径/39 操作不变。
- 检查点续跑、关联新任务重试、受控取消、公共结构化失败与分块认证下载仍未实现；
  整块历史选择/JSONL+AEAD 内存、其余租户/gRPC 准入、M4/M5 和整体规划未完成。
  未新增整栈、浏览器、Hosted Login UI、生产容量、正式 WORM/合规或业务执行验收。

### 批次 12：所有者取消、关联重试与安全失败分类（完整工程/Web 门禁通过）

- 新增 cancel/retry 两个 owner-only POST，沿用 export 权限、可信 tenant 与 subject，
  平台不能代办。取消与撤销 lease/清输出引用/一次 export.cancelled 同 CAS；重复取消
  零写入。迟到外部 Put 不回滚、不删除 WORM，旧执行不能覆盖终态。旧无围栏 running
  与错绑元数据失败关闭，取消只返回安全 control 摘要，不绕过详情/下载 Hold。
- retry 创建 retry_of 新任务，原终态保持、清旧 cursor、重做读自审计/Hold/共享
  准入。共享 CAS 每个原任务最多一个直接后继，重复请求返回同一后继元数据；需要
  再试显式指定失败后继。失败分类为闭集，旧记录缺失不猜测、任意值转 unknown，
  治理摘要单列 cancelled。无 SQL/proto/证据格式迁移，REST 增为 33 路径/41 操作。
- Web 列表/详情接入本人确认操作、关联与固定失败提示；未知提交先只读刷新，身份/
  Token/权限/tenant/任务变更清确认并丢弃晚响应，不把摘要当证据下载授权。
- 服务/HTTP/domain 专项 race 三轮通过；真实 PostgreSQL 17 独立连接池的取消竞争、
  唯一后继、共享 tenant 配额及取消释放预算三轮 race 通过；文件重启/Save 未知结果/
  迟到 Put/失败关闭读自审计/当前 Hold 覆盖。创建/重试共用各自一次服务 deadline，
  Hold 预检捕获独立深快照后锁外 Get；真实第二次 Get 暂停时续期/取消仍可完成，
  撤销/取消 deadline 不重启。历史物化与其他导出读/执行的锁内长 I/O 仍待改造。
- Web 最终完整复验 24 文件/97 测试、Svelte/TS/lint/build 通过；新字段缺失/负值不
  显示假零。安全扫描退出 1：可达/导入包 0、1 未调用模块警告，gosec 148 文件/
  33130 行/nosec=0、同一 7 项，无新增/消音/豁免。最终完整 quality 退出 0、QUALITY
  PASS：普通 Go（含缓存）239.2s、全量 uncached race 364.4s、vet/build、Python 156/1
  可选跳过及全部工程/契约/隔离门禁。最终日志为
  `/tmp/audit-productization-batch12-quality-final.log`、`...-web-check-final.log`、
  `...-boundaries-race-final.log`、`...-pg-final.log`（均 .exit=0），安全日志
  `...-security-final.log`（.exit=1），安全发布仍阻塞。
- 初次专项编译因新增 Hold fixture 调用签名/参数顺序错误失败，已修正；新增 Web
  权限测试错误地把 platform=true 当普通缺权主体，改为真实非平台最小权限 fixture，
  未改生产授权。初次质量启动工具超时、未输出阶段且进程已退出；随后普通 Go 全量
  通过后，为改造预检锁内读取主动停止自有 race 树，两轮均不作 QUALITY PASS。
  最终预检改造后重新全量通过，保留失败/超时/停止日志，不减测、不降低阈值。
- ADR-0020、README、Web/runbook 同步；新旧 control writer 不可混跑，防旧版本丢弃
  retry_of/失败字段；保留原任务/后继/lease 备份。检查点续跑、分块认证导出、历史
  成本、M4/M5 和整体规划仍未完成；没有新增整栈/浏览器/容量/正式合规验收。

### 24h runner 0001：密封对象检查点 / 终态提交续跑（本轮自验通过，独立验收未启动，M3 整体 Partial）

- 新增私有 version=1/seal_version=2 control 检查点，绑定原任务/原筛选/申请人/lineage/
  创建与首次启动时间，认证原对象及密封/JSONL 摘要/计数。原选中证据先验证精确 Schema、
  内容与 committed receipt/链坐标；选择/检查点验证 Get 锁外，操作 deadline 不逐对象重启。
- Put 成功后，未过期 owner 将 checkpoint + export.checkpointed 同 CAS 提交；只有原
  running lease 到期才可换新 owner，以原 2/1 running 预算续跑终态提交。保留 started_at，
  不将任何终态退回 pending，不重选输出、不重新密封或 Put。重查原对象/账本坐标、当前
  Hold 并失败关闭自审计；Hold 集变化/撤销/换主 fence 阻止晚终态，完成事实同 CAS 一次。
- 文件重启跨过 15 分钟 cursor 有效期，迟到事件不改变原输出；Save 未提交/提交但结果未知、
  版本/绑定/摘要/计数/AEAD/ledger 污染、Get/读自审计失败、实际 deadline、Hold 变化、
  Get 阻塞时续期/取消和终态不回退覆盖。检查点前崩溃仍失败；旧未知申请人保持内部服务
  自审计，不赋予用户代办权限。完成/失败/取消只清已知活跃元数据，不删除事实或 WORM。
- 完整 `python3 cli.py quality` 退出 0、QUALITY PASS：普通 Go 382.7s、全量 uncached
  race 583.3s、vet/build、Python 156/1 可选跳过及全部工程/契约/隔离门禁。完整
  `make web-check` 退出 0：24 文件/97 测试、Svelte/TS/lint/build；本轮未改 Web 源码，
  既有 Vite sourcemap/PURE 注释警告保留。专项 checkpoint race 3 轮通过（service 50.723s、
  store 1.223s）；隔离 PostgreSQL 17 两个独立连接池专项 race 3 轮通过（4.914s），覆盖
  唯一接管、共享预算、tenant delta 与跨副本取消/晚 Get。专有容器/network 已移除，无旧卷操作。
- 真实 `python3 cli.py security-scan` 退出 1：可达/导入包漏洞 0、1 项未调用模块警告，
  gosec 153 文件/33538 行/nosec=0、7 项（G101×2/G404×1/G103×4），无新增/豁免/规则变更。
  发布仍 Blocked，不把工程验收等同安全批准。初始专项因旧未知 requester 兼容收敛失败，
  修复内部服务读自审计后通过；后续一次专项编译 unused import 已修正，所有失败日志保留，
  未改旧测试、阈值、认证或生产超时。
- 日志根：`/home/u1/.abr/projects/snaplink-audit-governance-a4087374b5d05637/runs/productization-24h-sol-max-20261016T013158Z/rounds/0001/`；
  `quality.log/.exit`、`web-check.log/.exit`、`checkpoint-race-final.log/.exit`、
  `checkpoint-postgres.log/.exit` 均退出 0；`security-scan.log/.exit` 退出 1。
- README、ADR-0021（及既有 ADR 后续边界）、OpenAPI、部署手册同步；仍为 33 REST 路径/
  41 操作、4 topic、3 gRPC 方法，无 SQL/密封格式迁移。新旧 control writer 不得混跑，
  备份保留 checkpoints/leases/原任务/后继及既有账本、journal、确认和密钥。
  逐页/逐块生成续跑、分块认证下载、历史成本、整栈/Hosted Login、容量、M4/M5 未交付；
  不重复计算批次 12 租约/取消/关联重试，也没有业务执行或正式合规验收。

### 24h runner 0002：上一轮检查点的当前 Hold 门禁修复（本轮自验通过，随后独立 quality 失败，安全发布阻塞）

- 先核对上一轮 verification.json/runner-result.json/失败日志及 33 文件哈希：源码与上一轮
  自验结果绑定一致，但独立验收 commands={}，在 quality.log 的 O_EXCL 创建处重名失败，
  不能把模型报告或旧自验当作独立验收通过。本轮自验全部写入 rounds/0002/component/，
  为外部验收保留根目录日志名；不修改验收脚本、窗口、其他轮证据，不回滚/重放外部写入。
- 本轮只修复同一 sealed 检查点终态 Hold 复查：先认证 receipt 锚定的当前证据，再评估
  原查询/Hold；精确重建原值，按条件格式重算搜索谓词，拒绝由被污染的正文/派生索引
  隐藏被 Hold 覆盖的证据。未知/坏当前证据保留原检查点，不把失败视为空选择；源 Get
  锁外、共用 deadline，终态 Hold 集/owner fencing 和共享预算不变。不重选原输出或 Put，
  不重复计功租约/心跳/取消/关联重试，也不交付新执行阶段或业务恢复。
- 修复前新增回归真实失败，日志 hold-regression-before.log 保留；新增测试类型名错误的
  编译失败 checkpoint-expanded.log 也保留。原有测试/门禁/阈值/认证/生产 timeout 未变。
  扩展专项三轮 race 最终退出 0（service 72.446s/store 1.052s），涵盖热/归档正文、Schema/
  加密内容/tenant 漂移、派生摘要缺失/值/格式污染、bound/legacy 条件与精确大整数、
  缺正文、迟到证据/最低 Hold ID、修复后正常接管、无候选/不相交的零源读，以及真实
  源 Get 时的续期/取消/读自审计/Hold 变化 fencing 和原生产 deadline。
- 最终完整 `python3 cli.py quality` 退出 0、QUALITY PASS（2026-10-16 03:00:20–03:14:18 UTC）：
  普通 Go 含缓存 240.6s、全量 uncached race 586.7s、vet、7 二进制 build、Python 156/1
  可选跳过及全部工程/契约/隔离门禁。完整 `make web-check` 退出 0，24 文件/97 测试与
  Svelte/TS/lint/build 通过；本轮未改 Web 源码，既有 Node/localStorage 与 Vite 注释/
  sourcemap 警告保留。没有删测试、放宽阈值/认证/生产超时或使用专项替代全量。
- 唯一本轮隔离 PostgreSQL 17 专项 `go test -race -count=3 ./internal/service -run
  '^TestPostgresExportCheckpoint' -v` 退出 0（3.628s）：独立 SQL pools、唯一接管/预算、
  跨副本取消，以及当前 Hold 源 Get 期间续期/另一 pool 改 Hold 的终态 CAS 拒绝与随后
  正常过期重查的一次阻断。只清理本轮带唯一 label 的容器/network 和临时凭据文件。
- 真实 `python3 cli.py security-scan` 退出 1：可达/导入包漏洞 0、1 未调用模块警告；
  gosec 154 文件/33646 行/nosec=0、7 项（G101×2/G404×1/G103×4），新增/移除均 0。
  用途核对不等于批准，**release_gate=blocked**；未修改扫描规则/排除/生成代码或豁免。
- 证据根：`/home/u1/.abr/projects/snaplink-audit-governance-a4087374b5d05637/runs/productization-24h-sol-max-20261016T013158Z/rounds/0002/component/`。
  `quality.log/.exit`、`web-check.log/.exit`、`checkpoint-expanded-02.log/.exit`、
  `checkpoint-postgres.log/.exit` 均退出 0；`security-scan.log/.exit` 退出 1，早期失败原样保留。
  Controller 修复其日志隔离后，独立验收已于 2026-10-16 03:20:42–03:31:57 UTC 执行，
  `rounds/0002/verification.json` 记录 quality exit1、Web exit0、安全扫描 exit1、diff exit0，
  且验收期间源码未变化。完整失败日志保留在
  `rounds/0002/independent-engineering-my3hcqcu/quality.log`：普通 Go 通过，全量 uncached
  race 唯一报告 `TestGRPCWriteStreamClaimsAreNotExtendedByEventFields` 第177行 `EOF`。
  服务端可在首条 Send 前拒绝无写权限流，旧测试错误要求 Send 必须 nil；人工随后在
  原样源码上的定向 race30 通过不能抵销完整失败。本轮未修改 controller 或外部脚本。
- README/ADR-0021/OpenAPI/runbook 与源码同步；无新格式、SQL、scope、路径/topic/gRPC
  方法。M3 仍 Partial：逐页/逐块生成续跑、分块认证下载、其余 Hold 读链路和历史成本、
  整栈/Hosted Login、生产容量/合规与 M4/M5 仍未完成，没有业务执行副作用。

### 24h runner 0003：人工授权的 gRPC 拒绝边界有界修复（完整自验通过，独立验收未完成，未计完成）

- 本轮不新增功能、不重交租约/取消/重试/检查点；保留前两轮及批次 1–12 全部现场。
  开工 748 个文件哈希与人工核对基线完全一致，未发现未知源码漂移；不重放外部写入。
- 先在真实 gRPC 流等待 trailers-only 拒绝后再 Send，确定性复现原测试 `EOF` 失败；
  不依赖 sleep、缩短 JWT 寿命、改变生产 timeout 或重跑碰绿。随后仅按 SendMsg 语义
  校准 oracle：该顺序精确要求 io.EOF，Recv 仍必须 PermissionDenied 且无成功回执。
  新增缓冲先到/并发 Send-拒绝边界回归，实际无权限 handler 的 RecvMsg 必须零调用；
  跨租户伪造及 actor/payload 均不能扩权，Events/Receipts/Outbox 和链状态不得新增。
  合法前缀同时核对 receipt/Outbox 持久见证，失败诊断不输出原载荷。
- 生产授权/持久化/超时/阈值和扫描规则未改。README、Accepted ADR-0001 与
  `api/README.md` 澄清既有客户端传输语义，无线面扩展。初轮 quality 在 Python
  `test_make_proto_idempotent` 失败（要求 api/proto 相对 Git 干净），尚未执行全量 Go；
  原因是本轮新增的 Proto 注释。仅撤回自己的该注释，并用原 pinned generator 重生成，
  原 Proto/生成字节恢复开工基线；不改门禁、不提交/重置，也不动原有未提交改动。
  初轮 quality/Web/security 的 exit1/0/1 及所有输出保留，随后重新完整验证。
- 定向 `go test -race -count=30 ./internal/grpcapi -run
  '^TestGRPCWriteStream(ClaimsAreNotExtendedByEventFields|PermissionRejectionSendBoundaries)$' -v`
  退出 0（7.931s）；完整相关 `go test -race -count=1 ./internal/auth ./internal/grpcapi -v`
  退出 0（auth 4.106s/grpcapi 60.933s），包含授权/claims/expiry/stream 全包，不代替 full gate。
- 最终完整 `python3 cli.py quality` 于 2026-10-16 06:32:01–06:41:26 UTC 退出 0、
  QUALITY PASS：普通 Go 含缓存 42.7s、全量 uncached race 516.6s、vet/7 二进制 build、
  Python 156/1 可选跳过及全部工程/契约/隔离门禁。`make web-check` 退出 0：24 文件/
  97 测试、Svelte/TS/lint/build；本轮没有 Web 源码改动，既有 Node/Vite 警告保留。
  最终 `python3 cli.py security-scan` 退出 1：可达/导入包漏洞 0、1 未调用模块警告，
  gosec 154 文件/33646 行/nosec=0、同一 7 项（G101×2/G404×1/G103×4），发布仍阻塞。
  748 文件开工基线及最终门禁前后哈希已保存，最终验证期间源码未变化；没有放宽规则。
- 本轮命令与输出独立保存于 `rounds/0003/component-repair/`；修复前确定性失败日志为
  `denied-before-send-regression.log/.exit`（exit1），首次 `quality.log/.exit` 的 Python
  失败及 Web/安全输出也保留。最终 `quality-final`/`web-check-final`/`security-scan-final`
  各有完整 `.command/.log/.exit/.started/.finished`（exit0/0/1），不是此前失败被覆盖。
  外部独立验收尚待随后真实完整命令；不勾选独立验收完成、不自授发布批准，M3 仍 Partial。

### 0003 后续独立验收校准（0004 开工核对，不是新交付）

- `rounds/0003/verification.json` 与 `runner-result.json` 记录真实独立命令：quality
  于 2026-10-16 06:46:13–06:57:56 UTC 退出 0；Web/diff 退出 0；security-scan 退出 1。
  原始日志在 `rounds/0003/independent-engineering-qg7435hg/`，源码验收期间未变。
- 0004 开工逐一核对这份 `source-after.json` 的 750 个文件哈希，变更/缺失/新增均为 0。
  因而将上方两个已有 sealed 检查点子项校准为已工程验收，保留 0001/0002/0003 当时的
  失败和待验历史；不重复计算租约、取消、重试或检查点实现，不自授安全发布批准。

### 24h runner 0004：完成后访问的认证 Hold / 锁外读取（完整本轮自验通过，独立待验，安全发布阻塞）

- 首先核对 0003 独立命令与当前 750 文件哈希完全一致，以上一轮真实已验证状态继续。
  不重交批次 12 的租约/取消/重试，也不把已有 post-Put 检查点再记为本轮新能力。
  M3 优先核对：私有 version=1/seal_version=2、任务 HMAC/原对象/receipt 绑定、精确未
  过期 owner、终态不回退、当前 Hold/读自审计与共享 128/32、2/1 预算均保留；没有
  外部批准依赖。本轮只做其完成后访问门禁，不将它冒充逐页/逐块生成续跑。
- GetExport/RecordExportDownload 复用检查点的 receipt 锚定、精确 Schema/重建内容/
  链认证后再过滤的 Hold 判定；从认证原值按条件格式推导搜索条件，不信任 sidecar。
  缺失/污染证据不作空选择。深快照捕获后 Get/认证在锁外，来源读取、自审计和 control
  复核共享各服务操作的一次既有 deadline；允许前重查当前租户活跃 Hold 与 job，下载
  自审计前后均复核。正常前向进展可返回较新详情，原终态与下载完整 job 要求不变；
  scope/Hold 变更或回退拒绝，不把旧快照写回。HTTP 详情/下载成功和全部错误 no-store。
- 新回归先真实复现“删除派生摘要后原详情漏掉当前 Hold”；随后覆盖热/归档正文、
  精确 Schema/加密内容/tenant/actor 漂移、bound/legacy 条件和大整数、缺失证据、
  真实源 Get 中的续期/取消/Hold 变更、自审计追加中的 Hold/job 变化/失败/取消、
  默认 deadline 和 HTTP 认证/脱敏/不输出明文。专项三轮 race 退出 0（service
  77.704s/httpapi 6.320s；原日志计时为准），不替代完整门禁。
- 专属 PostgreSQL 17-alpine（唯一身份 `audit-gov-r0004-d797fad9a88b`、loopback 随机
  端口、0.75 CPU/512 MiB/128 pids、tmpfs）独立 SQL pools 三轮 race 退出 0（4.222s），
  覆盖新访问门禁和既有 sealed 检查点接管/预算/Hold/cancel。只清理本轮容器/network/
  私有临时凭据文件，ownership label 与清理结果保存在 postgres-isolation.json；
  不访问生产/共享 DSN，不动旧卷/无关容器或宿主防火墙，不执行未知外部写入重放。
- 完整 `python3 cli.py quality` 于 2026-10-16 07:27:04–07:39:59 UTC 退出 0、QUALITY
  PASS：普通 Go 含缓存 315.0s、全量 uncached race 454.8s、vet/7 二进制 build、Python
  156/1 可选跳过及全部工程/契约/隔离门禁。完整 Web 退出 0（24 文件/97 测试、
  Svelte/TS/lint/build），无 Web 源码变更，既有 Node/Vite 警告保留。结果文档更新后
  的完整复验以同目录 quality-final/web-check-final/security-scan-final 原始日志为准。
- 本轮真实 security-scan 于 07:40:26–07:40:35 UTC 退出 1：可达/导入包漏洞 0、
  1 未调用模块警告；gosec 155 文件/33730 行/nosec=0、同一 7 项（G101×2/G404×1/
  G103×4），没有新增/豁免/规则或阈值变化；release_gate=blocked，不自授发布批准。
- 失败原样保留：新增 fixture 缺 tenant name、完整 job 比较误拒正常轮询进展、新 PG
  测试 unused import、首轮 quality 的 Python 固定 OpenAPI 200/404 文本标记失败。
  分别修正本轮 fixture、返回同查询较新状态、移除自己的 unused import，以及撤回
  自己在下载 200 行新增的结构化 header（no-store 仍在契约 description/运行时/回归
  中强制）；未改原测试/门禁/认证/生产 timeout，未用定向通过抵销完整失败。
- 证据根：`rounds/0004/component/`（位于本 24h run 目录），每条命令都有独立
  .command/.log/.exit/.started/.finished；`access-regression-before`、`access-boundaries`、
  `access-export-regressions`、`access-expanded`、`quality` 的失败保留；
  `access-expanded-02`、`access-checkpoint-postgres`、`quality-02`、`web-check` 已退出 0，
  `security-scan` 退出 1。完整命令清单/最终哈希由本轮 REPORT.md 及 source-final.json
  交接；独立外部验收须随后重新执行，尚不标独立完成。
- README、ADR-0021/0022、OpenAPI、runbook 同步。无新路径/scope/DTO、topic/gRPC、SQL
  或密封格式，33 REST 路径/41 操作、4 topic/3 gRPC 不变。历史深快照/扫描、创建/
  预检/执行选择等其余 Hold 路径、逐块生成/认证下载、M4/M5、整栈/Hosted Login、
  生产容量/容灾/正式合规仍未完成；审批不是执行，没有业务状态变更。

### 0004 后续独立验收及 0005/0006 停机核对（0007 开工校准，不是新交付）

- 0004 verification.json 的真实独立命令：quality/Web/diff 退出 0，security-scan 退出 1；
  2026-10-16 08:09:24 UTC 完成，源码验收期间未变，安全发布仍阻塞。原始日志保留在
  rounds/0004/independent-engineering-xnzh42ui/。上方待验历史不改写成当时已验。
- 0005/0006 原始 runner-console.log 仅报告 provider fetch failed，均无 verification，
  没有目标工程门禁失败或新交付。0004/0005/0006 source-after 完全一致，0007 开工
  757 文件逐一哈希、HEAD/index 与其相同，无新增/缺失/漂移或已知写入待重放；按
  PROVIDER-RECOVERY-002 人工指令继续，不改认证/网络/外部 runner，不重复旧切片。

### 24h runner 0007：持久固定成员计划 / selected 阶段续跑（已实现，完整自验结果见证据，独立待验，M3 整体 Partial）

- 生成前以私有 version=1/format_version=1 的 export_plans 固定有序原账本坐标、发生
  时间及精确 Schema ID/version/定义 HMAC；完整任务/筛选 HMAC 绑定，不复制载荷/
  密文/原始筛选/搜索摘要/owner。沿用10000成员/4MiB、共享2/1 running与128/32队列。
  先认证再过滤，内部读自审计后计划与 export.planned 同 CAS 提交；未知 Save 停止。
- 原 lease 合法到期后保留 running、新 owner 接管、started_at不变，仅从原成员认证
  读取继续整块生成，不重选或消耗旧 cursor；迟到事件不增加输出成员。当前 Hold
  仍核对原查询的当前认证证据，自审计/取证/复核共用一次既有 deadline、源 Get 锁外。
  Put前和sealed Save复核计划/Schema/owner，sealed Save原子消费selected；终态/取消
  只清合法活跃元数据。未知旧 Put 可留无引用WORM，新owner使用不同attempt对象；
  不重放未知旧key，不删除WORM或宣称仅Put一次，不执行业务恢复。
- 新回归覆盖真实Save未知、文件重启超cursor TTL、迟到事件、Schema/内容/坐标/MAC/
  顺序/预算、Hold/读自审计/deadline、续期/取消/换主与阶段消费；旧sealed/heartbeat
  测试按真实持久阶段校准，原失败日志保留，不降低阈值/删断言。初次工具120秒超时
  未形成测试结果；随后专项发现4个旧sealed-only预期，需要支持已持久selected。
  文件测试由计数Save改为读取已持久sealed的实际Save边界，旧无计划失败回归保留。
- selected/sealed/heartbeat 专项三轮 race 退出0（plan-regressions.log），专有 PG17
  独立SQL pools的全部PostgresExport三轮race退出0（plan-postgres.log，service6.757s）。
  唯一容器 audit-gov-r0007-c069bcf94d50 采用loopback随机端口、0.75CPU/512MiB/
  128pids/tmpfs，按本轮ownership label清理；postgres-isolation.json记录资源和私有
  临时凭据文件均已清理，不访问共享/生产DSN、旧卷或其他容器。
- 首次完整 quality 退出1（quality.log，未进入全量race/build）：HTTP取消测试在
  TempDir清理时仍有取消唤醒的后台写，补上关闭HTTP入口、观察全部真实终态Save及
  Store关闭；两个旧读自审计测试仍只计requester query，改为精确断言每个原事实加
  一次governance-worker selected读事实，GetExport前后事实仍严格不变。未改生产
  超时/认证或阈值。对应HTTP和服务读事实专项race30分别退出0，失败原日志保留。
- 本轮日志根 rounds/0007/component/；完整复验 quality-final、web-check-final、
  security-scan-final 的 .command/.log/.exit/.started/.finished 为实际结果，不能用
  上述专项代替全量或预先宣称通过。首次Web已退出0（24文件/97测试），安全扫描
  已退出1（可达/导入包0、1模块警告、gosec160文件/34184行/nosec0/同7项）；最终
  完整复验另存，独立工程验收仍待外部另跑。没有新增路径/scope/DTO、SQL或密封格式。
  README/ADR-0023/OpenAPI/api说明/runbook同步，逐块生成/认证下载及M3整体仍Partial。

### 新 24h run 20261016T091326Z / 0001：逐页生成检查点（完整自验通过，独立外验待随后，安全发布阻塞）

- 首先读取两份 HANDOFF、旧 run 0007 的 verification.json/独立原始日志、新 run 的
  transition/window/status/baseline：旧工程 quality/Web/diff 已于 09:42:03 UTC 通过，
  安全扫描 exit1，基线 771 文件/HEAD/源码哈希相同；不把旧 selected/sealed/Hold 访问
  或批次12租约/取消/重试重新计作本轮新增，不修改外部控制器/门禁/窗口或 Git index。
- 新多页选择（>100成员）使用私有 version=2/format_version=1 计划，每页最多100原
  成员；真实认证取证/剥离搜索 sidecar/JSONL/独立 AEAD/Put 后，页引用/范围/摘要与
  export.progressed 同 CAS。绑定完整原计划/Schema/格式/页/对象；源读、当前认证
  Hold（含迟到证据）、读自审计、Put/Save 共用本页一次原 deadline，锁外 I/O，精确
  owner/原计划 MAC/Schema/Hold 集及共享预算复核。4MiB 总计划内预留页引用空间，
  原10000成员、队列128/32、running2/1、租约2分钟/心跳30秒均不扩大。
- 崩溃接管认证复用原已提交页，只生成剩余页，不重选或再生成前缀。缺/坏已提交页
  保留计划；未知 Save 立即停止，以实际耐久前缀恢复；未提交/迟到 Put 留无引用
  WORM，新 owner 用不同 attempt key，不猜旧 key/回滚、不删除对象。所有页提交后
  才按原最终格式整块密封，sealed Save 原子消费计划/页；原终态/Hold/下载门禁不变。
  v1 历史计划/新单页保持原路径，最终合并/整体密封/Put 和大文件流式下载仍 Partial。
- 严格证据复核新增回归先真实复现：共享 verifyQueryReferences 只检查 map key/链坐标，
  漏核 receipt 正文 event_id。补上精确正文身份比较后，修复及阶段转换三轮 race0，
  不以已认证页掩盖当前账本身份漂移。未修改旧测试、生产超时、认证或阈值；早期
  新增 Hold 释放测试参数顺序错误的 exit1、修复前绑定回归 exit1 原样保留。
- 完整 python3 cli.py quality 于 10:09:28–10:29:24 UTC exit0/QUALITY PASS：普通 Go
  542.3s、全量 uncached race641.7s、vet/7二进制 build、Python156/1可选跳过及全部
  工程/契约门禁；Web exit0（24文件/97测试、Svelte/TS/lint/build，原警告保留），
  本轮未改 Web 源码。初版逐页边界专项 race3 exit0；共享身份修复/新阶段转换专项
  race3 exit0；最终专属 PG17 独立 SQL pools 的全部 TestPostgresExport 三轮 race
  exit0（77.522s）。真实 Put/Get/Save/append、文件重启超 cursor TTL、迟到成员/Hold、
  认证/版本/范围/Schema/ledger污染、deadline、取消/续期/换主/共享预算覆盖。
- 本轮真实 security-scan 于10:30:13–10:30:26 UTC exit1：可达/导入包漏洞0，1未调用
  模块警告；gosec162文件/34513行/nosec0/同7项（G101×2/G404×1/G103×4），未改规则/
  排除/生成代码或自授豁免，release_gate=blocked。工程自验不是独立验收/正式发布。
- 证据根：新 run `rounds/0001/component/`；quality、web-check、security-scan、
  generation-race、ledger-binding-before/fixed、generation-transitions-final、
  final-generation-postgres 均有独立.command/.log/.exit/.started/.finished。
  两次 PG 仅用本轮唯一 label 的本地容器/network（0.75CPU/512MiB/128pids/tmpfs、
  loopback随机端口），资源及私有临时凭据均已清理，见 postgres-isolation.json 和
  final-postgres-isolation.json；不访问生产/共享 DSN、旧卷或无关资源。
- README/ADR-0021/0023/0024/OpenAPI/api说明/runbook 与源码同步；33 REST 路径/41操作、
  4topic/3gRPC、最终密封格式和 SQL 表不变。独立外部工程验收随后仍须完整运行；
  结果文档更新后的复验另存 quality-final/Web/安全 final，不覆盖上述通过或失败证据。
  整体M3、初次/当前Hold历史成本、流式大文件下载、M4/M5、整栈/Hosted Login、生产
  容量/容灾/正式合规未完成，没有业务执行或历史/WORM清理。

### 新 run 20261016T091326Z / 0002：旧工程 gate 的启动/退出边界修复（独立验收待随后，安全发布阻塞）

- 开工 783 文件、HEAD/index 与 0001 的 source-after 完全一致。先读真实独立
  verification.json 和完整有界失败日志：quality exit1，Web/diff exit0、安全 exit1；
  不用旧自验或新授权抵销失败，不重复计功逐页生成、selected/sealed、Hold 或批次12。
- 真实 startup bucket HEAD 暂停后发送 SIGTERM，确定性复现 worker 尚未注册信号
  而 signal: terminated。生产修复只提前注册进程取消并让启动 Ready 继承它；保留
  原5秒预检、存储/归档/pass上限及失败关闭认证，无取消的就绪失败仍非零退出。
  取消启动不运行治理 pass、正常关闭/释放锁，不改终态、租约、计划、页或证据。
- worker 归档取消夹具先在唯一临时目录准备真实 v2 数据，再启动真实子进程并在
  object HEAD 同步；原启动护栏及信号后退出上限仍各10秒。并发输出同步捕获，
  Wait/清理单一归属；新增 socket 取消、锁释放和原回执未被误报归档成功断言。
- API drain 的1秒配置、派生退出上限和无空闲流对照不变。仅准备阶段对服务端固定
  store-timeout Unavailable 复用完整同一事件幂等核对；不能换时间戳/身份，不吞
  其他错误。真实 handler 提交后注入未知结果，unary/stream 均须收敛到一份原回执；
  成功流不继承准备TTL，最终退出仍一次测量，耐久身份/序号/hash断言加强。
- signal-fixed-race 三轮、signal-regression-race30 均 exit0；后续完整 quality/Web
  仍以本轮 component 中各自原始命令/退出码为准，专项不替代全量。修复前日志
  startup-signal-before-02 exit1 保留；首个新 fixture 未匹配 bucket 尾斜线的
  startup-signal-before exit1 也保留，不冒充已复现生产缺口。
- 本轮已真实 security-scan exit1：可达/导入包0、1未调用模块警告，gosec162文件/
  34527行/nosec0/同7项（G101×2/G404×1/G103×4）；未改规则/阈值/排除/生成代码，
  release_gate=blocked。README、ADR-0017、api说明/runbook 与此有限行为同步。
- 证据根为本 run rounds/0002/component/；每项有.command/.log/.exit/.started/
  .finished，完整门禁及最终源码哈希另留。没有本轮新增 Web 源码、PG/外部容器、
  业务执行、整栈/Hosted Login、容量/合规验收；独立外部工程验收随后须完整重跑。
  逐页能力不在本轮重复计功，M3 整项/最终整体密封/流式下载与 M4/M5 仍未完成。

### 本 run 20261017T002957Z / 0001：先复验启动修复，再推进分页时间线（独立待验，M3 仍 Partial）

- 开工 785 个源码文件与本轮 source-before 完全一致，HEAD/index 不变；保留全部原有未提交改动。
  先核对 HANDOFF、transition、旧 0001 独立 quality exit1 与 0002 fetch failed/无独立验收，
  没有重放现有信号补丁。原样完整 quality 于 00:38:10–00:49:20 UTC 退出0（含全量 race），
  Web 退出0、安全退出1，期间源码未变；这仅是本轮自验，不洗白旧失败或代替独立 gate。
- M2 有限切片：两个分页时间线先捕获独立深快照，Get/精确 Schema 身份与版本/重建内容/
  链认证和谓词锁外；捕获、取证、分页、一次失败关闭读自审计共享原服务 deadline。
  receipt/map 错绑、坏/缺证据不能被 operation/aggregate 条件隐藏或输出部分页；hot-only
  不是事实，返回保留保护格式。live 顺序/count/cursor 语义不变，不宣称固定成员或完整性。
- 先新增真实 Get 暂停回归复现两个入口持 Store.mu（timeline-lock-before exit1），再做最小
  修复；没有修改旧测试、认证、生产 timeout、扫描规则或工程阈值。新/既有时间线及读事实
  三轮 race、HTTP 权限/脱敏三轮 race、隔离 PG17 独立 pools 的新时间线及全部既有导出
  TestPostgresExport 三轮 race 已退出0；捕获目录并发/其余扩展回归随改后完整门禁验证。
  PG 只用本轮唯一 label、loopback 随机端口、0.75CPU/512MiB/128pids/tmpfs，已按所有权
  清理容器/network/私有环境文件；不访问生产/共享 DSN、旧卷或无关容器。
- 原始命令、退出码及源码前后哈希在本 run `rounds/0001/component/`，改后完整
  quality/Web/security 以及外部独立工程验收分别记录，不能以专项或模型文本替代。
  README、ADR-0025、api说明/OpenAPI 与源码同步；无新路径/scope/公开字段、迁移、
  密封格式或生成 Proto 改动。安全扫描当前仍7项 gosec 和1项未调用模块警告、发布阻塞。
- 未消除全历史物化/认证扫描，旧全量/summary/replay/Console、初页/其余 Hold、最终整块
  密封/认证流式下载、M4/M5 与整栈/容量/合规仍未交付；不重复计功原租约/心跳、取消/
  关联重试、selected/generating/sealed 或认证 Hold，不把批准或回放说成业务执行。

### 本 run 0001 后续独立验收校准（0002 开工核对，不是新交付）

- rounds/0001/verification.json 与 runner-result.json 真实记录：quality 01:34:39–01:56:30 UTC
  exit0，Web/diff exit0、安全 exit1，独立验收 01:57:17 UTC 完成、源码期间未变。
  原始日志在 independent-engineering-2ccjz5mk/；791 文件/HEAD/index 与 0002 开工完全相同。
- 将上方两个已有分页时间线/逐页生成子项校准为已工程验收，保留旧独立失败和旧 fetch
  failed/无 verification 历史；不重复实现信号补丁或计算既有续跑交付。0002 原样启动/
  drain/文件锁专项 race 再次 exit0，不能以此专项替代随后完整门禁或安全批准。

### 本 run 20261017T002957Z / 0002：Console 页与 facets 锁外认证读取（独立待验，M3 仍 Partial）

- M2 最小切片：从独立深快照 committed receipts 锚定，锁外认证 map/正文身份、精确
  Schema/重建内容与链后再评估任何条件、排序/统计。捕获/Get/认证/分页或聚合/一次
  失败关闭读自审计共享一次原服务 deadline，不保存旧快照覆盖并发 control。敏感条件
  从认证原值按条件格式推导，不信任派生 sidecar；保护格式/HTTP 摘要剥离保持。
- 原 tenant claim 和批准的 all-tenant Console 例外不变；保留 newest-first live 页、
  cursor 后 count 及 facets 全匹配/providers 空语义。HTTP 成功/错误均 no-store。
  没有新筛选 URL、scope、公开字段、持久模型/迁移或 ClickHouse/业务执行。
- 先在实际暂停 Get 处复现四个作用域/入口持 Store.mu（console-lock-before exit1）；
  修复后新 Console、HTTP 授权及原读自审计回归三轮 race exit0。专属 PG17 独立 pools
  的新 Console、原分页时间线和全部 TestPostgresExport 三轮 race exit0；Get 暂停时
  同 Store 续期、另一 pool 取消/改 Hold 和策略均保留，已清理本轮唯一 label 的容器/
  network/私有临时凭据，不访问生产/共享 DSN、旧卷或无关资源。
- 新专项首次错误复用了 tenant-a-only 事实 helper，随后一次短预算测试受 append 前
  文件读取影响；各自失败原样保留。只修本轮 all-tenant 事实观察及预先捕获精确夹具
  字节，在真实 Get/append 等待原短预算；没有改旧测试、门禁、认证或生产 timeout。
- 首次完整 final-quality 的普通 Go 全量通过，但 race 的 service 包触发默认 10 分钟
  timeout（当时原归档 DLQ 测试运行 13 秒，栈位于真实 tenant 文件 fsync），退出1，
  源码期间不变。未提高测试/生产 timeout 或改旧测试。仅减少本轮纯认证/条件/预算
  矩阵多余的文件准备 fsync：隔离内存对象保留真实 Put/Get/Save/append 及全部认证；
  四个文件 Get/锁、HTTP 和 PG 回归仍为真实 FileStore。改前后三轮全部用例名称/数量
  一致，新 Console 累计用时 62.35→13.70 秒（不是生产 SLA），专项均 exit0，完整
  失败日志和成本对照 fixture-cost-comparison.json 保留，不以专项抵销 full 失败。
- 第二次完整 final-quality-02 普通 Go 全量通过，race 再次 service 包 10 分钟超时，
  停在另一原测试的空域初始化 fsync；HTTP 原 500 成员上界测试同时因文件存储超时
  返回 503。仍未提高 timeout/减测，只收敛测试准备：通用新建文件夹具以相同三个
  管理 API 在隔离区生成配置与三条原事实，再逐字段合入一次目标 Save，新增完整
  逻辑等价/保留其他终态、Hold、策略的回归；原业务测试仍用文件后端，故障/CAS
  专用 helper 不变。HTTP 500/501 全部断言保留，健康内存依赖跑真实 ingest/Save，
  强化顺序 accepted 兼容响应与持久 receipt 分开核对；真实 Save 等待原默认 deadline
  仍须 503/store_timeout，超计数零 Save，不能把准入/accepted 当账本或容量证明。
- 上述准备层专项首轮被组件工具 300 秒上限中断，不算完整通过；服务包三轮通过，
  HTTP 新断言误把 accepted 兼容响应当 archived，失败原样保留。按既有 README/API
  语义校准自己的该断言，保存真实 committed receipt 的强断言，随后重新验证；无
  原始载荷/密文/搜索摘要/Token 进入新日志，未盲目清理中断留下的未知临时目录。
- README、ADR-0026、api说明/OpenAPI 与源码同步。全部命令/真实退出码/前后源码哈希
  在 rounds/0002/component/；修复后完整 final-quality-03/final-web/final-security
  各自留证，不从专项或模型文本推断完整通过，独立外验仍须随后完整运行。
- 历史物化/认证扫描、ClickHouse facets/全租户完整性、旧全量/summary/replay/兼容
  helper、其余 Hold、最终整块密封/认证流式下载、M4/M5 和整栈/容量/合规仍未完成。
  不重复交付租约、心跳、取消/关联重试、selected/generating/sealed 或认证 Hold；
  M3 整体仍 Partial，安全扫描非零继续阻塞发布，不自授安全豁免。

### 本 run 0002 后续独立验收校准（0003 开工核对，不是新交付）

- verification.json / runner-result.json 和 independent-engineering-8rue0lqm 原始日志记录：
  独立 quality 于 03:56:56–04:15:10 UTC exit0，Web/diff exit0、安全 exit1，04:15:40 UTC
  完成、期间源码不变。801 文件/HEAD/index 与 0003 开工相同，未发现源码漂移。
- 0002 component/final-quality、-02、-03 均为真实失败（前两次 race、最后一次普通
  service 包默认10分钟超时；-02 还有 HTTP 批次 store_timeout），不能宣称完整自验
  通过。此校准仅承认随后独立工程通过，不覆盖那些失败或安全发布阻塞。

### 本 run 20261017T002957Z / 0003：先收敛真实 query gate，再推进操作摘要（独立待验，M3 仍 Partial）

- 原样完整 baseline-quality 04:20:35–04:37:33 UTC exit1：API/worker race 通过，service
  的 query 锁测试在原5秒调用预算内未恢复，随后 chronology 准备的真实 tenant fsync
  处触发包默认10分钟 timeout；源码期间未变，未盲目重放已有启动信号补丁。
- 先修同一块查询夹具/锁 oracle：两个纯 scope/tamper 用例采用生产内存状态后端；
  锁回归保留真实 FileStore 源和原5秒调用上限，control/trail 不再要求无关文件 fsync
  吞吐。补读结果身份/成员、一次真实读事实、并发 control 保留及所有 reader/writer
  退出断言；文件 restart/expiry 和按坐标归档续页仍保留文件后端。新增 file/memory
  逐字段原事件/回执/Schema/流/段/checkpoint 与两页/读事实等价回归，未删旧断言。
  修复后完整 preparation-quality 04:40:18–04:58:59 UTC exit0（含全量 uncached race），
  期间源码不变，之后才实现新摘要；未提高测试/生产 timeout、阈值或减测。
- M2 有限切片：Operation 复用 receipt 锚定、精确 Schema/内容/链认证源，深快照后
  Get/认证/谓词/聚合锁外；捕获到一次读自审计共享原服务 deadline，失败零摘要。
  保留 live count、最早/最晚发生时间和排序去重 outcomes，不套用旧全量时间线的
  10000条上限；10001条语义回归不是容量证明。HTTP 原租户/权限与全部 no-store，
  坏证据不能隐藏或计入部分统计，不回写旧快照覆盖终态、租约、Hold 或策略。
- 新回归先在真实 Get 暂停处复现 Store.mu 阻塞与热/归档 operation/outcome/time/
  内容污染接受（summary-before exit1）；修复后的新/旧读自审计、查询 oracle 与 HTTP
  三轮 race exit0。真实隔离 PG17 独立 pools 的新 summary、原 timeline/Console 和全部
  TestPostgresExport 三轮 race exit0；同 Store 续期、另一 pool 取消/改 Hold/策略均
  保留。只清理本轮唯一身份/label 的容器/network/私有临时凭据，见 postgres-isolation。
- README、ADR-0027、既有 ADR 后续范围、api说明/OpenAPI 与源码一致；无新字段、
  路径/scope、持久模型、SQL/Proto 或密封格式。命令/源码前后哈希/退出码均在本轮
  component；新摘要改后完整 final-quality、final-web、final-security 和随后独立外验
  分别记录，不从专项/模型文本推断全量或发布通过，安全扫描非零继续阻塞发布。
- 全历史物化/认证扫描、旧全量/replay/restore/兼容 helper、其余 Hold、ClickHouse
  facets/完整性、最终整块密封/认证流式下载、M4/M5 与整栈/容量/合规仍未完成。
  不重复交付租约/心跳、取消/关联重试或 selected/generating/sealed，不执行业务恢复。

### 本 run 20261017T002957Z / 0007：人工核对后的有界工程恢复（不新增产品能力）

- 原窗口截止不变，0004–0006 的 fetch failed/runner exit1 与连续失败历史保留，不重放
  外部写入或既有信号补丁。0007 开工 810 文件逐个哈希、HEAD/index 与 source-before
  相同；对比最后独立通过 0003，恰为三个测试修改及两个新夹具，无缺失或额外漂移。
- 已逐项核对五处 scheduler/timeline 夹具：内存调度仍在每次成功 Save 观察共享 2/1
  上限、每个原任务一次终态与活跃元数据退役，保留实际暂停 Put/双 Service/原 10s
  窗口和 cleanup join。认证矩阵仍保留精确证据/Get/append/deadline/隔离/顺序/owned
  断言；已有真实 FileStore 源锁、file/memory 原字节/页/读事实对照和独立 PG pools
  回归均未删除。不把这些内存 oracle 称为文件耐久性、真实跨副本或容量证明。
- 0006/verification.json 是停机后新增真实检查，不是失败 runner 成功：完整 quality
  exit1，普通测试 internal/store/TestFileHotColdCapacitySlope 在 5000 对象准备的真实
  FileStore 目录 Fsync 处触发默认 10 分钟包上限，未进入 race/build；Web/diff exit0、
  security exit1。原 proof 及本轮 capacity-before 复现保留，不拿专项通过抵销失败。
- 本轮只调整同一文件容量夹具的准备：相同真实状态/账本、100/1000/5000 个 4 KiB
  与 100 个 8 KiB 对象、同一 t.TempDir 文件系统、原 Walk/stat 及全部阈值不变；独立
  真实 FileStore handle（至多 16 个，仅测试准备预算）写互斥 key，先 Ready，所有原 Put/fsync 成功且 writer 全部
  退出后，新 handle Get 每个原 key 核对精确字节再测量。新增串行/并发真实字节测量
  对照、实际 Put 边界的精确次数/分区/并发上限与失败取消/在途 join 回归；不用
  内存/直接 WriteFile/虚拟大小/跳过 fsync/新 timeout 替代真实测量。生产代码不变。
- 首个本轮完整 quality 普通 Go 的 store 包已通过（98.427s），但 API drain 无空闲流
  对照在新写入准备的原 30s guard 内取消，故整条 exit1、未运行 race/build，原日志
  quality 保留。没有再加等待/盲重放旧补丁：改为在启动被测二进制前，经真实认证
  gRPC 完成原事件的文件/归档提交并关闭原 writer；被测进程仍以原 1s store/drain
  配置认证同一原请求，返回精确同 hash/stream/status 的 duplicate 回执后才测退出。
  原长寿命 JWT/无 timer 空闲流、无空闲流对照、退出上限与单份原回执均保留；新增
  实际 Put 暂停时准备不得返回/释放锁、恢复后认证原文件字节回归，不替代新摄入
  在途提交/未知结果回归。首个新容量对照的 WrittenAt oracle 失败及专项工具中断
  同样留证，修后完整结果以 component 后续命令为准，不用定向通过抵销失败。
- 第二次完整 quality-02 普通 Go 全量通过，但 uncached race 真实失败：API holder
  在 bootstrap 阶段未于原 10s 内退出；gRPC 包在重复控制域初始化 fsync 处触发原
  10min 包上限；两个 HTTP 归档 Get 超时回归的真实摄入被提前启用的 150ms 预算
  拒绝。原日志和源码前后哈希保留，不能称该轮 full gate 通过。
- 继续只收敛这些准备边界：锁夹具预先生成真实域/目录，仍从实际 flock 和精确当前
  holder PID 同步，保留原信号后 10s，退出后重开核对域/零证据。gRPC 新文件域把
  相同三个真实管理 API 产生的目录/三条事实一次 Update 合入自有字段，保留其他
  control 和空 hot/cold 文件；实际摄入仍真实文件 Save/append/fsync，注入故障/CAS
  原 Save 序列不变，新增文件重启/完整逻辑等价对照。HTTP 保留真实 HTTP 摄入及
  同一文件，在实际归档热淘汰、准备 handler 全部退出并重开后才启用原 150ms 读
  预算，保留原黑洞 Get/503/脱敏/指标/耗时上限。生产代码、规则、认证和预算不改。
- quality-03 普通 Go 全量通过，race 的 API/worker/gRPC/HTTP/store 均通过，但 service
  包仍在原 100 健康事件归档回归的摄入准备/真实 ledger append fsync 处耗尽 10min。
  不改该用例的 100 成员、文件后端/热冷分支、调用次数或包上限，不以定向通过抵销
  此失败；完整最终复验的真实结果另存 component 后续日志，历史成本仍 Partial。
- README、ADR-0017 与 api/README 同步验证边界，无新线面契约/持久模型/迁移。命令、
  真实结果与源码绑定放在 rounds/0007/component；完整 quality（含全量 uncached
  race）、Web、安全扫描及 diff 分别留证，随后独立 controller 还须完整复验。
  安全非零继续阻塞发布，不以本轮自验或旧三轮工程通过推导当前发布授权。
- 本轮不交付/重复计功租约、心跳、取消、关联重试、认证 Hold、selected/pages/sealed、
  分页时间线、Console 或操作摘要。剩余 M1/M2/M3 待办与 M3 Partial 保持，未新增
  整栈、业务恢复执行、生产容量、掉电耐久性、正式合规或安全发布通过。

### 本 run 20261017T002957Z / 0008：重复父目录同步的有界修复（不新增产品能力）

- 按人工 REPAIR 核对 0007 独立 quality exit1、Web/diff exit0、安全 exit1；这次不是
  fetch-only 失败，0007 自验 quality-04 exit0 不能覆盖它。当前 814 文件/HEAD/index
  精确等于其 source-after，原有未提交改动全部保留，不重放外部写入或启动信号补丁。
- 原导出顺序/20 事件流折叠场景在改生产前的定向 race 通过，未复现原 running 失败；
  已编译原源码的完整 service uncached race 退出0、544.925s（仍接近原10min包上限）。
  逐例记录显示 10000 次真实读自审计170.84s、并发完整性58.48s，以及保留原10/20/30s
  deadline 的回归均贡献累计成本；不能把原超时瞬时栈当作死锁或宿主磁盘唯一根因。
- 生产最小修复仅合并 AtomicDataFile/ledger append 的重复父目录 fsync：实际捕获
  fd 同步一次，默认叶路径仍安全重开/关闭并核对 inode，所需祖先保持完整原顺序；
  文件 fsync、O_EXCL temp、rename、未知残留/结果未知和错误分类不变。无跨调用缓存、
  目录链省略、归档同步优化或持久格式变更。新计数回归先确定性报2次而失败，修后1次；
  全链/逐边界失败、关闭fd、父目录替换和rename后未知结果覆盖，原安全回归全部保留。
- 相同原两场景的实际 strace 同步调用561→450（无原始内容/owner日志），只证明去掉
  111次冗余同步；时延随运行变化，不能据此宣称吞吐/容量保证或所有历史成本风险消失。
  原文件/样本/次数/顺序断言和5s等待/生产deadline/默认包超时均未改；新增诊断仅记录
  准备/查询/创建/真实Get/Put/终态的安全阶段、计数和耗时，超时再捕获合法私有阶段
  类别但不输出其owner、绑定、对象或内容。不用更多内存夹具替代文件证明。
- 证据位于 rounds/0008/component/：initial-source-check、service-cost-before、
  cost-diagnosis、observed-scenarios-before/fixed、duplicate-parent-regression-before、
  durable-sync-boundaries 等均保留真实命令/结果；完整 quality/Web/security/diff 及
  后续独立工程门禁须分别读取其实际日志，不从本记录或专项推断完整通过。
- 首轮完整 quality 于16:23:31–16:59:43 UTC真实退出1：普通 service 包先报 detached
  Put计时14.177s（旧timer实际包围完整Ingest），后在AddSource空域的真实父目录fsync
  触发10min包上限；store也在原5000对象/16 writer准备的实际文件/目录fsync耗尽10min。
  未进入全量race/build，不可称完整通过；Web/diff退出0、安全退出1，同7项gosec/1模块
  警告，817文件验收期间稳定。已通过的专项不抵销这次失败，也不声称磁盘是唯一根因。
- 继续只修同一真实测量边界：新文件反例在实际第二次ledger append（Put之后）暂停
  1.25s，先确定性复现旧oracle总计6.482s失败，实际Put5.003s、其后提交1.373s；再从
  实际Put进入/context Done计时，原5秒及-500ms/+1s容差不变，另核对精确deadline、
  一次Put和原错误/回执断言，文件重启仍须indexed/非archived。没有把文件夹具改内存、
  放宽超时、删样本或跳过提交；包总成本仍Partial。相同导出文件专项三轮完整重跑已
  exit0，先前300s组件工具中断原样保留，不能称那次也通过。改后完整quality-02/Web/
  security/diff须分别看真实命令结果，不盲目重跑未改代码碰绿，不预先声称通过。
- README、ADR-0017、api说明同步；无线面/SQL/证据格式/权限/预算/终态/租约/Hold/自审计
  变更。原 running 失败未被确定性重现，包总成本风险未全部消除，稳定性根因闭环仍
  Partial，M3及其余M1/M2/M3待办不勾新完成项；安全非零继续阻塞发布，无业务执行。

### 本 run 0008 后续独立验收校准（0009 开工核对，不是新交付）

- 0008 verification.json / runner-result.json 与 independent-engineering-mngq6io9 原始日志
  记录：独立 quality 于 17:35:05–17:45:19 UTC exit0，Web/diff exit0、安全 exit1，
  2026-10-17 17:45:45 UTC 验收完成、源码期间未变。0009 开工818文件/HEAD/index与
  其 source-after 完全相同，不用旧自验抵销其之前失败，也不把成本修复重计产品能力。

### 本 run 20261017T002957Z / 0009：操作回放的锁外认证取证（本轮自验/独立验收分别留证，M3仍Partial）

- 原样完整 baseline-quality 于17:48:03–17:58:02 UTC exit0（含全量uncached race），
  Web exit0、安全exit1，源码期间未变；先核对旧原始失败与已存在启动修复，不盲重打。
- M2有限切片：ReplayOperation复用receipt锚定、精确Schema/重建内容与链hash认证源，
  深快照后锁外取证、认证、选择/排序/本地fold；捕获至一次原读自审计共享原deadline。
  坏/缺committed证据不能被条件隐藏或变成部分state；不回写旧快照覆盖并发control。
  保留10000匹配上限、原chronological/nil删除/最后事件sequence，HTTP全部no-store。
- 已有恢复预演/创建的空actor委托继承认证源，原零读事实、独立创建提交及审批不是
  执行保持；坏源无提案/新pending run。没有新业务scopes、版本计划或执行回执，不
  把整个恢复创建调用冒充一次预算/原子证据绑定。旧全量/aggregate replay和恢复整体
  流程、全历史物化/认证扫描与M3其余项仍未完成，导出既有阶段不重计功。
- 新回归先在实际FileStore Get暂停处确定性复现mutex阻塞，并复现热/归档operation/
  changed_fields等污染接受（replay-before exit1）；改后新服务/HTTP及原读自审计、
  restore静默兼容等专项三轮race exit0（replay-expanded）。文件重启、原上限、owned/
  live fold、精确数字/Schema/receipt、Load/Get/append共享预算、未知追加/撤销覆盖。
- 专属PG17独立pools的新replay、已有summary/timeline/Console和全部TestPostgresExport
  三轮race exit0（replay-postgres-02，45个顶层PASS/无skip，29.546s）。Get暂停时同Store
  续期、另一pool取消与Hold/策略写均保留，未覆盖终态。首次隔离脚本端口元数据处
  TypeError/exit1，尚未执行Go测试；原失败保留，修正私有准备脚本的端口护栏/本机
  隔离桥接后才重建全新唯一资源验证。两次本轮容器/network/私有凭据均按owner核对
  清理，无生产/共享DSN、旧卷、宿主防火墙或无关容器操作。
- README、ADR-0028及关联ADR后续范围、api说明/OpenAPI和本清单同步；无新路径/scope/
  DTO、持久模型、SQL、密封格式或Proto生成字节。全部命令/真实退出码/源码绑定在
  rounds/0009/component；改后完整quality/Web/security/diff与随后独立controller门禁
  分开记录，不从专项或本文预先推断通过；安全非零继续阻塞发布，不自授豁免。

### 本 run 0009 后续独立验收校准（0010 开工核对，不是新交付）

- 0009 verification.json / runner-result.json 与 independent-engineering-ln0e2c0o 的原始命令
  记录：quality 18:34:14–18:44:31 UTC exit0，Web/diff exit0、安全 exit1，2026-10-17
  18:44:59 UTC 完成、源码期间未变。0010 开工824文件/HEAD/index与独立 source-after
  逐一相同；不把旧run0002 fetch failed/无verification洗白为独立通过，也不重交回放。

### 本 run 20261017T002957Z / 0010：导出创建/新后继的认证 Hold 预检（独立待验，M3仍Partial）

- 原样完整 baseline-quality 于18:47:14–18:59:15 UTC exit0（含全量uncached race），
  baseline-Web exit0、源码期间未变；核对旧真实API准备store_timeout/worker未达HEAD/
  启动持锁SIGTERM失败与已有修复，不盲重放补丁或未知外部写入，保留全部原未提交改动。
- 预检复用receipt/map身份、精确Schema/重建内容/链认证，之后才评估原查询/当前Hold；
  bound/legacy敏感条件从认证原值推导，搜索sidecar缺失/值/格式不能隐藏真正受Hold
  覆盖的证据。本租户key或receipt正文错绑均失败关闭，坏/缺证据不作空选择。
- 允许预检仅返回请求内活跃Hold集HMAC，新任务同原共享control CAS在128/32准入前
  复核；变化409/零创建事实，CAS重跑仍核对原观察，不隐式刷新。无关tenant/policy
  不冲突或被旧快照覆盖；既有后继仍仅读安全幂等摘要，终态不回退。原query/一次读
  自审计、锁外Hold Get/认证、拒绝事实或新任务Save共用一次原create/retry deadline；
  当前Hold/后续下载/lease/阶段fencing与2/1运行预算不变。HTTP创建补全认证前no-store。
- 新回归先在真实第二次Get与Save冲突处复现stale Hold准入，并复现热/归档sidecar
  绕过（preflight-before-02 exit1）；改后三轮新/相关服务与HTTP race exit0
  （preflight-expanded-02），另以真实file restart专项三轮验证保存的故障确实存在，
  原receipt/Schema不改（preflight-file-restart exit0）。没有改旧测试、生产timeout、
  认证、门禁阈值或删断言；纯认证/预算矩阵不冒充文件耐久性。
- 专属PG17独立pools的新预检与全部TestPostgresExport及已有timeline/Console/summary/
  replay三轮race exit0（preflight-postgres-02，51个顶层PASS/无skip，28.161s）。源Get
  暂停时同Store续期、另一pool取消/改Hold和策略可提交，旧观察不覆盖终态/control。
  只清理本轮两个唯一身份/label、loopback随机端口、0.75CPU/512MiB/128pids/tmpfs的
  容器/network/私有凭据；原生产/共享DSN、旧卷或无关容器均未访问。
- 早期失败原样保留：新增测试调用方法名错误的编译失败、文件夹具误用control-only
  快照读取cold receipt，以及首次PG故障夹具试图全量重写原账本而被拒。分别修正自己
  的调用、改用完整Snapshot、仅在实际UpdateTenant hot边界写测试自有sidecar故障；
  加强重启/跨pool故障确实保留与原cold receipt/Schema完全不变的断言，不改生产存储。
- README、ADR-0029/关联范围、api说明/OpenAPI和本清单同步；无新公开字段、路径/scope、
  SQL/持久格式或Proto变更。命令/真实退出码/前后源码哈希在rounds/0010/component，
  改后完整quality/Web/security/diff和随后独立controller验收分别留证；专项/本文不是
  full gate或安全发布通过声明，安全扫描非零继续阻塞发布，不自授豁免。
- 原QueryEvents初页自身、全历史物化/认证扫描、旧全量/aggregate replay、导出最终
  整块合并/密封和认证流式下载、其余跨副本准入、M4/M5、整栈/Hosted Login与生产
  容量/正式合规仍未完成；不重交租约/心跳/selected/pages/sealed或执行业务恢复。

### 本 run 0010 后续独立验收校准（0011 开工核对，不是新交付）

- 0010 verification.json / runner-result.json 与 independent-engineering-g188nt2n 原始日志：
  quality 于19:35:07–19:45:04 UTC exit0，Web/diff exit0、安全 exit1，2026-10-17
  19:45:29 UTC 完成、源码期间未变。0011 开工831文件/HEAD/index与其 source-after
  逐一相同；只校准 ADR-0029 子项，不重复交付或洗白旧 fetch failed/无 verification。

### 本 run 20261017T002957Z / 0011：普通查询 reference 初页的认证前置（自验/独立验收分别留证，M3仍Partial）

- 原样完整 baseline-quality 于19:48:07–19:56:59 UTC exit0/QUALITY PASS（含全量
  uncached race），期间源码不变。重新核对旧三处真实失败、现有启动信号/Ready取消/
  真实边界修复，不盲重打补丁或未知外部写入，保留所有原未提交改动。
- 只改普通事件查询的 ledger-reference 初次选择，复用 Console 已有 owned/锁外认证
  扫描；receipt/map、精确 Schema 身份/版本、重建内容与链认证后才评估所有筛选。
  本租户坏/缺 committed 证据不藏为空匹配/部分成员，hot-only/journal 不是事实；
  其他租户不 Get/认证。敏感条件从认证原值按自身 bound/legacy 格式重算，不信任
  sidecar；新初页加密 searchable 因而支持双格式互查，返回保护值不替换为明文。
  旧 helper 原同格式/密文限制仍直接回归，不降低认证或偷偷放宽原错误断言。
- 保留原10000匹配身份/4MiB/tenant16/15分钟及 HMAC scope/source/subject/filter/
  page-size 绑定，Console 不新增上限；旧保存成员/续页不重选、已服务前缀不重扫，
  ClickHouse 不新增 fallback。捕获至成员Save/一次原读事实共享原deadline，追加后
  取消也零结果；未知已提交尝试事实/运行元数据不当回滚，不覆盖并发终态/control。
  原导出当前Hold/CAS预检、lease/阶段fencing与128/32、2/1预算仍独立权威。
- 新回归先确定性复现热谓词隐藏、receipt/Schema错绑、missing indexed body及sidecar
  误选（initial-before exit1）；改后新/相关服务与HTTP三轮race exit0（initial-expanded）。
  原10000/10001边界、Console不继承上限、热/冷/明文/加密/精确数字/双格式、scope、
  owned目录/输出、Get/append共享deadline、未知追加/取消后零结果覆盖。首次文件
  专项误用control-only helper观察hot/cold而失败，修正本轮oracle为真实完整Snapshot，
  加强故障确实保留及原receipt/Schema不变；原失败日志不删，生产存储/超时不改。
- 专属PG17独立pools的新初页与全部PostgresExport及既有timeline/Console/summary/
  replay三轮race exit0（initial-postgres）；真实持久sidecar/谓词故障、跨pool固定成员
  续页、Get暂停时同Store续期/另一pool取消与Hold/策略保留覆盖。唯一身份
  audit-gov-r0011-4679d33d3fb8，loopback随机端口/0.75CPU/512MiB/128pids/tmpfs；
  容器/network/私有凭据按owner核对均已清理，见postgres-isolation.result.json。
- README、ADR-0030/相关后续边界、api说明/OpenAPI与本清单同步；无新公开字段、
  路径/scope、SQL/持久模型、密封或Proto格式。命令/实际退出码/前后源码哈希在
  rounds/0011/component；改后完整final-quality/Web/security/diff与随后独立controller
  工程门禁分别留证，不从专项或本文预先推断完整通过，安全非零继续阻塞发布。
- 历史物化/认证扫描、旧全量/aggregate replay、ClickHouse facets/完整性、导出最终
  整块合并/密封及认证流式下载、其余准入、M4/M5、整栈/Hosted Login、生产容量与
  正式合规仍未完成。现有租约/心跳、owner取消/关联重试、selected/pages/sealed/
  认证Hold与原锁外Get/固定成员不重复计功，不调用业务系统或把审批表述为执行。

### 本 run 0011 后续独立验收校准（0012开工核对，不是新交付）

- 0011 verification.json / runner-result.json及independent-engineering-vuheldft原始命令记录：
  quality于20:26:06–20:36:20 UTC exit0，Web/diff exit0、安全exit1，2026-10-17 20:36:46
  UTC完成、期间源码不变。0012开工837文件/HEAD/index与source-after逐一相同；原独立
  失败和fetch failed/无verification历史仍保留，不重交reference初页或旧启动补丁。

### 本 run 20261017T002957Z / 0012：旧全量时间线与聚合回放的认证读取（独立待验，M3仍Partial）

- 原样完整baseline-quality于20:40:02–20:50:14 UTC exit0/QUALITY PASS（含全量uncached
  race），期间源码不变。确认上一轮独立工程通过/安全失败及专属PG清理记录，不盲目
  重放补丁、未知外部写入或改动原未提交现场。
- M2有限切片：旧OperationTimeline/AggregateTimeline与服务层ReplayAggregate复用receipt
  锚定、精确Schema/重建内容/链认证源，独立深快照后锁外取证，认证先于所有谓词/排序/
  本地fold；捕获至一次原读自审计共享原deadline，失败或追加后取消零结果，不回写旧
  快照覆盖并发终态/lease/Hold/control。移除已无调用者的锁内eventsFor包装。
- 保留10000个匹配事件的拒绝上限、不截断成功，operation发生时间/序号/ID与aggregate
  版本/ID升序，聚合回放nil删除/最后折叠sequence（不是最大值/水位）及空actor零事实
  策略。HTTP仍走原分页入口，ReplayAggregate当前无生产调用/HTTP路由，不增加执行入口。
- 新回归先在真实FileStore Get暂停处复现三个入口阻塞续期/取消，并复现热/归档谓词/
  aggregate_version/changed_fields等污染接受或隐藏（legacy-before exit1）。改后新/相关
  服务三轮race exit0（legacy-expanded）；精确Schema/receipt、hot-only/tenant/owned/
  live顺序/原匹配上限、Load/Get/append同预算、未知追加/取消零结果及真实文件重启覆盖。
- 专属PG17独立pools的新legacy与全部TestPostgresExport及既有initial/timeline/Console/
  summary/replay三轮race exit0（legacy-postgres，60个顶层PASS/无skip，29.452s）。实际
  Get暂停时同Store续期、另一pool取消/改Hold/策略保持；独立pool可见的持久谓词故障
  不能过滤隐藏，原receipt/Schema不改。唯一身份audit-gov-r0012-1a2f8e0abb39，loopback
  随机端口/0.75CPU/512MiB/128pids/tmpfs；容器/network/私有凭据按owner核对已清理，
  见postgres-isolation.result.json，不访问生产/共享DSN、旧卷或无关资源。
- README、ADR-0031/相关后续边界、api说明/OpenAPI及本清单同步；无新公开字段、路由/
  scope、SQL/持久模型、密封或Proto格式。命令/真实退出码/前后源码哈希在rounds/0012/
  component；改后完整final-quality/Web/security/diff与随后独立controller工程验收
  分别留证，不用专项或本文预先推断全量/发布通过，安全非零继续阻塞发布。
- 全历史物化/认证扫描、其余兼容helper/历史成本、ClickHouse facets/完整性、导出最终
  整块合并/密封和认证流式下载、其余准入、恢复整体版本计划/scopes/执行回执、M4/M5、
  整栈/Hosted Login、生产容量与正式合规仍未完成；不重复计功已有租约/心跳、owner取消/
  关联重试、selected/pages/sealed/当前认证Hold或业务恢复执行。

### 本 run 0012 后续独立验收校准（0013开工核对，不是新交付）

- 0012 verification.json / runner-result.json及independent-engineering-_0nmx2fg原始命令：quality
  于21:21:24–21:30:56 UTC exit0，Web/diff exit0、安全exit1，2026-10-17 21:31:21 UTC完成，
  期间源码不变。0013开工842文件/HEAD/index与独立source-after逐一相同；原旧run独立失败
  和fetch failed/无verification历史仍保留，不重交旧全量时间线/聚合回放或启动信号补丁。

### 本 run 20261017T002957Z / 0013：按坐标读取的身份/精确目录边界（独立待验，M3仍Partial）

- 原样完整baseline-quality于21:33:20–21:44:44 UTC exit0/QUALITY PASS（含全量uncached
  race），baseline-Web exit0、安全exit1，期间源码不变。核对旧API准备store_timeout、worker
  未达真实object HEAD和持锁启动SIGTERM失败与现有修复；不盲目重放补丁/未知外部写入。
- M1/M2有限语义切片：单事件/固定成员续页/投影实际页先核对请求tenant/event与receipt
  正文，错绑在源Get前失败关闭；共用resolver核对精确Schema正文tenant/id/version，再
  认证原内容/链，停用历史定义仍可读、不取最新版本替代。单事件audit append成功后复核
  原context，取消/超时零事件，不撤回耐久尝试事实或声称字节交付。原成员/count/游标/
  来源/TTL/配额、锁外Get/认证与一次deadline/读事实不变，不重选或重扫已服务前缀。
- 新回归先确定性复现错绑Schema被三个实际输出入口接受、错误lookup key下另一有效
  event/receipt被读取，以及耐久append后取消仍输出（coordinate-before exit1）；改后
  新/相关服务及HTTP三轮race exit0（coordinate-expanded），包含真实文件重启、精确停用
  版本/保护大整数/owned输出、捕获目录并发、Get/append共享预算和未知/取消零结果。
- 专属PG17独立pools的新CoordinateEvidence与全部PostgresExport及已有initial/legacy/
  timeline/Console/summary/replay三轮race exit0（coordinate-postgres）。真实持久目录故障
  可见、原成员续页不重选；实际Get暂停时同Store续期、另一pool取消及Hold/策略/目录
  保留，晚owner不能发布。仅本轮唯一身份/label、loopback随机端口、0.75CPU/512MiB/
  128pids/tmpfs资源，按owner核对清理容器/network/私有凭据；不访问生产/共享DSN或旧卷。
- README、ADR-0032、api说明/OpenAPI与本清单同步；无新公开字段、路由/scope、持久模型、
  SQL/密封或Proto格式。原代码/测试/门禁规则/认证/超时/阈值均未弱化，命令/真实退出码/
  前后源码哈希在rounds/0013/component；改后完整quality/Web/security/diff和随后独立
  controller验收分别留证，不用专项/本文冒充独立验收或安全发布，安全非零仍阻塞发布。
- 旧跨租户详情helper、其余历史物化/扫描、ClickHouse facets/完整性、完整M1语义矩阵、
  导出最终整块合并/密封/认证流式下载、其余准入、M4/M5与恢复整体版本/scopes/执行回执、
  整栈/Hosted Login、生产容量/正式合规仍未完成；不重复计功既有租约/心跳/owner取消/
  关联重试/selected/pages/sealed/当前认证Hold，不调用业务系统或把审批表述为执行。

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
- 批次 2–12 本机完整工程门禁已通过；随后推进其余查询/统计与治理作业容量；
  PostgreSQL v1 兼容对账仍物化旧快照，容量部署须完成 v2 cutover。
  broker 迁移/重建不能根据旧确认声称新目的地已完整。
- 安全工具已安装并运行：可达漏洞已修复，gosec 已从 62 项减少为 7 项；剩余
  G101/G404/G103 及模块警告继续保留，源码用途核对不等于豁免，安全发布门禁失败；不能继续把“工具未安装”
  作为当前状态，也不以零可达漏洞或工程门禁通过替代整套扫描。
- 批次 11 租约/心跳、批次 12 所有者取消/关联重试及真实 PG 边界回归、完整工程
  门禁已通过；runner 0001 追加的密封检查点限于终态提交续跑，runner 0002 仅修复其
  当前 Hold 完成门禁；0004 推进详情/下载放行的相同认证与锁外门禁；0007 增加固定成员 selected 计划并继续整块生成。新 run 20261016T091326Z/0001 继续实现逐页生成切片，
  验收状态见上方待办及该轮原始证据。最终整体合并/密封检查点、流式大文件下载、其余
  细分失败类别、分块下载、其余跨副本预算，
  M4 合规生命周期和 M5 版本绑定业务执行均仍未实现；不将审批表述为恢复执行成功。
