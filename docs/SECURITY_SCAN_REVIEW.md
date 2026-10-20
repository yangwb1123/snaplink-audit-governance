# 安全扫描整改记录

日期：2026-10-16。状态：整改中，**安全发布门禁尚未通过**。
此记录是检查证据与待办，不替代 README、Accepted ADR 或 api/**。

## 批次 8 整改（历史基线）

以批次 7 的 gosec 62 项为基线，未修改扫描规则、排除项、严重性阈值或
生成代码，未添加 `nosec`。已处理的 26 项：

| 规则 | 数量 | 处理与回归 |
|---|---:|---|
| G112 | 2 | consumer/replay 指标服务使用私有 mux；5s header、10s read/write、60s idle、16 KiB header；只注册 GET /metrics。独立 TCP 回归验证不完整 header 被关闭，未调用指标读取；准备完成后才启动服务读计时。 |
| G115 | 12 | 保留期在就地校验 1–36500 天后转换；聚合签名拒绝负版本/计数与非正序号，创建前校验 checkpoint；投影写在编码/SQL 前拒绝非正序号。指标负时长不转换，计数使用非负来源；gRPC 用协议类型迭代已限量批次；e2e schema version 在拨号前检查 1–MaxInt32。 |
| G104 | 12 | DLQ 状态的追加/短写/修复/临时文件/锁失败路径合并原错误与关闭错误。锁 descriptor 只关闭一次；成功关闭后才更新内存 mark。关闭失败结果未知，不能据此提交 DLQ offset，也不推断已写字节回滚；下轮允许幂等重试。 |

合法聚合 v2 的二进制签名输入不变，黄金摘要覆盖 MaxInt64 序号；无需重签名、
重建账本或迁移。坏的已提交聚合仍返回 `valid=false` 诊断并追加读自审计，
不是提前改成 409。关闭失败回归在真实 flock 边界注入，覆盖 v1/v2、追加/重写、
重试与跨租户隔离。Store/归档/API 的既有操作超时及认证没有放宽。

指标超时不是认证、连接数准入或速率限制。指标监听仍由部署配置控制，应限制为
监控网络；没有在本批开放 debug/admin endpoint 或配置公共 TLS 网关。

## 批次 8 保留的 36 项（历史基线）

下表按文件/规则列出全部剩余扫描项（同位置的不同 unsafe 调用分开计数）。
**没有因为路径来自运维配置、文件有父目录限制或代码已生成就自动豁免。**

| 文件 | 规则/数量 | 当前处理状态 |
|---|---|---|
| `internal/fsutil/fsutil.go` | G304 ×2 | 待核对调用来源、父目录/symlink 与文件系统信任边界。 |
| `internal/grpcapi/tls.go` | G304 ×1 | 待核对证书路径、文件所有者及部署挂载权限。 |
| `internal/kafka/replay_state.go` | G304 ×4 | 待核对加载、分类、lock 与 legacy 状态路径的完整边界。 |
| `internal/store/hotcold_files.go` | G304 ×5、G302 ×1 | 待核对寻址、symlink/祖先目录及新旧文件权限；不删除账本换成功。 |
| `internal/store/hotcold_split.go` | G304 ×2 | 待核对迁移/加载路径与目录边界。 |
| `internal/store/query_snapshot.go` | G304 ×2 | 待核对元数据寻址与过期回收范围；回收不能删除事实。 |
| `internal/store/state_lock.go` | G304 ×2 | 待核对独占锁路径及所有权；不弱化单写者约束。 |
| `internal/store/store.go` | G304 ×4、G302 ×2 | 待核对控制文件、备份/临时文件路径和权限；兼容部署须明确迁移方式。 |
| `internal/store/trail.go` | G304 ×1、G302 ×3 | 待核对自审计文件、压实临时文件及组读权限。 |
| `internal/runtimeconfig/runtimeconfig.go` | G101 ×2 | 已核对命中值是 `AUDIT_SIGNING_SECRET`/`AUDIT_ALLOW_DEV_SECRETS` 环境变量名称，不是凭据；本批仍保留扫描告警，未添加豁免。 |
| `internal/store/store.go` | G404 ×1 | 已核对 rand 仅用于 CAS 重试退避，不生成 token、密钥、签名或安全标识；本批仍保留扫描告警，未添加豁免。 |
| `api/proto/audit.pb.go` | G103 ×4 | protoc 生成的 protobuf runtime unsafe 调用；须结合生成器/依赖与 proto-sync 评审，未手改或排除生成代码。 |

汇总：G304 23、G302 6、G103 4、G101 2、G404 1。
文件权限与路径整改必须兼顾既有账本、WORM、备份、恢复 journal 和租户隔离，
不批量 `chmod` 不相关数据，也不以自动清理覆盖未知历史残留。

## 批次 8 验证边界

- 新边界及相关回归定向 `go test -race -count=3` 已通过（9 个包）；不代替完整 quality。
  完整日志：`/tmp/audit-productization-batch8-boundaries-race-final.log`、
  `/tmp/audit-productization-batch8-quality-final.log`、
  `/tmp/audit-productization-batch8-web-check-final.log`、
  `/tmp/audit-productization-batch8-security-final.log`（各有对应 `.exit`）。
- 首轮普通测试因新 latency fixture 的 Authenticator 类型错误失败，已修正。
  首轮宽正则三轮测试误纳入旧 `metrics-probe` 重复 sql.Register fixture 而失败；
  三轮复验精确选择本批指标测试，旧 pool fixture仍由完整单轮门禁覆盖，未删改它。
- 最终完整 `python3 cli.py quality` 退出 0、`QUALITY PASS`：普通 Go 测试 607.4s、
  全量 race 700.0s、vet/build、Python 156 测试/1 可选跳过与全部工程门禁通过；
  未取消 race 或降低阈值。使用 `/usr/bin/python3`（PyYAML 环境）和 Go 1.26.9。
- `make web-check` 退出 0：23 文件/82 测试、Svelte/TS/lint/build 通过。
  本批未改 Web 源码；复验保留批次 7 工作树。
- 最终 `python3 cli.py security-scan` 退出 1：govulncheck 可达/导入包漏洞均为 0，
  仍保留 1 项未调用模块级警告；gosec 134 个生产文件、32197 行、36 项、`nosec=0`。
  与基线按文件/规则/描述比较、忽略行号，新增 0、移除 26（G115 12/G112 2/G104 12）。
  所有上述 G115/G112/G104 已报告项消失；剩余项仍需评审，**不声明安全发布通过**。
- 本批没有新增真实 PostgreSQL/ClickHouse/Kafka/WORM、浏览器、完整 Compose、容量、
  容灾或正式合规验收。全部产品化规划、M4/M5 及正式发布仍未完成。

## 批次 9：文件数据安全边界（工程门禁通过，安全发布仍阻塞）

- `internal/fsutil/secure_file*.go` 使用 Linux descriptor-relative openat、O_NOFOLLOW、
  O_NONBLOCK、regular/single-link inode 验证；操作只接受 basename。原子写、renameat
  与清理共用 pinned parent，未知 `.tmp` 不截断/删除，新文件 0600/目录 0700。
- Store 控制/热/冷/发布确认/查询元数据/独占锁/自审计使用该边界。旧权限不在读取或
  账本追加时自动 chmod；保留原格式、账本、签名、恢复 journal、单写者和操作 deadline。
  查询回收只作用于捕获目录，读取始终有 4 MiB 上限。升级/备份/回滚见 ADR-0017。
- 这是实际路径/权限防护，不是将风险 I/O 移入无校验包装或关闭扫描。临时文件创建
  必须 O_EXCL，验证 inode 在截断之前；不修改本地 WORM 对象模式/保留期，不声称
  防恶意 mount、特权主机管理员或真实目录搬迁。
- 最终安全扫描退出 1：可达/导入包漏洞 0，保留 1 项未调用模块级警告；gosec
  136 文件/32392 行、12 项、nosec=0，未更改规则/豁免/生成代码。较批次 8 新增 0、移除 24
  （G304 18、G302 6）。剩余完整清单：

| 文件 | 规则/数量 | 仍需处理 |
|---|---|---|
| `internal/kafka/replay_state.go` | G304 ×4 | DLQ state 的加载/lock/legacy 路径及独占 temp 重写独立评审。 |
| `internal/grpcapi/tls.go` | G304 ×1 | 配置证书/CA 路径、文件上限和 symlink secret mounts 的兼容信任边界。 |
| `internal/runtimeconfig/runtimeconfig.go` | G101 ×2 | 命中环境变量名称，不是凭据；保留扫描告警，未豁免。 |
| `internal/store/store.go` | G404 ×1 | 非安全 CAS jitter；保留扫描告警，未豁免。 |
| `api/proto/audit.pb.go` | G103 ×4 | 生成器/runtime unsafe 评审；不手改或排除生成代码。 |

- 新边界与相关文件/查询/发布/自审计/journal 回归 race 3 轮通过；完整 fsutil/archive
  race 通过。初次普通测试暴露 join-wrapped missing-file 与 os.IsNotExist 的兼容问题，
  改为保留简单 PathError/使用 errors.Is；新增 fixture 的确认类型和特殊 inode errno
  归类已修正。宽正则三轮运行命中旧 SQL driver 的重复注册，精确三轮复验通过，
  旧 driver 测试未删改，仍由完整单轮质量门禁覆盖。
- 最终完整 `python3 cli.py quality` 退出 0、`QUALITY PASS`：普通 Go 测试 479.6s、
  全量 race 761.0s、vet/build、Python 156 测试/1 可选跳过及全部门禁通过。
  `make web-check` 退出 0，23 文件/82 测试及 Svelte/TS/lint/build 通过；本批无 Web 改动。
  日志：`/tmp/audit-productization-batch9-quality-final.log`、`...-web-check-final.log`、
  `...-security-final.log`，各有 `.exit`；定向 `...-boundaries-race-final.log` 与完整
  `...-durability-race.log` 均退出 0。使用 Go 1.26.9 与 `/usr/bin/python3` 的 PyYAML 环境。
- 剩余 12 项仍阻塞安全发布。未新增真实组件、整栈/浏览器、容量、容灾或正式合规
  验收；租约/检查点、M4/M5 和 M1–M5 整体仍未完成。

## 批次 10：TLS/DLQ 路径整改（工程门禁通过，安全发布仍阻塞）

- gRPC cert/key/client CA 使用受限 root 内的 regular PEM loader，每文件 4 MiB，
  实际读取同样有界，O_NONBLOCK 拒绝阻塞 FIFO。运维配置父目录受信，兼容 root
  内 `..data` 相对 symlink，拒绝叶 root escape/绝对链接/loop，不改 TLS/mTLS 鉴权。
  仅启动/预检加载，不作跨文件原子快照；非 Linux 失败关闭，见 ADR-0018。
- DLQ load/classify/append/repair/legacy/rebuild/lock 接入严格数据 fd 边界。唯一
  O_EXCL temp、actual fd fsync、renameat/cleanup 绑定同一个 parent；未知残留不
  读取/删除，旧文件权限不批量更改。v1/v2/tenant identity/有界 flock/union/O(1)
  append 和关闭未知结果的幂等/offset 契约不变，完整历史读取成本未宣称已解决。
- 最终 security-scan 退出 1：可达/导入包漏洞 0、1 项未调用模块级警告；gosec
  139 文件/32545 行、nosec=0，12→7 项（新增 0，移除 G304 5），
  全部本次已报告 G304 已清，但不证明所有文件/路径风险已消失；未修改规则、
  豁免、生成代码或严重性阈值。剩余完整清单：

| 文件 | 规则/数量 | 状态 |
|---|---|---|
| `internal/runtimeconfig/runtimeconfig.go` | G101 ×2 | 环境变量名称，不是凭据；未豁免，告警保留。 |
| `internal/store/store.go` | G404 ×1 | 非安全 CAS jitter，未用于 token/key/signature；未豁免。 |
| `api/proto/audit.pb.go` | G103 ×4 | pinned protoc/runtime unsafe；未手改/排除，仍需正式评审。 |

### 剩余告警的源码核对（不等于豁免/正式发布批准）

- G101 的两个常量只保存 `AUDIT_SIGNING_SECRET` / `AUDIT_ALLOW_DEV_SECRETS` 名称，
  API/worker 的 `os.Getenv` 与 fail-closed secret 校验消费实际配置，不把常量当密钥。
  未通过字符串拆分/改名避扫；仍保留原告警，配置/密钥泄露的其他风险不因此消失。
- G404 唯一调用是 `snapshotConflictBackoff` 的 `rand.IntN(5_000_000)`，使用方是
  `internal/store/operation.go` 的取消感知重试 timer；不生成凭据、安全 nonce、
  签名或证据身份。可预测退避不是授权基础；仍保留原扫描结果，不以替换成不必要的
  crypto 调用或豁免文本换扫描成功。
- G103 是 `audit.pb.go` 两个生成位置各一个 `unsafe.StringData` 与 `unsafe.Slice`。
  输入为编译期 `const file_audit_proto_rawDesc`，slice 长度正好为该字符串的 len，
  不是网络 payload、用户提供的长度或本项目可变 buffer。一次进入 `CompressGZIP`，
  pinned protobuf v1.36.11 的实现只读 input、另行分配 output；另一次进入生成的
  TypeBuilder/RawDescriptor 构造。静态常量生命周期与 `sync.Once`/包 init 路径已核对。
  protoc v3.21.12、protoc-gen-go v1.36.11、grpc generator v1.5.1 和 runtime pins 保持，
  proto-sync 按 pinned generator 重放。这里只记录用途与必要的输入/只读约束，**不是
  对所有 runtime unsafe 的证明**；升级生成器/runtime 后须重新核对。未修改生成代码、
  exclude 或 `nosec`，4 项继续保留，正式评审/批准不能由本记录自授。

- 新边界与相关回归 race 3 轮通过；完整 replay（含两进程 union、legacy tenant、
  crash/torn-tail、auth-blocked/offset barrier）及 TLS/plaintext socket race 3 轮
  通过。真实 parent replacement 与 content-sync/rename/cleanup 边界同步，不靠
  准备工作的短时间假设或放宽生产超时；初次普通定向测试通过。
- 最终完整 `python3 cli.py quality` 退出 0、`QUALITY PASS`：普通 Go 测试 390.5s、
  全量 race 1108.5s、vet/build、Python 156 测试/1 可选跳过及全部工程门禁通过。
  `make web-check` 退出 0：23 文件/82 测试、Svelte/TS/lint/build 通过，本批无 Web 改动。
  日志：`/tmp/audit-productization-batch10-quality-final.log`、`...-web-check-final.log`、
  `...-security-final.log`（分别 .exit=0/0/1）；`...-boundaries-race.log`、
  `...-replay-tls-race-final.log`、`...-tls-loop-race.log` 均退出 0。Go 1.26.9 与
  `/usr/bin/python3` 的 PyYAML shim 环境，阈值/生产超时/扫描规则均不变。
- 剩余 7 项与模块级警告继续保留，不以无 G304 或源码核对自授正式安全发布通过；
  未新增真实组件、浏览器/整栈、容量、容灾、Hosted Login UI 或正式合规验收。
  这是批次 10 当时的状态；租约/心跳后续见批次 11，检查点续跑、M4/M5 与整体规划仍未完成。

## 批次 11：导出运行租约（完整工程/Web 门禁通过，安全发布仍阻塞）

- 新增私有版本化 control 租约、2 分钟期限/30 秒心跳、精确 owner/tenant/job fencing；
  到期失败与 export.recovered 同一次 CAS，失败不报成功恢复数，终态不重新排队。
  owner 使用 crypto/rand，公共 DTO/摘要/事实详情/日志不输出；对象身份只使用不透明
  owner 哈希派生。迟到 Put 可能留下无引用 sealed 对象，不删除 WORM 或推断回滚。
  新旧二进制不能混跑；时钟/备份/回滚边界见 ADR-0019。
- 新边界与关联回归三轮 race 通过：实际 Save/Put/取消/克隆/重启/过期/错绑/终态
  fencing；真实 PostgreSQL 17 独立连接池三轮 race 覆盖领取竞争、共享全局/tenant
  预算、跨实例续期与到期收敛。只是组件行为验收，不是 PG 生产容量或完整 Compose。
- 最终 security-scan 退出 1：govulncheck 可达/导入包漏洞 0，保留 1 项未调用模块级
  警告；gosec 143 文件/32789 行、nosec=0、7 项。与批次 10 清单同为 G101×2、
  G404×1、G103×4，未新增/消除告警，未改规则、排除、生成代码或豁免。上文用途核对
  仍不是正式批准，安全发布继续失败。
- 日志：`/tmp/audit-productization-batch11-security-final.log`（.exit=1）、
  `...-lease-boundaries-final.log`、`...-pg-final.log`（均 .exit=0）。首轮普通测试因
  snapshotJSONCopy 未包含新租约而失败，补全深克隆后通过；首次边界测试发现取消
  后仍可能在忽略 ctx 的 backend 保存，control checked 回调改为共享 deadline 前后
  检查，保留真实持久边界的未知结果。完整 quality 曾在最终 map 身份核对前主动
  停止，该轮不是通过记录。随后完整普通测试失败于旧 worker 测试预期取消恢复仍
  成功；改为断言 export_recovery_error=context canceled，补测取消不保存/不清租约/
  不写恢复事实，worker pass race 3 轮通过，随后完整最终质量门禁通过。保留失败日志
  `...-quality-worker-cancel-failed.log`（.exit=1），阈值/生产超时未放宽。
- 最终完整 python3 cli.py quality 退出 0、QUALITY PASS：普通 Go 测试（含缓存）8.8s、
  全量 race 365.3s、vet/build、Python 156/1 可选跳过及全部门禁。日志
  `/tmp/audit-productization-batch11-quality-final.log`（.exit=0）；没有降低阈值或减测。
- Web 源码未变，完整 make web-check 复验通过：23 文件/82 测试与类型/lint/build。
  `/tmp/audit-productization-batch11-web-check-final.log`（.exit=0）。
  本轮临时 PG 容器已清理，不涉及旧卷、宿主防火墙或无关进程。未新增浏览器/整栈、
  Hosted Login UI、生产容量、容灾或正式 WORM 验收；检查点续跑/取消重试、M4/M5
  和整体规划仍未完成。

## 批次 12：所有者导出控制（完整工程/Web 门禁通过，安全发布仍阻塞）

- 新增同 tenant/可信 subject 的所有者取消、每原任务单后继关联重试和闭集失败类别；
  不接受调用方替换身份/筛选/租约。成功与错误 no-store，仅返回安全 control 摘要。
  原终态不退回 pending，不删除 WORM；未知 Save/迟到 Put 不推断回滚，详情/下载
  Hold、摘要和 AEAD 认证不变。API/worker/Web 版本和备份/回滚见 ADR-0020。
- 服务/HTTP 新边界与 lease 回归三轮 race 通过；文件重启、已提交但报错的未知 Save、
  失败关闭读自审计、当前 Hold 和错绑关系覆盖。真实 PostgreSQL 17 独立连接池三轮
  race 覆盖取消竞争、单后继、共享 tenant 队列及取消释放预算（仅组件行为验收）。
  最终预检改造后再次定向三轮通过：创建/重试在独立深快照上锁外 Get，共用服务
  deadline；真实第二次 Get 暂停时可续期/取消，不重启期限，不声明恒定历史内存。
  日志：`/tmp/audit-productization-batch12-boundaries-race-final.log`、`...-persistence-race.log`、
  `...-pg-final.log`（均 .exit=0）。本轮专有临时 PG 容器已移除，不涉及旧卷或其他服务。
- Web 完整 make web-check 退出 0：24 文件/97 测试、Svelte/TS/lint/build 通过；覆盖
  未知结果只读刷新、tenant/token/权限/任务代次以及 Hold 阻断详情后的元数据控制。
  最终日志 `...-web-check-final.log`（.exit=0）。早期 Hold fixture 签名错误和平台权限
  fixture 的错误预期已修正，未放宽生产认证；早期失败日志保留。
- 最终安全扫描退出 1：可达/导入包漏洞 0、1 未调用模块警告；gosec 148 文件/
  33130 行/nosec=0，仍为同一 G101×2/G404×1/G103×4，无新增/消除/豁免。日志
  `/tmp/audit-productization-batch12-security-final.log`（.exit=1）。用途核对不是正式批准。
- 最终完整 quality 退出 0、QUALITY PASS：普通 Go（含缓存）239.2s、全量 uncached
  race 364.4s、vet/build、Python 156/1 可选跳过及全部工程/契约/隔离门禁。
  日志 `/tmp/audit-productization-batch12-quality-final.log`（.exit=0）。没有取消 race、
  放宽阈值或生产超时。初次质量工具启动超时无阶段输出、进程已退出；随后普通全量
  Go 测试通过后，为改造预检锁内取证主动 SIGTERM 自有 race 树，再启动最终全量。
  保留 `...-quality-launch-timeout.*`、`...-quality-before-preflight-io-review-stopped.*`，
  两轮不作 QUALITY PASS。
- 无扫描规则/阈值/排除/生成代码/豁免修改。没有新增整栈/浏览器/Hosted Login UI、
  生产容量、容灾、正式合规或业务执行验收；检查点续跑、分块认证、M4/M5 未交付。

## 24h runner 0001：密封对象检查点（本轮自验通过，独立验收未启动，安全发布仍阻塞）

- 本轮仅增加 Put 后的私有版本化 sealed 检查点及 fenced 终态提交续跑；任务/原筛选
  使用独立用途 HMAC 绑定，v2 AEAD、密封/JSONL 摘要/计数和 committed receipt 坐标
  重新认证。无载荷/密文/原始筛选副本进入运行元数据，无 owner/对象字节/摘要进入日志。
  读自审计与当前 Hold 失败关闭，Hold 集变化/所有者取消/换主后晚验证不得发布路径。
  不重选输出/重新密封/Put，不删除事实/WORM，不把 M3 整项宣称完成。见 ADR-0021。
- 本轮完整 `python3 cli.py security-scan` **退出 1**：govulncheck 可达/导入包漏洞为 0，
  仍有 1 项未调用模块级警告；gosec **153 文件 / 33538 行 / nosec=0 / 7 项**。本次输出
  全部为上表同类同文件：`runtimeconfig.go` G101×2（环境变量名）、`store.go` G404×1
  （非安全 CAS jitter）、生成的 `api/proto/audit.pb.go` G103×4。新增/移除均为 0，行号
  随 control 字段增加移动。未改规则/阈值/排除/生成代码，没有 nosec、重命名避扫或
  不必要的 crypto 替换；用途核对仍不是正式评审/豁免，**release_gate=blocked**。
- 独立证据根：
  `/home/u1/.abr/projects/snaplink-audit-governance-a4087374b5d05637/runs/productization-24h-sol-max-20261016T013158Z/rounds/0001/`。
  `security-scan.log/.exit` 为原始完整输出/退出 1；不得用既有批次扫描替代此次结果。
  `quality.log/.exit` 退出 0、QUALITY PASS（普通 Go 382.7s、全量 uncached race 583.3s、
  vet/build、Python 156/1 可选跳过与全部门禁）；`web-check.log/.exit` 退出 0，24 文件/
  97 测试、Svelte/TS/lint/build，既有构建警告保留。本轮没有 Web 源码改动。
- `checkpoint-race-final.log/.exit` 三轮 race 退出 0；`checkpoint-postgres.log/.exit` 为
  专有隔离 PostgreSQL 17、独立 SQL pools 三轮 race 退出 0。真实 Put/Get/Save/append、
  文件重启、绑定污染、未知 Save、当前 Hold、取消/续期、同一服务 deadline 与跨副本
  预算/fencing 覆盖；只清理该轮自建容器/network/临时凭据文件，无旧卷/生产 DSN 操作。
  早期专项 requester 兼容失败与 unused import 编译失败日志保留，均修正后重新验证，
  不是完整门禁通过记录；未降低阈值或删旧测试。
- 这不是正式安全发布批准，也没有新增整栈/浏览器/Hosted Login UI、生产容量、容灾、
  正式 WORM/合规、M4/M5 或业务恢复执行验收。下轮仍须完整复验质量与安全门禁。

## 24h runner 0002：检查点当前 Hold 证据门禁修复（本轮自验通过，安全发布阻塞）

- 先核对上一轮真实文件/哈希与日志：其本轮 quality/Web 自验退出 0，但独立工程验收
  在创建已存在日志时失败，未执行任何目标命令，不能算独立通过。Controller 已自行
  修复验收日志隔离并保留失败；随后 0002 独立验收已执行，quality exit1、Web exit0、
  security-scan exit1，验收期间源码未变化。失败位于全量 race 的
  `TestGRPCWriteStreamClaimsAreNotExtendedByEventFields` 首次 denied.Send 的 `EOF`，
  不是 JWT 小时级过期或检查点测试成功的证明；旧测试错误要求 Send 必须 nil。
  完整失败保留于 `rounds/0002/independent-engineering-my3hcqcu/quality.log` 和
  `rounds/0002/verification.json`，人工在原样源码上的定向 race30 通过不能抵销它。
  本轮未修改外部脚本，后续有界修复仍须新的完整独立验收。
- 修复 sealed 检查点完成前的当前 Hold 取证缺口：在原查询/Hold 谓词之前认证 receipt
  锚定的当前正文、精确 Schema、内容和链；搜索 sidecar 不受 SourceDigest 认证，改用
  精确重建原值按条件自身的 bound/legacy 格式推导，包含加密字段和大整数。未知或坏
  当前证据保留原检查点、拒绝完成；当前 Hold 集/lease fencing、一次服务 deadline、
  锁外源 Get、自审计与共享预算不变。没有原文/密文/摘要进入新元数据或日志，没有
  重选原输出、再次 Put、WORM 删除或业务恢复。这只修复检查点终态，不宣称全部
  预检/详情/下载 Hold 路径和历史成本已改造；其余路径仍须独立推进。
- 修复前新增的真实回归失败（hold-regression-before）与新增测试类型名错误的编译
  失败（checkpoint-expanded）均保留，已修复后完整复验。扩展专项三轮 race 退出 0
  （service 72.446s/store 1.052s），隔离 PostgreSQL 17 独立 SQL pools 三轮 checkpoint
  race 退出 0（3.628s）。只清理本轮唯一标识/label 的临时资源和凭据文件，不涉及
  生产/共享 DSN、旧卷、其他容器或宿主防火墙。
- 本轮最终 `python3 cli.py quality` **退出 0**、QUALITY PASS：普通 Go 含缓存 240.6s、
  全量 uncached race 586.7s、vet、7 二进制 build、Python 156/1 可选跳过和全部门禁。
  `make web-check` **退出 0**：24 文件/97 测试、Svelte/TS/lint/build；无本轮 Web 源码
  改动，既有 Node/localStorage 和 Vite sourcemap/PURE 注释警告保留，不冒充零警告。
- 本轮真实 `python3 cli.py security-scan` **退出 1**（2026-10-16 03:14:55–03:15:04 UTC）：
  govulncheck 可达/导入包漏洞 0，仍有 1 项未调用模块级警告；gosec **154 文件/33646 行/
  nosec=0/7 项**，仍为 `runtimeconfig.go` G101×2、`store.go` G404×1、生成 proto G103×4。
  比上一轮新增/移除静态 finding 均为 0；修复本次源码风险不等于扫描清零或正式评审。
  未改规则/排除/阈值/生成代码/豁免，没有重命名避扫或无必要 crypto 替换，发布仍
  **release_gate=blocked**，不能凭零可达漏洞或 quality/Web 自验自授发布通过。
- 完整命令/退出码/时间及原始输出位于：
  `/home/u1/.abr/projects/snaplink-audit-governance-a4087374b5d05637/runs/productization-24h-sol-max-20261016T013158Z/rounds/0002/component/`。
  对应 `quality.*`、`web-check.*`、`security-scan.*`、`checkpoint-expanded-02.*`、
  `checkpoint-postgres.*`；随后独立验收的实际失败证据在上述独立 namespace 保留，
  不能以本轮自验或本记录替代、不能继续称独立验收待执行。
  整体 M3 仍 Partial，逐页/逐块生成续跑、整栈/Hosted Login、生产容量/正式合规、
  历史密钥/生命周期与跨仓业务执行仍未交付。

## 24h runner 0003：gRPC 拒绝边界测试 oracle 有界修复（完整自验通过，独立待验，安全阻塞）

- 在真实流等待服务端 trailers-only 拒绝后再 Send，确定性复现旧测试的 EOF 失败；
  校准为该顺序精确 io.EOF、Recv 最终 PermissionDenied 且无成功回执。新增传输先
  缓冲/并发拒绝回归，实际无权限 handler 不调用 RecvMsg，不产生事件/receipt/Outbox
  或链状态；合法前缀保留持久见证，失败日志不输出原载荷。生产授权/超时未改。
- 初轮完整 quality 在 Python 的 make-proto Git 干净断言失败，未到全量 Go；只撤回
  本轮自己的 Proto 注释并按原 pinned generator 重生成，Proto/两份生成字节与开工
  基线完全一致。客户端语义改记 `api/README.md`；不修改检查器、Git index、阈值或
  扫描配置，不提交/reset。该次 quality/Web/security exit1/0/1 和修复前回归失败均保留。
- 最终 `python3 cli.py security-scan` 于 2026-10-16 06:41:39–06:41:49 UTC **退出 1**：
  govulncheck 可达/导入包漏洞 0、1 项未调用模块级警告；gosec **154 文件/33646 行/
  nosec=0/7 项**，仍是 G101×2/G404×1/G103×4，较 0002 新增/移除均 0。用途核对
  不等于正式批准，**release_gate=blocked**，本轮没有整改或豁免这 7 项。
- 最终完整 `python3 cli.py quality` 退出 0、QUALITY PASS：普通 Go 含缓存 42.7s、
  全量 uncached race 516.6s、vet/build、Python 156/1 可选跳过和全部门禁；完整 Web
  退出 0（24 文件/97 测试、Svelte/TS/lint/build，既有 Node/Vite 警告保留）。拒绝
  边界 race30、完整 auth/grpcapi race 退出 0；最终门禁期间源码哈希不变。没有放宽
  JWT 寿命/生产超时/权限/工程阈值、删测试或降低扫描规则，不把定向通过抵销全量失败。
- 完整证据根：
  `/home/u1/.abr/projects/snaplink-audit-governance-a4087374b5d05637/runs/productization-24h-sol-max-20261016T013158Z/rounds/0003/component-repair/`。
  `quality-final`/`web-check-final`/`security-scan-final` 的 `.log/.exit` 分别为 0/0/1；
  所有命令另有 `.command/.started/.finished`，初次失败未覆盖。独立外部工程验收仍须
  随后重新完整执行，不能由自验/本记录冒充通过。未新增组件数据库/容器、整栈/
  Hosted Login、生产容量/合规或 M1–M5 完成项，也没有业务执行或 WORM 清理。

## 24h runner 0004：导出访问当前 Hold（本轮工程自验通过，独立待验，安全阻塞）

- 开工核对 0003 的真实独立验收：quality/Web/diff 退出 0、security-scan 退出 1，
  验收期间源码未变；本轮 750 文件哈希与其 source-after 完全一致。因此不把前两轮
  已实现 sealed 检查点当新交付，也不继续声称 0003 的独立命令尚未运行。
- 本轮将同一 receipt 锚定、精确 Schema/内容/链认证及从原值推导敏感条件的 Hold
  判定扩展到详情和下载放行。独立深快照后 Get/认证锁外，同一服务操作 deadline
  覆盖全部源读、自审计及 control 复核；当前租户 Hold/任务选择变化拒绝访问，下载
  自审计前后均检查。原终态不重排、不删 WORM、不复制载荷/密文/筛选到持久运行
  元数据；HTTP 成功/错误 no-store，未经认证/Hold/自审计不输出明文。见 ADR-0022。
- 本轮真实 `python3 cli.py security-scan` 于 2026-10-16 07:40:26–07:40:35 UTC
  **退出 1**：govulncheck 可达/导入包漏洞 0、1 项未调用模块警告；gosec **155 文件 /
  33730 行 / nosec=0 / 7 项**。仍为 runtimeconfig.go G101×2、store.go G404×1、
  生成 proto G103×4，较上一轮新增/移除均 0。未修改扫描规则/排除/严重性阈值/
  生成代码/豁免，没有重命名避扫或不必要 crypto 替换；用途核对不等于正式批准，
  **release_gate=blocked**。后续最终复验的原始完整输出继续单独保留，不覆盖本次。
- 完整 `python3 cli.py quality` 于 07:27:04–07:39:59 UTC 退出 0、QUALITY PASS：
  普通 Go 含缓存 315.0s、全量 uncached race 454.8s、vet/7 二进制 build、Python
  156/1 可选跳过及全部门禁。完整 Web 退出 0，24 文件/97 测试与 Svelte/TS/lint/
  build；无本轮 Web 源码变化，Node/Vite 原警告保留。新增专项三轮 race 和隔离
  PostgreSQL 17 独立 SQL pools 三轮 race 均退出 0；唯一容器/network/私有临时
  凭据已按本轮 label 核对清理，无生产/共享 DSN、旧卷或无关资源操作。
- 保留修复前真实回归失败、新 fixture 缺租户名称、过严轮询状态比较、unused import
  和首轮完整 quality 的 Python OpenAPI 200/404 固定文本标记失败。只修本轮代码/
  fixture/契约表示（no-store 仍由 description、运行时和回归强制），原测试、门禁、
  阈值、生产 timeout 和认证未改；之后完整 quality/Web 再次通过，不用专项抵销失败。
- 证据根：`rounds/0004/component/`（本 24h run 下），`security-scan.log/.exit` 为
  原始失败输出；`quality-02`/`web-check`/`access-expanded-02`/
  `access-checkpoint-postgres` 的 .log/.exit 为通过输出。结果文档补充后的完整复验
  另存 `quality-final`/`web-check-final`/`security-scan-final`；每项均有完整命令、
  退出码与时间。独立工程验收仍须随后完整重跑，不由本记录自授通过或发布。
- 历史物化/扫描、创建/预检/选择等其余 Hold 路径、逐块生成续跑/认证下载、M4/M5、
  整栈/Hosted Login、生产容量/容灾和正式合规仍未完成，没有业务执行或生命周期删除。

## 24h runner 0007：selected 固定成员计划（安全扫描失败，独立工程待验）

- 开工757文件/HEAD/index与0004独立通过状态完全一致；0005/0006只是provider
  fetch failed，无源码变化或待重放写入。0004实际独立quality/Web/diff退出0，安全
  扫描退出1；不重复旧sealed/访问Hold/租约/取消/关联重试，不修改外部恢复策略。
- 新增生成前私有计划，绑定原任务、有序账本坐标、精确Schema版本/定义HMAC；
  认证前置选择、不信任派生搜索sidecar，内部读自审计与当前Hold失败关闭。源Get
  锁外、一次deadline，过期新owner继续原成员生成，不重选，不回退终态。计划与
  export.planned同CAS，sealed保存原子消费计划并复核Schema/owner。未知Put可能
  留无引用对象，新attempt不重放未知旧key、不删WORM；仍无逐块字节进度/业务执行。
- 本轮真实 `python3 cli.py security-scan` 于2026-10-16 09:10:05–09:10:13 UTC
  **退出1**：govulncheck可达/导入包漏洞0，仍有1项未调用模块警告；gosec **160文件/
  34184行/nosec=0/7项**，仍为runtimeconfig.go G101×2、store.go G404×1、生成proto
  G103×4，较0004新增/移除均0。未改规则/阈值/排除/生成器/生成代码/豁免，没有
  拆分常量避扫或无必要crypto替换；上述源码用途核对不是正式批准，发布仍Blocked。
- 首次完整quality退出1，普通Go报告HTTP临时目录清理与取消唤醒的后台Save竞争，
  以及两个旧测试漏计新增selected内部读事实；未进入全量race/build。已补真实终态
  Save等待/Store关闭，精确保留原读事实及新增worker阶段身份断言，GetExport前后
  事实严格不变，HTTP/服务专项race30退出0。此前4个sealed-only预期按新增selected
  持久边界校准，不降低阈值/删断言；未知Save、文件重启、当前Hold、Schema/内容/
  坐标/MAC/预算、取消/换主/deadline及专有PG17独立pools三轮race均有独立原始日志。
- 证据根：本24h run的 `rounds/0007/component/`。首次 `quality.log/.exit` 为1，
  `web-check.log/.exit` 为0（24文件/97测试及类型/lint/build，原Node/Vite警告保留），
  `security-scan.log/.exit` 为1。最终完整复验另存 `quality-final`、`web-check-final`、
  `security-scan-final` 的.command/.log/.exit/.started/.finished，以真实退出码为准，
  不预先冒充通过；随后独立外部验收仍须完整执行。无本轮Web源码/生产配置改动。
- 仅清理本轮唯一label的隔离PG容器/network/私有临时凭据，资源清理证据在
  postgres-isolation.json；没有生产/共享DSN、旧卷或无关服务操作。没有新增整栈/
  Hosted Login、生产容量/容灾、正式WORM/合规或M4/M5验收；M3整体仍Partial。

## 新 24h run 20261016T091326Z / 0001：逐页生成检查点（自验工程通过，安全发布阻塞）

- 开工先核对旧0007真实独立验收：quality/Web/diff exit0、security-scan exit1，于
  09:42:03 UTC 完成且源码未变，新run基线哈希一致；没有跳过旧失败或重复计功旧
  selected/sealed、访问Hold、租约/取消/关联重试。两份HANDOFF及绝对窗口均保留。
- 新多页 v2 计划持久保存真实已生成/独立认证页的引用，只生成未提交成员；原任务/
  Schema/格式/页范围/对象绑定，精确 owner/计划MAC/当前Hold集/共享预算 CAS，读自
  审计与锁外页 I/O 共享本页一次原 deadline。缺/坏已提交页不再生成替代，未知/迟到
  Put 不删除或猜回滚，终态不回退。最终仍整块合并/密封与原下载门禁，不是流式下载、
  恒定内存、业务执行或正式 WORM/合规验收，见 ADR-0024。
- 当前 receipt 身份复核新增回归先真实失败：坐标检查漏掉 receipt 正文 event_id。
  补上正文身份精确比较，拒绝用 map key/已认证页掩盖账本漂移；没有改鉴权、阈值、
  生产超时或旧测试。修复前 ledger-binding-before exit1 及早期新增测试 Hold 释放
  参数顺序错误的 generation-01 exit1 均保留；修复后专项三轮race0、完整quality0。
- 本轮真实 `python3 cli.py security-scan` 于 **10:30:13–10:30:26 UTC 退出1**：
  govulncheck 可达/导入包漏洞0，仍1项未调用模块警告；gosec **162文件/34513行/
  nosec=0/7项**，完整清单仍是 runtimeconfig.go G101×2、store.go G404×1、生成
  audit.pb.go G103×4，较旧0007新增/移除均0。未改规则/排除/严重性阈值、生成代码或
  豁免，未拆分/改名常量避扫或换无必要crypto。用途核对不是批准，**release_gate=blocked**。
- 完整 quality 于10:09:28–10:29:24 UTC 退出0/QUALITY PASS：普通Go542.3s、全量
  uncached race641.7s、vet/7二进制build、Python156/1可选跳过及全部工程/契约门禁。
  Web退出0（24文件/97测试、Svelte/TS/lint/build，原Node/Vite警告保留），无本轮Web
  源码变更。最终独立SQL pools专有PG17全部PostgresExport三轮race退出0（77.522s），
  页Put/Get/Save/append、文件重启、绑定/版本/内容/ledger污染、Hold/自审计/deadline、
  取消/续期/换主/共享预算均有组件证明，不以专项代替全量或正式容量验收。
- 证据根：
  `/home/u1/.abr/projects/snaplink-audit-governance-a4087374b5d05637/runs/productization-24h-sol-max-20261016T091326Z/rounds/0001/component/`。
  quality/web-check/security-scan/ledger-binding-before/fixed/generation-transitions-final/
  final-generation-postgres 各有.command/.log/.exit/.started/.finished，初版逐页专项
  三轮race日志为generation-race。source-test-stage与source-after-first-gates的783
  文件manifest一致，HEAD/index保持开工基线、无源码删除。结果文档更新后的完整复验
  单独保留quality-final/web-check-final/security-scan-final，不覆盖任何旧证据。
- 两次本轮唯一label的本地PG容器/network及私有临时凭据均清理，见postgres-isolation
  和final-postgres-isolation.json；未访问生产/共享DSN、旧卷、其他容器或改宿主防火墙。
  独立外部工程验收随后仍须完整执行；本记录不自授独立验收通过、安全豁免或发布批准。
  M3整体、历史成本、流式大文件下载、M4/M5、整栈/Hosted Login与容量/合规仍未完成。

## 新 run 20261016T091326Z / 0002：旧 gate 启动信号修复（安全扫描失败）

- 先核对 0001 的真实独立验收：quality exit1，Web/diff exit0，security-scan exit1；
  783 文件/HEAD/index 与验收后的源码完全一致。不以该轮自验通过代替独立验收。
- 真实启动 bucket HEAD 边界确定性复现 worker 在持锁后、信号注册前默认终止。
  提前注册进程取消、启动 Ready 继承取消，保持原5秒上限和无取消就绪失败的非零
  退出，不绕过 WORM/认证或改变导出阶段；取消启动不运行治理 pass、正常关闭锁。
  API 准备的未知提交仅用同一完整事件幂等核对，实际 drain 配置/上限不变，非
  固定 store-timeout 错误不重试。没有业务写入、终态回退或证据/旧卷清理。
- 本轮真实 `python3 cli.py security-scan` **exit1**：govulncheck 可达/导入包漏洞0，
  仍1项未调用模块警告；gosec **162文件/34527行/nosec=0/7项**，仍为上表
  runtimeconfig.go G101×2、store.go G404×1、生成 audit.pb.go G103×4。新增/移除均0，
  未改扫描规则/排除/阈值/生成代码或加豁免，未改名避扫或引入无必要crypto。
  用途核对不等于正式评审或批准，**release_gate=blocked**。
- 原始命令/退出码/时间/完整输出在本 run `rounds/0002/component/security-scan.*`。
  signal-fixed-race 三轮、signal-regression-race30 exit0；完整 quality/Web 的真实
  结果及最终源码manifest另在同目录留证，独立外部验收随后仍须完整执行，不预先
  宣称通过。该轮不新增逐页交付、跨副本组件、整栈/浏览器、容量或正式合规验收。

