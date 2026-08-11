# Release Notes

## 2026-08-11 — key-framing 字符集不变量：所有非受信 API 边界拒绝控制字符/空白/路径分隔符标识符

**写给运营（行为变化，破坏性类）：**

- **事件/来源/模式标识符收紧**：`POST /api/v1/events`、`events:batch`、
  `POST/PUT /api/v1/sources`、`POST /api/v1/schemas`、`POST /api/v1/tenants`
  现在拒绝包含控制字符（含 0x1F）、空白、`/`、`\` 的 `event_id`、
  `source_system`、`aggregate_type`、`aggregate_id`、`operation_id`、
  source `id`、`schema_id`、tenant `id`，返回 400，且**不落任何数据**。
  此前这类标识符会被接受，并可能产生 `SplitTenantKey` 无法解析的多分隔符
  复合键，导致对应流在完整性校验与聚合 checkpoint 中被静默跳过。
- **平台 `?tenant_id=` 逃生口同步收紧**：`%1F` 等编码注入返回 400，发生在
  任何服务/存储访问之前——不再可能发生跨租户读取或在伪造租户名下追加
  自审计记录。空 `?tenant_id=`（全租户读取）语义不变。
- **dev token 主题收紧**：`dev:<subject>:<role>` 的 subject 含上述字符时
  认证失败（401），与畸形 token 同文案（无 oracle 区分）。合法 subject
  （`tenant-a`、`platform`、`crm` 等）行为不变。
- **gRPC 面同步生效**：`Write`/`WriteBatch`/`WriteStream` 中非法事件映射
  `InvalidArgument`；批次保持既有的部分接收语义（前缀合法事件仍入账）。
- **outbox/Kafka 无需变更**：`outbox.Insert` 在任何 SQL 之前校验，非法
  事件永不进入 outbox 表；API 的 400 被 `HTTPDeliverer` 分类为永久失败
  → 直接进 DLQ（`Failure` 记录），DLQ 是既定的处置面。

**写给开发（API 契约变化）：**

- 唯一实现 `domain.ValidKeyComponent(name, value)`（stdlib-only），
  `store.ValidTenantID` 委托之；`Event.ValidateBasic`、`normalizeSource`、
  `RegisterSchema`、`parseDevToken`、`tenantFor` 五处边界全部走同一规则，
  错误消息按组件命名（`"event id must not contain control characters"` 等），
  `errors.Is(err, domain.ErrInvalid)` 语义不变。
- `tenantFor` 签名改为 `(string, error)`，`server.go` 中
  `r.URL.Query().Get("tenant_id")` 仅存在于 `tenantFor` 一处（AST 守卫测试
  固化）。
- `PUT /api/v1/sources/{sourceId}` 增加防御性检查：目标租户记录不存在时
  返回 404（手改快照场景下防键碰撞；API 可达路径下不可达）。
- OpenAPI 契约在 8 个 body 字段与 3 个 `tenant_id` query 参数上补充
  pattern（query 形允许空串），并有存在性断言测试防漂移。
- 存量多分隔符键（如有）行为不变：完整性循环仍跳过（fail-closed）；
  修复不在本次范围，建议部署前跑一次只读清单扫描。
- 回滚：纯校验变更，无数据迁移；被拒事件从未入账，DLQ 保留证据。

**回归测试：** AC-1（HTTP 单发/批次拒绝 + 快照无多分隔符键 + 拒绝事件未
落库 + 无自审计记录）、AC-2（source/schema 拒绝且零副作用）、AC-3（平台
逃生口 5 端点 400 + 无伪造自审计）、AC-4（dev token 401 单元 + HTTP）、
AC-5（服务层五字段拒绝、快照全键可解析、密封流完整性全覆盖）、gRPC
`InvalidArgument` 三分支、outbox `Insert` 拒绝零 SQL、OpenAPI pattern 断言、
`tenantFor` 边界守卫。

## 2026-08-11 — DLQ replay: payload event_id 匹配 + 缺席原事件一轮收敛 + 空 event_id 不再发布 Failure

**写给运营（行为变化）：**

- **匹配口径改为 payload `event_id` 优先、key 兜底**：accepted topic 的
  消息 key 缺失或与 payload 不一致时，replay 仍能按 payload 恢复原事件并
  字节级原样重发；旧实现只按 key 匹配，这类记录会无限循环重扫。
- **原事件已过期/不存在的记录一轮收敛**：当一次完整扫描确认 accepted
  topic 中不存在原事件时（quiet window 判定，见下），该 DLQ 记录被标记
  `unresolvable` 并提交，不再每轮重试；每次标记都有持久日志行
  `unresolvable event_id=<id> reason=original-not-found-in-accepted-topic`。
- **`audit_dlq_replayed_total` 语义扩展**：该计数器现在也包含
  converged-unresolvable 事件（与既有的 unparsable/permanent-failure
  收敛路径一致）；其余四个计数器不变。可对该日志模式加告警。
- **持续灌入下扫描有界**：一轮扫描最多持续 2×drainTimeout（默认 10 秒）；
  若 topic 一直不安静，轮次被截断（cut-off），未确认的记录保持 pending、
  绝不误标记，下一轮重试。只有“整整一个安静窗口内无消息”才算扫描完成。

**写给开发（API 契约变化）：**

- 消费端不再发布空 `event_id` 的 `Failure` 记录：unparsable 路径取 payload
  探针、key 兜底，两者皆空时降级为 commit+log；`deadLetter` 对空 payload
  `event_id` 同样跳过发布。`Failure.event_id` 在 AsyncAPI 契约中收紧为
  `minLength: 1`，accepted channel 声明 Kafka key 必须等于 payload
  `event_id`（binding），且描述补充了 payload 匹配语义。
- 旧 DLQ 记录（含空 `event_id`）无需迁移：replay 对它们的现有行为不变。
- `Replayer.Metrics()` 签名与五个计数器不变；`RunOnce`/提交纪律不变
  （transport 错误仍不提交任何偏移，已解析记录也留到下一轮）。

**回归测试：** `internal/kafka/replay_test.go` 与 `kafka_test.go` 新增
AC-1..AC-7：空 key/错 key 按 payload 匹配、缺席原事件一轮收敛、空
`event_id` 不发布、持续灌入 cut-off（不标记、记录 pending、下一轮重试）、
transport 错误零提交、drained-vs-cutoff 边界（整窗口安静=标记、窗口内仍
有消息=截断、外层取消=不标记、已找到但瞬态失败=保持 pending）。

## 2026-08-10 — worker: 聚合 checkpoint 去重、空闲不写快照 + 每租户保留上限

**写给运营（新配置项）：** `audit-governance-worker` 新增环境变量
`AUDIT_AGGREGATE_CHECKPOINT_HISTORY`（默认 1000），覆盖每租户保留的聚合
checkpoint 上限；非法/非正数值回退默认值并打印
`warning=invalid_aggregate_checkpoint_history`，绝不因配置笔误禁用保留或杀死
worker。存量快照无需迁移：超限历史在下次写入时收敛（drop-oldest），下限 1
（最近一条记录永远保留）。

**写给开发（行为变化）：**

- **去重（FR-1）**：`CreateAggregateCheckpoint` 在候选记录（root 与签名）与
  租户最近一条记录相同时不再追加；比较在乐观锁闭包内进行，并发写入者
  追加的事件在重试后的新鲜快照上被观察到，绝不产生重复记录。
- **空闲不写（FR-2）**：`Store.UpdateChecked` 只在实际变更快照时 Save；
  无待封段且聚合根未变化的周期不序列化、不 bump version/updated_at，
  PG 行字节数保持不变（AC-3 断言）。
- **保留上限（FR-4）**：每租户最多保留 `N` 条聚合 checkpoint，与追加同窗
  裁剪；`VerifyIntegrity` 只验证保留记录（FR-5），聚合校验工作量由 `N`
  界定、与运行时长无关。
- 签名方案、`Signer` 接口、worker CLI/默认间隔、OpenAPI 路由与快照既有
  字段布局均不变；保留记录的篡改检测不弱化（C4）。

**回归测试：** `internal/service/aggregate_checkpoint_test.go`（AC-1 去重/控制
leg/租户隔离、空闲零 Save、封段安静零写、签名失败上抛、冲突重试下去重
语义、并发赢家后追加、trim 边界、AC-2 保留上限内验证 + 计数 Signer 证明
工作量有界 + 篡改保留记录仍失败）与 `internal/service/postgres_idle_test.go`
（AC-3：真实 PG 行上 M 轮空闲 pass 后 version/updated_at/字节数/记录数不变；
`AUDIT_TEST_POSTGRES_DSN` 未设置时干净跳过）。

## 2026-08-10 — worker: archive pass 一次乐观锁窗口提交 + 持久化冲突计数

**写给运营（worker 日志行形状变化）：** `audit-governance-worker` 的
`archive_error` 日志行自本版本起追加 `conflict_failures=%d` 字段（原行其余
字段不变）：

```
tenant=<id> archive_error=<err> archived=<n> conflict_failures=<n>
```

- `conflict_failures` 是**持久化**的每租户连续失败计数（快照 jsonb 新增字段
  `archive_conflict_failures`，旧快照缺失字段解码为空并自动归一化，**无迁移**）；
  仅在归档 pass 因乐观锁耗尽（`ErrSnapshotConflict`）失败时自增，下一次成功
  pass 在同一次原子提交中归零。重启后仍可查询：
  `SELECT snapshot->'archive_conflict_failures' FROM audit_state_snapshot WHERE id=1;`
  （文件后端直接读 state 文件）。

**写给开发（行为变化）：** `ArchivePending` 从每事件一次 `Store.Update` 改为
每个 pass **恰好一次** `Store.Update`（一个乐观锁窗口、一次全快照 jsonb 重写），
闭包基于本 pass 成功 Put 的事件列表重跑，Put 循环与幂等语义不变：

- **原子失败**：单次批量提交失败时 `archived=0` 且零收据被标记（不再有部分进度）；
  已 Put 对象字节级幂等，下一 pass 重 Put 后收据收敛。
- **Put 失败/收据缺失**：pass 中止，返回 `(0, err)`，不开窗口。
- **日志**：耗尽失败时 worker 先尽力自增计数器（写失败单独记录
  `archive_conflict_record_error`，绝不掩盖原 pass 错误），再打印带计数日志行。
- 存储重试预算（3 次、≤30ms 抖动退避）、`Store.Update` 语义、归档 WORM/
  幂等、worker 每租户调用序列均不变；无 API 路由/OpenAPI/迁移变化。

**回归测试：** `internal/service/archive_batch_test.go`（AC-1 单窗口 saves==1、
预算内收敛 saves==3/loads==3、AC-2 耗尽原子中止+自愈收敛、AC-3 计数自增/
重启存活/同窗归零、缺收据/空 pass/Put 失败边界）与
`cmd/audit-governance-worker/main_test.go`（worker 计数分支：耗尽自增、非冲突
错误不记录、计数写失败不掩盖原错误）。

## 2026-08-10 — search digests stripped from timeline responses

**写给读方（API 行为变化）：** `GET /api/v1/operations/{operationID}/timeline`
与 `GET /api/v1/aggregates/{aggregateType}/{aggregateID}/timeline` 的响应自本版本起
与 `/events`、`/events/{id}` 一致，在 HTTP 边界剥离事件 payload 中所有
`*__search_digest` 键（任意嵌套深度，仅响应副本，绝不改动存储快照）：

- 摘要值曾在响应中可见，可被用于跨租户关联（威胁模型边界 D）；两个时间线端点
  是最后一个未剥离的读路径，现已闭合。剥离失败时 500 失败关闭（不写任何响应字节）。
- **存储与导出不变**：摘要仍留在 store/投影/归档与导出的源快照中
  （`runExport` 本就在自身边界剥离）；`GET /api/v1/operations/{id}/replay` 返回
  派生状态（`ChangedFields` 回声，不含服务端派生摘要），不受影响。
- 响应形状不变：`200 {"items": [...], "count": n}`，`404`/`401`/`403` 语义不变，
  无 OpenAPI/权限/迁移变化。
- 新增回归测试（AC-1，两条时间线路由）+ 静态守卫测试（AC-2）：
  `TestEventReturningHandlersStripSearchDigests` 解析 `server.go`，要求路由表与
  `Handler()` 实际注册一致、每个调用事件返回型 `s.Service.<Method>` 的处理器在
  `writeJSON` 之前调用 `StripSearchDigests`——未来新增未剥离的事件返回型路由会直接失败。

**已记录的后续决策（不在本次范围内）：**

- **R1/F2（v1 未绑定摘要探测）**：`payload_digest` 查询仍接受 v1 未绑定摘要的相等匹配与
  v1 重派生（`service.go digestMatches`）；已捕获的 v1 摘要值仍可作为跨租户探测句柄。
  建议后续弃用 v1 匹配（仅保留 v2 绑定重派生）+ 遗留摘要轮换策略。
- **R2/F3（时间线无界）**：时间线无分页/上限，剥离增加每事件 CPU；后续需
  分页/封顶。
- **R3/F4（ingest 卫生）**：无 `AllowedFields` 的 schema 下，客户端植入的
  `*__search_digest` 键会原样入库并参与匹配；后续需在 ingest 时删除/拒绝未声明键。
- **F5（replay/restore 状态回声）**：`ChangedFields` 中的摘要命名键会出现在
  `/replay` 与恢复预览中（仅客户端自回声）；本变更不处理，后续在状态边界剥离。

迁移：无（纯响应边界修复）。

## 2026-08-10 — proto 漂移门禁：生成的 pb.go 与 audit.proto 强一致

**写给开发者的行为变化：** `api/proto/` 的 checked-in 生成代码（`audit.pb.go`、
`audit_grpc.pb.go`）从本版本起由质量门禁强制与 `api/proto/audit.proto` 同步，
不再可能“改了 .proto 但忘了重新生成”还保持全绿：

- **新门禁 `proto-sync`**（`checks/proto_sync.py`，随 `python3 cli.py quality` 执行，
  位于 `contract_fields` 之后、Go 阶段之前快速失败）：
  1. **描述符解析（始终开启、零外部依赖）**：解码 `audit.pb.go` 内嵌的
     `file_audit_proto_rawDesc`，与 `.proto` 逐消息逐字段比对
     （字段名/编号/repeated/类型）——新增字段未重新生成即报 FAIL 并点名字段；
  2. **生成器版本钉死**：生成文件头版本注释必须匹配 `engineering.yaml` 的
     `proto:` 块（protoc v3.21.12 / protoc-gen-go v1.36.11 /
     protoc-gen-go-grpc v1.5.1），且 `protoc-gen-go` 必须等于 `go.mod` 的
     protobuf 版本；
  3. **字节回放（工具链存在时）**：`scripts/proto-gen.py --check` 用钉死工具
     重新生成并逐字节比对；无工具链时显式跳过（绝不静默通过）。
- **`python3 cli.py generate` 不再谎报成功**：同步失败时以非零码退出；
  成功路径输出改为执行真实检查。
- **`make proto`**：钉死工具链引导（protoc zip → gitignored `bin/`、
  `go install @pin`）并重新生成。CI 断言：
  `make proto && test -z "$(git status --porcelain api/proto)"`。
- **对账**：checked-in 生成文件已用钉死工具重新生成——仅注释/格式差异，
  `rawDesc` 2234 字节逐字节一致，无任何线协议/字段变化，无 go.mod 变更。
- 漂移演练：`bash scripts/drift-simulate.sh` 在 `git archive HEAD` 副本注入
  漂移字段，要求门禁点名失败、恢复后通过（绝不改动工作树）。

迁移：无（纯门禁/构建工具链变化；运行期行为不变）。

## 2026-08-10 — body `stream_id` stripped on ingest (stream consistency)

**写给写入方：** `POST /api/v1/events`（及 batch）请求体中的 `stream_id`
自本版本起在 `Service.Ingest` 中被剥离——字段仍被接受（不做 400/422
拒绝），但不再参与流解析、存储、哈希、分段封存或归档路径。流由服务端
从 tenant + aggregate/operation/source 派生并回填（与 `tenant_id` DS-08
同级的 stream consistency 规则）；`stream_id` 仅作为查询/完整性校验的
只读过滤条件。回执与存储事件的 `stream_id` 恒为服务端派生值。

- 兼容性：无数据迁移、无回填；历史事件保持原流位置，链哈希与签名
  manifest 不受影响（混合流可共存，逐流校验）。
- 幂等语义不变：去重/冲突按 `event_id` + 内容摘要（不含 `stream_id`）。
- 新增机械门禁 `checks/stream_consistency.py` 随 `python3 cli.py quality`
  强制执行剥离顺序（DS-08 之后、`event.Stream()` 之前、服务端回填之后）。

## 2026-08-07 — B4-2 严格 scope registry 在验证栈启用：审计 scope 矩阵注册 + e2e 全链复验

**跨仓语义闭环（IdP 侧 B4-2 的验证栈落地）：**

1. **scope registry 启用**（deploy/idp.verify.yaml `oauth.scope_registry.enabled: true`
   + `extra_scopes` 注册全部 9 个审计 scope）：未注册 scope → 400
   `invalid_scope`（实测）；注册的审计 scope 正常发证。e2e 每次 mint 都走
   该严格门禁，验证栈配置同时是生产 B4-2 的参考注册表。
2. **G1 全链复验（registry 严格路径）**：真实 IdP token 全链路 15 项断言
   exit 0（dev token 401、gRPC 写入、账本/投影/归档/完整性、治理链、
   restore 职责分离双主体、legal hold release）。
3. **e2e 健壮性**：PostgreSQL 冷启动就绪等待（迁移前 pg_isready 循环）。

迁移：无。

## 2026-08-07 — 治理链 e2e 补全：restore 职责分离（双主体）+ legal hold release

**e2e 断言扩展（dev 8→10 项，G1 13→15 项）：**

1. **restore 审批职责分离端到端**：G1 模式用两个真实 IdP 主体
   （`demo` 创建 + `demo-admin` 审批）——同人审批 403、第二主体审批
   approved、状态机 pending_approval→approved 全链断言；dev 模式断言
   同人 403（dev token 单主体语义，sub 恒等于租户——本身就是职责分离
   验证）。
2. **legal hold release**：创建后释放 200 + 自审计行。
3. **IdP 双主体 fixture**：`deploy/idp.verify.yaml` 增加 `demo-admin`
   client；fullstack 第二次 mint 前保存/恢复 `AUDIT_OUTBOX_TOKEN`
   （relay 必须保持 demo 身份）、scope 收窄到 demo-admin 允许集。
4. **gRPC 真实写入**（B1-6）：`test/e2e/grpcwrite` 小客户端经真实
   socket 调用 Write RPC（bearer metadata + wait_for=ledgered + 回执
   断言），双模式通过。
5. **新检查自测**：dev_auth_manifest / tenant_consistency 支持注入 root，
   test_quality_checks.py 增至 10 用例（verify 豁免、生产清单命中、
   stamp-before-check、缺失检查等 FAIL/PASS 路径）。
6. **文档同步**：GLOSSARY（tenant_id DS-08 语义 + 6 个新术语）、README
   ADR-0006/0007 链接。

实测（2026-08-07）：G1 模式 exit 0（15 项断言）、dev 模式 exit 0（10 项
断言）、QUALITY PASS。

## 2026-08-07 — 自包含验证栈：audit-idp 容器 + governance-worker 入栈 + 治理断言链（G1 一键复现）

**验证栈完整性（fullstack.sh 单命令复现 G1，无需手动启动 IdP）：**

1. **audit-idp 容器**（deploy/idp.verify.yaml + compose 服务，从兄弟仓库
   snaplink 构建）：G1 模式不再依赖宿主手动启动的 IdP——`AUDIT_IDP_CLIENT_ID`
   /`AUDIT_IDP_CLIENT_SECRET` 两个环境变量即可复现完整 G1（token 铸造 →
   dev auth 关闭 → JWKS 验证 → dev token 401 断言 → 全链路）。
2. **audit-governance-worker 入栈**：与 audit-api 共享 PostgreSQL 控制面快照
   （`audit_state_snapshot` 单行 + 乐观锁）——B1-2 多副本冲突重试的真实
   载体；迁移 004 在应用启动前应用（audit-api bootstrap 需要表存在）。
   e2e 新增 worker `-once` 断言（留存评估输出）。
3. **治理断言链**：留存策略设置/评估、Legal Hold 创建、导出完成/下载、
   worker 单轮评估——G1 与 dev 双模式均通过。
4. **PG 切换保护**：audit-api 的文件→PG 切换检测（防数据丢失）在验证栈
   中由 `AUDIT_ALLOW_PG_EMPTY_LEDGER=true` 显式覆盖（仅 verify 栈；生产
   切换必须走受控迁移）。
5. **e2e 依赖顺序重构**：基础服务（postgres/redpanda/clickhouse/minio/
   jaeger）先行 → 迁移 → 应用服务；IdP 先行就绪再 mint token。

实测（2026-08-07）：G1 模式 exit 0（12 项断言）、dev 模式 exit 0（8 项
断言）、QUALITY PASS。

## 2026-08-07 — G1 e2e 全栈通过：真实 IdP token + dev auth 关闭（T-1.1/T-1.2 联合断言链）

**跨仓收口（本仓侧完成，B1-7/B4-1 联合）：**

1. **G1 模式 fullstack e2e 全绿**：`AUDIT_IDP_TOKEN_URL`/`AUDIT_IDP_CLIENT_ID`/
   `AUDIT_IDP_CLIENT_SECRET`/`AUDIT_IDP_SCOPE` 配置后，`fullstack.sh` 自动
   mint 真实 IdP token 并把栈切到 **dev auth 关闭 + JWKS 验证**（
   `AUDIT_ALLOW_DEV_AUTH=false`、`AUDIT_JWKS_URL=http://host.docker.internal:
   18082/.well-known/jwks.json`、issuer 校验、loopback 豁免）。断言链：
   relay → Kafka → consumer → 账本（真实 token 202）→ **dev token 401 负向
   断言（T-1.1）** → ClickHouse 投影 → MinIO WORM 归档 → 完整性 valid →
   Jaeger trace（T-1.2 完整链）。实测通过（本机 sso-server + 全栈 compose）。
2. **JWKS loopback 豁免扩展**：`loopbackHost` 接受 `host.docker.internal` /
   `gateway.docker.internal`（容器侧宿主机回环的规范别名，仅在该显式
   flag 下生效；生产 HTTPS 强制不变，默认关闭且 check-config parity 拒绝）。
   compose audit-api 增加 `extra_hosts: host-gateway`。
3. **compose G1 插值**：`AUDIT_ALLOW_DEV_AUTH`/`AUDIT_JWKS_URL`/
   `AUDIT_JWT_ISSUER`/`AUDIT_ALLOW_INSECURE_JWKS_LOOPBACK` 可经环境变量
   覆盖（过渡期默认 dev auth true 不变）。
4. **e2e 脚本健壮性**：ClickHouse 就绪等待（容器启动竞态，此前 curl 失败
   exit 56）。

迁移：无。IdP 侧剩余动作 = 部署仓将 sso-server 容器化接入（提供
`AUDIT_IDP_*`），本仓 G1 fixture 路径与断言链已完整验证。

## 2026-08-07 — G1 真实 IdP 集成闭环：sso-server 签发 token 全链路验证 + mint-token.sh 修复

**跨仓收口验证（B1-7/B4-1/B4-2 联合）：**

1. **真实 IdP（snaplink sso-server）签发 token 全链路验证通过**：本机启动
   sso-server（EdDSA 签名，`/.well-known/jwks.json`），audit-api 以
   `AUDIT_JWKS_URL` + loopback 豁免 + issuer 校验接入，**dev auth 关闭**。
   结果：dev token 全路由 401；IdP 签发的平台 token（9 个审计 scope）管理
   操作 201；服务 token（`client_id=audit-relay` + `tenant_id=tenant-a`，
   B4-1 claims 落地）经来源绑定写入 202；envelope tenant 不匹配 422；
   篡改 token 401（JWKS 验签）；scope→权限映射（现有实现）使 `scope=
   audit:event:write` 直接授权写入。这验证了 B1-7 的完整跨仓语义：
   `scripts/mint-token.sh` 可直接消费该 IdP。
2. **mint-token.sh 修复**：改为 source 时直接 `export`（同时保留打印供
   `eval "$(...)"`）；fail-closed 缺配置退出码验证为 1。实测：source 模式
   拿到 token 并写入事件 202。
3. **验证过程记录**：sso-server 的 bootstrap 运行时产物（bootstrap.json）
   如落入仓库根目录会被 root-files 门禁捕获（已清理）；临时 IdP 配置仅用于
   验证，未修改 snaplink 仓库任何文件。

迁移：无。G1 收口剩余动作 = IdP 部署仓将 sso-server 接入 compose 编排
（提供 `AUDIT_IDP_*` 配置），本仓 fixture 路径与验证链路全部就绪。

## 2026-08-07 — 真实 JWT 路径验证（B1-7 服务端前提）；gRPC 受支持入站契约声明

**验证（无 dev auth，本地签发 RS256 JWT 带 IdP 同款 claims）：**

1. **真实 JWT 全路径**（`AUDIT_JWT_PUBLIC_KEY_PEM` + 固定 alg，dev auth 关闭）：
   dev token 全路由 401；真实 JWT（`client_id`/`tenant_id`/`roles` claims，与
   snaplink IdP B4-1 `buildAccessPayload` 同构）管理操作 201、写入 202
   （client_id → 来源绑定租户解析）、envelope tenant 不匹配 422、篡改签名
   401。RBAC 在真实路径完整生效：service 角色只写（查询 403）、auditor
   只读、`audit:policy:read` 才可见 admin/actions；读自审计
   （`audit.event.read`）actor = token `sub`。这验证了 B1-7 的服务端前提：
   IdP 只需发出同构 claims（B4-1 已落地）即可收口 G1。
2. **gRPC 受支持入站契约声明（B1-6）**：`api/proto/audit.proto` 的 Ingest
   service 注释显式声明 Write/WriteBatch/WriteStream 为受支持入站，与 HTTP
   共享认证/租户一致性（DS-08）/schema 校验/幂等/服务层自审计约束。
3. **全链路 e2e 复验**：drain 窗口修复后 fullstack.sh 再次全绿（账本 →
   ClickHouse → MinIO WORM → integrity → Jaeger → gRPC 探活）。

Migration: none（proto 注释级变更，无需重新生成 pb.go）。

## 2026-08-07 — 真实容器 e2e 全链路验证；DLQ 重放 drain 窗口修复；B1-7 fixture 路径；规则草案 RCA 收口

**验证与修复（全链路真实容器）：**

1. **Full-stack e2e 真实容器验证通过**（outbox → relay → Redpanda → consumer →
   账本 → ClickHouse 投影 → MinIO WORM 归档 → integrity → Jaeger；含 gRPC
   监听探活与 DLQ replay 服务）。修复了脚本自身的问题：MinIO Object Lock
   bucket 自举移到 `/readyz` 之前（S3Store.Ready 要求 bucket 存在且启用
   versioning）、integrity 断言匹配紧凑 JSON（`"valid":true`）。
2. **DLQ 重放真实链路闭环**（死信 → 重放 → 入账）：向 accepted topic 注入
   schema 未注册事件 → consumer 422 permanent → DLQ Failure；注册 schema 后
   `-once` 重放 → 事件最终入账（账本可查）+ 状态文件持久化。
3. **Replay drain 窗口修复**：`RunOnce` 的 collectFailures 与 scanAccepted
   曾共享一个 5 秒 drain context——DLQ drain 耗尽窗口后 scan 拿到已过期
   context（真实 kafka-go 在 ctx 过期时优先返回错误而非排队数据），重放
   静默为 0。现在每个阶段各自持有 drain 窗口（单测的 fake reader 在队列
   非空时无视 ctx 过期，掩盖了该问题；真实 broker 暴露）。
4. **`-once` 独立 consumer group + 不启动 metrics**：与常驻实例共享 group
   会在 rebalance 中竞争 accepted partition 导致单轮扫描读不到消息；`-once`
   现在使用 `-group-once` 后缀的独立 group，且不再抢占 metrics 端口。
5. **B1-7 fixture 路径（本仓部分）**：新增 `scripts/mint-token.sh`（调用
   IdP `/token` client_credentials，fail-closed 配置校验，输出
   `AUDIT_OUTBOX_TOKEN`/`AUDIT_E2E_TOKEN`）；`fullstack.sh` 按优先级使用
   显式 env token → mint 脚本 → 过渡期 dev token（显式 WARN）；compose 的
   relay/consumer token 改为 `${AUDIT_OUTBOX_TOKEN:-dev:demo:service}` 可
   覆盖。G1 收口仍需 IdP 侧 B4-1。
6. **规则草案 RCA 收口**：DRAFT-20260805（幂等维度）——账本幂等键为
   `(tenant_id, event_id)` 租户维度收窄，client 经来源绑定唯一映射租户，
   重复入账风险已被消除，promote 为 resolved；DRAFT-20260806（不可解析
   消息死信）——`unparsable_message` DLQ + 计数已落地（eedcae1），promote
   为 resolved。两者补齐 requirements/verification 字段。
7. **新门禁**：`checks/tenant_consistency.py`（B1-8 机械守卫：禁止
   `event.TenantID = tenantID` 出现在 mismatch 判定之前）挂入 quality。
8. **基准刷新**：读自审计（F-06）后 `BenchmarkQuery` 448 µs → 13.4 ms（每次
   查询追加一次全快照 Update 的写放大，参考实现固有成本；生产查询走
   ClickHouse 投影）；`BenchmarkQueryLargeLedger` 3.8 → 65.9 ms；
   BENCHMARKS.md 记录新旧基线。
9. **B1-4 原子性显式断言**：`TestGovernanceMutationAtomicity`（租户不存在
   / 重复 ID / 释放缺失 hold → 零部分写入、零 admin action）。
10. **ERP 契约对拍测试编译修复**（远程协作者提交引入）：
    `internal/httpapi/erp_contract_test.go` 的 `service.New` 少一个返回值
    （且需 `AllowDevSecrets: true`），修复后 M0-1..M0-5 全部通过。

Migration: none。Rollback = revert；replay drain 修复与 `-once` group 分离
是行为修复，`-once` 重放依赖独立 group 语义。

## 2026-08-06 — B1 收口：启动路径 dev-auth 白名单、manifest 扫描、快照 fsync、容量 cutover 门禁、compose gRPC

**Behavior changes (contract B1 remainder):**

1. **Startup-path dev-auth allowlist (B1-1, AC-3).** The environment-only
   allowlist gate that previously guarded only `-check-config` now applies
   to the real startup path: `audit-api` exits non-zero when development
   auth is requested via `-allow-dev-auth` without `AUDIT_ALLOW_DEV_AUTH=true`
   in the environment. Flag-only dev auth can no longer start the server,
   so CI cannot bless a configuration the runtime would reject. The env
   allowlist path (compose.verify, local runs) is unchanged.
2. **Dev-auth manifest scan (B1-1).** New `checks/dev_auth_manifest.py`
   (wired into `cli.py quality`) fails any non-verify deployment manifest
   under `deploy/` that enables dev auth (`AUDIT_ALLOW_DEV_AUTH: true` /
   `-allow-dev-auth=true`). `*verify*` files are the explicit local
   validation stack and remain exempt; the gate exists so production
   manifests cannot silently reintroduce dev auth.
3. **Snapshot fsync (B1-2).** `fileBackend.Save` now writes the temp file
   through `*os.File` with `Sync()` before the atomic rename (and removes
   the temp best-effort on any write/sync/close error), so a crash after
   rename cannot leave an empty or partial control-plane snapshot. Mode
   0640 and the idempotent atomic-replace semantics are unchanged.
4. **Capacity envelope + cutover gate (B1-3, decision #7 = option B).**
   `docs/BENCHMARKS.md` now records the capacity envelope (1k/5k event
   query curve from `BenchmarkQuery`/`BenchmarkQueryLargeLedger`) and the
   cutover gate: ≥10⁵ events per tenant or p95 > 500 ms requires wiring
   the relational ledger (option A) instead of snapshot scans.
5. **gRPC ingest in compose (B1-6).** `deploy/docker-compose.verify.yml`
   enables `AUDIT_GRPC_LISTEN` (mapped to host 19051) and
   `test/e2e/fullstack.sh` asserts the listener is open, covering the
   declared gRPC inbound in the local stack.
6. **Test additions.** HTTP-boundary T-12 (read self-audit rows visible via
   `GET /api/v1/admin/actions`), T-13 (422 body carries `tenant_mismatch`
   code), error-matrix rows for `tenant_mismatch`-422 and
   `snapshot_conflict`-503, file-backend fsync atomicity, and DSN-gated
   PostgreSQL cases for concurrent-update convergence (T-6) and the
   `Ready` probe failing on an unavailable database.

Migration: none (no schema/data/config change). Rollback = revert the
commit; the startup gate and manifest scan are the only behavior flips.

## 2026-08-06 — Tenant consistency 422; read-path self-audit; snapshot-conflict retry; DLQ replay consumer + alerts

**Behavior changes (contract B1: DS-08, F-06, F-01, DLQ follow-up):**

1. **Envelope tenant consistency (DS-08, 422).** `Ingest` now rejects with
   `tenant_mismatch` (HTTP 422 / gRPC `FailedPrecondition`, new `Error.code`)
   any event whose non-empty body `tenant_id` differs from the tenant
   resolved server-side from the authenticated client — the silent
   re-stamping of a mismatched envelope tenant is gone. An empty body
   tenant is still derived from the `(client_id, source_system)`
   registration. T-13 semantics: envelope tenant-b + token tenant-a → 422,
   zero ingestion. `POST /api/v1/events`, `POST /api/v1/events:batch` and
   the gRPC writes share the check (gRPC envelopes never carried
   `tenant_id`, so only the HTTP surface gains the new 422 response;
   OpenAPI updated).
2. **Read self-audit (F-06).** `QueryEvents` and `GetEvent` now append an
   `audit.event.read` admin action (actor = token subject, target = query /
   event ID) and export downloads append `audit.event.export`
   (`RecordExportDownload`, called after the job is verified completed).
   The append lives in the service layer so no transport can bypass it, and
   a failed append fails the read closed. Export creation already ran the
   query through `QueryEvents`, so it now records the exporter's read fact
   too. Empty-actor (system-internal) reads record nothing.
3. **Snapshot-conflict retry (F-01).** `Store.Update` re-runs the mutation
   closure on a fresh snapshot with bounded jitter (3 retries, 5–25 ms
   exponential + jitter) when the Postgres optimistic-lock save reports
   `ErrSnapshotConflict`; closure errors are never retried. Exhausted
   conflicts surface as HTTP 503 `snapshot_conflict` instead of a bare 500
   (idempotent callers retry). `/readyz` now probes the store backend
   (Postgres ping; 503 `store_unavailable` when unreachable) in addition
   to the archive probe.
4. **DLQ replay consumer + traffic alerts (release-notes follow-up).** New
   `audit-kafka-dlq-replay` binary recovers dead-lettered events: DLQ
   `Failure` records carry only metadata, so the original message is
   recovered from `audit.events.accepted.v1` by key and re-published
   byte-for-byte (default) or re-ingested via the audit API
   (`-api-url`/`-token`). Replayed event IDs persist to `-state`
   (`AUDIT_DLQ_REPLAY_STATE`, default `./data/dlq-replay-state.json`),
   making full-topic re-scans idempotent; transient republish failures stay
   pending for the next round, permanent API rejections (4xx except 429)
   are marked replayed to converge. `-once` for scheduler use or daemon
   mode with `-interval`. `audit-kafka-consumer` and the replayer expose
   text-format `/metrics` (`AUDIT_KAFKA_METRICS`/`AUDIT_DLQ_REPLAY_METRICS`;
   `audit_consumer_dlq_published_total`, `audit_dlq_*`), and
   `deploy/prometheus-rules.verify.yml` adds `AuditDLQTraffic`,
   `AuditDLQBacklog` and `AuditDLQRepublishFailures` alerts. Compose
   (`deploy/docker-compose.verify.yml`) runs the replayer and scrapes both
   new endpoints.

Migration: none (no schema/data/config change; the state file of the
replayer is new). Rollback = revert the commit. Contract: OpenAPI gains 422
on write routes and rewords `Event.tenant_id`; `Error.code` gains
`tenant_mismatch`/`snapshot_conflict`; the admin trail gains
`audit.event.read`/`audit.event.export` actions.

## 2026-08-06 — tenant/field-scoped search digests; digests stripped from API and export responses

**Behavior change (security, threat-model boundary D):** search digests are
now bound to the tenant ID and field name. `security.SearchDigestBound`
derives `sd2:`-prefixed digests whose HMAC input is the canonical JSON value
plus the tenant ID and field name, so the same plaintext in two tenants (or
under two field names) yields different digests — an actor holding read
access to several tenants can no longer correlate records across tenants by
digest equality. The stored key name (`<field>__search_digest`) and the
wire/OpenAPI contract are unchanged.

- Search compatibility: `QueryEvents`, legal-hold filtering and export
  filtering accept both formats. Same-format digests compare directly;
  mixed formats re-derive the query-format digest from the stored plaintext
  (fail-closed when the plaintext is unavailable — encrypted+searchable
  fields are same-format only). Old v1 clients can search events ingested
  under the new format and new clients can search legacy events; event
  idempotency and `VerifyIntegrity` are unaffected (`SourceDigest` is
  computed pre-protection and `reconstructAndDerive` deletes digest keys by
  schema name).
- Response redaction: `GET /api/v1/events/{id}` and `GET /api/v1/events`
  now strip every `*__search_digest` key from the payload recursively, on
  deep copies only (the store keeps the digest for search; a strip failure
  returns 500 rather than leaking). Decrypted export JSONL is stripped the
  same way.
- Migration: none — digest values are derived at ingest; existing events
  keep their v1 digests and keep matching through the fallback. Plain
  deploy/revert; no backfill (WORM immutability).
- Scope guard: archive stripping, key rotation/HKDF separation, and
  timeline/replay endpoints remain explicitly out of scope (sibling
  findings).

## 2026-08-06 — outbox.Insert reports duplicate/conflict outcomes instead of silently dropping events

**Behavior change (data integrity):** `outbox.Insert` no longer discards the
`ExecContext` result of its targetless `ON CONFLICT DO NOTHING` insert. When
`RowsAffected() == 0` (one of the two uniqueness constraints — `event_id` or
`(tenant_id, idempotency_key)` — absorbed the row), `Insert` now classifies
the outcome inside the caller's transaction and returns a defined error
instead of nil, so a business transaction can no longer commit its domain
mutation believing the audit event was queued when no outbox row exists and
the relay will never deliver anything. Return contract (also on the `Insert`
doc comment): `nil` = durably recorded, or an exact duplicate of an existing
pending/delivered row (idempotent re-inserts never reset
`attempts`/`next_attempt_at`); `errors.Is(err, domain.ErrConflict)` =
deterministic conflict (same `event_id` with different canonical content,
idempotency-key reuse for another event, an identical row already
dead-lettered, or an unclassifiable zero-row outcome) — roll back, surface
409, **do not retry**; any other error = acceptance unknown (statement or
classification read failed) — roll back and retry with bounded backoff plus
jitter. Identical content is decided by jsonb equality first with an
`EventContentDigest` cross-check when jsonb differs only in representation
(same instant in another time zone, `1.0` vs `1`) — matching the ingest
path's canonicalization; a dead-lettered row is never silently resurrected.

- Public API unchanged: `Insert(ctx, tx Execer, event) error` and `Execer`
  are byte-identical; only unexported seams were added (`rowScanner`,
  `rowQueryer`, `classifyZeroRows`). No schema change, no new sentinel, no
  route/OpenAPI change.
- Callers: there are no production callers of `outbox.Insert` yet (the e2e
  suite writes `audit_outbox` directly); the change defines the contract
  future business-transaction writers rely on. Callers must roll back on
  any error and must not retry `ErrConflict`.
- Ops: none — the SQL shape is unchanged (still targetless `DO NOTHING`),
  so the relay's `ListPending` polling behavior is untouched. The payload
  parameter is now sent as text instead of `[]byte` (bytea has no cast to
  jsonb under the simple protocol), which is required for the insert to
  work on a real PostgreSQL.
- Known limits (accepted): under REPEATABLE READ/SERIALIZABLE a concurrent
  duplicate whose winner committed after this transaction's snapshot
  degrades fail-closed (raw unique violation, non-`ErrConflict` wrapped
  error) instead of classifying as idempotent; callers must keep the
  default READ COMMITTED. DSN-gated integration scenarios (S1–S8) cover
  this and the concurrent-duplicate determinism; they run only when
  `AUDIT_TEST_POSTGRES_DSN` is set.

## 2026-08-06 — Kafka consumer dead-letters permanently failing messages

**Behavior change (availability):** `audit-kafka-consumer` no longer retries
poison events forever. Ingest failures classified as permanent
(`outbox.DeliveryError{Permanent}` — the audit API's 4xx except 429) are
dead-lettered immediately; transient failures are retried **in place** — the
message is held in-process and re-delivered to the ingest callback up to
`-max-attempts` (default 8, env `AUDIT_KAFKA_MAX_ATTEMPTS`), and the next
message is fetched only after the held one is resolved (success, permanent
dead-letter, or cap exhaustion). This is load-bearing: kafka-go's reader
advances its fetch position past every message it returns, committed or not
(`reader.go`: `r.offset = msg.Offset + 1`), so re-fetching after a failure
would silently skip the failed message once a later message commits. The
pre-change loop had exactly that defect for transient failures; retrying the
held message closes it. A dead-letter publishes a `Failure` record
(`event_id`, `error_code`, `error_message` — the AsyncAPI
`audit.events.dlq.v1` contract, keyed by the failing event ID) via
`-dlq-topic` (default `audit.events.dlq.v1`, env `AUDIT_KAFKA_DLQ_TOPIC`),
commits the message, and continues. A DLQ publish failure or a consumer
without a publisher degrades to commit + log and never blocks partition
progress. New `error_code` values: `permanent_error`, `attempts_exhausted`.

- Wire-compatible: `audit-projector` keeps its existing flags and inherits
  the capped-retry behavior with commit+log degradation (no DLQ publisher);
  projection gaps after 8 failed inserts are rebuildable via `RebuildFrom`.
- Ops: pre-create `audit.events.dlq.v1` with ≥30d retention before deploy
  and verify the created topic's retention — broker auto-create (if
  enabled) would create it with default retention instead. Poison events
  are expected to clear consumer lag within one backoff cycle instead of
  stalling the partition.
- Known limits (accepted, tracked as follow-ups): (1) the cap also applies
  to outage-class errors — at defaults (30s HTTP timeout + 2s backoff × 8
  attempts) a dependency outage longer than ~4.3 min drains the partition
  into the DLQ as `attempts_exhausted`; the DLQ has no replay consumer yet,
  so recovery is manual (DLQ consumer + traffic alert are the follow-up).
  (2) Sustained 429 quota throttling beyond the same window dead-letters
  valid events. (3) There is no per-message ingest deadline (spec
  non-goal): a hung ingest (e.g. ClickHouse) can still stall a partition;
  the ledger path is bounded by the HTTP client `-timeout`. (4) A static
  `AUDIT_OUTBOX_TOKEN` that expires mid-run turns every event into a
  `permanent_error` DLQ record — rotate tokens before expiry.

## 2026-08-06 — Separation of duties enforced in restore approval

**Behavior change (security):** approving or rejecting a restore run now
requires a decision actor different from the actor who created the run. The
same principal can no longer create and decide a restore (previously the
happy path). The 403 response was already part of the documented contract
(`openapi.yaml` decide endpoints), so this is a wire-compatible tightening:

- The guard runs inside the atomic `Store.Update` closure in
  `transitionRestore` after the existing 404/409 checks, with precedence
  pinned **NotFound → Conflict → Forbidden** (404 masks cross-tenant
  existence; 409 dominates for decided runs). A refusal commits nothing:
  no version bump, no admin-action record.
- **Auth hardening (separately revertible):** the JWT `sub` claim is now
  parsed with the same strict rule as `client_id`/`azp` (`strictIdentityClaim`)
  — whitespace-padded or non-string subjects are rejected instead of
  slipping past the empty-string check. Without this, a padded `sub` could
  canonicalize the same principal into a different-looking actor string and
  bypass the exact-string same-actor guard.
- Dev-auth deployments become effectively single-principal per tenant
  (`sub == tenant ID == creator`), so every run is unapprovable via dev
  tokens. Documented escape hatch: a platform token
  (`audit:platform:cross_tenant`) naming the tenant via the `?tenant_id`
  query parameter — but even the platform cannot self-approve its own run.
- **Mixed-version window:** during a rolling deploy, the old binary can
  still approve runs the new binary refuses. Both write well-formed state
  (the old one simply lacks the check); no data migration or backfill is
  needed.

Migration: none (no data/config change). Rollback = revert the guard commit
(refusals write nothing, so reverting restores the old behavior with no
state repair). New tests: strict-sub rejection, same-actor 403 at HTTP and
service level, refusal atomicity (no admin action), concurrent distinct-actor
(1 winner / 1 conflict) and same-actor (all refused) races, platform escape
hatch.

## 2026-08-06 — Internal error details redacted from 5xx responses

**Behavior change (security):** HTTP error responses with status ≥ 500 no
longer carry internal error text. Previously `writeError` and the batch
partial-receipt path serialized `err.Error()` verbatim, so filesystem paths,
store paths, errno strings and crypto details (e.g. `open …/state.json.tmp:
is a directory`, archive `ENOTDIR`/`EISDIR` text) leaked to API clients on
download and ingest failures. `GET /api/v1/exports/{jobID}` also surfaced the
raw export-failure diagnostic via the `error` field.

Details:

- `errorBody(status, err, r)` is now status-keyed: every ≥ 500 response
  collapses to the fixed `internal server error` message / `internal_error`
  code (the code keeps the domain mapping, which by construction never maps
  to ≥ 500, so the two cannot contradict). The confirmed-dead
  `strings.Contains(message, "internal server error")` branch and the no-op
  `if status >= 400 { _ = r }` block were removed.
- The `Cache-Control: no-store` header on 5xx, the panic-recovery path, all
  4xx messages, the `os.IsNotExist`→404 download mapping and the envelope
  (`code`/`message`/`request_id`) are unchanged; OpenAPI `Error.message` is an
  unconstrained string, so the contract is schema-compatible with no spec
  edit.
- `getExport` masks a failed job's `error` field to the fixed
  `"export failed"` message at the API boundary on a value copy. Raw
  diagnostics stay in the operator-only snapshot (`state.json`) —
  persist-time sanitization is a documented deferral.

Migration: none (internal response-body change; no data/config migration).
Rollback = revert the commit. No new logging was added.

## 2026-08-06 — Dev authentication flips to fail-closed defaults

**Behavior change (security):** `-allow-dev-auth` now defaults to `false`, so a
bare `./audit-api` run with no JWT trust source fails startup with the
preserved string `no JWT verification trust source is configured` instead of
silently accepting unsigned `dev:<tenant>:<role>` tokens. Previously any
default deployment let an unauthenticated remote caller mint platform-admin
dev tokens (full cross-tenant read/write/governance access); malformed env
values even failed *open* (`boolEnv` fell back to the default `true`).

Details:

- **Default flip:** `flag.Bool("allow-dev-auth", strictBoolEnv("AUDIT_ALLOW_DEV_AUTH", false), …)`. Runtime semantics of an explicit `AUDIT_ALLOW_DEV_AUTH=true` / `-allow-dev-auth=true` are unchanged; `deploy/docker-compose.verify.yml` already sets the env (zero compose changes).
- **Strict env parsing:** `boolEnv` → `strictBoolEnv` for all four audit-api boolean vars (`AUDIT_ALLOW_DEV_AUTH`, `AUDIT_ALLOW_LOCAL_HS256`, `AUDIT_ALLOW_INSECURE_JWKS_LOOPBACK`, `AUDIT_ALLOW_DEV_SECRETS`). A malformed value exits 1 naming the variable *before* `flag.Parse` (so even `-h` exits 1); `strconv.ParseBool` literals (`1`/`TRUE`/`t`) behave like `true`; present-but-empty falls back to the default. The worker binary keeps its own lenient `boolEnv` (documented non-goal — its fallback is `false`, i.e. fail-closed).
- **Preflight gate:** `-check-config` now validates the authentication configuration with the same rules as startup (zero-auth configs fail preflight) and applies an **environment-only** dev-auth allowlist: `-allow-dev-auth` alone can never satisfy the gate (flag-only invocations fail with the auditable marker `check_config=fail auth=dev_auth_flag_not_allowlisted`); `AUDIT_ALLOW_DEV_AUTH=true` passes. Success output `check_config=ok …` is byte-identical.
- **Flag-beats-env precedence is unchanged** at runtime: `-allow-dev-auth=false` + env `true` keeps dev auth off; `-allow-dev-auth=true` + env `false` keeps it on — but the preflight fails in the latter cell, so CI cannot bless a config the runtime enables without the env allowlist.

Migration: set `AUDIT_ALLOW_DEV_AUTH` explicitly in the **runtime** environment (`true` only for dev; `false`/absent for prod), scrub any malformed values (they now hard-fail naming the variable), and remove `-allow-dev-auth` from CI `-check-config` invocations. Rollback = redeploy the previous binary; no data migration. Dev stacks should bind the published port to loopback (`127.0.0.1:19089:8089`).

## 2026-08-06 — Archive enablement keyed on the configured Store

**Behavior change (fix):** archiving is now enabled based on the configured
`archive.Store`, not the `ArchiveDir` string. In S3-only deployments (an S3
store injected via `external.Archive()` while `Config.ArchiveDir` stays empty)
the ingest gate now archives events and `ArchivePending` retries pending
receipts. Previously both treated the destination as unconfigured: ingest
skipped archiving and receipts stalled at `StatusIndexed` forever.

Details:

- New `archive.Configured(Store)` predicate (nil-safe; empty-dir `FileStore` →
  unconfigured, S3 and injected stores → configured), shared by the ingest
  gate, `ArchivePending`, and the readyz probe so the call sites cannot
  re-diverge.
- `FileStore.Put` now rejects an empty `Dir` before any filesystem access
  (same message as `Ready`) — previously it silently wrote into the process
  CWD.
- readyz now skips unconfigured stores instead of panicking on a nil store.

Only behavior flips: the S3-only bug itself, and the broken split-brain
configuration `&FileStore{Dir:""}` + `ArchiveDir:"/x"` (silent CWD writes →
loud `ErrInvalid`). Local file-mode behavior is unchanged. No migration
needed; rollback = redeploy the previous binary.

## 2026-08-06 — FileStore.Put fails loudly on corruption instead of reporting archived

**Behavior change (fix):** `archive.FileStore.Put` no longer treats *any*
pre-existing path as "already archived". Previously a write/sync failure
mid-Put left a truncated partial object at the final key, every later retry
hit EEXIST and returned nil, and the receipt was marked `StatusArchived` —
a permanently corrupt object in a WORM compliance archive with no repair
path. The same EEXIST branch silently accepted symlinks and directories.

New `FileStore.Put` contract (no signature/API/route changes):

- A pre-existing path that is not a regular file (symlink, directory,
  device) is an error.
- An existing regular file is only "already archived" when it is
  byte-identical to the new payload; a mismatch — or a file that cannot be
  read, and therefore cannot be verified — is an error, and the pre-existing
  object is never modified or removed.
- Any Write/Sync/Close error after the object was created removes it
  best-effort, keeping the invariant "Put returned an error ⇒ no object at
  the key". O_EXCL/`0o440` and the idempotent byte-identical retry behavior
  are unchanged.

**Impact:** identical retries (the normal replay path) still return nil;
only previously-silent corruption becomes loud. Ingest degrades such
failures to `StatusIndexed` (unchanged service behavior), so receipts stay
retryable via `ArchivePending` — operators must clear the blocking object
first. S3Store is unaffected. No data/config migration; rolling deploy with
a transient window where old and new binaries disagree on mismatch verdicts
(old: nil, new: error); rollback = redeploy the previous binary.

## Operator runbook — archive mismatch errors

**Symptoms:** `ArchivePending` returns `archive path ... already exists with
different content` (or `... cannot be verified`); affected receipts remain
`StatusIndexed` and are retried on every `ArchivePending` run.

**Diagnosis:** for the failing key (see `archiveEvent`/`archiveSegment` key
formats in `internal/service/service.go`), compare the object on disk with
the expected canonical JSON:

```sh
ls -la <archive-dir>/<key>
# regenerate the expected payload, e.g. for an event:
#   jq -S . <(git show HEAD:internal/service/service_test.go)  # reference only
od -c <archive-dir>/<key>   # truncated/corrupt content = crash-leftover partial
```

**Cause classification:**

1. Crash- or error-leftover partial object from a failed Put under the old
   binary (pre-deploy). The object is *not* the true event payload → safe
   to remove, then re-run `ArchivePending`.
2. Tampered object (an external process wrote the path). Removing it is a
   deliberate WORM exception — confirm with the tenant/compliance owner
   first; the re-archived payload is re-verified by `VerifyIntegrity`.
3. Genuine key collision (different payload, same key). Do **not** remove:
   investigate the producer; the receipt correctly refuses to mark it
   archived.

**Repair:** after removing a confirmed-partial object, re-run
`ArchivePending <tenant>` (or `audit-governance-worker`'s pending-archive
job); the receipt converges to `StatusArchived` only after a byte-identical
write. If `os.Remove` itself fails (read-only filesystem, permissions),
fix the destination and retry — the receipt stays `StatusIndexed` and is
never falsely reported archived.

**Verification:** `python3 cli.py check`; archive unit tests
`go test ./internal/archive/` cover the mismatch, unreadable, non-regular,
and write-fault (EFBIG/read-only) legs.
