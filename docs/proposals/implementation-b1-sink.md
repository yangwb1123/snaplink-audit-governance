# B1（sink P0 基线批次）实施提案 — snaplink-audit-governance

> 依据：`docs/campaigns/implementation-gate.md`（已批准审计契约 v2，GATE PASSED）、
> `docs/campaigns/campaign-sink-b1.yaml`。本提案**不修改代码**，仅产出逐项实施设计。
>
> 状态标记：✅ Verified（工作树/已提交代码中存在，附行号证据）｜⚠️ Partial（部分满足）｜
> ❌ 未做 ｜ [PROPOSED]（无法在本仓库验证，需实施时确认）。

> **当前树校正（2026-08-20）**：本文的逐项表格记录的是早期实施基线，不能
> 直接代表当前状态。当前代码已完成 B1-1 的启动/manifest fail-closed 门禁、
> B1-6/B1-8 的 gRPC 拓扑与 tenant 422 契约、B1-7 的可选 IdP token fixture，
> 以及 B1-3 选择的方案 B 容量 envelope（见 `docs/BENCHMARKS.md`）。关系
> ledger 方案 A、跨仓 IdP 的真实环境验收仍是外部部署边界；当前 `quality`
> 门禁为 `QUALITY PASS`。后续章节中的旧行号/“未做”字样保留作历史证据，
> 不应覆盖这条当前树校正。
>
> **重要基线事实**：工作树含一批**未提交原型**（`git status`）：`internal/domain/models.go`、
> `internal/grpcapi/server.go(+test)`、`internal/httpapi/server.go(+test)`、
> `internal/service/{service,governance}.go(+多测试)`、`internal/store/{store,postgres}.go`、
> `internal/kafka/kafka.go`，未跟踪 `internal/service/read_selfaudit_test.go`、
> `internal/store/conflict_retry_test.go`、`internal/kafka/replay.go(+test)`。
> **当前 `python3 cli.py quality` 门禁 FAIL（exit 1）**：`go vet` 报
> `internal/kafka/replay.go:205:49: (*log.Logger).Printf format %s has arg message.Partition of wrong type int`
> （`topic=%s` 绑定到了 int 型 `message.Partition`，缺 topic 实参）。该文件属于 B1-6 范围，
> 是实施开始前的**阻塞项**：修复格式串并接线，或删除原型。

---

## 1. 批次逐项设计

### B1-1 dev-token 关闭（S1） — ⚠️ Partial

**文件定位核实**

| 清单路径 | 核实结果 |
|---|---|
| `cmd/audit-api/main.go:45` | ⚠️ 行号漂移：`allowDev` flag 实际在 **:51**（`envDevAuth` 常量 :36；`-allow-dev-auth` 默认 `strictBoolEnv(envDevAuth, false)` **默认 false ✅**，提交 `146e227`） |
| `internal/auth/auth.go` `ValidateConfiguration` | ⚠️ 文件漂移：实现在 **`internal/auth/verifier.go:30`**（`internal/auth` 包内，路径近似成立） |
| `deploy/docker-compose.verify.yml` | ✅ 存在，:12 `AUDIT_ALLOW_DEV_AUTH: "true"` |
| CI manifest 扫描 | ❌ **不存在**：无 `.github/workflows`，`cli.py`/`checks/` 无 dev-auth manifest 扫描 |

**现状核实**
- ✅ 默认 false + dev token 拒绝：`Authenticator{AllowDev:false}` 时 `dev:` token → 401
  （`internal/auth/auth_test.go:139-143`，T-1.1 前半已绿）。
- ⚠️ "dev-only 配置拒绝启动" **只实现了一半**：`-check-config` 有环境白名单门禁
  （`main.go:246` `devAuthGateFailed := authenticator.AllowDev && !devAuthAllowlisted()`），
  但**真实启动路径**（`main.go` `ValidateConfiguration()` 调用）在 `AllowDev=true` 且无其他信任源时
  仍放行 —— `verifier.go:30` 末分支 `if !hasSecret && !a.AllowDev { error }`，且
  `auth_test.go:158` 显式钉住 "AllowDev alone must remain a valid runtime trust source"。
  与清单验收"dev-only 启动报错"直接矛盾。
- ❌ manifest 扫描缺失。

**改动设计**
1. `cmd/audit-api/main.go`：把 `runCheckConfig` 的 `devAuthGateFailed` 判定**上移到启动路径**
   （`authenticator.ValidateConfiguration()` 之后、开监听之前）：`AllowDev && !devAuthAllowlisted()`
   → `logger.Fatalf`。`-check-config` 保持同规则（两路径一致性已由注释承诺）。
2. 同步修订 `internal/auth/auth_test.go:154-159` 的钉住测试（"AllowDev alone must remain a
   valid runtime trust source" 需改为"仅 flag 不满足，须 env 白名单"）——这是**行为反转**，
   属有意契约变更，需在实现中显式更新该测试。
3. 新增 manifest 扫描：`checks/` 新检查（如 `checks/dev_auth_manifest.py`，挂入 `cli.py quality`
   的 `cmd_quality` 列表），扫描 `deploy/*.yml` 中 `AUDIT_ALLOW_DEV_AUTH: "true"`/`allow-dev-auth`，
   命中即 FAIL。`docker-compose.verify.yml:12` 在 B1-7 完成前用 `"false"` + IdP fixture 替换
   （见 B1-7），过渡期内 e2e 由 `scripts/mint-token.sh` 供给真实 token。

**验收断言映射**：T-1.1（`dev:platform:platform-admin` → 401 全路由 ✅ 已满足；
dev-only 启动报错 ❌ 本项补齐）+ 清单"manifest true → CI 失败"（本项新增扫描）。

**测试计划**
- 单元：`cmd/audit-api` 启动路径（或提取 `devAuthGateFailed` 为可测函数）+ `auth_test.go`
  行为反转测试；manifest 扫描自测（`checks/self_test.py` 模式）。
- 集成：`-check-config` 与真实启动对同一配置给出同一结论的 parity 测试。
- 回归：现有 `auth_test.go` 全部（除被反转的 :154-159）；`httpapi` dev-token 测试保持绿。

**风险**：破坏本地开发（仅传 flag 不传 env 的脚本立即启动失败）与现有 e2e（dev token 失效）。
回滚：删除启动上移判定即恢复（改动单点）；过渡策略：B1-1 与 B1-7 在同一 G1 门禁收口，
中间态 e2e 用 `AUDIT_ALLOW_DEV_AUTH=true` env（check-config 白名单语义）维持。

---

### B1-2 冲突重试（F-01） — ✅ 已实现（未提交原型），剩余：fsync

**文件定位核实**：`internal/store/store.go` / `internal/store/postgres.go` ✅；
`internal/httpapi/server.go` `statusForError` ✅（:1021-1028）。

**现状核实（全部 Verified）**
- `store.go:180-220`：`ErrSnapshotConflict` + `snapshotConflictRetries=3` +
  `snapshotConflictBackoff`（5ms 起、×2、cap 25ms + ≤5ms jitter）——**有界 jitter 重试已实现**。
- `postgres.go`：单行 `audit_state_snapshot` 乐观锁（`lastVersion` 基线 + version 条件更新）✅。
- `httpapi/server.go:1024-1026`：`ErrSnapshotConflict` → **503**（非裸 500）+ `errorBody`
  code `snapshot_conflict` ✅。
- `readyz` store probe：`store.go` `Ready` + `postgres.go` `Ready`（Ping）+ `server.go:172-176`
  503 映射 ✅。
- 测试：`internal/store/conflict_retry_test.go`（未跟踪，`conflictBackend` 伪造 2 次冲突 →
  闭包重跑 3 次、有界耗时断言）✅。

**剩余改动**：清单含 "fsync" 一条 —— `fileBackend.Save`（`store.go`）`os.WriteFile(tmp)` +
`os.Rename` 无 `fsync`。设计 [PROPOSED]：`tmp` 写后 `f.Sync()` 再 rename（`*os.File` 直写替代
`os.WriteFile`），保持原子替换语义。

**验收断言映射**：T-6 前半（等 digest → 1 行 + 202/Duplicate 无 500）——HTTP 层已有
`Duplicate`/`Conflict` 计数与测试；后半"停 PG → readyz 503"需 **PG 集成测试**（见下）。
T-6 的"PG CI 不 SKIP"要求 `postgres_test.go` 已有集成骨架（`TestPostgresBackend*`，含
`:212` 并发无冲突、`:256` 失败恢复），补一个真冲突用例：两实例同 DSN 并发 Update →
其中一方 retry 收敛不报错（[PROPOSED]，需 CI 提供 PG 服务）。

**测试计划**
- 单元：冲突重试（已有）+ fsync 路径（文件落盘后内容/权限断言）。
- 集成：PG 双后端并发 Update 收敛 + 停 PG（`db.Close` 后 `Ready` 报错 → readyz 503）。
- 回归：`store_test.go` 文件后端全部；`httpapi/server_test.go` 错误映射表（:653 附近）加
  `snapshot_conflict → 503` 一行。

**风险**：重试闭包**重跑副作用**（闭包必须纯读-改 snapshot；现状所有 Update 闭包符合，且
`LoadForUpdate` 提供私有副本，Save 失败原子——注释已论证）。回滚：原型未提交，直接丢弃即回到
旧单次 Update；兼容性：503 语义对客户端是"结果未知可重试"，幂等键已存在。

---

### B1-3 Relational ledger 或容量预算（决策 #7） — ❌ 未做

**文件定位核实**：`migrations/001_control_plane.sql` ✅ 存在且为完整关系 schema
（`ledger_events`/`ledger_streams`/`ledger_segments`/`signed_checkpoints`/`event_receipts`/
RLS policies），但 **Verified 死代码**：`postgresBackend`（`internal/store/postgres.go`）只读写
`audit_state_snapshot` 单行（migration 004），**从不触 ledger 表**。`QueryEvents`
（`service.go`）遍历 `data.Events` 全量 map —— **O(ledger) 未解决**。
`internal/service/bench_test.go:87` `BenchmarkQueryLargeLedger`（5,000 事件）已存在，
可作容量 envelope 基线。

**改动设计（二选一，需 gate 决策 #7 定案）**
- 方案 A（关系 ledger 接线，重）：在 `postgresBackend.Save` 的同一 DB 事务内做
  per-tenant 增量：`ledger_events` insert + `event_receipts` upsert + `ledger_segments` insert +
  快照行 version 更新；snapshot 仍作控制面读源，查询切 SQL（`QueryEvents` 走
  `ledger_events` 索引 `idx_ledger_events_tenant_occurred`）。文件后端保持现行为。
- 方案 B（容量预算，轻）：保留快照查询，但以 `BenchmarkIngest`/`BenchmarkQueryLargeLedger`
  产出容量 envelope（事件数 × 查询 p95 曲线），在 `docs/BENCHMARKS.md` 记录 cutover 门禁
  （如 >10⁵ 事件/租户必须切方案 A）；不解决 O(ledger) 本身，属"容量 envelope 记录"验收。

清单验收两行：①"容量 envelope 记录；高容量类 cutover 门禁"（B 满足）②"`QueryEvents` 不再
O(ledger)"（仅 A 满足）。**建议**：本批次做 B（低风险、无迁移），A 作为 G8 cutover 前置项
单独立项——需在实施评审时确认，标注 [PROPOSED]。

**验收断言映射**：G2 "容量 envelope"（B 方案）；"`QueryEvents` 不再 O(ledger)"（仅 A 方案）。

**测试计划**
- B：bench 基线入库（`cli.py bench`），BENCHMARKS.md 记录；回归现有查询测试。
- A（若选）：迁移 001 表接线后，PG 集成测试断言 ingest 后 `ledger_events` 行数 =
  入账数、`QueryEvents` 走 SQL 结果与快照一致；RLS policy 下跨租户查询返回空。

**风险**：A 方案双写一致性（快照 jsonb 与关系表不同事务=撕裂；同事务=快照行膨胀回退路径复杂）、
迁移执行顺序（001 在 compose e2e 已应用）。回滚：A 未执行新迁移即可整体回退；B 零代码侵入。
破坏兼容性：A 若改变 `QueryEvents` 分页/排序语义（快照 map 无序 vs SQL 有序）需回归对齐。

---

### B1-4 Self-audit writer（F13） — ✅ 已实现（基线提交）

**文件定位核实**：`internal/service/service.go` `adminAction`（:176-179）✅；
治理 handlers 全在 `internal/service/`（`service.go`/`sources.go`/`governance.go`）✅。

**现状核实（Verified）**：所有治理变更都在**同一 `Store.Update` 闭包**内追加
`data.AdminActions`（append 失败即闭包错误 → 整个变更回滚，无部分 legal hold）：
`CreateTenant`(:247)、`RegisterSchema`(:316)、`SetRetentionPolicy`(:365)、`AddSource`/
`UpdateSource`（sources.go）、`CreateLegalHold`/`ReleaseLegalHold`/`CreateRestore`/
`transitionRestore`/`CreateExport`（governance.go）。动作常量
`internal/domain/models.go:352-364`（`tenant.created`…`audit.event.export`）✅。

**剩余改动**：无代码改动。验收"治理失败 → 回滚（无部分 legal hold）"已由闭包语义保证
（`transitionRestore` 的 SoD 拒绝即例证，:365 注释"拒绝提交无 version bump、无 admin action"）。

**验收断言映射**：G2 "治理 fail-closed"。

**测试计划**：回归 `service_test.go`/`governance` 相关（release/approve/reject 失败路径
不产生 AdminAction 的既有断言）；补一条显式断言（[PROPOSED]）：`CreateLegalHold` 闭包中途
报错（如租户不存在）→ `LegalHolds` 与 `AdminActions` 均无新增。

**风险**：无新增（行为已存在）。回滚：不适用。

---

### B1-5 Read self-audit（F-06） — ✅ 已实现（未提交原型 + 测试）

**文件定位核实**：`internal/service/service.go` `QueryEvents`/`GetEvent` ✅
（签名已扩展 `actor` 参数，:607/:635）；export download 在
`internal/httpapi/server.go` `downloadExport`（:474-483 调用 `RecordExportDownload`）✅；
`internal/service/governance.go:46-55` `RecordExportDownload` ✅。

**现状核实（Verified）**：`GetEvent`(:607-624) 与 `QueryEvents`(:698-705) 在服务层追加
`audit.event.read`（`recordReadAction`，actor 为空跳过）；append 失败 → 读失败（fail-closed，
注释 :628-631 明示"不可审计的读不得报告为已服务"）；`RecordExportDownload` 追加
`audit.event.export`，失败中止下载（server.go :477-481）。**服务层实现 ⇒ 任何传输（HTTP/gRPC/
未来接口）不可绕过**，满足"直接 API 不可绕过"验收。测试：`internal/service/read_selfaudit_test.go`
（未跟踪）：查询/读取各产生 1 行 `audit.event.read`、actor 正确、空 actor 不记录、导出下载
产生 export 行 ✅。

**剩余改动**：无核心代码改动。可选 [PROPOSED]：`QueryEvents` 每查询 1 行（而非每事件 1 行）
的语义在 OpenAPI 注释中显式声明。

**验收断言映射**：T-12（查询后出现调用者 self-audit 行；直接 API 不可绕过）✅ 已满足
（服务层 + 测试钉住）。

**测试计划**：单元已存在（read_selfaudit_test.go）；补 HTTP 层一行断言（[PROPOSED]）：
`GET /api/v1/events/{id}` 后 `GET /api/v1/admin/actions` 出现该调用者 `audit.event.read`
（现测试只到服务层）。回归：全量 `service_test.go`（`GetEvent`/`QueryEvents` 签名变更已同步）。

**风险**：读路径每次读取多一次 `Store.Update`（写放大 2×，快照克隆成本）——高并发读时
p99 漂移（F6 drill 场景，B2-2 关注同款问题）。缓解：actor 为空跳过已挡系统内读；大查询
一次 1 行。回滚：移除 `recordReadAction` 调用点（单点、原型未提交）。

---

### B1-6 gRPC 拓扑收敛（F13） — ⚠️ Partial（代码 ✅，契约声明 ❌，kafka replay 阻塞）

**文件定位核实**：`internal/grpcapi/server.go` ✅；`cmd/audit-api/main.go` `--grpc-listen`
注册（:102-114 `grpcapi.Register(grpcServer, svc, authenticator)`）✅；`cmd/audit-outbox-relay`、
`cmd/audit-kafka-consumer`、`internal/outbox/` ✅ 全部存在且**已接线**（compose.verify 中
relay → redpanda → consumer → HTTP ingest 全链路，非孤儿）。

**现状核实（Verified）**：
- `grpcapi/server.go` Write/WriteBatch/WriteStream 全部经 `Service.Ingest`（:46/:79/:118）⇒
  与 HTTP **同一认证（bearer metadata→Authenticator）、同一校验、同一 self-audit（服务层）、
  同一 tenant 一致性**（`ErrTenantMismatch` → `codes.FailedPrecondition`，:195-197 与 HTTP 422 对齐）。
- `api/proto/audit.proto:88-90` 三 RPC 已声明。
- relay/consumer 非孤儿：`cmd/audit-outbox-relay/main.go`（HTTP 或 Kafka 投递）、
  `cmd/audit-kafka-consumer/main.go`（含 DLQ）均被 compose.verify 使用。
- 契约外无活动入站：`grep enqueue` 零命中 ✅。

**剩余改动**
1. ❌ **契约声明**：OpenAPI `writeEvent`（`api/openapi/openapi.yaml:14`）仍写
   "Body tenant_id is ignored" —— 与 B1-8 的 422 语义**直接冲突**，且 OpenAPI 无 422 响应、
   无 `tenant_mismatch` 错误码、无 gRPC 受支持入站声明。设计：更新描述为
   "envelope tenant_id 与令牌解析租户不一致 → 422 拒绝；空 envelope 由服务端派生"；
   `POST /api/v1/events` 与 `:batch` 增加 `'422': { $ref: Error }`；错误码枚举加
   `tenant_mismatch`；`audit.proto` 注释声明三 RPC 为受支持入站。
2. ❌ **kafka replay 原型**（`internal/kafka/replay.go`+`replay_test.go`、`kafka.go` 增量
   `Producer.Republish`/`ConsumerMetrics`）：**接线或移除**。该原型当前：
   (a) 无任何 `cmd/` 消费（`grep replay.` 零命中，孤儿）；(b) **go vet 失败导致质量门禁 FAIL**。
   设计：修复 `replay.go:205` 格式串（`topic=%s` 前补 `message.Topic` 实参或删占位符）后，
   二选一——移除（消费者 DLQ 已能 dead-letter，replay 属运维工具，移到 B2-5 死信重放的
   sink 侧对应物，[PROPOSED] 与 aero-id F9 对齐）或接线为 `cmd/audit-kafka-replay`（读 DLQ
   topic → `Producer.Republish` 回 accepted topic，供 compose 挂载）。
3. 可选 [PROPOSED]：compose.verify 增加 `AUDIT_GRPC_LISTEN` + 一个 gRPC 探活脚本，使 e2e
   覆盖 gRPC 入站（当前 compose 未启用 gRPC）。

**验收断言映射**：G2 "拓扑收敛"（契约覆盖全部活动入站：OpenAPI=HTTP ✅+422 补、proto=gRPC ✅、
asyncapi=Kafka ✅；契约外无入站路径 ✅）；"grep 无 enqueue" ✅。

**测试计划**：单元：`grpcapi/server_test.go`（现有）+ 422 对齐用例（:2 处改动已在原型）；
契约：`checks/contract_fields.py`/`route_contract` 回归 + OpenAPI 422 校验（新增断言
[PROPOSED]）；replay：修复后 `replay_test.go` 全绿，若接线则加 cmd 冒烟。回归：kafka
consumer 测试（`cmd/audit-kafka-consumer/main_test.go`）。

**风险**：契约声明后 gRPC 成为承诺面（认证/自审计/租户约束不得退化）；replay 接线引入
双投递（Republish 后消费端幂等键吸收，已有保障）。回滚：契约回退（文档）零代码风险；
replay 移除即恢复门禁绿。

---

### B1-7 跨仓 e2e（T-1.2） — ❌ 未做

**文件定位核实**：`test/e2e/fullstack.sh` ✅ 存在但 **Verified 仍使用 dev token**：
`Bearer dev:demo:tenant-auditor`（查询）、`dev:demo:compliance`（integrity）、
compose.verify 中 relay/consumer `AUDIT_OUTBOX_TOKEN: dev:demo:service`；
`deploy/docker-compose.verify.yml:12` `AUDIT_ALLOW_DEV_AUTH: "true"`。
`scripts/mint-token.sh` ❌ **不存在**（`scripts/` 目录不存在；`cli.py cmd_check_root` 允许列表
无 `scripts`，**新增目录需同步 root 允许列表**——见风险）。

**改动设计**
1. `scripts/mint-token.sh`（新，[PROPOSED]）：调用 IdP（B4-1 部署仓）`POST /token`
   client_credentials，产出带 `tenant_id`/`roles`/`scope` claims 的 JWT；输出
   `AUDIT_OUTBOX_TOKEN`/`Authorization` 值。**依赖 B4-1 的 IdP 端点与 client 凭据**
   （B4-1 未完成前无法在本仓验证脚本正确性——标注 [PROPOSED]）。
2. `deploy/docker-compose.verify.yml`：`AUDIT_ALLOW_DEV_AUTH: "false"`；audit-api 增加
   `AUDIT_JWKS_URL: http://<idp>:port/...`（或 `AUDIT_JWT_PUBLIC_KEY_PEM`，取决于 B4-1 部署）
   与 `AUDIT_JWT_ISSUER`/`AUDIT_JWT_AUDIENCE`；relay/consumer token 换 mint 产物。
3. `test/e2e/fullstack.sh`：token fixture 化（脚本内 `source scripts/mint-token.sh` 或
   环境注入）；断言不变（ingest 202 `ledgered` → read 200 → export 202 → integrity valid）。

**验收断言映射**：T-1.2（ingest 202 `ledgered` → read 200 → export 202）✅ 断言链已在
fullstack.sh（event in ledger / projection row / archive / integrity）；G1 联合门禁
（B1-1 + B4-1 + B1-7，T-1.1/T-1.2/T-8(a) 绿 + manifest 扫描绿）。

**测试计划**：集成=e2e 脚本本身（compose 全栈）；跨仓验证需 IdP（B4-1）就绪——
本仓只能做：token 注入后 dev token 全被拒的负向断言（`AUDIT_ALLOW_DEV_AUTH=false` 时
`dev:...` → 401，已有 auth 测试支撑）+ manifest 扫描把 compose `true` 判 FAIL。
回归：无 dev token 后既有 e2e 全部换新 token。

**风险**：B4-1（IdP 部署仓）未完成 ⇒ e2e 无法全绿 ⇒ **G1 阻塞**（联合门禁，非本仓可控——
跨批次依赖显式声明）；`scripts/` 新目录触碰 root-files 门禁（`cli.py cmd_check_root` 允许
列表需加 `scripts`，或脚本放 `test/` 下规避）。回滚：compose 恢复 env 白名单行即回退；
mint-token.sh 是纯新增。

---

### B1-8 tenant 一致性强制（S2/DS-08/SRE-07） — ⚠️ 代码已实现（未提交原型），契约未同步

**文件定位核实**：`internal/service/service.go:389` ⚠️ 行号漂移：`resolveIngestTenant`
在 :385，mismatch 判定在 **:397-399**。**清单描述"现状 `event.TenantID = tenantID` 静默重标"
已过时**：工作树在 :397 先判 `event.TenantID != "" && event.TenantID != tenantID` →
`ErrTenantMismatch`；:401 的 `event.TenantID = tenantID` 仅是**空 envelope 的服务端盖章**
（校验之后的派生赋值，非覆写）——实施时以工作树为准，无需"移除覆写"，需在评审中纠正清单表述。

**现状核实（Verified，均未提交原型）**
- 服务层：`service.go:397-399` mismatch → `ErrTenantMismatch`；`domain/models.go:42` 新增错误；
  零入账（判定在 `Store.Update` 之前）✅。
- HTTP：`httpapi/server.go:996-997`（code `tenant_mismatch`）+ :1024-1026（422）✅；
  测试 `server_test.go:123-134`（篡改 tenant → 422、零入账）✅。
- gRPC：`grpcapi/server.go:195-197` `FailedPrecondition`（对齐 422）✅。
- 服务层测试：`service_test.go:186` 附近 `TestIngestDerivesTenantFromUniqueServerSideSourceBinding`
  （mismatch → `ErrTenantMismatch`；空 envelope 派生；跨租户 hint → `ErrForbidden`）✅。

**剩余改动**
1. ❌ **OpenAPI 契约同步**（见 B1-6 第 1 点）：`writeEvent` 描述改 422 语义 + 422 响应 +
   `tenant_mismatch` 错误码（`components` 错误枚举）。
2. [PROPOSED] "grep 无覆写赋值"验收的机械检查：可加 `checks/` 规则——禁止
   `event.TenantID = tenantID` 出现在 mismatch 判定之前（或直接以现有测试 + 评审替代）。

**验收断言映射**：T-13（envelope tenant-b + token tenant-a → 422、零入账、producer 侧
dead 终态；grep 无覆写赋值）。"producer 侧 dead 终态"（relay 把 422 分类为永久失败 →
`StatusFailed`）已由 `internal/outbox/relay.go:48-53` `DeliveryError.Permanent`（client 错误
400/403/409 类 → 立即 dead-letter）支撑；**但 422 需确认进入 Permanent 分类**：`outbox/http.go`
`HTTPDeliverer` 的归类逻辑需核实 422 是否归类（见下）。

**测试计划**：单元/HTTP/gRPC 三层断言已存在；补：HTTP 422 响应体含
`error.code == "tenant_mismatch"` 断言（[PROPOSED]）；集成：relay 对 422 的 dead-letter
单测（`outbox/http.go` 若未归类 422 → 补 `Permanent` 分支 + `relay_test.go` 用例，对应
aero-vault B3-1 的 422→终态联动）。回归：全量 ingest 测试（tenant 派生/来源绑定/幂等）。

**风险**：行为变更（静默重标 → 422）破坏**依赖静默重标的生产者**（aero-vault/aero-id 若
envelope 带与 token 不一致的 tenant_id，将立即失败入账——v2 契约要求它们同源，但 rollout
顺序是 B1 先于 B2/B3，**存在中间态生产者兼容风险**；缓解：B1-8 与 B2-1/B3-1 的契约联动
在 G1 联合门禁评审中确认）。回滚：删除 :397-399 判定（单点、原型未提交）。

---

## 2. 依赖与顺序

**本仓批次内部依赖**（B1-1 → B1-8，按此序实施）：
1. **B1-1**（基线首项）：dev-auth 关闭先行，但其 manifest 扫描与 B1-7 强耦合（compose
   `AUDIT_ALLOW_DEV_AUTH` 两处同改）；
2. **B1-2**（独立）→ 提供 `ErrSnapshotConflict` 503 语义与 readyz probe，被 B1-6（gRPC 共享
   `Service.Ingest`/`Store.Update`）与 B1-8（入账路径）依赖；
3. **B1-4**（基线已满足，无新代码）→ B1-5 的 self-audit 注册表依赖它（同树已就绪）；
4. **B1-5**（依赖 B1-4）→ T-12；
5. **B1-6**（依赖 B1-2 的入账语义 + B1-8 的 422 对齐；**先修/移除 kafka replay 恢复门禁绿**）；
6. **B1-8**（依赖 B1-2 的 store 稳定性，实现已就绪；其 OpenAPI 同步与 B1-6 契约声明合并做）；
7. **B1-3**（独立，容量预算零依赖；若选关系 ledger 则依赖 B1-2 的 PG 事务路径）；
8. **B1-7**（依赖 B1-1 的 `AUDIT_ALLOW_DEV_AUTH=false` + **B4-1 跨仓**）→ 最后收口，与 B1-1
   一起过 G1。

**跨批次依赖**（v2 rollout 顺序：sink → aero-id(B2) → aero-vault(B3) → IdP(B4) → aero-im(B5) → console(B6)）：
- **B1 → B4-1（IdP token claims）**：B1-7 的 token fixture 与 `tenant_id` claim 依赖 IdP
  部署仓（snaplink HEAD `b3c839bb`）的 `buildAccessPayload` 落地；B4-1 未完成 ⇒ G1 联合门禁
  无法全绿（**本仓不可控的跨仓阻塞，需在 G1 评审中显式跟踪**）。
- **B1 → B4-2（scope registry）**：e2e token 的 `audit:event:write` scope 需 registry 配给
  （B1-7 的 202 断言依赖）；B4-2 未完成时 e2e 可用临时 scope 直发（过渡，标注 [PROPOSED]）。
- **B1 → B2（aero-id）/ B3（aero-vault）**：B1-8 的 422 语义是 B2-1/B3-1"producer 侧 dead
  终态"的服务端前提；B1-2 的 Duplicate/202 是 B2/B3 幂等重试的前提。
- **上游依赖本仓**：B2-B6 全部以 B1 为平台基线（sink 先 rollout，清单要点①）。

## 3. 风险汇总

| 项 | 破坏兼容性 | 回滚方案 |
|---|---|---|
| B1-1 | 仅 flag 无 env 的启动/脚本立即失败；e2e dev token 失效 | 撤销启动上移判定（单点）；manifest 扫描可独立开关 |
| B1-2 | 无（503 语义新增）；重试闭包重跑副作用需纯函数保证 | 原型未提交，直接丢弃 |
| B1-3 | 方案 B 无；方案 A 若改查询语义有对齐风险 | B：零侵入；A：不执行新迁移 |
| B1-4 | 无（基线行为） | 不适用 |
| B1-5 | 读路径写放大（p99 漂移，F6 drill 关注） | 移除 recordReadAction 调用点 |
| B1-6 | gRPC 成为承诺面；replay 接线双投递（幂等吸收） | 契约回退零代码；replay 移除 |
| B1-7 | 无（fixture 替换）；`scripts/` 新目录触碰 root 门禁 | compose 恢复 env 白名单 |
| B1-8 | 静默重标生产者（B2/B3 中间态）立即 422 | 删除 :397-399 判定 |

**跨仓风险**：B4-1/B4-2 未按时 ⇒ G1 阻塞（B1-7）；B2/B3 未对齐 ⇒ B1-8 上线后生产者
报错窗口。

## 4. 启动条件（批次门禁核对）

| 门禁 | 通过条件 | 本仓当前状态 |
|---|---|---|
| G0 | v2 三文档批准 | ✅ 已随系列完成（清单明确"本清单不阻塞"） |
| G1 | B1-1 + B4-1 + B1-7：T-1.1/T-1.2/T-8(a) 绿 + manifest 扫描绿 | ❌ 未满足：T-1.1 前半 ✅（401 已钉）；dev-only 启动拒绝 ❌；manifest 扫描 ❌；B1-7 ❌；B4-1 在 IdP 仓 ❌（跨仓） |
| G2 | B1-2..6 + B1-8：T-6、T-12、T-13、治理 fail-closed、拓扑收敛、容量 envelope | ⚠️ 未满足：T-6 PG 集成用例缺（retry 单测 ✅）；T-12 ✅（原型+测试）；T-13 ✅ 代码层（OpenAPI 未同步）；治理 fail-closed ✅；拓扑收敛 ⚠️（契约声明缺 + **kafka replay 破坏门禁**）；容量 envelope ❌（B1-3 未做） |

**前置阻塞（实施第一动作）**：修复或移除 `internal/kafka/replay.go`（go vet FAIL）→
`python3 cli.py quality` 恢复 PASS；随后按 §2 顺序实施 B1-1 → B1-8，每步后跑质量门禁
（AGENTS.md 要求）。

## 5. 未验证标注汇总（[PROPOSED] / 不可在本仓验证）

- [PROPOSED] `scripts/mint-token.sh`（B1-7）：需 B4-1 IdP 端点/client 凭据，本仓无法独立验证。
- [PROPOSED] B1-3 决策 #7 定案：方案 B（容量预算）vs 方案 A（关系 ledger）需 gate 评审拍板；
  本提案建议 B，A 列 G8 前置。
- [PROPOSED] `fileBackend.Save` fsync（B1-2 剩余项）；T-6 的"停 PG → readyz 503"PG 集成用例
  （需 CI PG 服务，本仓测试骨架 `postgres_test.go` 已具备接入点）。
- [PROPOSED] HTTP 层 T-12/T-13 断言补充（admin/actions 可见 self-audit 行；422 响应体
  `tenant_mismatch` code）。
- [PROPOSED] kafka replay 接线为 `cmd/audit-kafka-replay`（或移除）——与 aero-id B2-5 死信
  重放语义对齐后定案。
- ✅（已核实，非 PROPOSED）`outbox/http.go:24` 注释与代码："Client errors
  (400/403/409/422 etc.) are classified as permanent"，实现为除 429 外全部 4xx 判
  `DeliveryError.Permanent` ⇒ T-13 "producer 侧 dead 终态"在 relay 侧成立（422 →
  立即 `StatusFailed`，与 aero-vault B3-1 联动前提就绪）。
- 无法验证（本仓外）：B4-1/B4-2（IdP 部署仓）、B2/B3 生产者侧的 tenant_id 同源性与
  422 处理——跨仓门禁 G1 评审时核对。
