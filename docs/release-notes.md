# Release Notes

## 2026-08-28 — Idempotency-key collision no longer corrupts the original event's stored receipt (direction idempotency-key-conflict-permanently-corrupts-th-6365419a)

**For operators (behavior change):** When two ingests collide on the same tenant-scoped `idempotency_key`, the colliding (rejected) caller now observes a freshly-built, self-describing conflict receipt (`Conflict=true`, `ErrorCode="idempotency_key_conflict"`) and receives `domain.ErrConflict` exactly as before. The pre-existing *owner* event that legitimately owns the key is no longer affected: its stored receipt stays clean (`Conflict=false`, empty `ErrorCode`/`ErrorMessage`) and remains correct when re-read via `GetReceipt`. Previously the collision logic rewrote the owner's stored receipt to a conflict state, permanently corrupting a genuinely-successful ingest and returning the owner's corrupted receipt to the colliding caller.

**For developers:**
- `internal/service/service.go`: the legacy file-backed `Store.Update` closure now calls the bool-only `idempotencyKeyReused(...)` predicate and assigns `receipt = conflictReceipt(tenantID, event)` for the collider. The previous owner-receipt mutation (`data.Receipts[existingKey] = receipt`) is removed (R1/R3).
- `internal/service/tenant_ingest.go`: the hot/cold `ingestTenant` `Store.UpdateTenant` closure now calls `idempotencyKeyReusedTenant(...)` (bool-only) and assigns `receipt = conflictReceipt(tenantID, event)`. The previous owner-write `view.Ledger.SetReceipt(receipt)` is removed (R1/R3).
- `internal/service/archive_fallback.go` / `tenant_ingest.go`: `idempotencyConflictKey` / `tenantIdempotencyConflict` (which leaked the owner's key/receipt — the structural root cause) are replaced by the bool-only `idempotencyKeyReused` / `idempotencyKeyReusedTenant` predicates. Each has exactly one caller and cannot leak the owner.
- `internal/service/service.go`: new `conflictReceipt(tenantID, event)` constructor builds the self-describing conflict receipt from the *colliding* event's own identity (DRY, identical contract for both paths).
- R4 preserved: both closures still `return fmt.Errorf("%w: ...", domain.ErrConflict)`, so `errors.Is(err, domain.ErrConflict)` continues to hold for existing callers and the existing regression tests.
- The collider receipt is intentionally not persisted (the colliding event was never committed; `GetReceipt(collider)` still returns `ErrNotFound`) — no read-path regression. Tenant-scoped idempotency uniqueness semantics are unchanged.
- Regression/coverage: `internal/service/idempotency_conflict_test.go` adds `TestIdempotencyConflictKeepsOwnerReceiptClean` (AC1/AC2/AC3 for both `testService(t, false)` and `testService(t, true)`, via `assertOwnerReceiptClean` reading the persisted `Snapshot`/tenant `Ledger`) and `TestIdempotencyConflictKeepsArchivedOwnerReceiptClean` (archived-owner bonus). Existing guards `TestIdempotencyKeyCannotBeReusedAcrossEvents` and `TestArchivedReceiptProtectsIdempotencyKey` remain satisfied.
- No config, schema, API, or DB migration change. Rollback = redeploy previous signed binary; durable state is unchanged (the owner receipt was never corrupted going forward). Single-commit revert is safe.

## 2026-08-28 — Tenant-aware DLQ replay marks drained-lost originals unresolvable (direction internal-kafka-7ff0bf6a)

**For operators (behavior change):** `NewReplayer` always runs the tenant-aware replay path. Previously, when a DLQ record's original event was definitively absent from the accepted topic (retention expiry or never published there), the tenant-aware path left the record pending forever and never emitted the loss signal. Now, after a *drained* scan (the accepted topic has gone quiet) proves the original is gone, such a record is durably marked unresolvable: `audit_dlq_unresolvable_total` increments and the DLQ offset is committed exactly once. The `AuditDLQUnresolvableDrop` alert, which was dead on the production tenant-aware path, now fires for genuinely-lost originals. Lost originals no longer force a full per-round re-scan of the accepted topic.

**For developers:**
- `internal/kafka/replay_tenant.go`: `resolveTenantCandidates` gains a `drained bool` parameter. When `drained && !found[eventID]` and `candidates[eventID]` is empty, the record is durably marked via `MarkTenant(claimedTenantID, eventID)` (or an unscoped `Mark` only when the optional `Failure.tenant_id` is empty), counted once via `r.unresolvable.Add(1)`, and added to `resolved` so `commitResolved` advances the offset. Idempotency is guaranteed by `tenantUnresolvableAlreadyMarked`, so restarts/re-deliveries never double-count (FM-7).
- `R2/R3/R4` preserved: not-yet-drained scans, ambiguous candidates (≥2 tenant identities), and key-matched-but-unparsable originals all stay pending and are never counted unresolvable. The unresolvable branch relies only on the canonical key-slot occupancy recorded by `collectTenantCandidates` (`found`), so a key-matched accepted message whose payload decodes to a different event_id stays pending (tenant-replay regression preserved).
- `R7` legacy parity: only the local `replayed` return is bumped; `r.replayed` and the replay/permanent/unparsable counters are untouched. The legacy `scanAcceptedLegacy` path is unchanged.
- Regression/coverage: 10 new tests in `internal/kafka/replay_tenant_test.go` — `TestTenantUnresolvableDrainedCommitsOffset` (AC-1 + R7 split-counter matrix + republish-never-called), `TestResolveTenantCandidatesExpiredOriginal` (AC-3), `TestResolveTenantCandidatesAmbiguousNotUnresolvable` (R3 neg), `TestTenantFoundButUnparsableNotUnresolvable` (R4), `TestTenantUnresolvableOnlyWhenDrained` (R2), `TestTenantUnresolvableIdempotentAcrossRounds` (FM-7), `TestTenantUnresolvableEmptyClaimFallsBackToUnscopedMark` (FM-1 + tenant-safety), `TestTenantUnresolvableBarrierHeldBehindPending` (FM-8 both directions), `TestTenantUnresolvableMarkFailureKeepsPending` (FM-6), and `TestTenantUnresolvableIntegrationBroker` (AC-2, gated on `AUDIT_TEST_KAFKA_BROKERS`, skips without a broker).
- No config, schema, API, or DB migration change. Rollback = redeploy previous signed binary; durable state marks mean already-resolved records stay resolved.

## 2026-08-28 — Fix `fromProto` nil-Actor panic on missing actor field

A server-side panic was possible when a gRPC `Write`, `WriteBatch`, or `WriteStream` request omitted the `actor` field from the `EventEnvelope`. The `fromProto` function dereferenced the nil `*Actor` pointer in a struct literal before the nil guard could execute. The fix reorders the existing nil guard to precede the struct literal — no new logic, no API surface change. Missing-actor requests now return `InvalidArgument` with message `"actor is required"` instead of panicking with `Internal`. Four new tests (unit, Write, WriteBatch, WriteStream) cover the previously untested nil-Actor path.

## 2026-08-28 — Configurable clock skew tolerance for JWT `exp`/`nbf` validation

`Authenticator` now accepts a `ClockSkew time.Duration` field that widens the acceptance window for `exp` and `nbf` claim checks. A positive `ClockSkew` (e.g. `30 * time.Second`) lets the verifier accept tokens that expired up to `ClockSkew` seconds ago or are not-yet-active up to `ClockSkew` seconds in the future — absorbing typical IdP-to-verifier clock drift. The zero-value default preserves the existing strict zero-tolerance behaviour; no existing code is affected.

## 2026-08-27 — Executable OpenAPI HTTP contract

The checked-in OpenAPI contract now records every runtime handler, required permission, and `statusForError` classification. Success and error payload schemas cover all routes, including batch receipts, list wrappers, receipt diagnostics, and query stream filters. `checks/route_contract.py` validates this metadata, and `cmd/openapi-contract` performs pinned OpenAPI 3.1 validation with kin-openapi v0.148.0.

## 2026-08-27 — Explicit tenant scope for platform-capable API operations

Scoped API operations now reject omitted, empty, malformed, or repeated platform tenant selectors before service access. Tenant credentials remain bound to their signed tenant, while only Console event/facet reads and admin-action reads retain approved all-tenant filtering. Platform event writes select each event tenant from the body, and source, schema, retention, and legal-hold writes treat query selection as authoritative.

## 2026-08-27 — Strict accepted and ledgered Kafka channel schemas

Accepted messages may be pre-ledger, while ledgered messages now require server-assigned stream, sequence, and hash state (including predecessor rules). Producers and consumers enforce the payload `event_id` Kafka key; invalid ledgered/projector inputs are permanently dead-lettered before ingest or database access. Projection and archive Kafka channels remain inactive and undisclosed.

## 2026-08-27 — Strict DLQ Failure validation before replay

**For operators:** Malformed DLQ `Failure` values are now excluded from replay and quarantined only when their partition offset can be committed safely; they are never republished or replay-state marked. Monitor `audit_dlq_malformed_total` and the bounded malformed-record diagnostics.

**For developers:** Replay accepts exactly one complete JSON object with required string fields (`event_id`, `error_code`, and `error_message`), a non-empty `event_id`, and an optional string `tenant_id`. Trailing whitespace remains valid, while trailing data and multiple values are rejected. No topic, state-file, or persistence migration is required.

## 2026-08-26 — gRPC protobuf conversion rejects ambiguous and unknown data

**写给运营：**

- gRPC `Write`, `WriteBatch`, and `WriteStream` now reject repeated `changed_fields.field` names and unknown fields on supported envelope messages with `InvalidArgument`; rejected envelopes are not persisted.
- Oversized stream messages retain their existing skip-and-continue behavior, while duplicate and unknown-field errors terminate the stream.

**写给开发：**

- Validation uses protobuf reflection for `EventEnvelope`, `Actor`, `Target`, and `FieldChange` only. Request wrappers and `Timestamp` remain outside this validation scope.
- Distinct changed-field entries retain their existing strict JSON/`json.Number` conversion and canonical digest behavior; no protobuf or storage migration is required.

## 2026-08-26 — Ingest resource caps aligned across HTTP and gRPC

**写给运营：**

- HTTP event ingest now rejects persisted envelope strings over 8,192 UTF-8 bytes, more than 64 actor roles or targets, more than 256 changed fields, and batches over 500 events before the violating write. Single-event cap checks run before `Service.Ingest`; batch count rejection is atomic; member validation keeps the existing valid-prefix behavior.
- `before`/`after` changed-field values are capped after canonical JSON serialization. Clients must budget UTF-8 bytes and split larger batches while retaining idempotency keys.
- No persistence model or migration changes are required: cap failures happen before ledger and receipt writes.

**写给开发：**

- The caps are defined in `internal/domain/limits.go` and documented in both AsyncAPI and OpenAPI. gRPC parses changed-field JSON strictly with `UseNumber`, canonicalizes it, and measures the canonical bytes, keeping HTTP/gRPC admission parity.

## 2026-08-26 — Tenant-isolated DLQ replay resolution

**写给运营：**

- DLQ replay now resolves each physical record independently using the recovered canonical `(tenant_id, event_id)`. Same-ID records for different tenants no longer share replay marks, one-shot closures, deliveries, or commits.
- An incompatible tenant claim remains pending across replay rounds and restarts until a safe canonical match is available. `Failure.tenant_id` remains optional and unchanged.

**写给开发：**

- Replay membership, per-record error policy, durable resolution, and commit barriers use `(topic, partition, offset)`; the event ID is only a candidate lookup index. Focused tenant replay regressions cover sibling commit inheritance, independent tenant marks, authorization, and mixed error codes.

## 2026-08-24 — Projector rejects unsafe Kafka topology at startup

**写给运营：**

- `audit-projector` now fails closed before connecting to ClickHouse or Kafka when the effective source and enabled DLQ topics are identical, or when the consumer group is empty/whitespace-only. Configure distinct topics and a nonblank group before rollout.
- The existing source-topic fallback, flag/environment precedence, and explicit empty `-dlq-topic` behavior remain unchanged.

**写给开发：**

- Startup resolution and validation are isolated in `cmd/audit-projector/config.go`; invalid topology is rejected before any store, producer, or consumer factory is called. Regression coverage includes precedence, fallback collisions, blank groups, lifecycle ordering, disabled DLQ construction, and safe startup logging.

## 2026-08-22 — OTLP 网关路径前缀保留与端点校验

**写给运营：**

- `AUDIT_OTLP_ENDPOINT` 现在可配置 collector 网关前缀，例如
  `https://otel-gateway.example/otlp` 会向 `/otlp/v1/traces` 导出；此前固定的
  `/v1/traces` 会覆盖该前缀并导致网关后面的追踪静默丢失。
- 已含 `/v1/traces` 的完整端点不会重复追加；缺少 scheme、缺少 host 或使用
  非 HTTP(S) scheme 的端点会在启动时返回配置错误。

**写给开发：**

- `internal/telemetry.traceEndpointURL` 负责校验并幂等合成 signal path，exporter
  只接收一个完整 `WithEndpointURL`，不再用后置 `WithURLPath` 覆盖配置。
- 回归测试启动真实 `httptest` collector、生成 span 并在 `Shutdown` 刷新，直接
  断言收到 `/otlp/v1/traces`；发布前仍须运行 `python3 cli.py quality`。

## 2026-08-20 — 控制面 v2 热/冷分层与显式 PostgreSQL 切换

**写给运营：**

- 文件后端首次打开旧 v1 `state.json` 时会先写入 `.v1` 备份，再拆成控制面、每租户热文件和每租户 JSONL 冷 ledger；校验发现缺失租户文件、冷文件或残留 `.tmp` 会失败关闭。
- PostgreSQL 旧部署不会因新二进制自动重解释。切换前停止 API，应用
  `migrations/006_hot_cold_split.sql`，运行
  `go run ./cmd/audit-pg-migrate -confirm MIGRATE`（或调用
  `store.MigratePostgresSnapshot(db)`）；该操作锁定快照行，在同一事务中写入
  `audit_state_snapshot_v1_backup`、租户热表、冷 ledger，并最后标记控制面为 v2。
- 回滚前保留备份表和数据库 dump；文件回滚使用 `.v1` 恢复并移除 v2 兄弟目录，PostgreSQL 回滚恢复备份行、删除 006 表后再部署旧版本。切换未完成或目标布局不完整时服务应保持未就绪。

**写给开发：**

- `ReadTenant`/`UpdateTenant` 让 ingest、封段和归档 worker 只加载当前租户热文档；`TenantLedger` 以不可变 receipt 版本和 segment/checkpoint 家族索引支持重试幂等。
- ingest 使用 HotFirst，归档淘汰使用 ColdFirst；冷证据先落盘/入库，热事件随后删除，半完成状态可由下一轮 worker 收敛。
- 归档 receipt 的 idempotency lookup 不再扫描全量历史事件；热/冷 benchmark 覆盖 K=0、1,000、10,000、50,000，`TestIngestCostIndependentOfArchivedEvents` 固定 K=10,000 的近似 O(1) 成本边界。
- 迁移、布局完整性、容量斜率、账本版本继承、热/冷故障恢复、跨租户 CAS 和全量质量门禁由单测覆盖；发布前必须运行 `python3 cli.py quality`。

## 2026-08-20 — 投影脱敏与归档回退读取超时

**写给运营：**

- ClickHouse 投影继续只消费 post-ledger 事件；落库前会递归移除
  `__search_digest` 派生字段，schema 加密字段仍以 `enc:v1:` 密文保存，
  不会把明文复制到查询投影。
- 归档事件的回退读取新增单次 30 秒截止时间；WORM/S3 黑洞会让事件读取、
  查询、时间线、导出或完整性检查失败关闭，而不会无限占用请求或存储读锁。

**写给开发：**

- `internal/projection.projectionPayload` 通过 `security.StripSearchDigests`
  深拷贝后再做 canonical JSON，绝不修改账本事件 payload。
- `internal/service.archiveReadTimeout` 与调用方更短的 context deadline
  取交集；测试可缩短该包级 seam 验证取消语义。
- verify compose 已启用 `AUDIT_LEDGERED_BROKERS` 并创建
  `audit.events.ledgered.v1`，使 accepted → ledger → projector 链路闭合。

## 2026-08-20 — 归档事件热快照淘汰与可验证读回退

**写给运营：**

- 归档对象成功写入并验证后，事件正文会从控制面快照淘汰；receipt、序列、哈希链元数据仍保留，快照不会继续按归档事件正文增长。
- 事件详情、操作/聚合时间线、回放、导出和完整性校验会从 WORM 归档读回，并校验 tenant、stream、sequence、event_id 与 receipt hash；归档对象缺失或被篡改时请求失败关闭，不会返回静默 404。
- `GET /api/v1/events` 同样从 WORM 归档回退读取，已归档事件在淘汰热正文后仍保持可查询；归档后的 event_id 与 idempotency_key 仍继续执行重复/冲突保护。

**写给开发：**

- `EventReceipt.IdempotencyKey` 为兼容性新增字段；旧快照可加载，旧 receipt 缺少该字段时保留热事件路径的历史行为。
- `ArchivePending` 与 ingest 的归档状态提交在同一个快照写入中删除热事件；文件切 PostgreSQL 预检同时把 receipts 视为账本证据。
- 回退读取复用单一 archive key 编码和 `UseNumber` 解码，避免大整数在重启/归档回退路径丢失精度。

## 2026-08-20 — 时间线分页、ledgered 持久化补发与可选严格 mTLS

**写给运营：**

- operation/aggregate timeline 新增 `page_size`（默认 100、最大 1000）与作用域绑定的 opaque `cursor`，响应包含 `next_cursor`；operation 按 `(occurred_at, sequence, event_id)` 排序，aggregate 按 `(aggregate_version, event_id)` 排序。
- 配置 `AUDIT_LEDGERED_BROKERS`/`-ledgered-brokers` 后，post-ledger 发布失败会保留在快照 `ledgered_outbox`，API 重启和后台周期任务自动补发；账本仍先提交，发布链路为至少一次，投影消费者必须按 event_id 幂等。
- 新增 `AUDIT_GRPC_REQUIRE_MTLS=true`/`-grpc-require-mtls`。启用后 gRPC listener 必须同时提供 server cert/key 与 client CA，回环监听和 `AUDIT_ALLOW_INSECURE_GRPC_LISTEN` 都不能绕过该要求；默认 false 以兼容现有验证栈。

**写给开发：**

- `internal/domain` 增加版本化、作用域绑定的 timeline cursor；`internal/service/timeline.go` 增加分页服务方法，旧全量方法与 replay 仍受 `MaxTimelineEvents` 保护。
- `internal/store.Snapshot.LedgeredOutbox` 采用旧快照可归一化的可选字段；`internal/service.FlushLedgeredOutbox` 与 `cmd/audit-api` 的周期 loop 负责恢复发布，发布与确认由互斥区串行化以避免恢复竞态重复。
- `internal/grpcapi` 的 `MutualTLSCredentials` 与 API transport gate 共用 cert/CA 校验；`check-config` 与启动路径共用严格 mTLS 约束。
- 无新增数据库迁移；`python3 cli.py quality` 必须通过。

## 2026-08-16 — 治理 worker 归档写路径获得真实截止时间与取消：pass 级 2 分钟上限、ctx 贯穿三个 Put 站点、导出单写 30s 界（cmd/audit-governance-worker + internal/service，give-the-worker-s-archive-write-path-a-real-dead-f9f2be1e）

**写给运营（行为变化）：**

- **单次治理 pass 有硬上限（2 分钟）**：此前 `ArchivePending`/`archiveEvent`/`archiveSegment` 对归档写使用 `context.Background()`，且 minio-go 的 http.Client 没有整体超时——能通过 5s Ready 探针的黑洞端点可让每个对象卡住数分钟（Stat→Put→verify 三次往返 × 传输层超时），并阻塞整轮 pass 与 SIGTERM 优雅退出。现在每轮 pass 派生出 `archivePassTimeout`（2 分钟）截止上下文，并把 ctx 贯穿 `ArchivePending → archiveEvent/archiveSegment → Put`：minio-go 按请求 ctx 取消在途 HTTP 往返，pass 在界内返回；被截断的 pass 下个 tick（默认 5 分钟间隔）重试同一批事件（幂等，无数据丢失）。
- **新增日志行**：pass 在租户之间中止时输出 `pass_aborted=<err>`；归档错误行 `archive_error=` 现在可携带 `context deadline exceeded`/`context canceled`。`archived=N`/`dead_lettered=` 行语义不变。
- **导出单写 30s 界**：`runExport`（fire-and-forget，API 侧创建）的归档 Put 现在受 `archivePutTimeout`（30 秒）约束；超时 → 导出 job `failed`、`ObjectPath` 为空、`Error` 含 `context deadline exceeded`。重新请求 = 新的 `CreateExport`，失败语义不变。
- **新启动告警**：`-interval`（`AUDIT_GOVERNANCE_INTERVAL`）小于 pass 上限时输出 `warning=interval_below_pass_timeout`（仅告警，不失败）。
- **部署**：重建并滚动重启 worker 与 audit-api（共享 internal/service 包）；无数据迁移、无配置变更、无契约变更。回滚 = 回退二进制。

**写给开发（实现变化）：**

- `cmd/audit-governance-worker/main.go`：新增 `archivePassTimeout = 2 * time.Minute`；`runEvaluatePass` 顶部 `context.WithTimeout(ctx, archivePassTimeout)`（与信号 ctx 取交集，SIGINT/SIGTERM 仍更早生效）；租户循环**末尾**（非顶部）的 between-tenant 中止检查——顶部放置会破坏已固定的 `TestRunEvaluatePassAbortsOnCancelledContext`（预取消 ctx 下首租户仍须跑完完整步骤序列，产生断言中的 1 行 `checkpoint_error=` 与 1 行 `aggregate_checkpoint_error=`）。
- `internal/service/governance.go`：`ArchivePending(ctx, tenantID)` 签名变更（编译期破坏，仅 1 个生产调用方 + 测试调用点）；新增 `archivePutTimeout`（var 缝，测试可缩小，`newArchiveStore` 先例）；`runExport` 的 Put 包 `context.WithTimeout(context.Background(), archivePutTimeout)`。
- `internal/service/service.go`：`archiveEvent(ctx, …)`/`archiveSegment(ctx, …)`；`Ingest` 调用点透传 ctx——HTTP/gRPC 入口本就传 `context.WithoutCancel(r.Context())`，行为不变（F1 提交不因客户端断开而取消，`disconnect_test.go` 不变）。
- 瞬态分类不变：`isPermanentArchiveError` 仍只死信 `ErrArchiveKeyTooLong`/`ErrObjectConflict`；`context.Canceled`/`DeadlineExceeded` 是瞬态——无收据提交、无死信、无 `Store.Update` 窗口，下个 pass 重试同一事件，幂等端到端保持。
- 测试：worker 新增 `TestRunEvaluatePassAbortsWithinDeadlineOnBlockedPut`（AC-1）、`TestRunEvaluatePassCancelledCtxAbortsInflightPut`（AC-2）、`TestRunEvaluatePassAbortsBetweenTenants`（F-2 放置钉）；service 新增 `TestArchivePendingContextErrorIsTransientAndRetried`（AC-3）与 `TestRunExportPutTimeoutFailsJobAndHeals`（F-1，REQ-3 覆盖）；全部 `ArchivePending` 测试调用点（archive_batch/service/tenant_scoping 三个文件共 24 处）补 `context.Background()`。

## 2026-08-16 — 读取自审计足迹与快照解耦并加界：读路径事实写入独立追加尾流，快照不再被读重写（internal/store + internal/service + cmd/audit-api，bound-and-isolate-the-read-self-audit-trail）

**写给运营（行为变化）：**

- **读操作不再重写控制面快照**：此前每次带 actor 的读取（`GetEvent`/`QueryEvents`/timeline/replay/`VerifyIntegrity` 及导出下载两腿）都会在 `Store.Update` 内把一条 `audit.event.read` 等事实追加进 `Snapshot.AdminActions` 并**全文档 Save**（文件后端整文件 JSON 重写 + fsync + rename；PG 后端整行 `UPDATE` + `version` 递增 + 乐观锁重试）。现在这些事实落入**独立的追加尾流**——文件后端为 `<state>.admin-trail.jsonl`（JSONL、每事实一次 `fsync` 追加、`O_APPEND` 持久 fd），PG 后端为 `admin_action_trail` 表（单条 `INSERT`，无 version 递增、无整行重写）。快照文档/行在读取期间字节与 mtime 不变；`/admin/actions` 的合并视图把两条来源按 `created_at` 倒序合并展示，`count == len(items)` 与 limit（1..100）语义不变。
- **`Snapshot.AdminActions` 首次有界**：默认上限 `MaxAdminActions` = 10 000（drop-oldest、保留最新），在每个追加点强制执行，持久化文档永远不会超过该界；此前为无界增长。读取足迹走尾流，由 `MaxAdminTrailActions`（默认 100 000，`0` = 无界追加，显式运营选择）约束；尾流超界后压缩为最新 N 条（文件后端 tmp+fsync+rename 崩溃原子，PG 后端用 `seq` 水位 DELETE，绝不影响并发 INSERT）。
- **新增配置项**：`-admin-actions-cap` / `AUDIT_ADMIN_ACTIONS_CAP`（默认 10000）、`-admin-trail-cap` / `AUDIT_ADMIN_TRAIL_CAP`（默认 100000，`0` = 无界）。非法组合（`MaxAdminTrailActions` 非零但小于 `MaxAdminActions`）在启动与 `-check-config` 以 `ErrInvalid` 失败退出。
- **PG 部署**：上线前应用 `migrations/005_admin_action_trail.sql`（幂等）；`/readyz` 在表缺失时快速失败，首次尾流追加也会惰性 `CREATE TABLE IF NOT EXISTS` 自愈。多副本滚动窗口内，旧副本写入快照的读事实可能被新副本的首次封顶追加裁剪——仅限读取自审计事实、有界、自愈（FM-10；文件后端因 flock 单写者不存在此窗口）。
- **回滚**：回退二进制即可。旧二进制忽略尾流文件/表，恢复把读事实追加进快照（回到无界但完全可用）；孤儿尾流文件/表无害，可事后删除。既有 `admin_actions` 在首次封顶追加时被裁剪，无回填、启动不重写文件。

**写给开发（实现变化）：**

- `internal/store/trail.go`（新增）：可选的 `AdminTrail` 能力接口（`AppendAdminFact(fact, trailCap)`/`ReadAdminTrail()`）；`Store.AppendAdminFact`/`Store.ReadAdminTrail`——能力后端走尾流（**不持 `Store.mu`**，F-2：追加纯追加、与快照无耦合，后端自串行化，读不再阻塞 ingest）；无能力后端（测试缝）回退到锁内 `updateLocked` 全快照 Save，冲突重试与失败即关闭语义与 `Store.Update` 逐字节一致（F-1，`conflict_retry_test.go` 不变）。`Store.Update` 重构为 `Lock()` + 未导出 `updateLocked`（公开行为不变）。
- 文件后端：`<path>.admin-trail.jsonl`，惰性 `O_APPEND|O_CREATE` fd（父目录 0o750、文件 0o640、首建目录链 fsync），每事实 `file.Sync()`（与 `Save` 持久性对齐）；`trailCount` 惰性基线（首次使用扫描一次，之后 O(1) 判界，F-4）；超界 `compactTrail`（tmp+fsync+rename+目录链 fsync+重开 fd，F-7）。读取 newest-first；非末尾畸形行失败即关闭（FM-4），崩溃截断的末尾行容忍丢弃（FM-6）。内存模式（`-state ""`）为 `f.mu` 下普通有界切片。
- PG 后端：`trailMu` 串行化进程内追加；一次性 `CREATE TABLE IF NOT EXISTS`（F-5）；`INSERT` 无 version/`updated_at`/行重写；`trailCompactQuery` 用 `seq` 水位（`OFFSET $1 LIMIT 1`）删除，并发副本的更新 `seq` INSERT 永不误删（FM-7）；`Ready` 增查 `to_regclass('admin_action_trail')`。
- `internal/service/admin_trail.go`（新增）：`DefaultMaxAdminActions`（10000）/`DefaultMaxAdminTrailActions`（100000）；`resolveAdminTrailConfig`（F-6 零语义：`MaxAdminActions <= 0 ⇒ 默认`，`MaxAdminTrailActions < 0 ⇒ 默认`、`== 0 ⇒ 无界`；非零尾流界小于快照界 ⇒ `ErrInvalid`——设计草案的 `>= 100` 下限被移除：与 AC-3 验收夹具 `MaxAdminActions=5` 自相矛盾，已在注释记录）；`appendAdminAction`（14 个变更路径追加点统一接入，O(1) 切片重头、drop-oldest）；`mergeAdminActions`（快照 ∪ 尾流 newest-first，`CreatedAt` 相等尾流优先，tenant/platform 过滤先于 limit 收敛）。`recordReadAction` 与两条下载腿（`export.blocked`、`recordExportRejected` 保留 best-effort `_ =`）改走 `Store.AppendAdminFact`。`ListAdminActions` 空尾流快速路径为原逐字循环（`TestListAdminActionsPlatformTenantFilter` 不变）。
- `cmd/audit-api/main.go`：`-admin-actions-cap`/`-admin-trail-cap` flag + `AUDIT_ADMIN_ACTIONS_CAP`/`AUDIT_ADMIN_TRAIL_CAP` env（沿用 `-segment-size` 的 `flag.Int`+`intEnv` 模式），接入 `service.Config`；`runCheckConfig` 经 `service.New(nil, cfg)` 自动暴露校验失败（退出码 1，不开 store）。
- `migrations/005_admin_action_trail.sql`（新增，幂等）：`admin_action_trail`（`seq BIGSERIAL PK` 作压缩水位，`tenant_id+created_at DESC, seq DESC` 索引）。
- 测试：`internal/store/trail_test.go`（JSONL 往返/压缩保留最新/损坏失败关闭/追加失败关闭/回退冲突重试/回退 saveErr/回退快照封顶/不持 Store 锁/无能力空结果/PG 追加不 bump version/PG 压缩水位，PG 用例走 `AUDIT_TEST_POSTGRES_DSN` skip 模式）与 `internal/service/admin_trail_test.go`（AC-1 `TestReadsDoNotRewriteSnapshotFile` 10k 读 mtime/size 不变 + 尾流有界；AC-3 `TestListAdminActionsNewestAfterCap`；变更路径 drop-oldest；F-6 配置解析）。`read_selfaudit_test.go` 全部 14 个测试原样通过（能力/回退分裂保持失败即关闭与冲突耗尽语义）。

## 2026-08-16 — 路由契约门禁升级为方法/响应感知：`cli.py check` 开始强制 OpenAPI 契约（api/openapi，make-the-route-contract-gate-method-and-response-456cafeb）

**写给运营（行为变化）：**

- **`python3 cli.py check` 现在运行路由契约门禁**（此前只有 `cli.py quality` / `cli.py check-routes` 执行）：`make check` / CI 常规检查即校验运行期路由与 `api/openapi/openapi.yaml` 的契约，全绿输出多一行 `PASS: route contract`。门禁现在对比 **(HTTP 方法, 规范化路径) 对**而非仅路径——同一路径 GET/POST 漂移（规范文档化 GET、运行期注册 POST，或反之）都会 FAIL 并指名方法与路径。
- **运行期处理器实际写出的状态码必须被规范文档覆盖**（单向：字面量 ⊆ 文档；经 `statusForError` 动态映射的状态与中间件/鉴权/panic 产生的状态不在检查范围，属设计内限制）。例如 `downloadExport` 对缺失归档对象实际返回 404（`server.go` 的 `os.IsNotExist` 分支），规范此前只文档化 200/409/500——本次为它补上 404。
- **`api/openapi/openapi.yaml` 纯文档增补**：15 个操作的 16 个错误响应状态键（queryEvents 400/500、getEvent 500、两条 timeline 500、各 create/verify/preview/restore/setRetention 400、downloadExport 404、listAdminActions 400）+ `AdminAction` 200 响应接线（此前定义了但从未被引用）。无任何运行期行为/二进制变化；回滚 = 回退规范文件，门禁会立即大声失败。

**写给开发（实现变化）：**

- `checks/route_contract.py`：单一共享实现，三阶段门禁。R1 方法感知操作匹配（运行期 `HandleFunc("METHOD /path", s.spanWrap(s.handler))` 注册 vs 规范 `paths:` 操作，key = `(METHOD.upper(), normalize(path))`）；R2 响应状态覆盖（处理器函数体内字面 `s.writeError(w,r,http.StatusX)`/`writeJSON(w,http.StatusX)`/`WriteHeader(http.StatusX)` 必须 ⊆ 该操作文档状态；`http.StatusX` → 数字经 Python 标准库 `http.HTTPStatus` 转换，不可映射常量 fail-loud 不回溯）；R3 模式完整性（`$ref` 悬空引用、已定义未引用顶层 schema 均 FAIL，`schemas:` 子段作用域避免误报 `securitySchemes`）。`_build_index` 单遍扫描 `internal/**/*.go` 同时产出路由表与处理器字面状态表（消除 O(R·F) N+1，实测每次门禁运行 100 次文件读取 / ~13ms）；重复 `(method, path)` 注册与重复接收器方法名 fail-loud（拒绝 last-write-wins）；`//` 全行注释先剥离、函数头正则锚定 `\nfunc ` 部分起点（消除 `server_test.go` 注释/字符串误归属）；`normalize()` 冻结契约不变（`{param}` → `{}`），`run(root=None)` 保持零参兼容（`checks/self_test.py`、`cmd_quality` 继续零参调用，`ROOT` 默认取 `checks/config`）。纯标准库，无 YAML 依赖（延续仓库 regex 契约测试纪律）。
- `cli.py`：`cmd_check_routes()` 改为委托 `checks.route_contract.run(root=ROOT)`（调用时读 `cli.ROOT`，支持 test_proto_sync 式 monkeypatch 测试）；删除重复的 `normalized_route()` 与扫描正则（单一实现，消除双份正则漂移）；`cmd_check()` 元组加入 `cmd_check_routes`（R5：`cli.py check` 强制契约门禁）；`cmd_harness` 去掉冗余的显式 `cmd_check_routes`（该阶段已在 `cmd_check` 内）。
- 测试：`checks/test_route_contract.py`（新增，27 用例）——方法漂移双向失败/匹配控制、未文档化字面状态失败（含真实仓库 downloadExport 404 回归、`WriteHeader` 形态、flow 风格多状态单行、单向非目标钉住）、悬空/未引用 schema 与 `bearerAuth` 作用域控制、缺失规范文件/空 internal/双空 vacuous pass、重复注册/重复处理器 fail-loud、不可映射 `http.StatusTeapot` 常量无 traceback、`AUDIT_QUALITY_ACTIVE` 递归防护（quality 内跑套件时跳过两个重量级子进程用例）。

## 2026-08-16 — 投影端把 `projection.ErrNotLedgered` 分类为永久错误：pre-ledger 事件首次尝试即死信 `permanent_error`（internal/kafka，classify-projection-errnotledgered-as-a-permanen-3e0130dd）

**写给运营（行为变化）：**

- **audit-projector 指向 accepted topic（`-topic=audit.events.accepted.v1` 或空 `-topic` 回退 `TopicAccepted`）时快速失败**：此前每个 pre-ledger 事件都会烧满 8 次 × 2s 退避（≥14s 分区头阻塞），再以 `attempts_exhausted` 死信——重放会把预入账原件重新投递、再次烧满、再次死信，循环直到 replayed-mark 收敛。现在 `projection.Store.Insert` 返回的 `ErrNotLedgered`（裸哨兵或 `%w` 包装）与 `domain.ErrOccurredAtOutOfRange` 同级，在**第一次 ingest 尝试**即以 `error_code=permanent_error` 死信：零重试、零退避、零 `attempts_exhausted` 记录；重放侧进入一次性闭合类（REQ-PERM-1），不再循环。事件仍不会物化进投影（必须由运营修正 topic 配置；启动首行日志仍然可见解析后的 topic），变化的是速度、错误码语义与重放确定性。
- **确定性隔离**：仅 `errors.Is` 精确匹配哨兵；ClickHouse 瞬态故障（`insert projection: %w` 包装的非哨兵错误）仍走原重试/退避/上限路径，绝不会被误标永久。
- **既有 DLQ 记录不回迁**：修复前以 `attempts_exhausted` 写入的 pre-ledger 记录由重放 legacy 路径照常处理；运营修正配置后下一轮重放即可收敛。回滚 = 重新部署旧二进制（恢复慢速/错误码路径；DLQ 记录形态双向兼容，无重放器改动）。

**写给开发（实现变化）：**

- `internal/kafka/kafka.go`：`consume` 在 `ErrOccurredAtOutOfRange` 分支（kafka.go:429–431）之后、瞬态分支之前新增同级分支 `if errors.Is(err, projection.ErrNotLedgered) { return c.deadLetter(…, ErrorCodePermanentError, err) }`；新增 import `internal/projection`（`checks/architecture.py` 允许、无环）；`Consumer`/`consume` doc comment 与分支注释把 `ErrNotLedgered` 列为第二个确定性域拒绝。无哨兵/错误类型/词汇变更，无新计数器或指标，`deadLetter` 共享日志行复用（错误文本自带诊断）。
- 测试：`internal/kafka/kafka_test.go` 新增 `TestConsumerDeadLettersNotLedgeredOnFirstAttempt`（T1，裸/包装双形态表驱动 + `time.Hour` 退避零等待证明 + 分区越过毒消息前进）、`TestErrNotLedgeredSentinelIdentitySurvivesWrapping`（T2，`errors.Is` 跨裸/包装/无关错误）、`TestDeterministicDomainRejectionsNeverAttemptsExhausted`（T5，三类确定性拒绝单次 ingest + 零 attempts_exhausted）、`TestConsumerPermanentRecordsAllLandInOneShot`（T6，消费端产物记录全部落入 `wantedEvents.oneShot`）。`test/e2e/projector-permanent-dlq.sh`（新增，Docker 门控）+ Makefile `e2e-projector-permanent-dlq`（AC-3：one-off 容器覆盖 `AUDIT_KAFKA_TOPIC`，DLQ 恰好一条 `permanent_error` 记录、零重试日志行）。`internal/projection/projection.go`、`cmd/audit-projector/main.go`、`internal/kafka/replay.go`、`api/asyncapi/asyncapi.yaml`、`deploy/docker-compose.verify.yml` 均未改动。

## 2026-08-16 — Kafka consumer 租户作用域 + DLQ `Failure.tenant_id`（internal/kafka + cmd/audit-kafka-consumer + api/asyncapi，add-tenant-scoped-validation-to-the-kafka-consumer）

**写给运营（行为变化）：**

- **消费端新增可选租户作用域**：`-tenant` / `AUDIT_KAFKA_TENANT` 设置后，accepted topic 上 `event.TenantID` 与本实例配置租户不同的事件被**跳过并提交**——绝不入账、绝不进 DLQ、绝不重试，因此也绝不会再走 `tenant_mismatch` 422 → `permanent_error` → 重放一次性闭合（REQ-PERM-1）的永久丢失链。事件为空租户时照常透传给 API（服务端按 token 租户盖章，DS-08），不属于本实例；`AUDIT_KAFKA_TENANT` 未设置时行为逐字节不变（向后兼容，回滚即恢复旧缺陷路径）。
- **新指标 + 新日志 + 新告警**：`audit_consumer_foreign_tenant_skipped_total`（无任何 label，追加在 `/metrics` 末行，与 ingest/commit 和 dead-letter 两个家族不相交）；每条跳过记录一行 `skipped foreign-tenant … tenant=<事件声明的租户> consumer_tenant=<配置租户>`；Prometheus 规则 `AuditConsumerForeignTenantSkips`（`increase(…[15m]) > 0`，warning）覆盖 FM-3——`AUDIT_KAFKA_TENANT` 与 token 租户错配会把本实例全部事件静默跳过，计数/日志/告警是唯一信号。dev token 与 `AUDIT_KAFKA_TENANT` 错配时启动即打印非致命警告 `tenant scope mismatch tenant_claim=… consumer_tenant=…`（JWT 实例靠 skip 计数与日志对）。
- **verify 栈默认开启租户作用域**：`AUDIT_KAFKA_TENANT: ${AUDIT_KAFKA_TENANT:-demo}`，与默认 token `dev:demo:service` 对齐；覆盖 `AUDIT_OUTBOX_TOKEN` 到其他租户时必须同步设置 `AUDIT_KAFKA_TENANT`（错配由上述信号暴露）。e2e：`make e2e-tenant-scope`（Docker 门控、不在 `cli.py quality` 内，fullstack.sh 同级）。
- **契约（运营侧影响）**：DLQ `Failure` 载荷新增**可选** `tenant_id`——事件可解码的死信路径写入**事件信封声明的租户**（非服务端验证值；服务端解析租户仍是唯一授权权威），不可解析路径留空且 `omitempty` 省略该键，旧二进制写入的 3 键记录与新二进制写入的 4 键记录双向可解。Change Manifest：`docs/proposals/change-manifest-dlq-failure-tenant-id.md`。
- **回滚**：消费者二进制 + compose env 一起回退（旧二进制忽略 env，外来租户事件恢复死信/REQ-PERM-1 丢失链，DLQ `permanent_error` 上升即特征信号）；schema/struct/填充同变更集回退，两种记录形态双向兼容，无需重放器改动。

**写给开发（实现变化）：**

- `internal/kafka/kafka.go`：`Consumer` 新增 `tenant` 字段与 `foreignTenantSkipped atomic.Uint64`；`WithTenant(tenant)` 选项（空 = 不过滤，REQ-7）；`consume` 第一语句为租户门禁（`c.tenant != "" && event.TenantID != "" && event.TenantID != c.tenant` → 计数 + 日志 + 提交并返回，`ingested==0` 由测试钉死，FM-1）；`Failure` 新增 `TenantID json:"tenant_id,omitempty"`（F-1 语义注释：信封声明值、非服务端验证）；`deadLetter` 填充 `TenantID: event.TenantID`，`deadLetterUnparsable` 留空；`Metrics()` 与 `ConsumerMetrics` 新增 `ForeignTenantSkipped`（不相交第三类）。
- `cmd/audit-kafka-consumer/main.go`：`-tenant`/`AUDIT_KAFKA_TENANT` flag、`WithTenant` 接线、启动日志末位 `tenant=%s`、`metricsText` 追加不相交类行（doc 注释注明 no-labels 与三类关系）、`devTokenTenant` 启动诊断（非致命 F-2 警告，dev token 专用，JWT 不在此验证）。
- `api/asyncapi/asyncapi.yaml`：`Failure` 新增可选 `tenant_id: { type: string }` + 注释；`required` 不变；`checks/asyncapi_channels.py` 严格子集解析器可分类（FM-5 保持 fail-closed）。
- 测试：`internal/kafka/kafka_test.go` 新增 `TestConsumerSkipsForeignTenantWithoutIngestOrDLQ`（T1，`runConsumerWithMetrics`）、`TestConsumerTenantScopeBoundaries`（T2 四子测含 `WithTenant("")`）、`TestConsumerSkipCommitFailurePropagatesAndDoesNotIngest`（H2：`fakeReader.commitFunc` 失败缝，首次 Run 返回提交错误、二次 Run 重新跳过不 ingest、at-least-once 计数）、`TestConsumerDeadLetterTenantIDCarriesEnvelopeClaim`（P1/F-1：422 `tenant_mismatch` 死信携带信封值）；扩展 `TestFailurePayloadMatchesAsyncAPISchema`（T7 双子测：4 键精确串 + omitempty 省略）与 4 个死信测试的 `TenantID` 断言（T8/L2，含 attempts_exhausted、unauthorized 路径）。`cmd/audit-kafka-consumer/main_test.go`：`TestConsumerTenantFlagAndEnvWiring`（T3 三子测：flag/env/空值）、`TestConsumerBinaryTenantScopedToken`（T11）、`TestConsumerWarnsOnTenantTokenMismatch`（P2/F-2）、golden 扩展（6 行）。`checks/contract_fields.py`：新增 Failure 载荷键集/required 钉（M2/FM-6，YAML 侧；Go 侧由 T7 钉）。`deploy/prometheus-rules.verify.yml`：新增 `AuditConsumerForeignTenantSkips`（devops M4，同变更集落地）。`deploy/docker-compose.verify.yml`：consumer 新增 `AUDIT_KAFKA_TENANT`。`test/e2e/consumer-tenant-scope.sh`（新增，H1 有界 DLQ 读取 + M1 增量断言 + M3 双租户日志断言）；Makefile 新增 `e2e-tenant-scope`（M4）。
- **不变量**：错误码词汇不变；`contract_fields.py`/`tenant_consistency.py`/`invariants.py` 不涉及 Failure schema；无新 Go import、无新依赖边；`checks/sensitive_logging.py` 对 `c.logf(` 调用不匹配（无 `event`/`payload` 参数表达式）；`docs/evolution/state.jsonl` 追加 `change_manifest` 记录（本地 trace；`docs/evolution/` 被 gitignore，持久记录为 manifest 文件）。

## 2026-08-15 — outbox 写入拒绝超限 payload：`Insert` 在任意 SQL 之前按 `domain.MaxEventBytes` 收口（internal/outbox，outbox-insert-accepts-unbounded-payloads-enabling-table-bloat）

**写给运营（行为变化）：**

- **outbox 写入现在拒绝超限事件**：`outbox.Insert` 在 `json.Marshal(event)` 之后、任何 SQL 之前检查完整事件编码长度；`len(encoded) > domain.MaxEventBytes`（256 KiB）时返回 `domain.ErrInvalid`（`payload exceeds 262144 bytes`），不写任何行、不触发 `ON CONFLICT` 分类。此前超限事件会照常落库，随后被 audit API 以 422 拒绝并在首次轮询即死信（`status='failed'`），成为**永久存储垃圾**（无任何清理任务）并放大 relay 每轮批内内存（100 行 × 无界 payload）。现在新超限行不可能产生：relay 批内存上界约为 100 × 256 KiB ≈ 25 MiB，存储增长有界。
- **边界语义与 ingest 侧一致**：严格 `>`，编码恰好 256 KiB 的事件照常接受；写入路径（幂等 `ON CONFLICT DO NOTHING`、冲突分类、nil/`ErrConflict` 契约）对界内事件逐字节不变。
- **方向是有意收紧（NFR-3）**：outbox 度量的是**完整事件编码**（relay 实际 POST 的字节），而 ingest 侧（`service.go:975`）只度量 canonical payload；即使运营把 API 侧 `Config.MaxEventBytes` 调大，outbox 仍以常量 `domain.MaxEventBytes` 为界——SDK 可能拒绝 API 会接受的事件，但**永远不会**入队一个必然因大小死信的行。日志中可能出现两个不同数字（API 报配置值、outbox 报常量值），属预期。
- **遗留超限行不受影响**（预防性修复）：已存在的超限行仍会被 relay 照常 POST 并死信；无清理/压缩任务，无存储迁移。**行为回退点**：对遗留超限行的字节级重插，旧二进制返回 nil（幂等重复），新二进制返回 `ErrInvalid`——仅影响对永久垃圾的重试，快速失败更安全。
- **无存储迁移、无配置变更、无 API/route/proto 变更**；回滚 = 重新部署旧二进制（旧二进制重新接受超限写入，缺陷随之回归，请同步升级生产方）。

**写给开发（实现变化）：**

- `internal/outbox/sdk.go`：`Insert` 在 marshal 成功检查之后、`const query`/`ExecContext` 之前新增 4 行守卫 `if len(encoded) > domain.MaxEventBytes { return fmt.Errorf("%w: payload exceeds %d bytes", domain.ErrInvalid, domain.MaxEventBytes) }`——`%w` 包装 `domain.ErrInvalid`（与既有 `tenant_id is required` 同款），与 `ErrConflict` 可经 `errors.Is` 区分；复用已导出的 `domain.MaxEventBytes` 常量，无新 import、无新依赖边、无新导出符号。
- 测试：`internal/outbox/sdk_test.go` 新增共享 `sizedEvent` 辅助（运行时从 `domain.MaxEventBytes` 推导精确字节数，杜绝字面量漂移，AC-4）与 `TestInsertRejectsOversizedPayloadBeforeSQL`（AC-1：超限拒绝且零 SQL）、`TestInsertPayloadSizeBoundary`（AC-2：恰在界接受且单次往返、+1 字节拒绝且零 SQL）；`internal/outbox/postgres_test.go` 新增 `TestPostgresInsertRejectsOversized`（AC-3：`AUDIT_TEST_POSTGRES_DSN` 门控，超限插入 ErrInvalid + 回滚后行数 0，界内 fixture 落库后 `max(octet_length(payload::text))` ≤ 常量）。既有测试套件（幂等/冲突分类/键帧拒绝）原样保留以钉死界内行为不变。

## 2026-08-15 — gRPC ingest 监听器 TLS + 失败关闭的 `check-config` 门禁（internal/grpcapi + cmd/audit-api，add-tls-grpc-creds-to-the-grpc-ingest-listener）

**写给运营（行为变化）：**

- **入站 gRPC 监听器支持原生 TLS（推荐）**：新增 `-grpc-tls-cert` / `-grpc-tls-key`（env `AUDIT_GRPC_TLS_CERT` / `AUDIT_GRPC_TLS_KEY`，均为 PEM 文件路径，必须成对设置）。配置后监听器只接受 TLS 握手（grpc-go 在握手层拒绝明文 h2c 客户端），`authorization` bearer 元数据不再明文过网（RFC 6750 §1；此前 release-notes 已知问题「入站 gRPC 监听器默认无 TLS」由此关闭）。
- **失败关闭的 preflight 门禁**：`-check-config` 现在对 gRPC 监听器做传输校验，且**启动路径使用同一门禁**（preflight 与 runtime 不可能分歧）：非 loopback 监听（`AUDIT_GRPC_LISTEN` 含 `:50051`、`0.0.0.0:…`、任意主机名）在**未配 TLS 且未显式放行**时，preflight/启动均失败（exit 1，marker `check_config=fail grpc=plaintext_without_allowlist`，命名 `AUDIT_GRPC_LISTEN` 与 `AUDIT_ALLOW_INSECURE_GRPC_LISTEN`），启动在绑定任何端口之前中止。
- **三条受支持的生产/验证部署路径**：(a) 原生 TLS——`AUDIT_GRPC_TLS_CERT`/`AUDIT_GRPC_TLS_KEY`（首选）；(b) TLS 终结设施 + 仅回环监听——`AUDIT_GRPC_LISTEN=127.0.0.1:…`（回环例外，与 F3 `AUDIT_ALLOW_INSECURE_API_URL` 同款语义）；(c) 显式不安全放行——`AUDIT_ALLOW_INSECURE_GRPC_LISTEN=true` **仅限本地 verify 栈**（环境变量专用、无对应 flag；严格布尔解析，`tru` 等畸形值 exit 1 命名变量，无 fail-open 路径）。生产环境绝不设置 (c)。
- **`check_config=ok` 行新增末位无条件字段 `transport_grpc=<tls|insecure|disabled>`**（在 `transport_vault` 之后），监听器未配置时固定为 `disabled`；输出与退出码为解析后配置的纯函数，跨运行字节一致（不校验证书有效期，保持确定性）。
- **verify 栈配套（同 commit 落地）**：`deploy/docker-compose.verify.yml` 已为 `audit-api` 设置 `AUDIT_ALLOW_INSECURE_GRPC_LISTEN: "true"`（仅本机隔离网络），`19051:50051` 发布端口与 `test/e2e/fullstack.sh` 的明文 grpcwrite 客户端继续可用。
- **无 wire/契约变更、无存储迁移**；回滚 = 取消 env 或重新部署旧二进制（旧二进制恢复非 loopback 明文监听，缺陷随之回归，请同步升级生产方与 verify 栈）。

**写给开发（实现变化）：**

- `internal/grpcapi/tls.go`（新增）：导出 `ServerCredentials(certFile, keyFile) (credentials.TransportCredentials, error)`（`tls.LoadX509KeyPair` + `credentials.NewTLS`，无 MinVersion/密套覆写、无网络访问；错误返回不内置日志，preflight 与启动同文案）。
- `cmd/audit-api/main.go`：新增 `-grpc-tls-cert`/`-grpc-tls-key` flag（env 默认同款模式）；新增 `loopbackListenAddr`（空主机 `:50051`/通配 IP/Docker host-gateway 别名一律非回环，fail-closed）、`grpcAllowlisted`（`strictBoolValue` 严格解析）与单一共享门禁 `resolveGRPCTransport`（返回 label + creds + err，preflight 与启动共用，杜绝分歧与双份 TLS 加载）；启动在 `net.Listen` 前应用同一门禁，TLS 时 `grpc.Creds` 追加进既有 keepalive/容量/恢复选项集（panic-recovery 仍最外层）；`runCheckConfig` 签名新增 3 个 gRPC 输入并在 `check_config=ok` 前落地门禁；部分 TLS（cert xor key）、PEM 不可读/不匹配、畸形 allowlist 均为硬错误（即便监听器未配置也校验 TLS 文件）。
- 测试：`internal/grpcapi/tls_test.go`（`TestServerCredentials` 单元 + `TestGRPCTLSWriteEndToEnd` A1 + `TestGRPCTLSRejectsPlaintextClient` A2，自签名证书测试内生成、无入库私钥）；`cmd/audit-api/main_test.go`（`TestLoopbackListenAddr`、`TestRunCheckConfigGRPCPlaintextFailsClosed` A3 全矩阵、`TestRunCheckConfigGRPCDeterministic` A4、`TestRunCheckConfigGRPCFieldPosition` 位置契约、`TestStrictBoolEnvSubprocessGRPCGate` 子进程 + 跨运行确定性、`TestStartupGRPCPlaintextFatal` REQ-4 启动一致）；`internal/grpcapi/wiring_test.go` 静态钉新增 `grpcapi.ServerCredentials(`、`grpc.Creds(`、`-grpc-tls-cert`、`-grpc-tls-key`。

## 2026-08-15 — 归档键改用单射可逆编码：丢损 safeName 折叠消失（internal/fsutil，lossy-safename-archive-key-framing-collapses）

**写给运营（行为变化）：**

- **归档对象键不再丢损**：此前 `safeName` 把安全字母表（`[a-zA-Z0-9._-]`）之外的每个字符折叠成 `_`，不同合法标识符会落到同一 WORM 对象路径——例如租户 `a?b` 与 `a_b` 的事件、或同一租户内 `operation_id` 为 `a:b` 与 `a_b` 的两条流，都会写成 `events/…/a_b/…` 与 `segments/…/a_b/…`。先写者占用该键后，后写者 `verifyExistingObject` 字节比对失败 → 回执降级 `StatusIndexed` 且 `ArchivePending` 每轮重试同一键、**永远无法归档**（跨租户归档拒绝）。现在所有归档键组件经 `fsutil.EncodeKeyComponent` 单射可逆编码：`a?b` → `a%3Fb`、`a:b` → `a%3Ab`、`tëstant` → `t%C3%ABstant`，`%` 自身转义为 `%25`（`a%b`/`a%25b` 不再碰撞），空组件编码为 `%`（不再与 `unnamed` 碰撞），`.`/`..` 编码为 `%2E`/`%2E%2E`（保持根目录包含）。
- **UUID 标识符键逐字节不变**：平台契约要求不可变 UUID 主键，全部落在安全字母表内 → 编码后键与今天完全一致，无成本/无迁移；只有标点/非 ASCII 标识符的键变化。已归档对象为 WORM 不可变，**不做回溯迁移**。
- **碰撞卡住的回执自动收敛（已实测钉死）**：历史丢损折叠下“赢家/输家”共用一个键，输家永远卡在 `StatusIndexed`。升级后 `ArchivePending` 以新键重算：**新键与遗留键严格不相交**（每条流的 `Event.Stream()` 都内嵌 `:`，编码为 `%3A`，而遗留键从不含 `%`），故输家的新键必然空闲——首次 `ArchivePending` 即归档收敛（AC-2 E2E 钉子：`a?b`/`a_b` 跨租户对与 `a:b`/`a_b` 流层对均 `StatusArchived`，收敛后不回归不新增对象）。
- **API 边界新增标识符长度上限（FM-1 窗口关闭）**：所有会成为归档键组件的标识符（`tenant_id`、`aggregate_type`/`aggregate_id`、`event_id`、`source_system`、`operation_id`）在入账前上限 **85 字节**（`MaxArchiveComponentBytes`，= POSIX NAME_MAX ÷ 3，编码最坏 3× 展开后恰为 255）。超限请求在 HTTP 侧返回 422 / gRPC 侧 `InvalidArgument`，**不进入账本**，不再产生“验证合法但永远无法归档”的回执。UUID（36 字节）不受影响。
- **超长/冲突键失败已死信化（FM-1/F-2 加固，逐对象隔离）**：编码最长 3× 输入字节，归档存储对键做**前置预检**（类型化 `ErrArchiveKeyTooLong`，单组件 ≤255B / 总键 ≤1024B，先于任何文件系统变更或网络调用，绝不产生半写对象）——FileStore 组件约 **85 个 ASCII 标点字节 / 28 个三字节 rune** 起即超限（旧丢损编码约 255 rune 才超限，回归窗口 3×；S3 复合键约 **330 个内容字节** 即超 1024B 预算）。`ArchivePending` 对**永久性失败**（键超限、或目标键被字节不同的对象占用 `ErrObjectConflict`）记录**持久化死信**（回执 `ErrorCode=archive_dead_letter` 且排除出重试集），**不再每轮重试**；同一 pass 内其余事件照常归档（E2E 钉子：1 个超长标识符死信 + 100 个健康事件同 pass 全部 `StatusArchived`）。瞬时失败（存储异常）仍中止该 pass 交给 worker 下轮重试。
- **遗留碰撞处置（runbook，F-2 状态检测）**：历史丢损折叠的遗留对象是 WORM **COMPLIANCE 保留**（`AUDIT_ARCHIVE_RETENTION_DAYS` 内任何主体不可覆盖/删除）。升级后碰撞卡住的回执自动收敛（见上），**无需人工处置**；仅当某键出现字节不同的对象（篡改、跨部署键冲突、未来回归）时进入死信：检测——`ListDeadLetters(tenantID)` 列出死信（reason `archive_key_too_long`/`archive_object_conflict`），或 worker 成功日志 `dead_lettered=N`、回执 `ErrorCode=archive_dead_letter`；核验——读对象（canonical JSON 内嵌 `tenant_id`）确认归属；处置——若为遗留/误写对象且策略要求 WORM 归档：等 COMPLIANCE 保留期**届满**后经治理审批删除释放键，再 `ClearDeadLetter(tenantID, eventID)` 触发一次有界重试（冲突仍在则再次死信，循环有界）；若策略允许账本-only：标记回执例外并接受死信记录。新写入不再产生该状态。
- **无存储迁移**；回滚 = 重新部署旧二进制（旧二进制恢复丢损折叠，缺陷随之回归，请同步升级生产方）。

**写给开发（实现变化）：**

- `internal/fsutil/fsutil.go`：新增导出 `EncodeKeyComponent(value string) string`（单射、可逆、确定性；安全字节原样透传，其余按 UTF-8 字节 `%XX` 大写十六进制转义，`%` 自身转义，空→`%`，`.`/`..`→`%2E`/`%2E%2E`）、`DecodeKeyComponent(value string) (string, error)`（仅用于测试契约与运维工具，生产无调用方）、常量 `KeyComponentEmptyMarker`、`MaxFileStoreComponentBytes`(255)/`MaxS3KeyBytes`(1024)（归档预检与 FM-1 测试的单一事实源）；编码预分配 3× 最坏长度（热路径零重分配）；`internal/service/service.go` 的 `safeName` 变为弃用委托 shim（三个生产调用点与测试路径钉子自动改道新编码）。
- `internal/domain/`：新增 `MaxArchiveComponentBytes`(85) 与 `DeadLetter` 记录；`ValidTenantIDComponent`/`ValidStreamComponent` 及 `ValidateBasic`（`event_id`/`source_system`/`operation_id`）增加长度上限（入账前拒绝，HTTP 422 / gRPC InvalidArgument）。
- `internal/archive/archive.go`：两个 `Put` 均增加**前置键长预检**（类型化 `ErrArchiveKeyTooLong`，先于任何文件系统变更或网络调用）；`verifyExistingObject` 字节不匹配包类型化 `ErrObjectConflict`（WORM 不覆盖不删除）。
- `internal/service/governance.go`：`ArchivePending` 改为**逐对象死信**——永久失败（`ErrArchiveKeyTooLong`/`ErrObjectConflict`）写入快照 `DeadLetters` 集合并从重试集排除，回执 `ErrorCode=archive_dead_letter`，成功子集仍在单个原子批次提交（原子性不变）；新增 `ListDeadLetters`/`ClearDeadLetter` 运维面。`internal/store/store.go`：快照新增 `DeadLetters` map（旧快照解码为 nil 自动归一化，无迁移）。`cmd/audit-governance-worker`：成功归档日志新增 `dead_lettered=N`。
- 测试：`fsutil_test.go` AC-1 语料单射/往返、10k 轮确定性 fuzz、根包含与反像拒绝、**FM-1 长度边界钉子**（85B→255/86B→258、28 三字节 rune→252/29→261、330 内容字节复合键超 1024B）与编码零重分配 + `BenchmarkEncodeKeyComponent`；`domain` 边界拒绝测试（85B 通过/86B 拒绝、28 rune 通过/29 拒绝，租户/流/事件三层）；`service_test.go` AC-2 跨租户对、流层冒号对、收敛不回归、**API 边界拒绝 E2E**（86B 入账前拒绝、85B 端到端归档）、**死信 E2E**（1 超长死信 + 100 健康同 pass 归档、次 pass 排除重试）、**F-2 遗留碰撞检测 E2E**（字节不同对象 → 死信 + WORM 保留 + `ListDeadLetters` 精确标记 + `ClearDeadLetter` 有界重试）；`archive_test.go` 键长边界（255/256 组件、1024/1025 总键）；gRPC 边界拒绝用例。
## 2026-08-15 — `occurred_at` 账本时间窗收口：入账前拒绝超范围时间戳（internal/domain，bound-occurredat-at-the-domain-boundary）

**写给运营（行为变化）：**

- **`occurred_at` 必须在 `[1900-01-01T00:00:00Z, 2299-12-31T23:59:59.999Z]` 内**（ClickHouse `DateTime64(3,'UTC')` 投影层的平台契约范围）。此前该字段只校验非零：year 1800 / year 2300 的事件被 202 接受并**持久写入不可变账本**，但投影 INSERT 在 ClickHouse 侧永久失败——重试 8 次后死信 `attempts_exhausted`，事件对在线查询面永久不可见且入账时无任何报错。现在超范围事件在**域边界**被拒：HTTP 单条返回 **422 `occurred_at_out_of_range`**，批端点返回 422 并按 ERP 契约首错截断（已入账 receipts 作前缀返回，坏事件零值 receipt，`not_attempted` 语义不变）；outbox 写入与 gRPC 共享同一 `ValidateBasic` 边界，同样在写库前拒绝。**需要生产方动作**：将 `occurred_at` 校正到时间窗内；新事件永不落库。
- **历史合规回填（早于 1900 年）被拒**：此类时间戳无论如何都无法被投影层索引（ClickHouse `DateTime64` 下限），拒绝是平台契约的既定取舍；OpenAPI `occurred_at` 描述已带时间窗，新生产方客户端侧即失败。
- **预修复前已入账的超范围事实保留在账本中（不可变性，无数据迁移）**：投影器现在对这类事实**首次尝试即死信** `permanent_error`（错误文本自带时间窗，自描述 trace），不再烧 8 次退避后 `attempts_exhausted`。部署后观察 DLQ 的一次性 `permanent_error` 峰值属预期。
- **无存储迁移**；回滚 = 重新部署旧二进制（旧二进制重新接受超范围时间戳，缺陷随之回归，请同步升级生产方）。

**写给开发（实现变化）：**

- `internal/domain/models.go`：新增导出常量 `MinOccurredAt`/`MaxOccurredAt`（`time.Date(1900,1,1,…)` / `time.Date(2299,12,31,23,59,59,999ms,…)`，注释引用 ClickHouse `DateTime64` 平台契约）与哨兵 `ErrOccurredAtOutOfRange`（`%w` 包装 `ErrInvalid`，`errors.Is(err, ErrInvalid)` 对所有既有调用方保持成立）；`ValidateBasic` 在零值检查之后新增范围检查（顺序冻结：零值仍报 "occurred_at is required"；按 UTC 时刻裁决，与 `Store.Insert` 的 `event.OccurredAt.UTC()` 绑定一致）。
- `internal/projection/projection.go`：`Store.Insert` 在存在性守卫之后、payload 编码/DB 访问之前新增同一范围守卫（预修复账本事实的纵深防御；nil-`*Store` 探针证明守卫先于 DB 访问，`ErrNotLedgered` 优先级不变）。
- `internal/kafka/kafka.go`：consumer 将 `domain.ErrOccurredAtOutOfRange` 归类为永久错误，首次尝试即死信 `ErrorCodePermanentError`（`internal/kafka` 已导入 `internal/domain`，无新依赖边）。
- `internal/httpapi/server.go`：`statusForError`/`errorBody` 在 `ErrInvalid` 分支**之前**插入哨兵分支（顺序承载：哨兵包装 `ErrInvalid`，顺序颠倒会塌缩回 400 `invalid_request`）→ 422 `occurred_at_out_of_range`。
- `api/openapi/openapi.yaml`（+ `api/asyncapi/asyncapi.yaml` 镜像）：`Event.occurred_at` 描述与单条/批端点 422 响应描述带时间窗与拒绝语义。`api/proto/audit.proto` 未动（生成物需 protoc 再生成，超出范围）。
- 测试：`models_test.go` 新增边界表（拒绝 year 1800/floor−1ms/ceiling+1ms/year 2300/`+08:00` 偏移按 UTC 时刻裁决；接受 floor/ceiling 含边界；零值消息不变）；`occurred_at_boundary_test.go` 新增 service 层无变异探针（快照前后不变 + 后续合法入账成功）；`projection_test.go` 新增 nil-`*Store` 守卫探针与存在性优先级；`kafka_test.go` 新增首次尝试即死信探针；`erp_contract_test.go` 新增单条/批 422 契约测试并刷新 422 注释；`openapi_contract_test.go` 断言 OpenAPI 描述文本。

## 2026-08-15 — 流帧组件拒绝 `:`：aggregate 帧与租户 ID 层的冒号注入收口（internal/domain，internal-domain-6c1ab500）

**写给运营（行为变化）：**

- **`aggregate_type` / `aggregate_id` 禁止 `:`**：`:` 是流帧分隔符（`Event.Stream` 以
  `tenant:aggregate:<type>:<id>` 组帧），此前 `("a","b:c")` 与 `("a:b","c")` 会导出**字节相同**
  的流键 `tenant-a:aggregate:a:b:c`，两个不同聚合被静默合并进同一条哈希链账本流（同一序列
  计数器、同一条哈希链、同一 segment/checkpoint 血缘、同一归档前缀），且 `VerifyIntegrity`
  按存储流键重验时合并链一致、**不可检出**——租户内写者可故意把事件交织进另一聚合的链而
  不触发任何校验失败。现在携带 `:` 的聚合组件在入账前被拒：HTTP 单条与批端点均返回
  **400 `invalid_request`**（非 422），批端点仍按 ERP 契约在首个错误处截断（已入账 receipts
  作前缀返回，`not_attempted` 语义不变）；gRPC 与 outbox 写入共享同一 `ValidateBasic` 边界，
  同样在写库前拒绝。**需要生产方动作**：发送 `:` 聚合组件的生产方请改为无冒号的命名；
  新事件永不落库。OpenAPI 的 `aggregate_type`/`aggregate_id` pattern 已同步排除 `:`，新生产方
  客户端侧即失败。
- **租户 ID 禁止 `:`（创建与请求边界）**：`:` 是 dev token 的分隔符
  （`strings.Split(token, ":")`），租户 ID 含 `:` 时 `dev:<tenant>:<roles>` 会被静默重绑定到
  另一个租户（如 `dev:acme:prod:tenant-admin` → 租户 `acme`）。现在新创建租户的 ID 含 `:`
  即 400 拒绝（side-effect free，无租户/无 admin action）；平台 `?tenant_id=` 查询含 `:` 的租户
  过滤器返回 400；JWT `tenant_id`/`tenant` 声明含 `:` 的令牌认证失败（错误文本保持
  非 oracular 的 `token <name> claim is invalid`）。
- **既有含 `:` 租户不受数据迁移影响（行为收窄，无数据重写）**：已存储的含 `:` 租户继续
  保持全部存储键与入账路径（`Ingest` 解析到的租户不再重验字符集，流帧字符串零改动，序列/
  哈希链/segment/checkpoint/归档前缀逐字节不变）。这些租户仅在新创建、`?tenant_id=` 查询、
  JWT 声明与 dev token 边界被拒；如需恢复完整边界功能，请将其重命名为无冒号 ID
  （已存储账本不动）。dev token 的租户主体段位于前两个冒号之间，构造上不可能含 `:`——
  4 段客户端形式（`dev:tenant-a:service:crm`）保持可用，重绑定向量改在创建源收口。
- **无存储迁移**；回滚 = 重新部署旧二进制（旧二进制重新接受冒号组件，缺陷随之回归，
  请同步升级生产方）。`operation_id`/`source_system`/`event_id`/`schema_id` 仍允许 `:`
  （单组件帧，构造上单射），字符集规则与帧字符串本身均未改动。

**写给开发（实现变化）：**

- `internal/domain/identifiers.go`：新增导出 `ValidStreamComponent(name, value)` 与
  `ValidTenantIDComponent(name, value)`，均为 `ValidKeyComponent` + 恰好一条 `:` 拒绝
  （错误包装 `domain.ErrInvalid` 并命名组件）；`ValidKeyComponent` 本身对非租户/非流调用方
  保持不变（`:` 仍合法）。
- `internal/domain/models.go`：`ValidateBasic` 的聚合循环拆为两层——`aggregate type`/
  `aggregate id` 走 `ValidStreamComponent`，`operation id` 留在通用规则；循环顺序保持
  确定性错误优先级（event_id/source_system 循环仍先于聚合循环，聚合循环内 type 先于 id）。
  `Event.Stream()` 帧字符串零改动。
- `internal/store/store.go`：`ValidTenantID` 委托 `ValidTenantIDComponent`；`CreateTenant`
  （service.go:199）、`tenantFor` 两条分支（server.go:1087,1105）经此继承拒绝。
- `internal/auth/auth.go`：`parseDevToken` 主体校验与 `tenantClaim`（JWT `tenant_id`/`tenant`）
  切换到 `ValidTenantIDComponent`；错误文本逐字节不变（`invalid development token` /
  `token <name> claim is invalid`，非 oracular）。
- `api/openapi/openapi.yaml`：`aggregate_type`/`aggregate_id`/`Tenant.id` pattern →
  `^[^:\x00-\x1F\x7F\s/\\]+$`（排除 `:`）；`event_id`/`source_system`/`operation_id`/
  `schema_id`/`Source.id` 不变。
- 测试：`identifiers_test.go` 泛型 accepted 语料移出 `"a:b"`（通用规则以非租户名 `event id`
  另行钉死 `:` 合法），新增 `ValidStreamComponent`/`ValidTenantIDComponent` 拒绝钉、`ValidateBasic`
  冒号碰撞对与跨层确定性、`TestStreamFrameInjectiveOverAcceptedPairs`（往返+单射+
  at-most-one）、`TestStreamFrameFuzzColonFreeUnique`（固定种子 ≥1 万迭代，字母表含 `:`）、
  `TestStreamFramesCrossBranchDisjoint`（`:aggregate:`/`:operation:`/`:source:` 字面量互斥）；
  `tenantkey_test.go`/`tenant_validation_test.go`/`server_test.go`/`auth_test.go` 语料同步迁移
  （`"a:b"` 从 accepted 移入 rejected，dev token 4 段形式正向钉住，JWT 声明 `"a:b"` 拒绝）；
  `key_framing_test.go` 新增冒号入账拒绝 + 快照前后不变 + `TestStoredStreamFramingByteIdentical`
  （存储 `StreamID` 逐字节等于改动前常量 + `VerifyIntegrity` 同 `SegmentCount`）；
  `erp_contract_test.go` 新增批端点冒号 400 + 首错截断契约；`outbox` SDK 冒号拒绝（写库前）；
  `openapi_pattern_test.go` 双 pattern 计数（通用 5 处 + 流/租户 3 处）。`python3 cli.py quality` 全绿。
## 2026-08-15 — 本地 HS256 密钥强度门禁：JWT 密钥 ≥32 字节 + 与服务密钥互斥 + `jwt_secret_length` 预检字段（internal/auth，local-hs256-mode-has-no-secret-strength-gate-whi-b3c8be60）

**写给运营（行为变化）：**

- **`AUDIT_JWT_SECRET` 必须 ≥32 字节**（256 位，与 HS256 密钥尺寸一致）且显式设置 `AUDIT_ALLOW_LOCAL_HS256=true` 时，低于 32 字节的密钥在**启动与 `-check-config` 预检均失败关闭**（错误 `local HS256 JWT secret must be at least 32 bytes`，退出码 1，不打印 `check_config=ok`）。此前仅要求非空，短密钥可被离线暴力破解：HS256 签名可用单个捕获令牌离线验证候选密钥，一旦恢复即可伪造任意声明（含 `platform-admin` → `audit:platform:cross_tenant` 跨租户读写）。请升级前将密钥轮换为 ≥32 字节；旧短密钥签发的既有令牌随升级作废（这正是门禁的目的）。
- **JWT 密钥必须与 `AUDIT_SIGNING_SECRET`/`AUDIT_ENCRYPTION_KEY` 不同**：同一字符串不得既用于伪造令牌，又用于签名校验点（破坏 VerifyIntegrity）或解密受保护字段/导出。
- **预检新增 `jwt_secret_length` 字段**：`check_config=ok` 行在 `encryption_key_length` 之后新增该字段（无 JWT 信任源时为 0，仅输出长度、从不输出密钥值）；按前缀匹配该行的消费者不受影响。
- 门禁为**纯长度**判定（无熵评分），≥32 字节即通过，确定且可测；`AllowDev` 无 JWT 密钥、JWKS/PEM 信任路径、worker 二进制行为不变。无配置/API/数据迁移；回滚 = 重新部署旧二进制。

**写给开发（实现变化）：**

- `internal/auth/verifier.go`：新增 `minJWTSecretBytes = 32` 常量；`ValidateConfiguration` 在互斥性检查（无 opt-in / 与 JWKS/PEM 共享）之后、信任源分支之前新增长度门禁（`hasSecret && a.AllowLocalHS256 && len(a.JWTSecret) < minJWTSecretBytes`，字节计长，错误消息由常量格式化，保留既有错误优先级）。该函数已统一覆盖全部认证路径：每请求（`AuthenticateTokenContext`）、启动（`prepareServer`）、CI（`runCheckConfig`），门禁在令牌解析与 dev-token 快捷路径之前触发。
- `cmd/audit-api/main.go`：`check_config=ok` 新增无条件 `jwt_secret_length=%d`（值为 `len(authenticator.JWTSecret)`，未配置时为 0）；新增 `validateAuthConfig`（SEC-2 互斥性：JWT 密钥 ≠ 签名/加密密钥，因签署/加密密钥在 service 配置中，需在二进制层合流），由 `prepareServer` 与 `runCheckConfig` 共用。
- 测试夹具迁移：全部 HS256 夹具改用各包共享的 ≥32 字节密钥常量（`testHMACSecret`/`testJWTSecret`，41 字节），覆盖 `internal/auth`、`cmd/audit-api`、`internal/httpapi`、`internal/grpcapi`、`internal/service`（含证据遗漏的 4 处：grpcapi 168/198、secrets_integration 247、main_test 368、server_test 3392/3535/3696）。
- 新增测试：`TestValidateConfigurationRejectsShortHS256Secret`（AC-1 边界 31/32/33 + 多字节字节边界 16×é=32B 通过/15×é=30B 拒绝 + 优先级 + PEM 互斥单元）、`TestAuthenticateTokenContextRejectsShortHS256Secret`（AC-2 每请求门禁在解析前触发 + dev-token 失败关闭 + 合规密钥正向控制）、`TestRunCheckConfigRejectsShortJWTSecret`/`TestRunCheckConfigReportsJWTSecretLength`/`TestRunCheckConfigReportsZeroJWTLengthWhenUnset`/`TestCheckConfigJWTSecretStrengthParity`（AC-3 直接 + 子进程预检：退出码、错误标记、无泄漏、0-when-unset）、`TestRunCheckConfigRejectsJWTSecretSharedWithServiceSecrets`（SEC-2）；跨二进制 `check_config=ok` 格式契约测试（env_consistency_test.go）同步更新。`python3 cli.py quality` 全绿。

## 2026-08-15 — JWKS 未知 kid 强制刷新放大修复：负缓存 + 每间隔一次强制刷新 + 锁外取数（internal/auth，unauthenticated-jwks-kid-miss-forced-refresh-amp-d0642ad1）

**写给运营（行为变化）：**

- **未知 kid 不再按个触发 IdP 拉取**：此前每个不同的未知 `kid` 令牌都会强制一次 JWKS 拉取（N 个未知 kid → N+1 次，且未签名令牌即可触发），可被未认证流量放大成 IdP 负载。现在：同一刷新间隔内，任意数量的未知 kid 合计最多 **1 次初始拉取 + 1 次强制刷新**（跨 W 个间隔 ≤ 1+W 次，与 kid 数量无关）；最近被记为缺失的 kid 在间隔内再次出现时**零网络拉取**直接拒绝（负缓存，TTL 与刷新间隔一致，每 URL 上限 256 条 FIFO）。
- **认证不再被慢拉取阻塞（head-of-line 修复）**：JWKS 拉取不再持有缓存互斥锁——缓存集仍新鲜时，合法令牌的认证即时完成，不再排队等待在途的（可能被攻击者卡住的）拉取；拉取本身单飞（每 URL 同时最多一个在途请求），等待方受调用方 context 约束。
- **键轮换行为不变**：新轮换入的 kid 仍触发间隔内那一次允许的强制刷新并被采纳；出现在刷新后集合中的 kid 永不被负缓存。IdP 故障时 last-known-good 集继续服务已知 kid（stale-on-outage 不变）；未知 kid 仍 fail-closed 拒绝，错误文本与以前逐字节一致（HTTP 401 / gRPC Unauthenticated）。
- **无配置、无 API、无数据迁移**；回滚 = 重新部署旧二进制。唯一可感知的变化是：未知 kid 的拒绝不再伴随每次拉取，以及合法认证不再被慢/阻塞的 JWKS 拉取拖住。

**写给开发（实现变化）：**

- `internal/auth/verifier.go`：`jwksCacheEntry` 增加有界负缓存（`negCache`/`negOrder`，FIFO 上限 `negativeCacheCap=256`，kid 长度 > `maxNegatedKidBytes=64` 不保留）与强制刷新预算时间戳 `forcedAt`（与 `fetchedAt` 独立，成功强制刷新不重开预算窗口）；`sync.Mutex` → `sync.RWMutex`；拉取改为单飞 `beginFetch`/`finishFetch`（`fetchOutcome`，锁外 I/O，panic 安全，join 等待受调用方 ctx 约束）。`remoteVerificationKey` 重构为“只读取集 → kid 命中/缺失判定 → FR-1/FR-2 门控”，缺失路径先为**空缓存**做一次初始拉取（保留既有旋转测试的 initial+forced 语义），再走 `refreshForMissingKid`：负命中直接拒绝、预算耗尽 check-then-negate（并发刷新已落地该 kid 时直接服务且不写入负缓存）、预算可用时消耗预算并单飞强制刷新。`uniqueKey` 对空集与零匹配返回 `errKeyNotFound` 哨兵（消息与歧义 kid 错误逐字节一致），歧义 kid（重复 kid 的畸形集合）拒绝但不消耗预算、不写入负缓存。无新增依赖；`auth.go`、公开 API、路由、配置零改动。
- 测试（`internal/auth/verifier_test.go`）：新增 A1a（100 个不同 kid → 恰 2 次拉取 + 重放负缓存 kid 零拉取 + 拒绝文本一致）、A1a-evic（300 个 kid 触发 FIFO 上限且拉取数不变）、A1b（每间隔 ≤ 1 次强制刷新 + 负条目过期后重放恰多一次拉取）、A2（慢拉取不阻塞新鲜集合法认证，< 1s）、A3（预算窗口不被攻击流量重置）、S1（并发初始拉取单飞去重）、S2（join 等待受调用方 ctx 约束）、S3（并发轮换至多一次瞬态拒绝 + 刷新落地后零拉取服务 + check-then-negate 单元）、F5（leader ctx 取消后预算仍被消耗）、S5（歧义 kid 不消耗预算）、长 kid 不驻留内存、拉取 panic 后单飞状态可恢复；攻击令牌 helper 带非空签名段（空签名段在头部即被拒，测试否则真空）。三个既有缓存/轮换/宕机测试原样通过；`python3 cli.py quality` 全绿。

## 2026-08-15 — S3 归档 WORM 门禁落地：Ready 强制 COMPLIANCE 默认留存 + 每次 Put 显式 COMPLIANCE 留存（internal-archive-b9e968b8）

**写给运营（行为变化）：**

- **归档就绪门禁收口（R-1/AC-1）**：`S3Store.Ready` 现在要求 Object Lock 桶的**默认留存规则为
  COMPLIANCE 模式 + 正有效期（Days/Years > 0）**，否则失败关闭。此前仅检查 Object Lock
  “Enabled”：GOVERNANCE 默认留存（持有 `s3:BypassGovernanceRetention` 的主体可删留存对象）、
  无默认留存规则、或零有效期默认留存的桶都会让 worker/API 启动失败、`-check-config` 退出码 1
  （不打印 `check_config=ok`）、`/readyz` 返回 503。两类错误文本互斥且指明修复：
  - `no default retention`（R-3a）：桶无默认留存或有效期为 0/单位非法 → 需
    `mc retention set --default compliance 365d <bucket>`（或 Years > 0）；
  - `GOVERNANCE`（R-3b）：默认留存为非 COMPLIANCE 模式 → 需改为 COMPLIANCE。
- **每次写入显式 COMPLIANCE 留存（F1 leg (i)）**：S3 归档现在要求配置
  `AUDIT_ARCHIVE_RETENTION_DAYS`（正整数，如 365；上限 100 年）——缺失/为零时两个二进制
  `-check-config` 与启动均失败关闭并指明该变量。配置后，每次 `Put` 都携带
  `Mode=COMPLIANCE` + `RetainUntilDate=now+留存时长`，因此即使桶默认留存被移除/降级，写入窗口
  内的对象仍受 COMPLIANCE 保护，`StatusArchived` 回执与探测时机无关。每次写入的留存会覆盖该
  对象的桶默认值；请将 `AUDIT_ARCHIVE_RETENTION_DAYS` 设为合规要求时长（如 365）。
- **API 启动即探测归档就绪（F1 companion）**：`audit-api` 与 worker 一样在启动时对有界
  （5 秒）探测归档目的地，非 WORM-ready 桶（含上面的 R-3a/R-3b 状态）启动失败
  （`archive ready: …`）；编排重启会在桶修复后恢复。`/readyz` 轮询探测不变。
- **升级顺序**：先对每个既有桶跑 `./bin/audit-governance-worker -check-config`；R-3a/R-3b
  报错的桶先 `mc retention set --default compliance 365d <bucket>`，并为两个二进制配置
  `AUDIT_ARCHIVE_RETENTION_DAYS`（与桶默认一致或更长）再升级。已写对象留存固定在写入时刻，
  不受后续配置变更影响；回滚 = 重新部署旧二进制（旧二进制接受所有既有配置）。

**写给开发（实现变化）：**

- `internal/archive`：`Ready` 消费 `GetObjectLockConfig` 全部返回值并新增 R-1 检查（缺省留存
  → R-3a；非 COMPLIANCE 模式 → R-3b，经 `sanitizeModeEcho` 消毒回显：控制字符 → `?`、
  上限 64 rune，防恶意 S3 端点日志注入 CWE-117）；`S3Store` 新增 `retainFor` 字段与
  `NewS3StoreWithRetention`/`NewS3StoreRetention`（非正时长构造即失败），`Put` 在
  `retainFor > 0` 时携带显式 COMPLIANCE 留存；`retainFor=0` 保持字节一致（leg (ii)）。
- `internal/runtimeconfig`：新增 `AUDIT_ARCHIVE_RETENTION_DAYS`（`EnvArchiveRetentionDays`）
  与 `SigningArchive.ArchiveRetentionDays`；`Archive()` 对 S3 归档强制正留存（≤ 36500 天），
  经 `NewS3StoreRetention` 构造（seam 增加 `retainFor` 参数）。
- `cmd/audit-api`/`cmd/audit-governance-worker`：新增 `-archive-retention-days` 标志（默认读
  `AUDIT_ARCHIVE_RETENTION_DAYS`，非数字回退 0 → fail-closed）；API 新增有界启动探针
  （`archiveReadyTimeout=5s`，worker 同款 `probeArchiveReady`）。
- `test/e2e/fullstack.sh`：cutover 块导出 `AUDIT_ARCHIVE_RETENTION_DAYS`（默认 365，正数
  guard），`deploy/docker-compose.verify.yml` 两个服务补 `AUDIT_ARCHIVE_RETENTION_DAYS: "365"`。
- 测试：`fakeS3Client`/`scriptedS3Client` 增加 `lockMode/lockValidity/lockUnit`（默认
  COMPLIANCE/365/DAYS），`fakeS3Client` 增加 `putOpts` 录制；AC-1 六行表 + 未知/恶意模式行、
  AC-2 leg (i)（每次 Put 的 COMPLIANCE 留存下界断言 + 非正时长构造失败）、AC-3 两行
  check-config 新错误类、API 启动探针镜像测试；`python3 cli.py quality` 全绿。

# Release Notes

## 2026-08-15 — Verify 栈归档桶自举 COMPLIANCE 默认留存 + cutover 三态验证（test/e2e + deploy 配套）

**写给运营（行为变化）：**

- **归档桶默认留存收口（R-1 部署配套）**：`test/e2e/fullstack.sh` 现在在启动任何应用之前给
  `worm-audit` 桶设置 COMPLIANCE 365d 默认留存（`mc retention set --default compliance 365d`），
  并自动执行 cutover 三态验证：无默认留存 → `-check-config` 退出码 1 + `no default retention`
  （R-3a）；GOVERNANCE 365d → 退出码 1 + `GOVERNANCE`（R-3b）；COMPLIANCE 365d → 退出码 0 +
  `check_config=ok`。验证在一次性 `worm-audit-cutover` 桶上进行（无数据、可安全重建，重跑幂等）。
- **既有桶升级顺序**：R-1 门禁落地后，任何无默认留存/GOVERNANCE 默认的归档桶都会让 worker
  启动失败关闭。升级前对每个桶跑 `./bin/audit-governance-worker -check-config`，R-3a/R-3b 报错
  的桶先 `mc retention set --default compliance 365d <bucket>` 再升级。已写对象不受影响（留存
  固定在写入时刻）；回滚 = 重新部署旧二进制（旧二进制接受所有既有桶配置，已设的 COMPLIANCE
  默认无需撤销）。完整 runbook 见 README「部署 / 回滚 runbook（R-1 COMPLIANCE 默认留存门禁）」。
- **mc 语法注意**：pinned minio 镜像内置 mc 的 retention 子命令用位置参数模式
  （`mc retention set --default compliance 365d`），`--compliance` 标志形式会被拒绝。

**写给开发（实现变化）：**

- `test/e2e/fullstack.sh`：桶自举后新增 COMPLIANCE-default cutover 块（宿主 `go build`
  worker 二进制 + `checkconfig_expect` 断言 helper：退出码、必需 marker、marker 互斥
  （R-3a 不含 `GOVERNANCE`/`check_config=ok`，R-3b 不含 `no default retention`）），随后给
  `worm-audit` 设 COMPLIANCE 365d 并正向复核，再启动应用。
- `deploy/docker-compose.verify.yml`：worker/minio 服务注释文档化 R-1 桶不变式与自举命令。
- `README.md` / `docs/VALIDATION_PLAN.md`：`AUDIT_S3_*` 与 `-check-config` 契约更新为
  COMPLIANCE + 正有效期；新增 cutover 矩阵与部署/回滚 runbook。
- 本配套不包含 R-1 门禁代码（`internal/archive` 的 `Ready` 强制检查由独立实现任务落地）；
  负向断言只在门禁二进制部署后成立，fullstack.sh 与门禁代码须同一发布。

## 2026-08-15 — Replay 重放分类不再被 Kafka key 覆盖有效冲突 payload event_id（internal/kafka）

**写给运营（行为变化）：**

- **重放分类以 payload event_id 为准，key 仅作空探针回退**：`scanAccepted` 扫描
  accepted topic 时，若消息 payload 携带**有效但非 wanted** 的 `event_id`，不再回退到
  匹配的 Kafka key 重放该消息——此前 `event_id="B"` 的消息若 key 恰好等于 wanted
  的 `A`，会被当作 `A` 重放并错误闭环 `A` 的 DLQ 记录（真实的 `A` 原事件若已过期，
  该记录被静默关闭且 `AuditDLQUnresolvableDrop` 永不触发）。现在该消息被跳过，`A`
  的 DLQ 记录在 drain 时按 unresolvable 收敛：日志
  `unresolvable event_id=A reason=original-not-found-in-accepted-topic`、
  `audit_dlq_unresolvable_total` 递增、DLQ offset 提交一次。
- **无配置、无指标、无 API 变更**：行为与 asyncapi.yaml 契约一致（key MUST equal
  payload 的 event_id）；key 匹配 + 值不可解析（空探针）的 anti-loop 收敛不变。
  回滚 = 重新部署旧二进制。

**写给开发（实现变化）：**

- `internal/kafka/replay.go`：`scanAccepted` 的 key 回退分支增加 `payloadID == ""`
  守卫（与 `deadLetterUnparsable` 形状一致，D1 一行 diff），文档注释同步声明空探针
  守卫；其余分支（anti-loop、republish、永久/一次性闭环、drain-unresolvable、提交
  屏障、状态格式、指标词汇）逐字节不变。
- 回归测试：新增 `TestReplayConflictingPayloadKeyFallbackConvergesUnresolvable`
  （两阶段：sustained-ingest cutoff 轮无任何标记/提交，topic 静默后 drain 路径仅一次
  提交 offset 1 + 一次 durable mark；未修复代码在阶段 1 即失败）；AC-4 断言 on-disk
  状态文件在冲突扫描期间无 `A` 标记、收敛后恰一条标记；既有回归针（空 key 按 payload
  提交、key 不匹配按 payload 解析、unparsable anti-loop、崩溃后不重放）全部原样通过。
- 边界加固：`TestReplayWantedMarkedPayloadDoesNotFallBackToUnmarkedKey`（已标记的
  payload event_id 不回退到未标记 key，K 走 drain-unresolvable 收敛，K@2 不被错误
  闭环）、`TestReplayConflictingMessageDoesNotSuppressRealOriginal`（冲突消息在真实
  原事件之前时只跳过冲突消息，真实原事件仍按 payload 逐字节重放）、以及消费端兄弟
  规则子测试 `payload event_id wins over non-empty stale key on unparsable`——三个
  测试在未修复代码上均失败（区分器验证过）。

## 2026-08-15 — DLQ replay 尊重 Failure.ErrorCode：permanent_error 一次性闭环 + 诚实的 replayed + 按码指标（internal/kafka + cmd/audit-kafka-dlq-replay）

**写给运营（行为变化）：**

- **`error_code="permanent_error"` 的死信记录改为一次性重试后闭环**：此前这类记录在
  API 永久拒绝时被标记并计入“replayed”，而瞬态失败时每轮重试、永不收敛（毒丸）。
  现在每条 `permanent_error` 记录在其生命周期内至多重发一次；重发失败（无论永久拒绝
  还是瞬态故障）都进入**永久闭环**：状态标记、DLQ offset 提交、日志打印
  `PERMANENT closure event_id=... code=permanent_error one-shot republish attempt
  failed: <reason>; DLQ offset committed`，不再出现 `marking replayed`。重发成功则照常
  计入 replayed（留给晚部署 schema 的一次机会）。手动恢复仍可用既有模式：删除状态
  条目后重新摄取（DLQ 记录在 topic 保留期内仍在）。
- **`-once` 汇总与返回语义变诚实**：永久闭环不再计入 `replay round done replayed=%d`
  （纯闭环轮次现在报告 `replayed=0`），也不再计入 `audit_dlq_replayed_total`。
  `attempts_exhausted` / `unparsable_message` / 未知 code 的重放行为不变（对照测试
  保留）；非 permanent 码记录被 API 以永久错误拒绝时仍走遗留闭环（返回计入保留）。
- **指标改名（破坏性，仅影响外部消费者）**：`audit_dlq_permanent_rejections_total`
  → **`audit_dlq_permanent_total`**（行位置不变），语义扩宽：现在同时统计 legacy 活
  拒绝闭环与 `permanent_error` 记录的瞬态失败闭环——**新旧值不可直接对比**。仓库内
  无消费者（告警集未引用该指标）；外部仪表盘必须在同一部署窗口更新，旧名停止输出。
- **新指标 `audit_dlq_attempts_exhausted_total`**：按轮观测计数（每轮收集到
  `error_code="attempts_exhausted"` 记录即递增，重投递的记录每轮再计一次）——是流量
  计数器，不是解析/积压计数器，勿按积压解读。
- **崩溃窗口说明**：闭环标记在重发尝试之后持久化；若进程在“尝试 → Mark 持久化”之间
  崩溃，恢复后该记录会再重试一次（每轮至多一次；标记持久化后即闭环，不再重试）。
  状态文件格式未变，双向兼容；回滚 = 重新部署旧二进制（指标名与语义随之回退）。

**写给开发（实现变化）：**

- `internal/kafka/replay.go`：`wantedEvents` 改为返回 `wantedSet{replay, oneShot}`
  （不变式 oneShot ⊆ replay 按构造成立；按 event ID 归类，重复 ID 混码记录共享
  状态标记一并闭环）；`scanAccepted` 失败分支合并为单一闭环块——`permanent_error`
  记录任何失败都闭环（返回与 `replayed` 指标均不计入），非 permanent 码记录仅
  `DeliveryError.Permanent` 活拒绝走遗留闭环（返回贡献保留），检查顺序保证
  permanent 码记录永不落入遗留分支；闭环日志行对 event_id（64 runes）与失败原因
  （200 runes）做 CWE-117 清洗，原因经 `fmt.Sprintf("%v", err)` 抗 nil-Err panic；
  `collectFailures` 对 `attempts_exhausted` 记录递增新原子；`ReplayerMetrics` 字段
  `PermanentRejections` 改名 `Permanent` 并新增 `AttemptsExhausted`（观测计数，明确
  排除在“一次解析一个计数器”不变式之外）。
- `cmd/audit-kafka-dlq-replay/main.go`：`metricsText` 输出 11 行（permanent 行保持
  位置，`attempts_exhausted` 行位于 unparsable 与 republish_failures 之间）；`/metrics`
  处理函数抽为 `metricsHandler(func() kafka.ReplayerMetrics)`（挂载语义不变：仅常驻、
  `-once` 不挂载）。
- 回归测试：AC-1a/1b/1c（一次性闭环：永久拒绝 / 瞬态失败 / 非 permanent 码对照）、
  AC-3（`TestReplayPermanentFailureConverges` 改用 `permanent_error` fixture，返回 0）、
  AC-4（一次性成功照常重放）、S-1（遗留活拒绝闭环返回贡献 1）、REQ-PERM-3 计数、
  FM-1a/1b/FM-2a 崩溃注入（尝试→Mark 间崩溃恰好重试一次 / Mark 持久化失败不计
  closure 计数 / Mark 后崩溃不重试）、oneShot ⊆ replay 不变式、闭环日志 CWE-117
  清洗（镜像 auth-blocked 测试）；混合轮次测试扩展为一次性 + legacy 共用
  `permanent` 计数器并固定返回不对称；T7/T7b golden 更新为 11 行，新增 /metrics
  HTTP 精确值测试。

## 2026-08-15 — 401 类死信持久判别 + 无盲重放（`-replay-auth-blocked`）+ api-url 明文传输门禁

**写给运营（行为变化）：**

- **空/轮换 ingest token 现在有独立的失败信号**：`audit-kafka-consumer` 启动时若
  `AUDIT_OUTBOX_TOKEN` 为空会立即打印警告（与 relay 对齐），且 401 类重试耗尽的消息
  死信为 `error_code="unauthorized"`（此前与 API 故障一样是 `attempts_exhausted`），
  单独计数 `audit_consumer_unauthorized_total`。新增两条告警：
  `AuditDLQAuthBlocked`（15m 窗口内新的 401 类死信）与 `AuditDLQAuthBlockedBacklog`
  （replay 侧 `audit_dlq_auth_blocked` 积压 gauge > 0 —— 阻塞记录不计入
  `audit_dlq_pending`，`AuditDLQBacklog` 看不到它）。两条告警都依赖对应二进制挂载
  `/metrics`（`AUDIT_KAFKA_METRICS` / `AUDIT_DLQ_REPLAY_METRICS`，默认关闭；
  `-once` 模式不挂载 /metrics）。
- **`audit-kafka-dlq-replay` 不再盲重放 `unauthorized` 记录**：新 `-replay-auth-blocked`
  flag（默认**关**）——默认状态下 `error_code="unauthorized"` 的 DLQ 记录被
  auth-blocked：不重放、不标记、不提交（提交屏障保持其 offset pending），每轮计数
  `audit_dlq_auth_blocked_total`（计数器，逐轮递增）并更新积压 gauge
  `audit_dlq_auth_blocked`（最新一轮阻塞数，排空后归零）。flag 打开 = 运营者确认凭证
  已修复，阻塞记录恢复重放。**排空后必须把 flag 从常驻配置中移除**，恢复保护。
- **api-url 明文传输门禁（F3，RFC 6750 §1）**：三个客户端二进制（consumer / relay /
  replay HTTP 模式）启动时校验 `-api-url`/`AUDIT_OUTBOX_API_URL`：非 loopback 的
  `http://` 直接启动失败（bearer token 明文过网）；https 与 loopback（localhost /
  127.0.0.1 / ::1 / host.docker.internal）放行；本地验证栈显式设置
  `AUDIT_ALLOW_INSECURE_API_URL=true` 放行并打印醒目警告。**升级前必须处理**：现有
  部署若用非 loopback 明文 http，须先加 `AUDIT_ALLOW_INSECURE_API_URL=true` 或改用
  https，否则新二进制启动即失败。verify compose 已加该变量并标注 dev-only。

**事故处置 runbook（401 类死信 / token 轮换 / IdP 故障）：**

1. 观察 `AuditDLQAuthBlocked` / `AuditDLQAuthBlockedBacklog`（或
   `audit_consumer_unauthorized_total` 增长）。**先区分凭证问题与 IdP/JWKS 故障**：
   401 可能来自空/错 `AUDIT_OUTBOX_TOKEN`，也可能来自 IdP/JWKS 不可达或过期公钥
   （M4）——查 `audit-api` 日志与 `AUDIT_JWKS_URL` 可用性，不要只换 token。
2. 修复凭证（consumer 的 `AUDIT_OUTBOX_TOKEN`，以及 relay 的对应 token）；确认
   `AuditDLQAuthBlocked` 不再增长、`audit_consumer_unauthorized_total` 停增。
3. 用 `-once` 单轮排空：`audit-kafka-dlq-replay -once -replay-auth-blocked`（Kafka
   重发模式也可用；HTTP 模式确保 `-api-url`/`-token` 正确），观察
   `audit_dlq_auth_blocked` 归零。常驻实例则临时加 flag 重启，排空后移除。
4. 若 `audit_dlq_auth_blocked > 0` 但凭证健康且无新死信：检查 DLQ topic 中是否有
   伪造/残留的 `error_code="unauthorized"` 记录（明文 Kafka 可被注入，H2）——确认
   对应事件在 accepted topic 有原文后，再用 flag 排空或人工清理。
5. 排空后确认 `AuditDLQAuthBlockedBacklog` 清除；从常驻配置移除 flag。

**滚动部署与回滚顺序（含混部一次性盲重放窗口）：**

- **上线顺序（必须）**：① 先全部升级 replay 二进制（flag 默认关——旧版 consumer 尚
  不会写出 `unauthorized` 记录，因此新 replay 对旧版能产生的所有记录行为与旧版逐字节
  一致，零风险步）→ ② 再全部升级 consumer（开始写入 `unauthorized` 分类）→ ③ 最后
  应用规则文件。**顺序反了 = 一次性盲重放窗口**：新 consumer 写出的 `unauthorized`
  记录若被仍在运行的旧 replay 实例（不认识 error_code，盲重放一切）先取到，会被**盲
  重放一次**并提交——每个记录只有一次窗口，无法重阻塞。新 replay 全量就位后，后续
  记录才受保护。
- **既有 DLQ 记录**：升级前已存在的 `attempts_exhausted` 记录（含实际由 401 引起的）
  无法追溯重分类，首次新版本运行会被照常重放一次（状态维持原状，AC-7.3.3）。
- **回滚顺序**：① 先关闭所有实例上的 `-replay-auth-blocked`（若开着）→ ② 回滚
  consumer（停止新分类）→ ③ 最后回滚 replay：**回滚到旧 replay 时，仍处于阻塞的
  积压记录会被旧版一次性盲重放**——凭证已修复则自动收敛（等于排空）；凭证仍坏则
  回到改动前行为（每轮盲重放循环）。不想接受盲重放的，先在②之前用 flag 排空积压
  （`audit_dlq_auth_blocked` 归零）。状态文件格式未变，双向兼容；回滚时同步移除
  两条新告警规则（引用缺失指标的规则惰性但应清理）。
- **flag 开启窗口**：flag 仅在事故处置时开（凭证修复后），不要作为常驻配置；凭证仍
  坏时开启会每轮重发一次阻塞记录并由 consumer 重新死信（`AuditDLQTraffic` 持续触发），
  属预期且有界（轮间隔）。

**写给开发（实现变化）：**

- `internal/outbox`：`DeliveryError` 新增 `StatusCode int`（分类点与 2xx-unverified
  路径填充，非 HTTP 错误为 0）；新增 `ValidateAPIURL`/`IsInsecureHTTPURL`/
  `InsecureAPIURLAllowed`（F3 门禁，镜像 `internal/auth.validateJWKSURL` 的 loopback
  规则与 `AUDIT_ALLOW_INSECURE_API_URL` 逃生口）。
- `internal/kafka`：新增 `ErrorCodeUnauthorized = "unauthorized"`（AsyncAPI free
  string，契约不变）；`Consumer.consume` 达到重试上限时按 `DeliveryError.StatusCode
  == 401` 分类并递增 `Unauthorized` 计数（非 401/非 DeliveryError 退化为
  `attempts_exhausted`，FM-2）；`Replayer` 新增 `dlqRecord.errorCode/authBlocked`、
  `SetReplayAuthBlocked`、`authBlocked` 计数器 + `authBlockedPending` gauge；阻塞记录
  被 `wantedEvents` 排除且被提交屏障保持 pending（两个谓词同一判定，F-02）；逐记录
  阻塞日志按轮限流（每轮最多 10 条详情 + 1 条总数汇总，H1），日志字段经控制字符清洗
  与长度截断（M3/CWE-117）。`RunOnce` 的重置顺序未动，`replay_round_reset` 门禁
  不受影响。
- `cmd/audit-kafka-consumer`：启动空 token 警告（parity）；`metricsText` 抽出并新增
  `audit_consumer_unauthorized_total` 行（golden 固定顺序）。
- `cmd/audit-kafka-dlq-replay`：新增 `-replay-auth-blocked` flag（对常驻与 `-once`
  两个实例都生效）；`metricsText` 追加 `audit_dlq_auth_blocked_total` /
  `audit_dlq_auth_blocked` 两行（T7 两个 golden 同变更更新）；flag 开启且 HTTP 模式
  token 为空时额外警告（F7）。
- `cmd/audit-outbox-relay`：HTTP 分支增加 F3 门禁与 opt-in 警告。
- `deploy/prometheus-rules.verify.yml`：`audit-dlq` 组新增 `AuditDLQAuthBlocked` /
  `AuditDLQAuthBlockedBacklog`；`checks/prometheus_rules.py` 将其钉死（expr/severity/
  注解 + YAML 解析）。
- 测试：AC-7.1.x/7.2.x/7.3.x/7.4.x + 新 AC-D1（flag 恢复重放）/AC-D2（golden 更新）
  + F-01（StatusCode 端到端填充）/F-02（混合分区阻塞×屏障两种顺序）/F-05（401 后
  自愈、无 DLQ 时 401 计数）/F-06（词汇钉死）/F-03（阻塞积压 gauge 语义）/H1（详情
  日志限流）/M3（日志字段清洗）/F3（URL 门禁单元 + 二进制级启动失败/opt-in 警告）。

## 2026-08-15 — gRPC ingest receive-size cap and per-field envelope limits (HTTP parity, internal/grpcapi)

**写给运营（行为变化）：**

- **gRPC 传输接收上限收紧到 512KB**：`Write`/`WriteBatch`/`WriteStream` 单条消息超过
  `domain.MaxEventBytes*2` = 512KB 时返回 `ResourceExhausted`（此前继承 grpc-go 默认
  4MB，是 HTTP 请求体上限的 8 倍——每条事件都会触发全量快照存储更新与摘要计算，
  4MB 请求是纯存储/CPU 放大）。与 HTTP 表面对齐：`grpcapi.MaxRecvBytes` 由
  `domain.MaxEventBytes` 派生，两侧上限不会漂移。
- **新增逐字段上限**：所有逐字持久化进哈希链账本的 envelope 字符串字段（reason、
  trace_id、actor.*、target.*、changed_fields 的 JSON 字符串、payload_ref、各类 ID）
  超过 8KB 返回 `InvalidArgument`；`actor.roles` ≤ 64、`targets` ≤ 64、
  `changed_fields` ≤ 256、`WriteBatch` ≤ 500 条，违规同样返回 `InvalidArgument`。
  `payload_json` 不受 8KB 限制（仍受既有的 256KB 载荷上限与 512KB 传输上限约束）。
- **WriteStream 逐消息跳过（仅尺寸类拒绝）**：尺寸/数量超限的消息被拒绝且**不产生回执**，
  流保持打开；其余拒绝类别（key-framing、尾随 JSON、运行时错误）仍终止流。
  客户端按 `event_id` 对账：收到回执 = 已入账，未收到 = 被拒（服务端日志含
  截断的 `event_id` 前缀 + client_id/tenant 归属，供运营追踪）。
- **WriteBatch 计数超限原子拒绝**：> 500 条在提交循环前拒绝，零部分提交；
  批内逐事件尺寸违规保持既有部分提交语义（前缀已入账、线上回执丢弃——与
  key-framing 语义一致，gRPC 错误响应不带消息体，客户端需读账本区分）。
- **HTTP/gRPC 接受集不对称（有意为之）**：HTTP 只有整包 512KB 上限、无逐字段上限；
  gRPC 严格更严（gRPC 接受集 ⊂ HTTP）。跨传输迁移/混部负载的生产者必须满足
  gRPC 上限才能保证事件不丢。
- **传输安全提醒（既存问题，后续方向跟踪）**：入站 gRPC 监听器默认无 TLS，
  `authorization` 元数据明文过网。生产部署必须将 `AUDIT_GRPC_LISTEN` 置于
  TLS 终结设施之后（或实现 `grpc.Creds` + `-check-config` 失败关闭门禁）。
- **滚动部署**：纯服务端变更，无需配置/数据迁移；合规生产者（已在上述上限内）
  无需改代码。混部窗口内旧节点仍接受超限请求。建议部署后监控
  `"grpc stream: rejecting over-cap message"` 日志作为非合规生产者的早期信号。

**写给开发（实现变化）：**

- `internal/grpcapi/limits.go`（新增）：`MaxRecvBytes`、`MaxEnvelopeFieldBytes`、
  `MaxActorRoles`/`MaxTargetsPerEvent`/`MaxChangedFields`、`MaxBatchEvents`、
  哨兵 `ErrEnvelopeTooLarge`（包装 `domain.ErrInvalid`，经既有 `toStatus` 映射为
  `InvalidArgument`）、表驱动 `validateEnvelopeCaps`（校验顺序确定：nil → 上限 →
  occurred_at/actor/decode）与 `truncateEventID`（日志行内截断至 64 字节）。
- `internal/grpcapi/server.go`：`fromProto` 在复制前调用 `validateEnvelopeCaps`；
  `WriteBatch` 计数预检；`WriteStream` 对 `ErrEnvelopeTooLarge` 跳过并记录日志。
- `cmd/audit-api/main.go`：`grpc.NewServer` 增加 `grpc.MaxRecvMsgSize(grpcapi.MaxRecvBytes)`。
- 测试：AC-1 接线断言扩展（标识符 + `MaxRecvBytes == domain.MaxEventBytes*2`）、
  AC-2 `TestFromProtoRejectsOversizedEnvelope`（字段/数量超限 + 精确边界 + 端到端
  Write）、AC-3 `TestGRPCRejectsOverCapBatch`（501 条零部分提交 + 500 条全量提交）、
  AC-4 `TestGRPCWriteStreamOverCapContinues`（跳过 + 回执 + `io.EOF` + 日志断言）、
  以及传输层 `ResourceExhausted`（生产上限 harness）、批内成员部分提交、
  skip-后-非尺寸-终止、nil Logger 守卫、HTTP 接受/gRPC 拒绝不对称等固定测试。
- 已知限制（R1）：逐字段上限不约束 envelope 总量低于 512KB（最坏情况 ≈ 8.4MB），
  此类消息在传输层被拒（`ResourceExhausted`）并终止流——grpc-go 固有行为；
  后续方向可增加 per-envelope 总量上限使所有尺寸拒绝都可跳过。

## 2026-08-15 — gRPC ingest 拒绝首值后的尾随 JSON（严格单值解码，internal/grpcapi）

**写给运营（行为变化）：**

- **gRPC 收窄一类畸形输入**：`payload_json`/`before_json`/`after_json` 在第一个
  JSON 值之后携带非空白内容（例如 `{"a":1} extra`、`{"a":1}{"b":2}`）时，
  `Write`/`WriteBatch`/`WriteStream` 此前会**静默丢弃尾随内容并接受事件**——
  账本记录与生产者实际发送的字节不一致，`SourceDigest` 覆盖被截断的载荷；
  现在返回 `InvalidArgument`，事件不入账、无回执。错误文本与既有非法 JSON
  完全一致（`payload_json is invalid` / `invalid before_json` /
  `invalid after_json`），已处理过非法 JSON 的客户端无需改动。
- **合法输入不变**：尾随空白（`{"a":1}  `）仍被接受；所有此前被接受的良构事件
  照常入账，`SourceDigest` 与修复前逐位一致（`UseNumber` 路径未动）。
- **与 HTTP 对齐**：HTTP 表面自始拒绝该输入类（`decodeBody` 的
  “request body must contain one JSON value” 检查），本修复消除两传输对同一
  逻辑事件产生不同 `SourceDigest` 的差异，恢复跨传输摘要一致。
- **滚动部署**：新节点更严格（拒绝畸形输入），旧节点更宽松（接受并截断）；
  良构流量无互操作影响，任序升级安全。无数据迁移、无版本号变更。

**写给开发（实现变化）：**

- `internal/grpcapi/server.go` `decodeJSONNumber`：首值 `Decode` 成功后追加
  第二次 `Decode` 到临时 `any`，结果非 `io.EOF` 即返回
  `ErrInvalid`（"trailing content after first JSON value"），与 HTTP
  `decodeBody` 的耗尽检查同构；首值语法错误路径不变，`UseNumber` 保留，
  无调用点改动（三个调用点原有每字段包装与 `toStatus` 的 `InvalidArgument`
  映射原样生效）。
- 测试：`internal/grpcapi/server_test.go` 新增
  `TestFromProtoRejectsTrailingJSON`（AC-1：三字段 + 空白负控 + 相邻值边界
  + 空载荷负控）、`TestGRPCRejectsTrailingJSONPayload`（AC-2：三 RPC 全链路
  `InvalidArgument` + 快照无残留 + 干净负控）、`TestGRPCHTTPDigestParity`
  （AC-3：跨传输 `SourceDigest` 相等，覆盖 >2^53 大整数/嵌套对象/数组载荷/
  变更字段对；顶层数组载荷按两侧一致拒绝断言接受集一致）。

## 2026-08-15 — `GET /api/v1/admin/actions` now honors `tenant_id` for platform tokens

**写给运营（行为变化）：**

- **平台 token + `tenant_id` 过滤生效**：`GET /api/v1/admin/actions?tenant_id=<T>`
  在平台 token 下此前返回**所有租户**的自审计动作（`platform` 短路了过滤器，
  静默丢弃已验证的 `tenant_id`）；现在只返回 `<T>` 租户的动作，`count ==
  len(items)`。依赖该接口做合规巡检的下游工具若曾利用旧行为，需验证过滤参数
  的预期。
- **未过滤的平台读取不变**：不带 `tenant_id`（或空值，OpenAPI 允许）的平台
  token 仍返回全部租户的动作（all-tenants 读）。
- **租户 token 不变**：租户作用域 token 仍只能看到本租户动作。
- **平台导出需显式 `tenant_id`**：平台 token 调 `POST /api/v1/exports` 不传
  `tenant_id` 时返回 400（此前返回一个必然为空的任务 202）；带 `tenant_id`
  的平台导出不变。
- **滚动部署注意**：这是纯服务端修复，无需客户端协调；但混部窗口内旧节点仍
  会泄露过滤请求的全量数据，跨租户敏感数据的巡检请等待全部节点完成升级。

**写给开发（实现变化）：**

- `internal/service/governance.go`：`ListAdminActions` 过滤谓词由
  `platform || action.TenantID == tenantID` 修正为
  `(platform && tenantID == "") || action.TenantID == tenantID`，与
  `internal/service/service.go` 中已存在的契约注释（“platform caller sees all
  actions (or one tenant with the optional filter)”）对齐；签名不变，OpenAPI
  文档不变。
- `internal/httpapi/server.go` `tenantFor` 平台分支收敛：平台 token 且无
  `tenant_id`（或空值）时返回空哨兵 `""`，不再回落到 `claims.TenantID`。
  此前 dev token 的 `claims.TenantID` 恒等于其 subject（
  `dev:platform:platform-admin` → `"platform"`），无过滤读会被错误限定到
  不存在的 `"platform"` 租户；生产 JWT 平台 token 的 `tenant_id` claim 本为
  空，返回 `""`。此改动使 dev 与生产路径语义一致，与 `tenantFor` 既有注释
  “Empty stays legal (all-tenants read)” 对齐。
  **跨端点影响（dev 模式）**：`ListSchemas`/`QueryEvents`/`ListSources`/
  retention/hold/export 读取在平台 token 无 `tenant_id` 时按空作用域
  fail-closed（精确匹配空串 → 空结果，绝不跨租户）；对生产 JWT 无行为变化
  （其 claim 本就为空）。依赖 dev 模式下“平台 token 无过滤读=租户
  `platform` 的数据”的本地脚本需显式传 `tenant_id`。
- `internal/service/governance.go` `CreateExport` 新增空租户守卫（ADR-0009
  第 4 项）：空作用域导出按构造选不到任何事件（精确 TenantID 匹配），不存在
  “导出全部租户”语义，因此平台 token 无 `tenant_id` 的导出请求改为 400
  （此前 202 + 空任务）。带 `tenant_id` 的平台导出与租户 token 导出不变。
- 测试：`internal/httpapi/server_test.go` 新增
  `TestHTTPAdminActionsPlatformTenantFilter`（AC-1/AC-2/AC-2b/FM-8/limit）、
  `TestHTTPAdminActionsTenantTokenIgnoresOverride`（租户 token 忽略
  `tenant_id` 覆盖）、`TestHTTPCreateExportPlatformRequiresTenantID`
  （空租户导出 400 + 无残留）；`TestHTTPTenantForRechecksClaimTenantID`
  增加平台空哨兵 pin；`internal/service/read_selfaudit_test.go` 新增
  `TestListAdminActionsPlatformTenantFilter`（服务契约 + 排序 + 过滤先于上限
  pin）；`internal/service/service_test.go` 新增
  `TestCreateExportRequiresTenantScope`（服务层 fail-closed pin）；
  `internal/service/governance_hold_test.go` 的 `countAdminActions` 辅助函数改为
  `ListAdminActions("", true, 100)` 保持其 “platform view: every tenant” 意图。

## 2026-08-15 — DLQ replay: 拆分解析计数，unresolvable 永久丢失可告警（internal/kafka + cmd/audit-kafka-dlq-replay + deploy）

**写给运营（行为变化）：**

- **`audit_dlq_replayed_total` 语义收紧**：该计数器现在只统计成功重发。
  2026-08-11 的“语义扩展”（把 unparsable/permanent-failure/converged-
  unresolvable 收敛都计入 replayed）被本变更取代。
- **新增三个解析计数器**：`audit_dlq_unresolvable_total`（原事件在 accepted
  topic 中不存在——保留期过期或 topic 重建——DLQ offset 已提交、事件被永久
  丢弃）、`audit_dlq_permanent_rejections_total`（API 永久拒绝、标记收敛不再
  重试）、`audit_dlq_unparsable_marks_total`（key 匹配但 value 不可解析的
  反循环标记）。每条 DLQ 记录的首次解析只递增其中一个计数器；瞬态失败不递增
  任何计数器。
- **新告警 `AuditDLQUnresolvableDrop`（severity: warning）**：15 分钟内
  `audit_dlq_unresolvable_total` 增长即触发。unresolvable 丢弃此前只留下
  `unresolvable event_id=...` 日志行，对告警不可见；现在 15 分钟内可见，
  收到告警请人工确认 accepted-topic retention / 重建事件。
- **`-once`/cron 模式注意**：`/metrics` 只在常驻模式挂载（`-metrics-listen`
  非空且未传 `-once`），cron `-once` 单轮部署不暴露新计数器，
  `AuditDLQUnresolvableDrop` 不会触发；`-once` 部署如需不可解析丢弃告警，
  请改用常驻模式。
- **存量仪表盘注意**：依赖 `audit_dlq_replayed_total` 的看板在部署后会出现
  台阶式下降（收敛路径迁移到三个新计数器）。仓库内无其他消费者依赖旧语义
  （告警集从未使用该指标）。

**写给开发（实现变化）：**

- `internal/kafka/replay.go`：`Replayer` 新增 `permanentRejections` /
  `unresolvable` / `unparsableMarks` 三个 `atomic.Uint64`；`scanAccepted`
  四个解析路径各递增且仅递增一个计数器（成功重发→`replayed`、反循环标记→
  `unparsableMarks`、永久拒绝→`permanentRejections`、drained-unresolvable→
  `unresolvable`）；`republishFail` 保持瞬态+永久的全量语义。
- `Replayer.Metrics()` 改为返回具名结构 `kafka.ReplayerMetrics`（8 字段）；
  `cmd/audit-kafka-dlq-replay/main.go` 的 `/metrics` 渲染抽为纯函数
  `metricsText`，三个新指标行固定在 `audit_dlq_replayed_total` 与
  `audit_dlq_republish_failures_total` 之间；`Content-Type`、`/metrics`
  路径、daemon-only 挂载均不变。
- `deploy/prometheus-rules.verify.yml` 新增 `AuditDLQUnresolvableDrop`
  （`increase(audit_dlq_unresolvable_total[15m]) > 0`，severity warning）；
  新静态门禁 `checks/prometheus_rules.py` 接入 `cli.py cmd_quality`，校验
  alert 名/expr/severity/annotations，防止规则漂移（修复前文件失败、修复后
  通过，`checks/test_prometheus_rules.py` 覆盖）。
- 行为零变化保证：`RunOnce`/`scanAccepted` 返回值、`replay round done
  replayed=%d` 日志、`commitResolved` 提交纪律、`republish_failures_total`
  语义、AsyncAPI 契约均不变；`checks/replay_round_reset.py` 保持通过。

**回归测试：** `internal/kafka/replay_test.go` 新增 T1..T6（unresolvable /
permanent / 成功重发 / 反循环标记 / 混合轮次精确计数 + 提交屏障不变式）；
`cmd/audit-kafka-dlq-replay/main_test.go` 新增 `metricsText` golden 测试
（T7）；`checks/test_prometheus_rules.py` 覆盖门禁正反例（T8）；
`python3 cli.py quality` 全绿（T9）。

## 2026-08-13 — Ingest 回执状态转换失败改为失败关闭，不再上报未持久化的 Indexed/Archived（internal/service）

**写给运营（行为变化）：**

- **失败关闭语义**：当回执状态转换的持久化写入（`Store.Update`）失败时——并发副本乐观锁冲突重试 3 次后仍耗尽，或后端不可用——`POST /api/v1/events`（及批量接口的对应事件）现在返回 503/500，**不再返回声称 `indexed`/`archived` 的成功响应**。此前三个状态写入的错误被丢弃（`_ =`），本地内存回执被无条件推进到 `Indexed`/`Archived`，客户端看到的成功状态从未持久化（重启后 `GetReceipt` 回退为 `ledgered`）。
- **幂等重试安全**：503 沿用既有“按幂等语义重试”契约；重试同一事件返回已持久化的 `ledgered` 回执（`duplicate=true`），绝不返回虚构转换。`wait_for=indexed|archived` 只在转换已持久化后才成功。
- **无需客户端变更**：无 API/OpenAPI 版本变化；成功路径与今天逐字节一致。回执停留在 `ledgered` 的事件由既有 `ArchivePending` 收敛（归档对象/状态自愈）。

**写给开发（实现变化）：**

- `internal/service/service.go`：`Ingest` 中三处回执状态转换 `Store.Update`（归档成功→`archived`、归档降级→`indexed`、无归档→`indexed`）由 `_ =` 改为错误检查，失败时在本地回执变更之前 `return receipt, err`——返回的回执携带账本 CAS 已提交状态（`StatusLedgered`，`IndexedAt`/`ArchivedAt` 为零），与既有 `ErrConflict` 模式一致；错误原样返回（`errors.Is` 保持 503 映射）。`internal/store` 契约（重试/耗尽/保存即提交）与 HTTP/OpenAPI 层零改动。
- 测试：新增 `internal/service/ingest_durability_test.go`（`failSaveBackend` 窗口故障注入 + AC-1..AC-4 映射）；对修复前代码红（吞错被检出），修复后绿；`python3 cli.py quality` 全绿。


## 2026-08-13 — QueryEvents 按业务时间排序，分页游标改为时间坐标（internal/domain + internal/service）

**写给运营（行为变化）：**

- **查询结果改为确定性时间序**：`GET /api/v1/events`（及内部 `QueryEvents`）的返回顺序由
  `(occurred_at, sequence, event_id)` 决定，跨流按业务时间排列，不再按每流账本序号
  `(sequence, event_id)`。`sequence` 是每流独立空间，旧序在多流查询下并非全局时间序；
  新序与合规导出的排序完全一致（共享同一比较器，不再可能漂移）。单流、业务时间严格递增
  的既有客户端看到的顺序不变。
- **分页游标失效（需客户端动作）**：本次发布前签发的 opaque `cursor` 值在
  `QueryEvents` 中被拒绝，返回 HTTP 400 `invalid_request`，消息明确提示
  “cursor predates chronological ordering; re-run the query”。**没有兼容映射**：旧游标是
  每流 `(sequence, event_id)` 坐标，在全局时间空间中无确切切点，fail-closed 是唯一安全
  行为。客户端重跑查询获取新游标即可；无数据迁移、无环境变量、无 schema/快照变化。
- **新增确定性契约**：`next_cursor` 精确落在当前页最后一条的
  `(occurred_at, sequence, event_id)` 上；按 `next_cursor` 逐页翻页与原查询全集
  无重复、无遗漏。恶意/畸形游标与过去一样返回 400，不产生新攻击面。

**写给开发（实现变化）：**

- `internal/domain/canonical.go`：新增 `Cursor` 值对象（`OccurredAt/Sequence/EventID/Legacy`）；
  `EncodeCursor`/`DecodeCursor` 由 2 元组编解码改为 3 元组
  `[occurred_at_rfc3339nano_utc, sequence, event_id]`（经既有 `CanonicalJSON` 归一化），
  元素个数区分新旧格式（2 → `Legacy`，3 → 时间序，其余 `ErrInvalid` fail-closed）。
- `internal/service/service.go`：抽取 `compareEvents` 作为 `(OccurredAt, Sequence, EventID)`
  唯一比较器，`sortEvents`（导出路径）与 `QueryEvents` 排序共用，杜绝再次漂移；
  `QueryEvents` 对 `Legacy` 游标返回 `ErrInvalid` 包装的明确拒绝（`statusForError` 映射 400）。
- 契约文档：`api/openapi/openapi.yaml` 为 `GET /api/v1/events` 补充排序/游标语义描述（schema 不变）。
- 测试：`TestQueryEventsChronologicalAcrossStreams`（跨流时间序 + 单流正控）、
  `TestQueryEventsCursorReproducesSet`（分页复现全集 + legacy/垃圾游标负控）、
  `TestExportMatchesQueryEventsOrder`（API 首页为导出严格前缀，集合/顺序一致）；
  `TestCursorRoundTrip` 改钉 3 元组往返与 legacy 解码；`TestOutOfOrderOccurredAtEvents` 的
  `QueryEvents` 断言随新契约更新（其余钉：按写入序分配 sequence、时间线按业务时间、
  哈希链按 sequence——不变）。

**性能与分页成本（实测，2026-08-13，本机 Ryzen AI Max+ 395）：**

- **排序无回归**：`QueryEvents` 排序键由 `(Sequence, EventID)` 改为
  `(OccurredAt, Sequence, EventID)`，实测比较器增量 ≈ +1.4 ns/次比较——5,000 事件账本
  排序 519 µs → 602 µs（+83 µs，占单页总成本 ~0.1%）；端到端 `BenchmarkQuery`
  新旧代码在本机噪声带（±25%）内（12.5–17.4 ms @1k）。导出路径 `sortEvents` 比较器
  字节级未变（仅抽取为共享 `compareEvents`），**导出延迟不受影响**。
- **游标编解码可忽略**：3 元组 `EncodeCursor` 481 ns、`DecodeCursor` 1.1 µs
  （旧 2 元组为 214 ns / 644 ns），增量 < 0.5 µs，占单页成本 <0.01%。
- **逐页成本上界（本次契约的固有写放大，需要知晓）**：每一页都是
  **O(ledger) 全量扫描 + 排序 + 1 次读自审计快照写**（F-06，actor 非空时整快照
  clone+序列化+持久化）。N 页翻页 = N 次扫描 + N 条审计事实。实测单页：
  ~13–16 ms @1,000 事件、~68–73 ms @5,000 事件（内存后端；磁盘后端另付
  marshal+fsync）。1,000 事件账本全量翻页（`PageSize=100`，10 页）≈ 0.16 s；
  5,000 事件（50 页）≈ 3.6 s。**上界**：`PageSize ≤ 1000`（`MaxPageSize`），
  单页 p95 仍在既有容量 envelope 内（cutover 门禁 10⁵ 事件 / p95>500 ms 不变）；
  但**全量翻页成本随页数线性累积**，客户端应优先用大 `PageSize` 减少页数；生产热查询路径
  为 ClickHouse 投影（ADR-0004），不存在此写放大。

## 2026-08-13 — 保留 `*__search_digest` 负载命名空间：ingest 前 fail-closed 拒绝冲突顶层键（internal/service + internal/security）

**写给运营（行为变化）：**

- **顶层 `*__search_digest` 键不再可分配**：`POST /events`（单条、批量）与 gRPC 写入在 ingest 校验阶段拒绝任何顶层负载键以 `__search_digest` 结尾的事件，返回 400 `invalid_request`（`domain.ErrInvalid`），**不落库、不产生回执、不写管理痕迹**（校验先于字段保护与任何存储写入）。此前的行为是：生产方植入的冲突键会原样入库，随后被内部摘要覆盖/删除，导致流永久校验失败（历史问题 R3/F4“ingest 卫生”）。重试/去重此类历史负载同样返回 `ErrInvalid` 而非 `Duplicate`/`Conflict`——有意为之的 fail-closed 变更。
  **兼容性说明**：拒绝范围比“会触发损坏的冲突键”更宽——即使顶层键对应的字段不在 `SearchableFields` 中（本不会与内部摘要写入发生覆盖冲突），也一律拒绝；这是有意的命名空间卫生决策（AC-1/AC-2 钉住）。此前以非可搜索字段携带顶层 `*__search_digest` 自有数据的生产方需在升级前调整负载。
- **嵌套 `*__search_digest` 键不变**：仍可入库，仅在读取/导出边界由 `StripSearchDigests` 递归剔除（`TestExportJSONLStripsSearchDigests` 保持通过）。
- **`VerifyIntegrity` 对携带保留键的失配给出中立诊断**：已入库的、负载携带顶层 `*__search_digest` 键且内容校验失配的事件，报告专门指出保留命名空间（`search_digest`/reserved）的错误，替代笼统的 `content digest mismatch`。措辞中立且事实准确：不宣称“无法重建”（重构本身成功——正是靠它发现失配），无论失配源于遗留冲突还是对健康事件普通内容的篡改；多冲突键时报告的键名按字典序确定。有效性结论不变（事件/流仍为 invalid），仅措辞更可诊断；**无数据修复**（被覆盖的生产方明文已不可恢复）。
- **自洽的遗留形状不变**：顶层摘要键持有真实摘要、`SourceDigest` 覆盖剔除摘要键内容的旧事件照常验证 `Valid`。
- **升级提示**：无环境变量、无数据迁移、无 proto/OpenAPI 变更；二进制同批发布（模块内部接口，既有锁步要求）。

**写给开发（实现变化）：**

- `internal/security/fieldcrypto.go`：新增导出常量 `SearchDigestSuffix = "__search_digest"`，替换 4 处字面量（写入/重构删除/匹配/剥离）——写者、重构者、匹配器、剥离器与新校验器共用同一后缀。
- `internal/service/reserved_namespace.go`（新增）：`rejectReservedSearchDigestNamespace`（REQ-1，仅顶层键，`ErrInvalid` 包装，先于 `AllowedFields` 循环）与 `firstReservedSearchDigestKey`（REQ-3 归因辅助；多冲突键时按字典序确定报告键，保证 triage 确定性）。
- `internal/service/service.go`：`validateEvent` 在 `rejectSensitive` 之后新增保留命名空间校验；`verifyContentDigest` 失配分支在负载含顶层保留键时返回中立、事实准确的专属诊断（不包含“cannot be reconstructed”断言）。
- `internal/service/reserved_namespace_test.go`（新增）：AC-1 拒绝且零落库、AC-2 即使 `AllowedFields` 显式列出仍拒绝、AC-3.1 被拒事件不影响完整性、AC-3.2 遗留碰撞得到专属错误（且不含笼统 mismatch 串）、嵌套键合法，以及两项评审回归钉：健康事件被篡改且带保留键时不误报为遗留碰撞（F-1）、多冲突键报告键名确定（F-4）。

## 2026-08-13 — Vault Transit 签名密钥绑定与可取消请求路径（internal/security + service + httpapi/grpcapi/worker）

**写给运营（行为变化）：**

- **签名现在绑定到配置的 Transit 密钥**：`Sign` 收到 Vault 返回的签名时校验其
  `vault:v<ver>:<key>:…` 前缀中的密钥名。若密钥名与 `AUDIT_VAULT_TRANSIT_KEY` 不一致
  （Vault 被攻破、被换、或配置漂移）、或前缀畸形（缺失/空 payload）→ **签名请求失败**，
  该证据**绝不落库、绝不归档**（原子写中止）。此前任何非空签名都会被当作权威检查点
  记录，密钥漂移只会在事后 `VerifyIntegrity` 才暴露。正确配置的部署行为零变化。
- **Vault 请求可被取消**：`Sign`/`Verify` 全程携带调用方 context——HTTP 客户端断开、
  gRPC 取消、worker 收到 SIGTERM 都会**立即中止在途 Vault 请求**，不再死等 10s 客户端
  超时。
  - **ingest 提交不受客户端断开影响**：`POST /events` 的落库提交使用
    `context.WithoutCancel(r.Context())`——客户端中途断开**不会**回滚已提交事件
    （事件照常入账，客户端按 `event_id`/`receipt_url` 查询）；取消只中止签名往返。
  - worker 改用 `signal.NotifyContext`：SIGTERM 在 pass 中途取消 Vault 往返并快速退出。
- **拒绝跟随重定向**：Vault 客户端设置 `CheckRedirect: http.ErrUseLastResponse`，任何 3xx
  按非 200 状态失败关闭——**`X-Vault-Token` 永不转发到跨域重定向目标**（此前会原样
  带过去，配合被攻破的 Vault 前端/代理可被窃取）。
- **Vault 往返有 3s 预算**：即使取消被剥离（ingest 提交路径），单次签名调用在持锁状态下
  最多阻塞 3s（原本可达 10s 客户端超时）；调用方更早的 deadline 仍然优先。
- **杂项加固**：Vault 非 200 响应体回显截断为 256 字节并 `%q` 转义（防日志注入）；
  `X-Request-ID` 请求头截断至 128 字节（响应头/错误体/span 属性均受限）。
- **升级提示**：无环境变量、无数据迁移、无密钥轮换；正确配置下行为不变。两个二进制
  必须同批发布（接口为模块内部契约，二进制锁步既有要求）。回滚 = 回退二进制，无持久化
  格式变化。

**写给开发（实现变化）：**

- `internal/security/vaultsigner.go`：`Sign`/`Verify`/`call` 改为接收 `context.Context`；
  新增 `checkSignatureKey`（解析 `vault:v<ver>:<key>:<payload>`，密钥名非机密可指名，
  payload 永不回显；ASCII-only 版本段，unicode 数字/trailing-colon 一律 fail-closed）；
  `NewVaultTransitSigner` 保留原签名，新增 `newVaultTransitSigner` 构造 seam（超时可注入）；
  `vaultCallBudget`（3s）叠加在客户端超时之上；`redirectReject` 拒绝跟随重定向；
  `truncateEcho` 截断响应体回显；构造注释更新（明文 http 仅经上游
  `SigningArchive.resolveVaultTransport` 的回环 opt-in 可达）。
- `internal/service/service.go`：`Signer` 接口 `Sign`/`Verify` 增加 `ctx`（本地 `hmacSigner`
  忽略之）；`Ingest`/`sealSegment`/`SealPendingSegments`/`CreateAggregateCheckpoint`/
  `VerifyIntegrity` 线程化调用方 context；`isInterrupted` 将 `context.Canceled`/deadline
  分类为「中断」而非签名不匹配，`VerifyIntegrity` 取消时返回 `Valid=false` 且不记录
  虚假的 mismatch 自审计事实。
- `internal/httpapi/server.go`：ingest 提交边界 `context.WithoutCancel(r.Context())`；
  `X-Request-ID` 截断至 128 字节。`internal/grpcapi/server.go`：RPC ctx 传入 ingest。
  `cmd/audit-governance-worker/main.go`：`signal.NotifyContext` + `defer stop()`。
- 测试：`internal/security`（外键密钥拒绝/畸形矩阵/取消/在途取消确定性证明/重定向零请求
  断言/慢响应超时）、`internal/service`（seal 路径签名失败原子中止且零归档、ctx 链取消、
  `VerifyIntegrity` 取消语义）、`internal/httpapi`（客户端断开事件保留、请求 ID 截断）、
  `FuzzCheckSignatureKey`（不 panic、错误文本不含 payload）。

## 2026-08-13 — 导出下载完整性：租户/任务绑定密封与摘要校验（export:v2，internal/security + service/governance + httpapi/server）

**写给运营（行为变化）：**

- **下载现在验证内容后才返回**：`GET /api/v1/exports/{jobId}/download` 在写出任何字节前校验
  密封对象与任务记录的绑定和摘要。此前下载直接解封并 200 流出，归档对象被换/损坏时照常
  当作权威数据返回；现在：
  - 归档对象字节被篡改（位翻转）、被换成其他任务/租户的对象、或内容与任务摘要不符 →
    **500 `internal_error`**（redacted 信封，无路径/errno/事件内容），不再返回任何 ndjson 字节；
  - 新导出的归档对象以 `export:v2:` 密封并绑定任务（租户+任务 ID 派生 AAD）；历史
    `export:v1:` 对象继续可下载（v1 分支语义不变）；
  - 任务记录 `Digest` 为空或畸形（异常状态）→ 下载 fail-closed 500，绝不静默放行。
- **审计事实语义收紧**：`audit.event.export` 只对**通过验证**的下载记录；归档 404/500 的失败
  下载不再记录该事实（此前在取对象前就记录）。验证失败（篡改/摘要不符）新增
  `export.download_rejected` 自审计事实（target 为任务 ID，无内容）——篡改尝试现在在治理
  痕迹中可见。
- **HTTP 状态语义保留**：404（不存在/跨租户）、403（法律保留）、409（未完成）不变；仅产生
  500 的条件变宽（验证失败）。OpenAPI 下载操作补充 `500` 响应。
- **升级提示**：无数据迁移、无密钥轮换、无配置变更；单版本发布即可。回滚 = 回退二进制，
  回滚窗口内写入的 v2 对象无法被旧版 v1-only 解封（500），任务记录保留摘要，重新导出可恢复。

**写给开发（实现变化）：**

- `internal/security/fieldcrypto.go`：新增 `exportV2Prefix`、`ExportBinding`（长度前缀的
  tenant/job AAD 绑定，不可碰撞）、`EncryptBytesBound`（v2 密封）、`DecryptExport`（v1/v2
  版本分发开放；v1 分支复用 `DecryptBytes` 原语义）。`EncryptBytes`/`DecryptBytes` 签名与
  行为不变。
- `internal/service/governance.go`：`runExport` 密封改为 `EncryptBytesBound` + 任务派生绑定；
  新增下载咽喉 `VerifyExportDownload`（租户重查 → 409 防御 → 绑定解封 → 摘要格式
  `^[0-9a-f]{64}$` → 摘要相等 → 仅全过返回明文）与 `recordExportRejected`（失败事实，
  best-effort）。
- `internal/httpapi/server.go`：`downloadExport` 顺序改为 取对象 → `VerifyExportDownload` →
  `RecordExportDownload` → 200。
- `internal/domain/models.go`：新增 `AdminActionExportRejected = "export.download_rejected"`。
- `api/openapi/openapi.yaml`：下载操作补充 `500` 响应。
- 测试：绑定矩阵/黄金帧/版本分发/注入性属性/Fuzz、服务层篡改与换 blob 矩阵与拒绝事实、
  HTTP 层篡改/换 blob（v1+v2）/摘要不符/空摘要 500、未完成 409、验证事实计数；两个既有
  服务测试改为经 `DecryptExport` + 绑定打开 v2 blob 并断言 `export:v2:` 前缀。

## 2026-08-13 — 外部归档/签名传输强制 TLS：S3 新增 `AUDIT_S3_USE_SSL`，Vault 地址做 scheme/回环校验（internal/runtimeconfig）

**写给运营（行为变化）：**

- **S3 归档新增 TLS 开关 `AUDIT_S3_USE_SSL`（对应 `-s3-use-ssl` 标志，默认 `false`）**：
  此前归档传输被硬编码为明文 HTTP，`AUDIT_S3_ENDPOINT` 指向任何非本机端点时，静态的
  `AUDIT_S3_ACCESS_KEY`/`AUDIT_S3_SECRET_KEY` 与归档审计证据都走明文；且操作员即使写
  `https://…` 端点也无法使用（minio 拼出 `http://https://…` 报错）。现在：
  - `AUDIT_S3_USE_SSL=true` 时端点按 TLS 解析（`Secure=true`）；
  - 端点带 `https://` scheme 但开关未开 → **启动/`-check-config` 直接失败**，错误信息
    指名要设置 `AUDIT_S3_USE_SSL=true` 或去掉 scheme，绝不静默降级为明文；
  - 端点带 `http://` scheme 且开关为 true → 冲突报错；
  - 本地开发端点（`localhost:19010`、`deploy/` 的 `minio:9000`）保持默认明文可用，行为不变。
- **Vault 地址开始校验 scheme 与主机**：`AUDIT_VAULT_ADDR` 此前原样拼接进请求 URL 并明文发送
  `X-Vault-Token`。现在：
  - 必须带 scheme（`https://` 或回环主机上的 `http://`）；schemeless（`host:8200`）、
    非回环 `http://`、带路径/query/userinfo/空白一律**启动失败**，错误信息给出修复方式；
  - 回环主机（`localhost`、`host.docker.internal`、`gateway.docker.internal`、回环 IP）上的
    明文 `http://` 需要显式设置 `AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK=true`
    （对应 `-allow-insecure-vault-loopback`，默认 `false`）——镜像既有 JWKS 回环白名单纪律，
    该 opt-in **永远不会放宽到非回环主机**。
- **`-check-config` 输出新增传输标签**：`check_config=ok … transport_s3=%s transport_vault=%s`，
  每个外部腿解析为 `tls`/`http`/`local`（未配置腿为 `local`）；解析失败时整行不打印、
  退出码 1。两个二进制输出逐字节同形，CI 可直接断言部署解析出 `transport_s3=tls`。
- **新增布尔开关均为 fail-closed**：`AUDIT_S3_USE_SSL`/`AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK`
  的值格式错误（如 `tru`）时进程退出码 1 并指名变量，绝不静默回退到明文。
- **升级提示**：两个二进制必须同批发布；旧二进制不识别新环境变量（保持明文默认），
  新二进制对「明文非回环 Vault / schemeless 地址」的配置会启动失败——请先对每个环境执行
  `-check-config` 确认 `transport_s3=tls`/`transport_vault=tls` 后再切流。

**写给开发（实现变化）：**

- `internal/runtimeconfig/runtimeconfig.go`：新增 `EnvS3UseSSL`/`EnvAllowInsecureVaultLoopback`
  常量与 `S3UseSSL`/`AllowInsecureVaultLoopback` 字段；`resolveS3Transport()`/`resolveVaultTransport()`
  为唯一解析源（返回归一化 `url.Parse` 的 scheme 标签，杜绝上报与实建不一致），
  `Transport() (s3, vault string, err error)` 逐腿上报；`newS3Store` 构造 seam（`Archive()`
  内不再有字面量布尔参数）；`sanitizeAddr` 剥离 userinfo/query/fragment 后再进错误文本，
  凭证永不回显；`loopbackHost` 按既定纪律复制自 `internal/auth/verifier.go`（drift 由
  镜像验收表钉住）。
- `cmd/audit-api/main.go`、`cmd/audit-governance-worker/main.go`：新增 `-s3-use-ssl`/
  `-allow-insecure-vault-loopback` 严格布尔标志（worker 补齐 `strictBoolEnv`，仅限这两个
  开关，其余布尔保持宽松语义），启动路径 `logger.Fatalf` 失败即止，`check_config=ok` 增加
  `transport_s3=%s transport_vault=%s`（两二进制格式串逐字节一致）。
- 测试：`internal/runtimeconfig/runtimeconfig_test.go`（AC-1/AC-2 全表、非法输入矩阵、
  per-leg 标签、上报≡实建钉、凭证不回显钉）、新 `cmd/audit-api/main_test.go`（AC-3/AC-4、
  严格布尔子进程、启动 fail-fast）、`cmd/audit-governance-worker/main_test.go`（AC-4、
  probe 有界超时）、`internal/service/env_consistency_test.go`（两二进制引用新常量、
  `transport_*=%s` 针）。
- `python3 cli.py quality` 全绿；`security-scan` N/A（govulncheck/gosec 未安装）。

## 2026-08-13 — relay 投递改为 receipt 校验：无 API 确认不再标记 delivered（internal/outbox）

**写给运营（行为变化）：**

- **HTTP 投递现在校验审计 API 的 receipt**：relay 请求 `POST /api/v1/events?wait_for=ledgered`，2xx 响应体必须解码出 receipt 且同时满足 V1–V4（`event_id` 与载荷一致、状态 ∈ {ledgered/indexed/archived}、`ledgered_at` 非零、`hash` 非空）才把记录标记为 delivered。此前任何 2xx（包括代理剥掉 `wait_for`、代理伪造 2xx、API 接受后失败）都会直接标记 delivered，之后 `ListPending`（只查 pending）永远看不到该行。
- **`api_status`/`delivered_event_id` 不再伪造**：HTTP 投递存 API 返回的真实状态（`indexed`/`archived`/`ledgered`）；Kafka 投递写 NULL（迁移 003 的注释契约 "API receipt status observed at delivery time" 现在成真）。已确认无任何 Go/SQL 消费者读取这两列。
- **401 从永久死信改为可重试**：token 过期/轮换不再一次轮询把整个积压死信到 `failed`；凭证修复后下一轮自动恢复投递（403/409/422 仍永久死信，429/5xx/网络错误仍可重试）。
- **2xx 但无法验证的响应保持 pending 并指数退避重试**，`last_error` 写明原因（"without a verified receipt: …"），达到上限后死信（可恢复的 `failed` 状态）。`last_error` 现在能区分"API 拒绝"（4xx 永久）与"已接受但未确认"（2xx 未验证、可重试）。
- **receipt 读取上限从 4096 字节提升到 64 KB**：大但合法的 `event_id` 不再因回显截断被误判为解码失败；超限响应仍按可重试处理，绝不把已入账的事件误死信。
- **投递不再跟随重定向**（3xx 一律可重试，审计请求体不会转发到重定向目标）。
- 升级提示：已 delivered 的历史行保留旧伪造值（`api_status='accepted'`），不在自动对账范围；如需一次性清扫请走死信/回放方向。

**写给开发（实现变化）：**

- `internal/outbox/relay.go`：`DeliverFunc` 签名改为 `func(context.Context, domain.Event) (*domain.EventReceipt, error)`；`RunOnce` 消费 receipt，`succeed` 只持久化 receipt 派生字段（`receipt == nil` 时两列留空 → SQL NULL）。
- `internal/outbox/http.go`：`HTTPDeliverer` 解析 2xx 为 `ReceiptResponse` 信封并调用独立函数 `verifyReceipt`（V1–V4）验证；401 改可重试；`maxReceiptBytes = 64KB`；`CheckRedirect = http.ErrUseLastResponse`。
- `internal/kafka/kafka.go`：`Producer.Deliver` 成功返回 `(nil, nil)`（Kafka 无 receipt，不伪造）。
- `cmd/audit-kafka-consumer/main.go`：deliverer 提升到消息循环外构建一次（复用同一 HTTP client/连接池）；`cmd/audit-kafka-dlq-replay/main.go` 适配新签名；`cmd/audit-outbox-relay/main.go` HTTP 模式空 token 时启动告警日志。
- 构建门禁补齐 `audit-kafka-dlq-replay`（`checks/build.py`、`cli.py`，与 `engineering.yaml` 六个二进制对齐）。
- 测试（`internal/outbox/relay_test.go`、`postgres_test.go`）：AC-1..AC-5 全部落地（含真实 API E2E 与重复投递、Postgres 变体 DSN 门控），并覆盖评审补测：空/截断 2xx body 可重试、V1–V4 全分支表测试、succeed-Update 失败保持 pending、401 重试回归钉、大 event_id 边界、重定向不跟随、双实例乐观更新单次落账。
- `python3 cli.py quality` 全绿。


## 2026-08-12 — DLQ 回放状态改为追加式日志 + flock 串行化（ReplayState.Mark 跨实例竞态与 O(n²) 重写修复）

**写给运营（行为变化）：**

- **`AUDIT_DLQ_REPLAY_STATE` 文件格式变为 JSONL 追加日志**：首行 `{"format":"replay-log-v1"}`，之后每行一个 `{"event_id":"<id>"}` 标记；旧版单对象 `{"replayed":{...}}` 文件继续正常加载，并在下一次 `Mark` 时就地转换为新格式（零数据迁移、惰性逐文件转换）。常驻守护进程与 cron `-once` 现在通过 `<state>.lock` 兄弟文件的排他 advisory flock 串行写入，丢失标记（last-writer-wins）与共享 `.tmp` 临时 inode 相互撕裂这两类跨进程竞态被构造性消除；每个标记的落盘成本为 O(1)（不再每标记重写全量集合）。
- **升级窗口**：同一部署的写方（守护进程 + cron `-once`）应在同一窗口升级——旧二进制仍用确定性 `path+".tmp"` + rename 覆盖目标，会覆盖新格式行（旧代码与自己也会竞态，修复无法防御未修复的同伴）。旧文件转换无需人工操作。
- **状态文件与锁必须位于本地 POSIX 文件系统**（默认 `./data/` 满足）：flock 与 `O_APPEND` 的原子性在网络文件系统（NFS/SMB）上无保证；撕裂尾部在加载时被跳过、写入前自愈，残余风险有界。
- **故障语义不变**：加载失败（未知格式、真正损坏）仍 fail-loud 引导退出；`Mark` 失败本轮中止、DLQ 偏移不提交、记录留待下轮重试；崩溃窗口任意位置的文件均可解码（旧状态或新状态）。**新增一条更强的不变式**：`Mark` 仅在持久化成功后更新内存集合——DLQ 偏移永远不会在没有对应持久标记的情况下被提交（记录要么已落账，要么下轮重试）。
- **上线后验证**：`jq -s '[.[] | .event_id? // empty] | unique | length' <state>` 应等于全部已回放 event_id 的并集大小；状态目录无 `*.tmp` 残留；两个进程日志均无启动 `Fatalf`。

**写给开发（实现变化）：**

- `internal/kafka/replay.go`：`ReplayState` 新增未导出 `mu`（REQ-8 进程内互斥）、`newTemp`（唯一临时文件工厂，默认 `os.CreateTemp`，AC-2）、`lockWait`（flock 有界等待，默认 2s，防止存活的卡死同伴无限阻塞恢复）。`Mark` 在 flock 临界区内分类目标（缺失/空/日志/撕裂/legacy）后：日志文件走 `O_APPEND` 追加 + 文件 fsync（无 rename、无目录同步）；首写/撕裂修复/legacy 转换走「唯一临时文件 → 写 → 内容 fsync → rename → 父目录 fsync」（REQ-4 顺序逐字保留）。追加前 `repairTornTail` 自愈撕裂尾部（O(1) 窗口探测，罕见时全量重建），保证崩溃撕裂的尾部永远不会变成损坏的非末行。
- `LoadReplayState` 规则：缺失/零长/空白 → 空集（钉住不变）；首行恰为 v1 头 → 逐行扫描（撕裂末行跳过、损坏非末行包装解码错误 fail-loud）；其余 → legacy 单对象路径逐字节不变。格式判别为 JSON 感知（protocol F-1）：带 `format` 字段但版本未知或头部畸形的内容 fail-loud（`unknown replay state format` / `invalid replay state header`），绝不静默解码为空 legacy 状态。
- **REQ-7 修正（安全评审 F1）**：内存集合只在持久化成功后更新；fsync-after-append 失败后磁盘为可解码超集、内存不含该标记，下轮重读记录重试（至少一次，被幂等吸收）。
- 测试（`internal/kafka/replay_test.go`）：AC-1 并发风暴并集（8 写方 × 50 ID + 共享核心，独立实例 + 共享实例 `-race` 清洁）、AC-2 唯一临时文件/无残留/崩溃残留/撕裂尾部自愈、AC-3 O(M+K) 字节界与 `AllocsPerRun` 增长无关性 + `BenchmarkReplayStateMark`（M∈{100,1k,10k} 平坦：~8µs/op、22 allocs/op）、AC-4 双进程集成（helper-process 模式，3 子进程模拟 daemon+`-once`）、REQ-1 锁失败（ENOTDIR 与有界超时）；五个持久化钉（:240/:319/:394/:534/:679）按新写入路径适配，:214/:729 零适配保持全绿。
- `python3 cli.py quality` 全绿。

## 2026-08-12 — 读路径自审计闭环补齐：`GET /api/v1/operations/{operationID}` 追加 audit.event.read 事实

**写给运营（行为变化，观察类）：**

- **最后一个此前不落账的事件内容读端点现在记录 `audit.event.read` 事实**：
  `GET /api/v1/operations/{operationID}`（操作摘要）返回 200 前在服务层追加
  一条自审计事实，编码 `(operation, id, summary)`，携带调用者 subject
  （`actor`）。2026-08-11 的「五个读端点闭环」随之变为**六个**，读路径自审计
  闭环（“谁读过什么”）对事件内容读全部成立；`GET /api/v1/admin/actions`
  现在也能回答“谁读过这个操作摘要”。
- **失败关闭语义与既有读一致**：事实追加失败（存储不可写、乐观锁冲突耗尽）
  时返回 500/503，**不返回读结果**；操作不存在/非法请求（404/400）不落账。
- **历史读不回溯**：本变更前的摘要读未留事实，不补写（追加只向前，与
  2026-08-11 批次发布方式一致）。
- **明确不审计的读路径保持不变**：`POST /api/v1/restores/preview` 与
  `POST /api/v1/restores` 按 F1 设计门显式拒绝仍为零 `audit.event.read`
  （仅 `restore.created` 等既有事实）；`GET /api/v1/exports/{jobID}`（导出
  任务状态，含事件内容）继续不落读账，由行为固定测试钉住。

**写给开发（API 契约变化）：**

- `Service.Operation` 签名增加 `actor string`（位于 `tenantID` 之后，与
  2026-08-11 六方法的约定一致）：`Operation(tenantID, actor, operationID)`；
  空 `actor` 不落账（内部调用者静默，`recordReadAction` 约定不变）。
- `Operation` 保持自有 `eventsFor` 扫描（不委托 `OperationTimeline`，避免
  双重追加）；追加点位于摘要构建成功之后、返回之前，早退路径
  （`ErrInvalid`/`ErrNotFound`/读取错误）不落账。
- `getOperation` 处理器转发 `claims.Subject`；HTTP 路由/权限
  （`audit:operation:read`）/200 响应体/错误码逐字不变（仅新增 trail 行）。
- 调用点共 2 处（`internal/httpapi/server.go:getOperation`、
  `internal/service/read_selfaudit_test.go` 测试），无 gRPC 暴露。
- 回归测试：`TestHTTPReadEndpointsAppendSelfAuditFacts` 六路由清单与
  `performReadEndpoints` 同步扩为六条；新增
  `TestHTTPOperationSummaryAppendsExactlyOneFact`（单请求恰好一条事实）、
  `TestHTTPOperationSummaryNotFoundAppendsNothing`（404 两腿零行）、
  `TestReadSelfAuditOperationInvalidAppendsNothing`（空 id → ErrInvalid 零行）；
  作用域钉 `TestReadSelfAuditOutOfScopeReadsRecordNothing` 反转：`Operation`
  移出未审计集（1→2 条事实期望），`GetExport` 仍钉住零自审计。
- `python3 cli.py quality` 全绿。

## 2026-08-12 — DLQ 回放状态 `Mark` 落盘增加 fsync（internal/kafka 持久化加固）

**写给运营（行为变化）：**

- **`audit-kafka-dlq-replay` 的幂等账本落盘语义增强**：`ReplayState.Mark` 此前只做
  临时文件写入 + rename，从不 fsync——断电可能留下零字节/撕裂的状态文件，导致启动时
  `LoadReplayState` 解码失败、命令在引导阶段退出（`state: …` Fatalf），DLQ 恢复离线直至
  人工修复；或 rename 回退，下一轮 accepted 主题重扫重复重发已回放的事件。现在 `Mark` 按
  「写临时文件 → fsync 临时文件内容 → rename → fsync 父目录」的顺序落盘：`Mark` 返回
  `nil` 即代表已回放决策对断电持久，撕裂/零字节目标文件不会再被新写入产生。
- **残留窗口与失败语义**：rename 与父目录 fsync 之间的断电最多让 rename 回退一次，下一轮
  至多重复重发该事件一次（至少一次语义，被 audit API 的 `event_id` 幂等吸收）；`Mark`
  失败时本轮中止、DLQ 偏移不提交，记录留待下轮重试。受影响范围：仅
  `AUDIT_DLQ_REPLAY_STATE`/`-state`（默认 `./data/dlq-replay-state.json`）文件路径。
  修复前已撕裂的旧状态文件仍需人工删除/修复（解码失败保持 fail-loud，行为不变）。回滚：
  仅需部署旧二进制（单文件 revert），无状态迁移、无配置/环境变量变化。

**写给开发（实现变化）：**

- `internal/kafka/replay.go`：`ReplayState` 新增未导出 `syncFile`/`syncDir` 钩子（nil →
  `fsutil.SyncFile`/`fsutil.SyncDir`，镜像 `archive.FileStore.syncDir` /
  `store.fileBackend.syncDir`）；`Mark` 改为 `os.WriteFile(temp, 0o600)` → `syncFile(temp)`
  → `os.Rename` → `syncDir(filepath.Dir(path))`。rename 前的失败（写/文件同步/rename）尽力
  删除 `.tmp`、返回包装错误、旧状态保持权威；目录同步失败返回错误但保留新目标内容（D4：
  内存 map 已含新标记、新旧内容均可解码，下一轮 `Mark` 重试持久化）。钩子字段不被
  `encoding/json` 序列化，磁盘字节逐位不变（零迁移、混合版本滚动重启安全）。
- 新增回归测试（`internal/kafka/replay_test.go`，6 个函数）：
  `TestReplayStateMarkSyncsFileBeforeRenameAndDirAfterRename`（AC-1 顺序证明 + 生产默认钩子）、
  `TestLoadReplayStateToleratesCrashLeftovers`（AC-2：陈旧 `.tmp`/零长/空白/缺失）、
  `TestReplayStateMarkSurvivesSimulatedCrash`（AC-3：三个崩溃点 + 撕裂字节非空对照）、
  `TestReplayStateMarkErrorPathsCleanTempAndPreserveTarget`（FM-1/2/3 错误路径、无 `.tmp` 残留）、
  `TestReplayStateOnDiskFormatIsStable`（golden bytes + `0o600`）、
  `TestReplayCrashAfterMarkRebootDoesNotRepublish`（崩溃重启幂等：持久标记后 0 次重发、
  rename 回退时至多重发一次）。
- `python3 cli.py quality` 全绿。

## 2026-08-12 — 文件快照 `Save` 在 rename 后 fsync 父目录链（internal/store）

**写给运营（行为变化）：**

- **无 Postgres 时（文件快照即控制面账本）的落盘语义增强**：`fileBackend.Save` 此前在
  `os.Rename` 成功后只 fsync 了临时文件内容，从未 fsync 父目录——按 POSIX 持久性语义，
  rename 在父目录条目同步前不保证持久，断电可能把已提交的账本（事件、哈希链头、聚合
  检查点、法律保留、导出、管理操作）回滚或丢失，破坏哈希链连续性（即归档 M-12 窗口在
  快照路径上的同款缺陷）。现在 `Save` 返回 `nil` 意味着：rename 后父目录链已逐叶到根
  fsync，快照条目对断电持久。
- **失败语义不变且更严**：目录链 fsync 失败时 `Save` 返回错误（`sync directory chain
  for <path>: …`，`errors.Is` 可达），此前已持久化的快照在内存中继续作为权威，并尽力
  把旧快照写回目标路径（"no rename visible"）；目标路径绝不会出现撕裂快照，也不会残留
  `.tmp`。
- 受影响范围：仅 `AUDIT_STATE_PATH`/`-state` 文件后端路径。Postgres 后端（自己处理
  提交持久性）与内存模式（空路径）逐位不变。代价：每次 `Save` 多一次目录 open+fsync
  （新建中间目录时最多多两次），与控制面提交路径的既有写代价同级。回滚：仅需部署旧
  二进制（单文件 revert），无状态迁移、无配置/环境变量变化。

**写给开发（实现变化）：**

- `internal/store/store.go`：`fileBackend` 新增未导出 `syncDir` 钩子（nil →
  `fsutil.SyncDir`，镜像 `archive.FileStore.syncDir`）；`Save` 在 `MkdirAll` **之前**用新
  私有助手 `deepestExistingAncestor` 探测链根（快照无配置根目录，链终止于最深的既有
  祖先；Stat 错误只可能过度同步、绝不欠同步），rename 后调用
  `fsutil.SyncDirChain(root, parent, syncDir)`（叶到根），失败时先 `restoreSnapshot()`
  （写→sync→close→rename 同序，全部错误吞掉，旧快照保持权威）再返回包装错误；
  `f.data = data` 仅在链同步成功后执行。`Backend` 契约注释更新为：返回 `nil` 额外蕴含
  rename 条目的父目录链已 fsync（持久）。新增 `store → fsutil` 依赖边（fsutil 仅 stdlib，
  无环）；`internal/fsutil` 与 `internal/archive` 未改动。
- 新增回归测试（`internal/store/store_test.go`）：`syncRecorder` 钩子（记录同步顺序/
  故障注入/failN/sentinel）与 `crashDrop` 断电模型；`TestFileBackendSaveSyncsParentDirectoryChain`
  （AC-1：既有父目录坍缩为一次同步、新建嵌套目录叶到根、二次 Save 坍缩、钩子内
  `os.Stat(path)` 证明同步在 rename 之后且目标内容为提交快照）、
  `TestFileBackendSaveDirSyncFailureKeepsPreviousAuthoritative`（AC-2：`errors.Is` 可达、
  内存权威 + 全新 `Open` 读到旧快照、无 `.tmp`、同步确被尝试）、
  `TestFileBackendSaveSurvivesSimulatedCrash`（AC-3：全链存活 / 修复前无同步断电丢失
  账本 / 仅根同步仍丢失——三者均非空）。
- `python3 cli.py quality` 全绿（gofmt/vet/单测/race/全部生产二进制构建）；负向对照：
  AC-3 的"修复前无同步"与"仅根同步"子用例在未修复代码上丢账本，证明模型非空。

## 2026-08-12 — Dev 令牌 `platform` subject 不再隐含 Platform 权限（internal/auth）

**写给运营（行为变化）：**

- **`dev:platform:<非 platform-admin 角色>` 令牌降权（fail-closed）**：此前 `parseDevToken` 把
  subject/租户名 `platform` 与 `platform-admin` 角色并列设为 `Platform`，导致 `dev:platform:auditor`
  （角色只有只读 `auditor`）被当作 Platform 主体——`POST /api/v1/tenants` 可创建租户（201），且任何
  读路径的 `?tenant_id=` 覆盖都会生效（跨租户读取任意租户审计数据）。现在 Platform 只由
  `platform-admin` 角色（或其映射的唯一权限 `audit:platform:cross_tenant`）授予，与 JWT 信任路径
  完全一致；`dev:platform:auditor` 等令牌保持租户上下文（`TenantID="platform"`）但不再拥有任何
  Platform 能力，创建/列举租户返回 403，`?tenant_id=` 覆盖被忽略。
- **受影响范围**：仅 `dev:platform:<非 platform-admin 角色>` 形式（含 4 段 service 形式）行为变化；
  `dev:platform:platform-admin` 与所有其他 dev/JWT 形式逐位不变。dev auth 仍受
  `AUDIT_ALLOW_DEV_AUTH=true` 白名单门禁（无生产暴露）。回滚只需部署旧二进制（单行 revert，无状态/配置变化）。

**写给开发（实现变化）：**

- `internal/auth/auth.go`：`parseDevToken` 删除 `parts[1] == "platform"` 分支，`claims.Platform` 改为在
  `claims.Permissions = permissionsForRoles(roles)` 之后用与 `parseJWT` **逐字节相同**的表达式推导
  （`Permissions["audit:platform:cross_tenant"] || contains(roles, "platform-admin")`）；两条信任路径
  结构性对齐，`Claims.Allows`/`require`/`tenantFor`/`createTenant`/JWT 解析均未改动。
- 新增回归测试：`internal/auth/auth_test.go` 的 `TestDevTokenPlatformRequiresPlatformAdminRole`（subject ×
  role 矩阵，Platform == (role == platform-admin)，含 4 段 service 形式）与
  `TestJWTPlatformTenantWithAuditorRoleIsNotPlatform`（AC-3 JWT 对等 pin）；`internal/httpapi/server_test.go`
  的 `TestDevAuditorCannotCreateTenant`（403 + 不落盘）、`TestDevAuditorTenantOverrideIgnored`（
  `?tenant_id=` 覆盖 404/404/200 对比）、`TestDevPlatformComplianceExportScopedToOwnTenant`（导出任务
  落在自身租户）、`TestDevPlatformTenantAdminSourceStampedToOwnTenant`（source 租户盖印到自身租户）、
  `TestDevPlatformAuditorSourceTamperRejected`（跨租户篡改 403 + 受害列表逐字节不变）。
- `python3 cli.py quality` 全绿（gofmt/vet/单测/race/全部生产二进制构建）；改动前负向对照：上述新测试在
  未修复代码上均失败，证明其捕获真实漏洞。

## 2026-08-12 — S3 归档 Put 对 Stat 失败改为 fail-closed，并新增写后校验（internal/archive）

**写给运营（行为变化）：**

- **S3 瞬时故障不再盲写覆盖**：`S3Store.Put` 此前把 `StatObject` 的**任何**失败都当作“对象不存在”并
  直接 `PutObject`——限流（`SlowDown`）、鉴权失败、网络错误、上下文取消都会被当作 key 缺失，静默覆盖
  该 key 上已存在的对象（若已被篡改/损坏，则被无声替换且收据被标记为 archived），与 `FileStore` 的
  fail-closed `Lstat` 行为背离。现在只有 minio 的 `NoSuchKey`（HEAD 404 的类型化错误）才算“确实不存在”；
  其余 stat 错误一律中止归档（`s3 stat <key>: …`），**绝不调用 `PutObject`**。S3 不稳定时错误与重试会
  变多，但永远不会再出现静默覆盖——这是本修复的预期行为，不是回归。
- **新版本首次写入后强制读回校验**：对不存在 key 的写入在返回成功前会 `GetObject` 读回并逐字节比对；
  若并发写入者在 stat→put 窗口内抢先落盘了不同内容，或读回失败，`Put` 返回响亮错误（
  `archive object <key> cannot be verified after write: …` 或既有的 `already exists with different content`），
  对象保持不动（WORM），收据不会标记为 archived，后续字节一致的重试会经既有对象路径收敛。
- 回滚：仅需部署旧二进制（单文件 revert），无状态迁移、无配置/环境变量变化。

**写给开发（实现变化）：**

- `internal/archive/archive.go`：`S3Store.Put` 重写为 fail-closed stat 分支（
  `minio.ToErrorResponse(err).Code != minio.NoSuchKey ⇒ 中止`；`ToErrorResponse` 是类型 switch，被包装的
  `NoSuchKey` 也判为失败，偏向 fail-closed 永不 fail-open），新增未导出 `verifyAfterWrite` 与共享有界读回
  `readBack`（唯一 `len(data)+1` 上限站点，`verifyExistingObject` 改为委托它）。无公开 API 变化；
  minio-go 仍钉在 v7.2.1（`NoSuchKey`/`ToErrorResponse` 均已验证存在）。
- `internal/archive/archive_s3_test.go`：`fakeS3Client` 升级——全状态加互斥锁、缺失 key 返回类型化
  `NoSuchKey`（原为裸 `errors.New`）、`statErrs` 脚本队列、`putHook` 并发写入模拟、`putCalls/putData/gets`
  调用与字节记录；新增 AC-1..AC-4 对应测试（stat 失败 fail-closed 且 `PutObject` 零调用、NoSuchKey 快乐
  路径、写后校验三态、`-race` 下“nil ⇒ verify GET 曾返回调用方载荷”并发不变量）。
- `python3 cli.py quality` 全绿（gofmt/vet/单测/race/全部六个生产二进制构建）。
## 2026-08-12 — 投影死信接入 DLQ 发布 + 可配置最大尝试次数（audit-projector）

**写给运营（行为变化）：**

- **投影失败不再静默丢失**：`audit-projector` 此前用零 `ConsumerOption` 构建消费者，死信路径退化为
  commit + 日志（`internal/kafka/kafka.go` 的 `else if c.dlq != nil` 守卫被跳过），ClickHouse 瞬时故障
  重试耗尽后不产生任何 `Failure` 记录，`audit-kafka-dlq-replay` 无从恢复——违反架构计划 §6.4
  （“投影失败不回滚账本，通过重试和 DLQ 修复”）。现在消费者的**默认行为**是：超过 `-max-attempts`
  （默认 8，与消费者端默认上限一致）的投影失败在提交前发布 `attempts_exhausted` Failure 到
  `audit.events.dlq.v1`（AsyncAPI 三字段载荷），恢复 §6.4 恢复路径。
- **新增两个配置项**（与 `audit-kafka-consumer` 完全一致的命名/默认/优先级）：`-max-attempts`
  （env `AUDIT_KAFKA_MAX_ATTEMPTS`，默认 8，非数字 env 回退 8，`≤0` 启动即致命退出）与 `-dlq-topic`
  （env `AUDIT_KAFKA_DLQ_TOPIC`，默认 `audit.events.dlq.v1`；显式 `-dlq-topic ""` 可禁用发布器，恢复
  原来的 commit+log 降级）。首行启动日志现在额外输出 `max_attempts=` 与 `dlq_topic=`。
- **恢复路径的运维要求（重要）**：投影死信的事件位于 `audit.events.ledgered.v1`，因此运行
  `audit-kafka-dlq-replay` 恢复投影失败时必须指定 `-accepted-topic audit.events.ledgered.v1`
  （env `AUDIT_KAFKA_ACCEPTED_TOPIC`），否则默认按 accepted topic 查找会找不到原事件。按字节重发布
  保持哈希链完整。
- **DLQ 不可达不阻塞**：发布失败自动降级为 commit+log，分区进度永不阻塞；`-dlq-topic ""` 等价于
  旧版本行为。回滚只需部署旧二进制（零选项 = 原 commit+log 行为），无需兼容开关。

**写给开发（实现变化）：**

- `cmd/audit-projector/main.go`：新增 `-max-attempts`/`-dlq-topic` flag（`intEnv`/`envOr` 回退，新增
  `strconv` 导入与 `intEnv` helper，与兄弟二进制逐字一致）；`*backoff <= 0 || *maxAttempts <= 0`
  启动致命校验（REQ-5，早于任何构造）；消费者以 `[]kafka.ConsumerOption{kafka.WithMaxAttempts(...)}`
  构建，`-dlq-topic` 非空时 `kafka.NewProducer(strings.Split(*brokers, ","), *dlqTopic)` + `defer Close`
  + `kafka.WithDLQ`（REQ-1/REQ-2）；首行日志扩展为
  `brokers= topic= group= clickhouse= max_attempts= dlq_topic=` 且仍在任何阻塞调用之前输出（REQ-6）。
  仅 stdlib 新增依赖，无 `internal/kafka`/`internal/projection`/consumer/replayer 改动。
- `cmd/audit-projector/main_test.go`：新增 4 个二进制测试 `TestResolvedDefaultDLQConfig`（默认
  `max_attempts=8 dlq_topic=audit.events.dlq.v1`）、`TestDLQConfigFlagOverrides`（flag 覆盖）、
  `TestDLQConfigEnvOverrides`（env 覆盖）、`TestMaxAttemptsNonPositiveRejected`（`-max-attempts 0`
  非零退出且报错提及 max-attempts）。行为契约（permanent_error 先发布后提交、attempts_exhausted
  恰好 `maxAttempts` 次、发布失败降级）由 `internal/kafka/kafka_test.go` 既有 fake-reader 测试原样覆盖。
- `python3 cli.py quality` 全绿（gofmt/vet/单测/race/构建，含 `audit-projector` 二进制）。

## 2026-08-12 — 单个损坏 audit_outbox payload 不再永久卡死 relay（audit-outbox-relay）

**写给运营（行为变化）：**

- **修复一个永久性卡死缺陷**：之前只要 `audit_outbox` 里有一条 payload 无法解码成事件（例如
  `'[]'::jsonb`、`'"text"'::jsonb`、`'42'::jsonb`——jsonb 列保证语法合法，但结构上无法反序列化为
  `domain.Event`），`ListPending` 就会对整个批次返回 `decode outbox payload id=<id>: …` 错误，
  `RunOnce` 返回 0 条处理，`main` 每轮重试同样失败——该行永远保持 `status='pending'`（不触发任何
  UPDATE），因此**所有租户的事件投递无限期停摆**，只能靠人工 SQL 清理。
- **现在解码失败的坏行会被自动隔离（dead-letter）**：`ListPending` 跳过坏行继续扫描并把它们报告给
  relay；relay 在投递任何有效记录之前先把坏行置为 `status='failed'`（`attempts+1`，`last_error` 写入
  原始 `decode outbox payload id=<id>: …` 错误文本），然后照常投递本批次其余有效记录。坏行一旦
  `failed` 便不再被 `WHERE status='pending'` 选中，卡死被永久打破——**无需人工 SQL 清理**，新版本首个
  poll 即自动隔离存量卡死行（可用 `-once` 立即冲刷）。
- **隔离是尽力而为**：若隔离 UPDATE 失败（数据库瞬时故障），仅记录日志，坏行保持 `pending` 下轮
  重报，有效记录照常投递；并发 relay 实例通过乐观锁 `WHERE status='pending'` 幂等去重。隔离绕过
  重试/`MaxAttempts` 分类（解码损坏是永久不可投递，重试无意义）。
- **已知限制（REQ-7，有意不改）**：payload 为 `null` 或 `{}` 时 `json.Decode` 成功得到一个零值事件，
  不会被隔离，仍走原投递路径（这是另一个缺陷，见工程文档，不在本次范围内）。
- 观测签名：`run: decode outbox payload …` 日志不再出现；替代为 `corrupt payload quarantined` 日志与
  `status='failed'` 行（`idx_audit_outbox_failed` 已为恢复工具预建索引）。回滚只需部署旧二进制；
  回滚窗口内坏行会再次卡死投递，重新部署新版本后自动恢复。

**写给开发（实现变化）：**

- `internal/outbox/relay.go`：新增导出类型 `CorruptRecord{ID, Attempts, Err}`；`Store.ListPending` 签名
  改为 `([]Record, []CorruptRecord, error)`（编译期强制更新全部实现者/调用点：`PostgresStore`、测试
  double、relay 与 3 处测试调用）；`RunOnce` 先隔离坏行再投递有效记录，`handled = len(records) +
  len(corrupt)`（与既有“handled (成功或失败)”契约一致）；新增私有方法 `quarantine`，直接以
  `StatusFailed` 隔离、绕过 `r.fail` 的重试分类，Update 失败/返回 false 仅记日志不阻断。
- `internal/outbox/postgres.go`：`ListPending` 保持只读，扫描时收集坏行到 `CorruptRecord` 报告并继续
  下一行；复用共享解码助手 `decodeEvent`（`sdk.go`，保留 `UseNumber` 保证摘要一致），错误文本
  `decode outbox payload id=%d: %w` 逐字节不变；查询级/行扫描错误仍照旧中止批次（不在范围内）。
- `internal/outbox/sdk.go`：`decodeEvent` 文档注释同步。`cmd/audit-outbox-relay/main.go` 无改动
  （REQ-6，runOnce 契约不变，只是不再收到卡死错误）。
- 测试：`relay_test.go` 新增 T1 `TestRelayRunOnceQuarantinesCorruptAndDeliversValid`、T2
  `TestRelayRunOnceQuarantinesOnlyCorruptBatch`、T3 `TestRelayRunOnceQuarantineFailureDoesNotBlockDelivery`、
  T5 `TestRelayRunOnceProgressAfterQuarantine`（+ 混合到期行进度测试），`fakeStore` 新增 `corrupt`/
  `updateErr` 映射与 `withCorrupt`/`withUpdateErr` 助手；`postgres_test.go` 新增 DSN 门控的 T4/T6
  `TestPostgresStoreCorruptPayloadWedgeBreak`（种子用 `'[]'::jsonb`，禁用 `null`/`{}`），既有 3 处
  `ListPending` 调用点补 `corrupt` 断言。`python3 cli.py quality` 全绿（gofmt/vet/单测/race/构建）。

## 2026-08-12 — 修复 DLQ 中批记录被后续提交跳跃丢失 + 每轮重建 DLQ reader（audit-kafka-dlq-replay）

**写给运营（行为变化）：**

- **修复一个静默数据丢失缺陷（F1，同模块既有缺陷）**：`audit-kafka-dlq-replay` 之前按记录逐条提交
  DLQ 偏移，而 Kafka 每个（group, partition）只保存一个已提交偏移，kafka-go `CommitMessages` 提交的是
  传入消息中最高偏移 +1（`offsetStash.merge` 取分区内最大值）——因此同一分区里，一条瞬时发布失败的
  DLQ 记录（保持未提交、待下轮重试）会被后续成功记录的一次提交“跳过”：分区已提交偏移越过它，下一轮
  从新偏移继续，这条记录永不再投递（无状态标记、无日志、无恢复路径）。同时，kafka-go 的 group reader
  在一次会话内绝不重投已取消息（`FetchMessage` 每次都推进本地位置，与提交无关），所以常驻模式里
  瞬时失败记录即使未提交，下一轮（同一 reader 会话）也不会重新读到——文档承诺的“瞬态失败留待下轮
  重试”在常驻模式下实际不成立。
- **现在每轮开始时重建 DLQ reader（同一 `-dlq` group）**：新会话从 broker 已提交偏移开始拉取，因此
  未提交的瞬时失败记录会被重新读到并重试（与 `-once` 模式语义一致，常驻 ≡ 一次性）；同时
  **提交加了分区级屏障**：已提交偏移永远不会越过本分区第一条仍未解决（pending）的记录，已解决但位于
  pending 之后的记录留待下轮重新提交（状态文件保证不重复重发布）。修复后“瞬态失败留待下轮重试”
  真正成立。无配置/无新 flag/无状态文件格式变化。
- 已受影响数据：修复前因中批跳跃而被永久跳过的 DLQ 记录无法自动恢复（其事件未进入状态文件，也
  没有 unresolvable 日志）；如需人工恢复，在 DLQ topic 中按失败 event_id 查找对应记录并检查其原始
  事件是否在 accepted topic 保留期内，手动重发布即可。回滚部署旧二进制即可；回滚窗口内缺陷复现。

**写给开发（实现变化）：**

- `internal/kafka/replay.go`：`Replayer` 新增 `dlqNew func() messageReader` 工厂；`RunOnce` 在
  `collectFailures` 前调用新的 `resetDLQToCommittedOffset()`（关闭旧 DLQ reader、以相同配置新建），
  使每轮从 broker 已提交偏移开始拉取；`commitResolved` 增加分区级提交屏障（先求每分区
  `firstPending` = 第一条 `wanted && !resolved` 记录的偏移，只提交偏移低于它的已解决记录），防止
  提交越过 pending 记录；`pending` 指标改为仅在扫描进行中置位，两个 reset 失败中止轮次都不会泄漏
  陈旧读数。文档注释同步修正（REQ-2）。
- `internal/kafka/replay_test.go`：新增 broker 语义测试缝 `brokerDLQ`/`brokerReplayReader`（提交推进
  分区已提交偏移、新会话只投递已提交偏移之上的记录）；新增 `TestReplayMidBatchPendingRecordIsNotSkipped`
  （中批 pending 不被跳跃：第 1 轮 offset 1 失败 + offset 3 成功时屏障保证 0 提交，第 2 轮重读并收敛，
  提交偏移最终推进到 4）与 `TestReplayResetFailureAbortsRound`（两个 reset 的 Close 失败中止轮次、零
  重发布/零标记/零提交/`pending==0`）；`TestReplayTransientRepublishFailureRetriesNextRound` 改用工厂
  缝并修正注释（重投靠的是每轮重建，而非“偏移不推进”）。修复前两项突变（去掉屏障、去掉 DLQ 重建）
  均使新测试失败，修复后通过；`go test -race` 干净。

## 2026-08-12 — 常驻 DLQ 回放每轮重建 accepted reader，从首个保留偏移重新扫描（audit-kafka-dlq-replay）

**写给运营（行为变化）：**

- **修复一个静默数据丢失缺陷**：常驻（daemon）模式的 `audit-kafka-dlq-replay` 之前复用同一个
  accepted reader 跨轮扫描，kafka-go 只在 reader 创建时应用 `StartOffset: FirstOffset`，`FetchMessage`
  每次调用都会推进本地取数位置（与提交无关），因此第 N+1 轮从第 N 轮结束的位置继续扫描——引用
  早于第 1 轮扫描终点 accepted 原消息的旧 DLQ 事件会被 `unresolvable` 标记并从状态文件/ DLQ 提交
  中永久丢弃（DLQ 偏移被提交，失败证据只剩状态文件里的标记）。`-once` 模式因每轮新建 reader 而不受影响。
- **现在每轮扫描前重建 accepted reader（同一 `-accepted` group、`StartOffset: FirstOffset`、从不提交）**，
  保证每轮都从首个保留偏移开始扫描；旧事件只要仍在保留期内即可恢复并重新发布。DLQ reader 的每轮
  重建与提交屏障见同日下一条“修复 DLQ 中批记录被后续提交跳跃”条目。无配置/无新 flag。
- 已受影响数据：修复前被误标 unresolvable 且 DLQ 偏移已提交的事件无法自动恢复；如需人工恢复，
  从状态文件中提取修复前窗口内的事件 ID，在 accepted topic 中按 event_id 查找原消息后重新发布
  （原消息仍在保留期内）。回滚只需部署旧二进制；回滚窗口内缺陷会复现。

**写给开发（实现变化）：**

- `internal/kafka/replay.go`：`Replayer` 新增 `acceptedNew func() messageReader` 工厂字段（生产路径
  用与 `NewReplayer` 相同的 `kafka.ReaderConfig` 创建）；`RunOnce` 在 `scanAccepted` 前调用新的
  `resetAcceptedToFirstOffset()`（REQ-1），关闭旧 accepted reader 并以相同配置新建（kafka-go 对
  group reader 的 `SetOffset` 返回 `errNotAvailableWithGroup`，重建是唯一 seek 机制）。
  无 wanted 事件的轮次提前返回、不重建不扫描。文档注释改为说明机制（REQ-2）。
- `internal/kafka/replay_test.go`：新增 T1/T2/T3（两个 `RunOnce` 轮次的旧事件恢复、第 2 轮重取第 1 轮
  已消费消息、daemon ≡ `-once` 结果等价——断言重发布/状态/DLQ 提交结果而非裸计数，因 unresolvable
  标记会虚增 `replayed` 计数）；`fakeReplayReader` 增加可选 fetchLog，新增 fresh-factory 测试缝。
- `checks/replay_round_reset.py`（新 grep 门，接入 `cli.py` `cmd_quality`）：`RunOnce` 必须在
  `scanAccepted` 之前包含 `resetAcceptedToFirstOffset`/`seekAcceptedToStart` 调用，结构性防止回归；
  `checks/test_replay_round_reset.py` 覆盖缺省/顺序/解析失败/当前树。修复前 T1–T3 与门均失败，修复后通过。

## 2026-08-12 — 消费端拒绝单值外/尾随垃圾 Kafka 消息（audit-kafka-consumer）

**写给运营（行为变化）：**

- **一条消息里包含两个 JSON 对象、或合法 JSON 后跟非空白垃圾，整条消息现在按
  `unparsable_message` 死信，不再静默只摄取第一个值**：此前 `Consumer.Run` 只解码
  一次，双值消息的第一个事件会被当作正常事件摄取并提交，第二个事件被静默丢弃
  （无任何死信证据）；尾随垃圾消息也会被当作正常事件摄取。现在第二段解码非 `EOF`
  即视为信封非法：`audit_consumer_dead_lettered_total` /
  `audit_consumer_dlq_published_total` 计数各 +1，DLQ 记录 `error_code=`
  `unparsable_message`（而非此前可到达的错误归类 `permanent_error`），`event_id`
  取消息 key（payload 探针在多值/尾随垃圾上探测不到 event_id）。消息仍然**总是提交**，
  分区进度不被阻塞；DLQ 未挂载或发布失败时降级为提交+日志（既有契约）。
- **合法消息零行为变化**：单值消息（含尾随空白/换行）仍走 解码→摄取→重试/退避→
  提交 路径，摄取失败的死信码仍为 `permanent_error` / `attempts_exhausted`。
- 回滚只需部署旧 `audit-kafka-consumer` 二进制；已死信消息留在 DLQ 可回放，已摄取
  事实不丢失也不重复（修复从不单独摄取非法首值）。旧二进制的静默部分摄取行为会
  在回滚窗口内复现，需尽快重新发布修复。

**写给开发（实现变化）：**

- `internal/kafka/kafka.go`：`Consumer.Run` 解码阶段在第一段 `Decode` 成功后增加
  第二次 `Decode(&extra)`——非 `io.EOF` 即拒绝整条消息（第二段解码成功时合成错误
  `message value must contain one JSON value`，与 `internal/httpapi/server.go` 的
  `decodeBody` 单值契约一致；解析错误时透传真实错误文本）；新增私有助手
  `deadLetterUnparsable`（原内联不可解析死信块逐字提取，两条拒绝分支共用：记录
  死信证据→probe/key 解析 event_id→发布或降级→总是提交），`Run` 保持
  `continue` 语义（提交失败仍使 `Run` 退出，与修复前一致）。`eventIDFromValue`、
  `consume`/`deadLetter` 状态机、`Failure` 三键载荷、AsyncAPI/OpenAPI/Proto 均未改动。
- `internal/kafka/kafka_test.go`：新增 AC-1 `TestConsumerRejectsMultiValueMessageAsUnparsable`
  （双值→零摄取、提交 1 次、一条 `unparsable_message` Failure、event_id 取 key）、
  AC-2 `TestConsumerRejectsTrailingGarbageAsUnparsable`（尾随垃圾→同上且 ErrorMessage
  含 `invalid character 'g'`）、AC-4 正向控制 `TestConsumerAllowsTrailingWhitespaceAfterSingleValue`
  （尾随换行仍走正常摄取→提交）。两个拒绝用例在修复前代码上确实失败（首值会被摄取）。

## 2026-08-12 — 卡死导出作业恢复（governance worker）

**写给运营（行为变化）：**

- **worker pass 现在会失败卡在 `running` 超过阈值的导出作业**：audit-api 崩溃/重启
  会在 `runExport` 的 fire-and-forget goroutine 中途留下永远 `running` 的作业（此前无
  任何组件会重新检查非终态导出）。新 worker 部署后，下一次 pass（启动 pass 立即执行）
  会把超过阈值（默认 24 小时，从 `CreatedAt` 起算）的卡死作业置为 `failed`，`Error`
  写明 `stuck in running since <RFC3339>; re-request the export`（API 仍按既有规则掩盖为
  `export failed`，原始详情在 `state.json`），并原子写入一条 `export.recovered` 自审计
  事实（Actor=`governance-worker`，`ListAdminActions` 可查）。
- **阈值可调**：`AUDIT_GOVERNANCE_STUCK_EXPORT_AGE`（或 `-stuck-export-age`，`0` =
  默认）。有合法长时导出（接近 24 小时）的租户应在滚动前调高阈值；非法值回退默认，
  不会静默关闭恢复。
- 恢复**只失败、不重跑**：随机 nonce 密封使字节级重试必然产生误判篡改，且无租约字段，
  跨副本重跑有竞态——终态（worker 判 failed 或原 goroutine 完成）均有效。终态作业
  永不被回退（CAS 闭包内重新检查）；空闲 pass 零快照写入。回滚只需部署旧 worker 二进制
  （已失败作业保持终态，操作员经 API 重新发起）。

**写给开发（实现变化）：**

- `internal/service/governance.go`：新增 `DefaultStuckExportAge`（24h）与
  `RecoverStuckExports(tenantID) (int, error)`——一个 `Store.UpdateChecked` 窗口内完成
  状态迁移 + `export.recovered` 事实（`mutated=false` 零写入，CAS 重试时闭包在新鲜快照
  重跑，计数在每次调用开头重置）。
- `internal/service/service.go`：`Config.StuckExportAge`（`<= 0` 走默认，镜像
  `AggregateCheckpointRetention`）。
- `internal/domain/models.go`：新增 `AdminActionExportRecovered = "export.recovered"`。
- `cmd/audit-governance-worker/main.go`：`-stuck-export-age` flag 与
  `AUDIT_GOVERNANCE_STUCK_EXPORT_AGE` env；`runEvaluatePass` 每租户第一步执行恢复
  （`stuck_exports_recovered=%d` / `export_recovery_error=%v` 日志行），位于归档探测门
  之外（恢复不做任何归档 I/O）。
- 无快照格式变更（`ExportJob` 不变，无 `StartedAt`）、无 OpenAPI/AsyncAPI/Proto 变更；
  新旧二进制双向兼容。

## 2026-08-12 — AsyncAPI 频道声明与运行时 Kafka 符号对齐门禁

**写给运营（行为变化）：**

- **工程门禁新增 `asyncapi channels` 检查**：`python3 cli.py quality` 现在会解析
  `api/asyncapi/asyncapi.yaml` 并核对频道声明与 Go 运行时 Kafka 常量（`internal/`、
  `cmd/` 下的 `const Topic*`）双向一致——spec 声明了频道但 Go 没有对应常量、或 Go
  存在未声明频道的 dotted `Topic*` 常量，门禁即失败并点名地址/符号。此前
  `audit.events.ledgered.v1` / `audit.events.projection.v1` / `audit.events.archive.v1`
  三个声明频道在 Go 中完全没有符号，门禁无法发现；本次补上三个常量使门禁转绿。
- **新增三个惰性主题常量**：`TopicLedgered` / `TopicProjection` / `TopicArchive`
  仅声明以满足契约对齐，当前没有任何生产者/消费者接线（不产生消息）。未来接线见
  `docs/proposals/ledgered-projection-pipeline.md`。
- 无运行时行为变化，无新依赖；回滚只需部署旧二进制（删除门禁检查与三个常量即恢复）。

**写给开发（实现变化）：**

- `checks/asyncapi_channels.py`：新增工程检查（纯 stdlib、fail-closed 的严格
  YAML 子集解析器），`run(root)->int`；Rule A（spec→Go 符号与值必须一致）+ Rule B
  （dotted `Topic*` 常量必须是已声明频道）；符号推导表驱动测试固定
  （`dlq→DLQ` 经显式缩写表，其余首字母大写）。
- `checks/test_asyncapi_channels.py`：35 个用例——AC-1 ghost fixture（缺符号失败/
  有符号通过/值不一致失败）、AC-2 回归形状（accepted 经 outbox-relay、dlq 经
  kafka-consumer）与无豁免证明（行为+静态）、AC-3 tuple 顺序静态测试、Rule B 套件、
  fail-closed 套件（未知/缺失 action、孤儿频道、解析器健壮性）、const 块与跨包
  重复符号、注释/raw-string 假常量。
- `internal/kafka/kafka.go`：新增三个常量（注释注明“仅满足 AsyncAPI 门禁，尚未接线”）。
- `cli.py`：`cmd_quality` 在 `contract_fields` 与 `proto_sync` 之间接入新检查。
- `python3 cli.py quality`、`python -m unittest discover -s checks -p test_*.py`、
  `go build ./... && go vet ./...` 均通过。

## 2026-08-12 — 归档目录链 fsync 与键包含性（FileStore）

**写给运营（行为变化）：**

- **`FileStore.Put` 现在同步完整目录链（叶→根）**：归档对象写入后，其直接父目录与
  到归档根之间的每一级目录都会被 fsync（此前只同步归档根）。修复崩溃后新创建的
  中间目录（如 `events/<tenant>/<stream>/`）连同对象一起丢失的窗口。幂等重试路径
  不做任何新同步（行为不变）。
- **越界键（`..` 穿越/绝对路径/空键）在写入前被拒绝**：此前 `Put("../x")` 会把对象
  写到归档根之外（`Get` 同理可读越界），现在两者都会报错且不产生任何文件系统变更。
  审计对象、封段、导出文件均使用服务端生成的受控键，正常路径无影响。
- **并发重试的 "nil ⇒ durable" 保证收紧**：同一个键的并发 Put 现在被串行化，失败的
  Put 不再可能删除一个并发重试刚验证为已归档的对象（归档记录不再"报成功却丢失"）。
- 回滚只需部署旧二进制；磁盘布局、权限、键格式均未变。

**写给开发（实现变化）：**

- `internal/fsutil`：新增 `SyncDirChain(root, dir, syncDir)`（叶→根目录链 fsync，
  `errors.Is` 可区分链成员失败；`nil` 回退到 `SyncDir`），首个单元测试
  `internal/fsutil/fsutil_test.go`。
- `internal/archive`：`FileStore` 新增未导出 `syncDir` 测试缝与 `mu` 互斥锁；
  `Put` 用 `SyncDirChain` 替换根目录单点同步，`Put`/`Get` 经 `containedPath` 在
  任何文件系统变更前拒绝越界键；错误路径保持"失败 ⇒ 键上无对象"不变式。
- 回归测试：链顺序/崩溃回放模型（非空洞，证明仅同步根会丢子树）/父、中间、根
  三级同步失败不残留对象/幂等重试零同步/键越界拒绝（写与读对称）/同键并发
  丢失归档 hammer（`-race`）/并发 Get 无撕裂读。
- `python3 cli.py check`/`quality` 通过。

## 2026-08-11 — worker 启动/`-check-config`/每轮 evaluate 前探测归档 WORM 就绪

**写给运营（行为变化）：**

- **audit-governance-worker 启动时探测归档目的地**：S3 桶缺少 Object Lock
  或 versioning、桶不存在，或本地归档目录不可写/被占用时，worker 拒绝启动
  （fatal，退出码非 0）；此前只在每轮 Put 失败时记日志。`-once` 模式同样
  生效。探测有 5 秒超时（`archiveReadyTimeout`），健康时仅新增一行
  `archive_ready=ok`。
- **`-check-config` 现在探测归档目的地**：S3 配置会做一次有界网络探测，
  本地归档会做可写性探测；失败时退出码 1 且不打印 `check_config=ok`（此前
  桶配置错误也打印 ok）。该 flag 的"不触网"契约已变更——依赖离线执行
  `-check-config` 的 CI 需更新（`audit-api` 的 `-check-config` 不受影响，
  仍不发起网络）。健康配置的退出码与输出格式不变。
- **每轮 evaluate 前探测一次**：失败时该轮跳过归档（不标记任何 receipt 为
  archived、不开 Store.Update 窗口、不重置冲突计数），每租户记录
  `archive_skipped=ready_probe_failed`；封段/聚合检查点/保留评估照常执行。
  探测按轮一次、与租户数无关，默认 5 分钟一轮。
- **升级前置条件**：S3 部署须先启用桶 versioning 与 Object Lock
  （`put-bucket-versioning` + `put-object-lock-configuration`），本地归档须
  确保目录对 worker UID 可写；否则新 worker 启动即失败（这是预期行为，
  可用 `-check-config` 预检）。回滚只需部署旧二进制，无需数据迁移。

**写给开发（实现变化）：**

- `internal/archive`：导出 `S3Client` 接口并新增
  `NewS3StoreWithClient(client, bucket)`（`s3Client` 保留为别名）；
  `NewS3Store`/`Ready`/`Put` 对既有调用者逐字节不变。
- `cmd/audit-governance-worker`：新增 `archiveReadyTimeout` 常量、
  `probeArchiveReady`/`runEvaluatePass` 助手与 `newArchiveStore` 测试缝；
  启动（含 `-once`）与 `-check-config` 在归档前探测，`runEvaluatePass`
  按轮探测一次、失败跳过归档；`ArchivePending` 及其原子批量语义未改动。
- 回归测试：T1–T4 + T7（`-check-config` 探测、启动探测、轮内门控与逐轮
  日志、5 秒超时）、子进程级 T1e/T2e（真实二进制的退出码），
  `test/e2e/fullstack.sh` 的 MinIO WORM 桶自举提前到应用启动之前并断言
  `archive_ready=ok`；`python3 cli.py check`/`quality` 通过。

## 2026-08-11 — Server span 命名与 `http.route` 改为取自匹配到的 ServeMux pattern

**写给运营（行为变化）：**

- **span name 与 `http.route` 不再包含原始路径中的 ID 值**：命名改为
  `<method> <pattern>`（如 `GET /api/v1/events/{eventID}`），基数从无界
  收敛为 ≤ 35 个固定名。此前导出到 OTLP 的事件 ID、导出 job ID、操作 ID、
  聚合 ID、hold ID、run ID、source ID 等敏感标识不再离开服务边界；依赖原始
  ID 形状的 span 名/`http.route` 的看板与告警将停止收到这些值。
- **未匹配任何路由的请求（ServeMux 404）不再产生 span、不再输出
  `traceparent`**，`X-Trace-ID` 回退为请求 ID——与未配置 OTLP 时的既有行为
  一致。匹配路由的请求行为不变：`traceparent` 延续/注入、`X-Trace-ID` =
  span trace ID、`X-Request-ID`、指标与错误契约全部不变。

**写给开发（实现变化）：**

- `internal/httpapi/server.go` 内部重构，外部 API 面零变化（零路由/零
  OpenAPI/零 metrics/零配置变更）：外层 `middleware` 只保留 request ID 与
  指标；新增 per-route `spanWrap`（35 条 `mux.HandleFunc` 全部包装），在
  ServeMux 匹配后从 `r.Pattern` 创建唯一 server span、注入 `traceparent`、
  设置 `X-Trace-ID`，并独占 panic 恢复——恢复与 `span.End()` 同在一个
  defer 函数中，保住 `RecordError` 落在活 span 上的既有顺序（F3 陷阱）。
- 新增 `routeFromPattern`：按 pattern 自身的方法 token 截断（`HEAD` 命中
  `GET` pattern 时 `r.Pattern` 仍是 `"GET …"`），无方法/空 pattern 回退
  `/unmatched`（有界）。未匹配请求按 REQ-4 选项 (a) 不建 span。

**回归测试：** `TestHTTPSpanUsesMatchedPattern`、
`TestHTTPSpanNeverContainsRawIDs`、`TestHTTPSpanNameCardinalityBounded`、
`TestHTTPUnmatchedNoSpan`、`TestHTTPSpanWrapPanicRecovered`、
`TestHTTPSpanHeadUsesPatternMethod`、`TestHTTPSpanRouteCoverageAllPatterns`
（35 条 pattern 全覆盖，未来未包装注册即失败）、
`TestRouteFromPatternFallback`（`internal/httpapi/server_test.go`）；
`TestEventReturningHandlersStripSearchDigests` 的 AST 巡检更新为要求
`s.spanWrap(s.<handler>)` 包装形态。

## 2026-08-11 — JWT 租户声明收紧：tenant_id/tenant 声明委托 canonical key-framing 字符集规则

**写给运营（行为变化）：**

- **JWT `tenant_id`/`tenant` 声明包含 `/` 或 `\` 时认证失败**（HTTP 401
  `unauthorized`；gRPC `Unauthenticated`），错误文案与既有字符集违规完全
  相同（无 oracle 区分）。此前这类声明能通过认证，但因 `CreateTenant` 早已
  拒绝此类 ID，它们永远无法解析到任何租户，只会得到 404/空结果——因此**没有
  合法租户受影响**，只是失败点从下游 404 前移到认证阶段。控制字符（含
  0x1F）、空白、两侧填充的拒绝行为与之前完全一致。
- **非平台 `tenantFor` 边界防御加深**：token 解析出的租户上下文在每次读取/操作
  前用同一字符集规则复核（防御纵深；若未来出现绕过声明的 token 方言，返回
  400 `invalid_request`）。空租户上下文（全租户读取）语义不变。

**写给开发（API 契约变化）：**

- `tenantClaim`（`internal/auth/auth.go`）删除自有的 `IsControl/IsSpace` 循环与
  `TrimSpace` 检查，改为委托 `domain.ValidKeyComponent`——与 `CreateTenant`、
  dev token、`?tenant_id=` 逃生口完全同一实现。拒绝集合仅新增 `/` 与 `\`
  （`unicode.IsSpace` 覆盖 `TrimSpace` 的全部裁剪字符，填充检查被完全包含）；
  错误文本 `"token %s claim is invalid"` 逐字节保留（单一日志串，无字符集
  oracle）。
- `tenantFor`（`internal/httpapi/server.go`）非平台分支对非空 `claims.TenantID`
  执行 `store.ValidTenantID`，返回原始 `ErrInvalid`（与平台分支同一 400 路径）；
  变更后 `auth.go` 不再导入 `unicode`。
- 无 wire/OpenAPI/存储变更：零路由、零 proto、零 key-format 变化；`exp` 等
  既有声明校验不变。

**回归测试：** `TestJWTRejectsKeyFramingTenantClaims`（两个声明别名 × 7 类
非法值 + 精确串/空串正向对照，`internal/auth/auth_test.go`）、
`TestHTTPJWTRejectsKeyFramingTenantClaim`（签名 JWT → 401 `unauthorized` +
零自审计副作用 + 404 正向对照）、`TestHTTPTenantForRechecksClaimTenantID`
（白盒 `errors.Is(domain.ErrInvalid)`）、
`TestGRPCRejectsKeyFramingTenantClaimUnauthenticated`（两个别名 × 3 非法值 →
`Unauthenticated` + 快照无多分隔符键 + 正向 ingest）、AC-5 注入性扩展
`TestCompositeKeyInjectivityForValidTenantIDs`/`TestSchemaKeyInjectivityForValidTenantIDs`
与 `TestFramingDemonstration`（`internal/store/tenantkey_test.go`）。

## 2026-08-11 — 读路径自审计闭环：timeline/replay/receipt/verify 全部追加 audit.event.read 事实

**写给运营（行为变化，观察类）：**

- **五个此前不落账的读端点现在记录 `audit.event.read` 事实**：
  `GET /api/v1/events/{eventID}/receipt`、`GET /api/v1/operations/{operationID}/timeline`、
  `GET /api/v1/operations/{operationID}/replay`、
  `GET /api/v1/aggregates/{aggregateType}/{aggregateID}/timeline`、
  `POST /api/v1/integrity/verify`。事实携带调用者 subject（`actor`）与
  确定性目标编码（FR-4）：`(operation, id, timeline/replay)`、
  `(aggregate, id, aggregateType/replay)`、`(event, id, receipt)`、
  `(integrity, stream_id, verify)`。`GET /api/v1/admin/actions` 现在能回答
  “谁读过这条时间线/这个事件”——此前只有 `getEvent`/`queryEvents` 落账。
- **失败关闭语义与既有读一致**：事实追加失败（存储不可写、乐观锁冲突耗尽）
  时读请求返回 500/503，**不返回读结果**；`Valid:false` 的完整性校验仍
  落账（记录的是“读”，不是“结论”）。
- **恢复预览/创建不落读账（设计门决定，F1 显式拒绝）**：
  `POST /api/v1/restores/preview` 与 `POST /api/v1/restores` 内部重放事件
  内容，但保持零 `audit.event.read` 行（仅 `restore.created` 等既有事实）。
  这是有意识的产品决策，由行为固定测试钉住；如需预览读可见需产品决策。
- **`stream_id` 边界（安全评审 F2）**：`POST /api/v1/integrity/verify` 的
  `stream_id` 现在拒绝控制字符/空白/路径分隔符（复用 key-framing 字符集
  规则）且长度上限 256 字节，违规返回 400 且不落账——防止攻击者用超大
  `stream_id` 无界膨胀单一 JSONB 行内的自审计足迹。合法值行为不变。

**写给开发（API 契约变化）：**

- 六个内部 service 方法签名增加 `actor string` 参数（位于 `tenantID` 之后）：
  `GetReceipt`、`OperationTimeline`、`AggregateTimeline`、`ReplayOperation`、
  `ReplayAggregate`、`VerifyIntegrity`；空 `actor` 不落账（内部调用者静默）。
- 重放不再委托给带审计的公开方法：提取 `operationTimelineNoAudit` /
  `aggregateTimelineNoAudit` 内部核心，一次重放调用恰好追加一条事实。
- 五个 HTTP 处理器转发 `claims.Subject`；`PreviewRestore`/`CreateRestore`
  传 `""`（F1 显式拒绝）。
- OpenAPI 仅更新 `admin/actions` 描述（纯文本，无路由/模式变化）。
- 测试调用点机械更新 41 处（`VerifyIntegrity` ×36、`GetReceipt` ×3、
  `OperationTimeline` ×1、`ReplayOperation` ×1），全部传 `""`。

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
  **（已被 2026-08-15 的拆分计数变更取代：`audit_dlq_replayed_total` 恢复
  为仅统计成功重发，收敛路径拆分为三个独立计数器并新增 unresolvable 告警。）**
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
