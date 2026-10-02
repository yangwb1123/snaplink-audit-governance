# 本机性能基线

状态：参考实现基线，不是生产 SLO 承诺  
日期：2026-08-04  
机器：Zorin OS 18.1，AMD Ryzen AI Max+ 395（16 核 32 线程），124GiB 内存  
方法：`go test -run=^$ -bench=Benchmark -benchmem -benchtime=1s ./internal/service/`

## 结果

更新：2026-08-20（hot/cold v2 分层后重测，`-benchtime=1s`；以下新增成本门禁为 `-benchtime=100ms` 快照）

| Benchmark | 单次耗时 | 分配 | 说明 |
|---|---:|---:|---|
| `BenchmarkIngest` | 1.27 ms/op | 292 KB / 2.0k allocs | 单事件全链路（Schema 校验、规范化、敏感字段扫描、哈希、流链接、快照持久化） |
| `BenchmarkQuery` | 13.4 ms/op | 14.6 MB / 55.7k allocs | 1,000 事件账本过滤查询，100 条页。**F-06 读自审计后每次查询追加一次全快照 Update**（actor 非空时），查询成本由 448 µs 上升约 30×——参考实现的固有写放大，生产查询路径走 ClickHouse 投影（ADR-0004），不存在该成本 |
| `BenchmarkQueryLargeLedger` | 65.9 ms/op | 47.9 MB / 260k allocs | 5,000 事件账本同型查询；含读自审计快照写（同上） |
| `BenchmarkEventDigest` | 9.9 µs/op | 5.8 KB / 126 allocs | Canonical JSON 编码 + SHA-256 摘要（哈希链最小单元） |
| `BenchmarkVerifyIntegrity` | 22.8 ms/op | 16.3 MB / 267k allocs | 1,000 事件混合账本（半数敏感字段，10 个密封段）；内容认证（深拷贝 + 解密 + 重新规范化） |

## Hot/cold ingest 成本门禁（2026-08-20）

`BenchmarkIngestWithArchivedLedger` 在内存 v2 store 预置仅含 receipt 的冷账本，
再测量新事件写入。冷账本按租户建立 O(1) receipt/idempotency 索引，单次 ingest
只克隆有界热文档；数字会随机器和 Go 版本变化，不是生产 SLO。

| 冷 receipt 数 K | ns/op | B/op | allocs/op |
|---:|---:|---:|---:|
| 0 | 460,641 | 554,264 | 1,545 |
| 1,000 | 435,313 | 403,675 | 1,342 |
| 10,000 | 519,840 | 538,717 | 1,524 |
| 50,000 | 427,155 | 555,495 | 1,550 |

运行方式：

```sh
go test ./internal/service -run '^$' -bench '^BenchmarkIngestWithArchivedLedger$' -benchmem -benchtime=100ms
```

`TestIngestCostIndependentOfArchivedEvents` 同时钉住 K=0 与 K=10,000 的 wall/allocation
不超过 3×；若超限，说明 ingest 路径重新物化了冷 ledger，应阻断发布。

旧基线（2026-08-04，读自审计前）：`BenchmarkIngest` 2.2 ms、`BenchmarkQuery` 448 µs（1,000 事件）、`BenchmarkQueryLargeLedger` 3.8 ms、`BenchmarkEventDigest` 10.0 µs、`BenchmarkVerifyIntegrity` 4.19 ms → 22.8 ms（内容认证改造）。

运行方式：

```sh
python3 cli.py bench        # 全仓库
go test -bench=Benchmark -benchmem -benchtime=1s ./internal/service/
```

## gRPC `WriteStream` capacity decision-gate observations (2026-09-28)

**Status: evidence only.** This section records a real listener/client load
observation; it does not select a handler capacity, establish a safe default,
create an overload contract, or define a production SLO. The existing
reference-implementation disclaimer above remains in force.

### Reproduction metadata

- **Revision:** `950d5b7` (`HEAD`) plus the working-tree measurement changes
  recorded in this report; rerun from a clean revision before comparing results.
- **Date:** 2026-09-28 UTC.
- **Host/container:** local host `u1-FAEX9`, Linux `7.0.0-31-generic`,
  `linux/amd64`; not a production container or multi-node environment.
- **Runtime:** Go `go1.26.5`; gRPC `v1.83.0`.
- **Server:** real `net.Listen("tcp", "127.0.0.1:0")`, registered
  `Ingest.WriteStream`, production keepalive parameters, 512 KiB receive cap,
  stream recovery interceptor, in-memory hot/cold store, `SegmentSize=100000`,
  and an injected test `LedgeredPublisher`. Local insecure transport is used
  only to isolate this test; authentication is still performed with a signed
  HS256 JWT.
- **Client connections:** two independently created `grpc.ClientConn` values
  to the same listener (`client-conn-1` and `client-conn-2`), with streams
  assigned round-robin.
- **Authentication/expiry:** JWT `sub=writestream-capacity-load`,
  `tenant_id=tenant-a`, `client_id=crm`, `roles=[service]`, HS256 local test
  secret, `exp=now+30m`; every stream therefore exercises a non-zero effective
  expiry and its `Recv` worker path.
- **Exact command and implementation:**

  ```sh
  go test -tags=writestream_load ./internal/grpcapi \
    -run '^TestGRPCWriteStreamCapacityLoad$' -count=1 -timeout=60s -v
  ```

  The implementation is `internal/grpcapi/writestream_capacity_load_test.go`.
  The `writestream_load` build tag is test-only; it is not a server setting.

### Workload matrix and measurement method

The fixed offered levels are 4, 12, and 24. They are observations, not
approved limits. The mix is idle expiry-aware / normal producer / slow reader:
1/2/1, 4/5/3, and 8/10/6 respectively. Slow readers withhold receipt reads
for 700 ms; normal streams send and read repeatedly. The dependency fault is
injected only at level 24, at approximately +650 ms for a requested 350 ms.

Active handlers and expiry-aware `Recv` workers are counted by a test-only
start/finish interceptor, which has no admission method and is absent in production.
Goroutines use `runtime.NumGoroutine`; Go heap uses
`runtime.ReadMemStats().HeapAlloc`; process RSS uses `/proc/self/status` `VmRSS`; file descriptors use `/proc/self/fd`. A zero/unavailable `/proc`
value is not interpreted as a limit. Latencies are milliseconds, with p50/p95/
max; stream-open is the observed stream establishment timing, while admission
latency is explicitly `n=0` because no admission path exists. Receipt latency
is the message round trip from successful `Send` to receipt `Recv`. Terminal
latency is measured from the workload-level shutdown signal to each final
terminal `Recv` observation after load workers drain; it is a harness drain
measurement, not a per-RPC SLO.

### Aggregate process observations

| Offered | Opened / active sample | Mix (idle/normal/slow) | Peak handlers | Peak expiry `Recv` workers | Peak goroutines | Peak Go heap | Peak RSS | Peak FDs |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 4 | 4 / 4 | 1 / 2 / 1 | 4 | 4 | 35 | 3,804,848 B | 29,310,976 B | 12 |
| 12 | 12 / 12 | 4 / 5 / 3 | 12 | 12 | 64 | 5,645,096 B | 33,427,456 B | 12 |
| 24 | 24 / 24 | 8 / 10 / 6 | 24 | 24 | 109 | 6,091,480 B | 36,737,024 B | 12 |

| Offered | Stream-open p95 | Admission p95 | Send p95 | Receipt p95 | Terminal p95 | Recorded status/error distribution |
|---:|---:|---:|---:|---:|---:|---|
| 4 | 0.615 ms | n/a (0) | 0.039 ms | 699.749 ms | 11.895 ms | `send:EOF=2`, `terminal:Canceled=3`, `terminal:EOF=1` |
| 12 | 0.032 ms | n/a (0) | 0.077 ms | 1,552.274 ms | 19.292 ms | `receipt:Canceled=7`, `send:EOF=1`, `terminal:Canceled=8`, `terminal:EOF=4` |
| 24 | 0.009 ms | n/a (0) | 0.040 ms | 1,610.071 ms | 18.394 ms | `receipt:Canceled=15`, `send:EOF=1`, `terminal:Canceled=16`, `terminal:EOF=8` |

The process resource columns include both the server and in-process load
clients because this test intentionally keeps the real server and clients in
one Go test process. They are observations for this harness, not a production
server resource budget.

### Independent connection observations

| Level | Connection | Offered / opened / active sample | Idle | Normal | Slow reader |
|---:|---|---:|---:|---:|---:|
| 4 | client-conn-1 | 2 / 2 / 2 | 1 | 1 | 0 |
| 4 | client-conn-2 | 2 / 2 / 2 | 0 | 1 | 1 |
| 12 | client-conn-1 | 6 / 6 / 6 | 2 | 2 | 2 |
| 12 | client-conn-2 | 6 / 6 / 6 | 2 | 3 | 1 |
| 24 | client-conn-1 | 12 / 12 / 12 | 4 | 5 | 3 |
| 24 | client-conn-2 | 12 / 12 / 12 | 4 | 5 | 3 |

### Controlled dependency degradation

At offered level 24, `service.Config.LedgeredPublisher.Publish` returned a
controlled error after a 120 ms bounded delay while degraded. The requested
fault window was 350 ms; the observed window was 300 ms. The captured run
recorded 71 publisher calls, 3 dependency errors, and 38 successful
publications after fault recovery. This observational test does not assert
durable outbox state; existing ingest/outbox tests remain the durability
oracle. The fault result is not converted into a capacity threshold.

### Limitations and decision gate

This is one local, single-process run with an in-memory store, local HS256
authentication, insecure loopback transport, and a synthetic publisher fault.
It does not model production TLS, kernel/container limits, external storage,
network hops, replicas, broker/database saturation, or production traffic
mix. Repeat it in a production-relevant environment and publish the raw output
and environment metadata before making a decision. No row above is a safe
numeric handler capacity, runtime default, or overload contract. A benchmark
result alone does not approve a global `ResourceExhausted` admission limit or
its numeric capacity.

## Post-ledger publication intent: one commit, and the window that remains

Not a benchmark: a measured finding about the tenant (hot/cold) ingest path, kept
here because it changes how the ingest cost gate above should be read.

**Finding.** On the tenant path the durable publication intent for
`audit.events.ledgered.v1` (`Snapshot.LedgeredOutbox`) used to be written by a
*second* store operation — `Store.UpdateControl` — after `Store.UpdateTenant` had
already committed the event and its cold receipt. `Store.UpdateControl` takes its
own `Store.mu` hold and rewrites the control document, so a crash, SIGKILL or
failed control write between the two commits left a committed ledger record plus a
WORM object with nothing owed to the projection feed. No path repaired it: the
duplicate branch returned before the intent write, `FlushLedgeredOutbox` iterates
only existing rows, and the outbox loop only calls that flush. The event was then
permanently absent from the ClickHouse projection — with no DLQ record, no replay
path and no metric. This is an at-least-once violation on a post-ledger feed.

**Cost consequence (measured direction, not a new number).** Ingest with a
configured `LedgeredPublisher` performs **one** durable control-plane write per
event instead of two: the tenant commit now carries the intent, and the separate
`UpdateControl` write is gone. The write set per ingest is therefore the same
tenant hot document plus one control write, i.e. strictly less control-plane
I/O than before. Re-measure `BenchmarkIngestWithArchivedLedger` before quoting a
new cost gate; this entry does not claim one.

**Residual window.** Moving the intent into the closure removes the
*application-level* window only. The durable order on both backends is still hot
document → cold ledger → control document (`internal/store/hotcold_commit.go`
`commitTenant`; `internal/store/postgres_hotcold.go` `updateTenantOnce`), so a
commit can still die between the cold append and the control write. The
compensating control is a repair on the idempotent retry path, gated on a receipt
that is still `ledgered` with no pending intent row, bounded to one re-creation
per event key per process.

**How to read the metric.** `GET /metrics` exposes
`audit_ledgered_intent_repaired_total` (unlabelled, monotonic, bounded
cardinality). Each increment is direct evidence that a tenant commit died between
its durable cold append and its control-plane write. It must read `0`; a *rising*
value alongside a flat `audit_ingest_duplicates_total` means a lost intent is
still owed, because a repair only happens when a client retries. A fire-and-forget
producer that never retries leaves the intent owed with nothing to raise — the
counter is the cheap detector that justifies building a boot-time reconciler over
`Receipts \ LedgeredOutbox` later, with real data behind the decision.

## 容量 envelope 与 cutover 门禁（B1-3 决策 #7）

v2 file store 采用**热/冷分层**：控制面和每租户热文档只承载有界写集，receipt、
segment、checkpoint 进入租户 append-only ledger；兼容性的 `Snapshot`/查询读取仍会
按需 materialize 全量 ledger。PostgreSQL 在应用 `006_hot_cold_split.sql` 并显式完成
cutover 后使用同样的关系表；`NewWithBackend` 和未迁移的旧 PG 表仍保留单文档兼容路径。

容量 envelope（本机基线，机器见上）：

| 账本规模 | Ingest（单事件，累计摊销） | QueryEvents p95 |
|---|---:|---:|
| 1,000 事件 | 2.2 ms/op（全链路） | 448 µs/op |
| 5,000 事件 | O(n²) 累计写入（见下） | 3.8 ms/op |

**读取容量门禁**：单租户累计事件数超过 **10⁵（100,000）** 或查询 p95 超过
**500 ms**（30 天范围操作时间线 SLO，架构计划 §3）时，不能继续依赖兼容
`Snapshot` 全量读取，应把查询切到关系账本/ClickHouse 索引路径。该门禁是
G2 验收“容量 envelope 记录；高容量类 cutover 门禁”的落点；`cli.py bench` 提供
回归基线（`BenchmarkIngest`/`BenchmarkQueryLargeLedger`）。

## 已知特征（如实记录）

- **兼容快照写路径仍是 O(n²) 累计写入**：`Store.Update`、旧 PG 表和测试脚本
  仍对 materialized 全量快照做克隆 + 原子持久化；v2 ingest/归档/封段走租户
  API，冷 ledger 不进入每次热写。失败闭包仍通过私有副本/提交顺序保证不丢数据。
- 生产形态（ADR-0001/0003/0004）中，事件写入走 Kafka → 关系表/账本流，
  控制面快照只承载元数据，不存在该 O(n²) 路径。
- 本机数字只验证功能正确性与算法正确性；50,000 events/s 稳态必须经过
  多节点 Kafka + PostgreSQL + ClickHouse 真实容器压测。

## 后续基准点

- 5,000 / 50,000 / 100,000 事件账本上的查询延迟曲线。
- 大 payload（64KB/256KB）的 Ingest 分配与延迟。
- 并发写 + 并发查混合场景（`-cpu` 矩阵）。
- `VerifyIntegrity` 按流验证与全量验证的成本对比；`DecryptJSON` 一致性改造后的
  大整数敏感字段回归（`TestVerifyIntegrityLargeIntSensitiveField` 哨兵）。

## PostgreSQL control-plane pool decision gate (2026-08-21)

**Status: no numeric pool default is approved by this change.** This section
records the knobs that now exist, the measurement that is possible today, and
the criteria an operator must satisfy before choosing a value. The
reference-implementation disclaimer at the top of this file remains in force:
nothing below is a production SLO.

### Verification table

| Claim | Evidence | Result |
|---|---|---|
| Every serving binary resolves the pool from one implementation | `internal/runtimeconfig/postgres_pool.go`; called by `cmd/audit-api`, `cmd/audit-governance-worker`, `cmd/audit-outbox-relay`, `cmd/audit-pg-migrate` | verified |
| An UNSET knob applies no setter at all | `TestPoolConfigFromEnv/unset-means-no-setter` asserts `db.Stats().MaxOpenConnections == 0` after applying a fully unset config | verified |
| A SET knob is validated and read back | `TestPoolConfigFromEnv/every-knob-applied`, `TestApplyPoolConfig` semantics | verified |
| An invalid value fails closed naming the variable | `TestPoolConfigFromEnv/invalid/*`, `TestOpenStoreAppliesTheSharedPoolConfiguration`, `TestWorkerOpenStoreAppliesTheSharedPoolConfiguration` | verified |
| The pool shape is observable without a statement | `TestMetricsExposesPoolStatsAndDeadlineCounter` (`/metrics`, no labels) | verified |

### Reproduction metadata

- **Revision:** the worktree commit that adds `internal/runtimeconfig/postgres_pool.go`.
- **Date:** 2026-08-21 UTC.
- **Host/container:** local host; **not** a production container, a replica set,
  or a saturated PostgreSQL.
- **Runtime:** Go as pinned in `go.mod`.
- **Store:** PostgreSQL `postgres:17-alpine` from `deploy/docker-compose.verify.yml`.

### Operator knobs

| Variable | Effect when set | Unset behaviour |
|---|---|---|
| `AUDIT_POSTGRES_MAX_OPEN_CONNS` | `db.SetMaxOpenConns(n)` | no setter call; `database/sql` default (unlimited) |
| `AUDIT_POSTGRES_MAX_IDLE_CONNS` | `db.SetMaxIdleConns(n)` | no setter call; `database/sql` default (2) |
| `AUDIT_POSTGRES_CONN_MAX_LIFETIME` | `db.SetConnMaxLifetime(d)` | no setter call; connections live forever |
| `AUDIT_POSTGRES_CONN_MAX_IDLE_TIME` | `db.SetConnMaxIdleTime(d)` | no setter call; idle connections are closed immediately |
| `AUDIT_STORE_OPERATION_TIMEOUT` | per-store-operation deadline (`store.Options.OperationTimeout`) | `store.DefaultOperationTimeout` (10s) |

`AUDIT_STORE_OPERATION_TIMEOUT` must be **strictly below** audit-api's 30s HTTP
`WriteTimeout`; the binary refuses to start otherwise, because the retryable
`503 store_timeout` could not otherwise reach the client.

### Effective-default table (the shipped shape)

| Metric | Shipped value | Meaning |
|---|---:|---|
| `audit_store_pg_pool_max_open` | `0` | unlimited open connections — no operator limit |
| `audit_store_pg_pool_idle` | database/sql default | 2 retained idle connections |
| `audit_store_pg_pool_wait_count_total` | monotonically increasing | how often a caller had to wait for a connection |

### Measurement recipe

Against the verify stack (a real `postgres:17-alpine`, no pool knobs set):

```sh
docker compose -f deploy/docker-compose.verify.yml up -d
curl -s http://127.0.0.1:19089/metrics | grep '^audit_store_pg_pool'
```

To evaluate a candidate value, set the four variables, restart the three
services, drive the ingest load, and re-read the same five lines.

### Saturation decision criteria

1. `audit_store_pg_pool_wait_count_total` rising per operation while
   `audit_store_pg_pool_in_use` sits at `max_open` ⇒ the pool is the
   bottleneck; the candidate value is too low.
2. `audit_store_pg_pool_in_use` well below `max_open` with a non-zero wait
   count ⇒ the bottleneck is PostgreSQL, not the client-side pool; raising
   `max_open` will not help.
3. `audit_store_operation_deadline_exceeded_total` non-zero ⇒ statements are
   already being aborted at the store deadline; a larger pool makes the
   database slower, not the process faster.

### Observation actually run

Only the static shape was verified in this change: the unset configuration
applies no setter, the metrics lines appear verbatim with no labels, and the
gate is green. **No saturated-load measurement was run**, so no pool value is
recommended.

### Limitations and decision gate

This is a local, single-node, single-replica observation with no load
generator and no PostgreSQL saturation. It does not model production TLS,
network hops, `max_connections` on the server, replica fan-out, or a real
traffic mix. **This change approves no numeric pool default**: `unset`
remains the shipped configuration, and choosing a value requires operator
evidence from a production-relevant environment plus the published raw
metrics. Re-measure and record the result here before treating any number as
approved.
