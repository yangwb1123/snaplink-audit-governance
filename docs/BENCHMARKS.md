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
