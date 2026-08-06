# 本机性能基线

状态：参考实现基线，不是生产 SLO 承诺  
日期：2026-08-04  
机器：Zorin OS 18.1，AMD Ryzen AI Max+ 395（16 核 32 线程），124GiB 内存  
方法：`go test -run=^$ -bench=Benchmark -benchmem -benchtime=1s ./internal/service/`

## 结果

| Benchmark | 单次耗时 | 分配 | 说明 |
|---|---:|---:|---|
| `BenchmarkIngest` | 2.2 ms/op | 1.25 MB / 9.5k allocs | 单事件全链路（Schema 校验、规范化、敏感字段扫描、哈希、流链接）。快照式存储每次 Update 读-改-写整个控制面快照，成本随累计事件数线性增长（O(n²) 总体），见下 |
| `BenchmarkQuery` | 448 µs/op | 1.98 MB / 26 allocs | 1,000 事件账本上的时间范围 + 类型过滤查询，100 条页（Read 路径零拷贝） |
| `BenchmarkQueryLargeLedger` | 3.8 ms/op | 12.6 MB / 32 allocs | 5,000 事件账本同型查询；从 1k→5k 约线性扩展（查询路径 O(n)），快照直接构造（绕过 O(n²) 预填） |
| `BenchmarkEventDigest` | 10.0 µs/op | 8.2 KB / 186 allocs | Canonical JSON 编码 + SHA-256 摘要（哈希链最小单元） |

运行方式：

```sh
python3 cli.py bench        # 全仓库
go test -bench=Benchmark -benchmem -benchtime=1s ./internal/service/
```

## 已知特征（如实记录）

- **快照式控制面存储是 O(n²) 累计写入**：每个 Update（含每次事件落账）都对
  全量快照做一次克隆 + 原子持久化。该语义保证任何失败闭包（冲突、幂等键
  冲突）都不可能污染已提交状态（`LoadForUpdate` 私有副本）。事件量在
  数千级时毫秒内完成；这是单节点参考实现的固有上限，**不是生产形态**。
- 生产形态（ADR-0001/0003/0004）中，事件写入走 Kafka → 关系表/账本流，
  控制面快照只承载元数据，不存在该 O(n²) 路径。
- 本机数字只验证功能正确性与算法正确性；50,000 events/s 稳态必须经过
  多节点 Kafka + PostgreSQL + ClickHouse 真实容器压测。

## 后续基准点

- 5,000 / 50,000 / 100,000 事件账本上的查询延迟曲线。
- 大 payload（64KB/256KB）的 Ingest 分配与延迟。
- 并发写 + 并发查混合场景（`-cpu` 矩阵）。
