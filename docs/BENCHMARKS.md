# 本机性能基线

状态：参考实现基线，不是生产 SLO 承诺  
日期：2026-08-04  
机器：Zorin OS 18.1，AMD Ryzen AI Max+ 395（16 核 32 线程），124GiB 内存  
方法：`go test -run=^$ -bench=Benchmark -benchmem -benchtime=2s ./internal/service/`

## 结果

| Benchmark | 单次耗时 | 单核等价吞吐 | 分配 | 说明 |
|---|---:|---:|---:|---|
| `BenchmarkIngest` | 15.9 µs/op | ≈ 62,800 events/s | 10.8 KB / 240 allocs | 单事件全链路：Schema 校验、规范化编码、敏感字段扫描、哈希、流链接；内存 store（无磁盘/网络） |
| `BenchmarkQuery` | 490 µs/op | ≈ 2,040 queries/s | 1.98 MB / 26 allocs | 1,000 事件账本上的时间范围 + 类型过滤查询，100 条页 |
| `BenchmarkEventDigest` | 10.2 µs/op | ≈ 98,000/s | 8.2 KB / 186 allocs | Canonical JSON 编码 + SHA-256 摘要 |

运行方式：

```sh
python3 cli.py bench        # 全仓库
go test -bench=Benchmark -benchmem -benchtime=2s ./internal/service/
```

## 与生产 SLO 的关系

- 架构基线（50,000 events/s 稳态）必须经过多节点 Kafka + PostgreSQL + ClickHouse
  真实容器压测验证；本机数字只证明参考实现路径无明显的单点算法瓶颈。
- 本机 Ingest 为同步直写（accepted 即 ledgered），生产链路由 accepted→ledgered
  是 Kafka 异步路径，延迟特征不同，不可直接对比。
- Query 基准未包含 ClickHouse 投影；参考实现直接扫描内存快照。

## 后续基准点

- 5,000 / 50,000 / 100,000 事件账本上的查询延迟曲线。
- 大 payload（64KB/256KB）的 Ingest 分配与延迟。
- 并发写 + 并发查混合场景（`-cpu` 矩阵）。
