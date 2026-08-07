# SLO、告警与错误预算

状态：Draft（基线数值来自架构计划 §3，上线前必须用真实负载重新校准）  
日期：2026-08-05

## 1. SLO

| 指标 | SLO | 测量来源 | 说明 |
|---|---:|---|---|
| 接入可用性 | 99.99% / 月 | `/metrics` `audit_http_requests_total` vs `audit_http_errors_total`（5xx） | 写入链路 |
| 查询可用性 | 99.95% / 月 | 同上（query 路径） | 查询链路 |
| accepted 写入延迟 | p99 < 150ms（同区） | `audit_http_request_duration_seconds`（ingest 路径） | 含网络；本机参考实现远低于此 |
| ledgered 延迟 | p99 < 2s | accepted→ledgered 时间差（生产异步链路） | 参考实现同步，无此差距 |
| 查询投影延迟 | p99 < 5s | accepted→indexed 时间差 | ClickHouse 投影 |
| 操作时间线查询 | 30 天范围 p99 < 500ms | query 直方图 | 生产投影 |
| 同区 RPO | 0（已确认 accepted） | Kafka 多副本 + 幂等消费 | 生产 |
| 跨区 RPO / RTO | ≤60s / ≤15min | 灾备演练 | 未在本机验证 |

## 2. 告警规则（映射到现有指标）

> 已落地：`deploy/prometheus-rules.verify.yml`（12 条规则，3 组：audit-dlq /
> audit-slo / audit-production）。本机可验证的 7 条 SLO 规则直接映射
> audit-api `/metrics`；消费滞后与签名失败依赖生产形态指标（本机不提供，
> 规则已声明待生产接入）；readyz 的 store/archive 503 由部署侧探针负责
> （本机以 `AuditAPIDown`（up=0）兜底）。

| 告警 | 表达式 | 级别 | 动作 |
|---|---|---|---|
| 接入错误率超限 | `rate(audit_http_errors_total[5m]) / rate(audit_http_requests_total[5m]) > 0.01` | P1 | 检查 ingest 与存储依赖 |
| 去重命中异常 | `increase(audit_ingest_duplicates_total[5m]) > 10×increase(audit_ingest_requests_total[5m])` | P3 | 检查来源重试风暴 |
| 内容冲突（疑似重放/错误重发） | `increase(audit_ingest_conflicts_total[5m]) > 0` | P2 | 调查 event_id 复用 |
| 配额拒绝 | `increase(audit_ingest_quota_exceeded_total[5m]) > 0` | P3 | 通知租户，检查配额 |
| 完整性验证失败 | `audit_integrity_checks_total{result="invalid"} > 0` | **P0** | 立即隔离证据链，人工核查 |
| 请求延迟劣化 | `histogram_quantile(0.99, rate(audit_http_request_duration_seconds_bucket[5m])) > 1` | P2 | 检查资源与背压 |
| 归档落后 | readiness 探针 `archive_unavailable` | P1 | WORM 目标不可用时停止新归档声明 |
| 消费滞后 | Kafka consumer lag > 阈值（生产） | P2 | 扩容 consumer/projector |
| 签名失败 | worker 日志 `sign aggregate checkpoint` 错误 | P0 | KMS/Vault 不可用时停止新 checkpoint |

## 3. 错误预算

按 30 天窗口：

- **接入**：可用性 99.99% → 预算 4.32 分钟停机/月；5xx 或 accepted 延迟超 SLO 的请求计入消耗。
- **查询**：可用性 99.95% → 预算 21.6 分钟/月。
- **ledgered 延迟**：p99 > 2s 的分钟数计入消耗（生产异步链路）。
- **完整性**：验证失败不计入可用性预算，而是独立的安全事件（P0），与错误预算解耦。

消耗超过 50%：冻结低风险发布，优先修复可靠性问题（与 §21 分阶段退出条件一致）。

## 4. 未在本机宣称的 SLO

跨区 RPO/RTO、50k/100k events/s 吞吐、三可用区故障切换的 SLO 必须经多节点
演练与容量报告验证后才能声明（见 VALIDATION_PLAN「不在本机宣称通过的项目」）。
