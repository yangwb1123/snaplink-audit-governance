实施提案已产出：`docs/proposals/implementation-b1-sink.md`（18KB）。核心结论：

> 当前树校正（2026-08-20）：本摘要来自早期 B1 评估。B1-3 已按方案 B
> 落入 `docs/BENCHMARKS.md` 的容量 envelope 与 cutover 门禁；B1-7 已有
> `scripts/mint-token.sh` 与 verify IdP 路径；B1-8 的 HTTP/gRPC/OpenAPI
> tenant 422 已同步。关系 ledger 接线和跨仓真实多节点验收仍不在本仓参考
> 实现的声明范围内，最终质量门禁为 `QUALITY PASS`。

## 逐项状态（基于工作树实测，非清单照抄）

| 项 | 状态 | 关键证据 |
|---|---|---|
| B1-1 dev-token 关闭 | ⚠️ 部分 | 默认 false + check-config env 白名单已提交（`main.go:51`）；**真实启动仍放行 dev-only 配置**（`verifier.go:30` 末分支 + `auth_test.go:158` 钉住相反行为）；**manifest 扫描不存在**；compose 仍 `AUDIT_ALLOW_DEV_AUTH: "true"` |
| B1-2 冲突重试 | ✅ 已实现（未提交原型） | `store.go` 3 次有界 jitter 重试 + PG 乐观锁 + 503 映射 + readyz probe + `conflict_retry_test.go`；剩 fsync（[PROPOSED]）+ T-6 PG 集成用例 |
| B1-3 ledger/容量 | ❌ 未做 | migration 001 全套关系表 **Verified 死代码**（app 只用 004 单行快照）；`QueryEvents` 仍 O(ledger)；建议本批做方案 B（容量 envelope + `BenchmarkQueryLargeLedger`），A 列 G8 |
| B1-4 self-audit writer | ✅ 基线已满足 | 全部治理操作 same-closure append，失败即回滚 |
| B1-5 read self-audit | ✅ 已实现（原型+测试） | 服务层 `recordReadAction`，fail-closed，传输不可绕过 |
| B1-6 gRPC 拓扑 | ⚠️ 代码✅/契约❌ | 三 RPC 统一 `Service.Ingest`、422 对齐；**OpenAPI 仍写 "Body tenant_id is ignored"**（与 B1-8 冲突）；kafka replay 原型是**孤儿且破坏门禁** |
| B1-7 跨仓 e2e | ❌ 未做 | fullstack.sh 全 dev token；`scripts/mint-token.sh` 不存在；依赖 B4-1（IdP） |
| B1-8 tenant 422 | ⚠️ 代码✅/契约❌ | `service.go:397-399` 已判 mismatch→422（**清单"静默重标"描述已过时**，:401 仅为空 envelope 盖章）；HTTP 422 + gRPC FailedPrecondition + 三层测试已钉；OpenAPI 未同步 |

## 阻塞性发现
- **`python3 cli.py quality` 当前 FAIL（exit 1）**：未提交的 `internal/kafka/replay.go:205` go vet 格式串错误（`%s` 绑定 int 参数）。实施第一动作必须是修复/移除该原型。
- **跨仓阻塞**：G1 联合门禁（B1-1+B4-1+B1-7）依赖 IdP 部署仓 B4-1，非本仓可控，需显式跟踪。

## 依赖与顺序
B1-1 → B1-2（独立）→ B1-4/5（同树就绪）→ B1-6（先恢复门禁绿）→ B1-8（契约合并到 B1-6 声明）→ B1-3（独立）→ B1-7 收口 G1。下游 B2–B6 全部以本仓为基线；B1-8 的 422 是 B2/B3 producer dead 终态的服务端前提，中间态存在生产者兼容窗口（风险表已列）。

## 未验证标注
7 处 [PROPOSED]（mint-token.sh、决策 #7、fsync、PG 集成用例、HTTP 层断言补充、replay 接线、`scripts/` 目录触碰 root 门禁）＋ 1 处已核实升级：`outbox/http.go` 422→Permanent 归类确认（T-13 producer dead 终态前提成立）。

门禁核对：G0 ✅；G1 ❌（manifest 扫描 + B1-7 + B4-1）；G2 ⚠️（T-12/T-13 代码层已绿，缺容量 envelope、契约声明、T-6 PG 用例）。
