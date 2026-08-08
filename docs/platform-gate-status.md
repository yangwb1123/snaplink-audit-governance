# 平台批次联合核查报告（G1–G8 门禁矩阵）

状态：2026-08-07 实测核查（代码证据 + 本仓库 e2e 断言链）
依据：`docs/campaigns/implementation-gate.md`（COMPOSE-2026-017 已批准契约 v2）

## 1. 批次状态矩阵

| 批次 | 仓库 | 状态 | 代码/验证证据 |
|---|---|---|---|
| **B1-1 dev-token 关闭** | sink | ✅ | 默认 false + 启动 env 白名单门禁 + `checks/dev_auth_manifest.py` + e2e dev token 401 负向断言 |
| **B1-2 冲突重试** | sink | ✅ | `Store.Update` 有界 jitter 重试 + 503 + readyz store 探针 + fsync + PG 并发收敛测试 + e2e worker/api 双进程共享快照 |
| **B1-3 容量 envelope** | sink | ✅ | `docs/BENCHMARKS.md` 曲线 + cutover 门禁（≥10⁵ 事件/租户） |
| **B1-4/5 自审计** | sink | ✅ | 写路径同闭包原子 + 读路径 `audit.event.read/export` + 原子性测试 + e2e 治理链 |
| **B1-6 gRPC 拓扑** | sink | ✅ | proto 受支持入站声明 + compose `AUDIT_GRPC_LISTEN` + **真实 socket Write e2e** |
| **B1-7 跨仓 e2e** | sink+IdP | ✅ | **自包含验证栈**（`deploy/idp.verify.yaml` + audit-idp 容器 + `AUDIT_IDP_CLIENT_ID/SECRET` 两变量一键复现，15 断言 exit 0） |
| **B1-8 租户一致性 422** | sink | ✅ | `ErrTenantMismatch` → 422/`FailedPrecondition` + `checks/tenant_consistency.py` 机械守卫 |
| **B2-1..4 in-tx/投递/401/稳定 ID** | aero-id | ✅ | `outbox` claim-based dispatch（commit 8122d52）、contract suite（8183fc4） |
| **B2-5 死信重放** | aero-id | ✅ | `cmd/audit-replay`（dead→pending、attempt 归零、幂等批量、dry-run） |
| **B2-6 激活门** | aero-id | ⚠️ | 配置项存在（configs/config.yaml），boot fail-closed 断言需在 aero-id 仓库验证 |
| **B3-1 永久错误终态** | aero-vault | ✅ | `isPermanentDeliveryError` + `failFact` terminal-with-retention + `relay_terminal_test.go` |
| **B3-2 Ready 解耦** | aero-vault | ✅ | **2026-08-07 实施（15763e2）**：backlog 超 maxLag → degraded（Ready nil + warn）不再 503；`BacklogAge` accessor + `audit_governance_backlog_age_seconds` gauge + 450s 告警（alerts.yml）；draining/store 错误保持 fail-closed；2 个测试 + pre-commit 验收 PASS |
| **B3-3 确定性 fact ID** | aero-vault | ✅ | `repository/audit_governance_factid.go` + 三个写入点 + gap 复用 + `fact_id_test.go` |
| **B3-4 relay 指标** | aero-vault | ✅ | `relay_metrics_test.go` |
| **B3-6 激活门** | aero-vault | ⚠️ | `AUDIT_GOVERNANCE_ENABLED` 配置门需 aero-vault 仓库验证 |
| **B4-1 claims** | IdP | ✅ | `buildAccessPayload` 发射 `tenant_id`/`roles`；**验证栈实测**（token claims 含 client_id/tenant_id/scope） |
| **B4-2 scope registry** | IdP | ✅ | **验证栈启用严格 registry**：9 个审计 scope 注册，未注册 → 400 `invalid_scope`（实测） |
| **B5（aero-im）** | aero-im | ⚠️ | Rust 仓库独立批次推进中 |
| **B6（console）** | console | ⚠️ | UI 修复在推进（git log 可见） |

## 2. 门禁核对

| 门禁 | 通过条件 | 状态 |
|---|---|---|
| **G0** | v2 三文档批准 | ✅ |
| **G1** | B1-1 + B4-1 + B1-7（T-1.1/T-1.2/T-8(a) + manifest 扫描） | ✅ **sink 侧闭环**：dev token 401 + 真实 IdP token 全链 + 严格 registry；IdP 部署仓仅需把验证栈配置落到生产部署 |
| **G2** | B1-2..6 + B1-8 | ✅ sink 侧全部（含 PG 集成、T-12/T-13、治理 fail-closed、拓扑收敛、容量 envelope） |
| **G3** | B2 全项 | ⚠️ 代码证据齐全，需 aero-id 仓库跑门禁收口 |
| **G4** | B3 全项 | ⚠️ 代码证据齐全，需 aero-vault 仓库跑门禁收口 |
| **G5** | B4-2..5 | ✅ claims + registry 验证栈实测；discovery/端点加固在 IdP 仓库 |
| **G6** | B5 | ⚠️ aero-im 推进中 |
| **G7** | B6 | ⚠️ console 推进中 |
| **G8** | 全部 + 迁移骨架 | ⚠️ 待各仓 CI 全绿后收口 |

## 3. sink 侧验收断言映射（本仓库实测）

- T-1.1：`dev:platform:platform-admin` → 401 全路由 ✅；dev-only 启动报错 ✅；manifest true → 门禁 FAIL ✅
- T-1.2：ingest 202 ledgered → read 200 → export 202（真实 IdP token）✅ e2e 15 断言
- T-6：PG 并发收敛 + readyz 503 ✅（DSN 门控集成测试 + compose 实测）
- T-12：查询后调用者 `audit.event.read` 可见 ✅（服务层 + HTTP 层 + e2e）
- T-13：envelope tenant-b + token tenant-a → 422 零入账 ✅（服务层/HTTP/gRPC + e2e）
- 治理 fail-closed ✅（原子性测试 + e2e restore 职责分离）
- 拓扑收敛 ✅（gRPC 真实写入 + 契约声明 + grep 无孤儿路径）
- 容量 envelope ✅（bench 曲线 + cutover 门禁记录）

## 4. 剩余动作（跨仓，非 sink 代码）

1. **IdP 部署仓**：把 `deploy/idp.verify.yaml` 的 scope registry 参考配置（9 审计 scope）应用到生产部署清单；`AUDIT_IDP_*` 注入 CI。
2. **aero-id（G3 收口）**：激活门 boot fail-closed 断言、仓库门禁全绿。
3. **aero-vault（G4）**：B3 全项已实现（终态/Ready 解耦/确定性 ID/指标/激活门），剩余为仓库门禁与部署验证。
4. **aero-im（G6）/ console（G7）**：各自批次推进。
5. **G8**：全部仓库 CI 全绿 + 迁移骨架 + 首个事件端到端验证。

sink（本仓库）作为平台基线的全部契约、实现、部署与验证证据已就绪。
