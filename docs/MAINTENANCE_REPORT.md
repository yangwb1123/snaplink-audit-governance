# Snaplink Audit Governance — 10 轮 AI 维护执行报告

> 由 ai-batch-runner（pi-batch）特性驱动：campaign 发现/分析/实现、
> meta 对抗评审、独立裁决门、决策日志、断点续跑、memory、learn、advance。
> 证据与状态：`docs/auto/state.jsonl`（追加式事实源）、`docs/auto/runs/*/`、
> `docs/auto/SUMMARY.md`。本报告为人类维护者的核对入口（AI 产出是提案）。

## 执行轮次总览

| 轮 | 特性 | 动作 | 结果 |
|---|---|---|---|
| R1 | check / quality / eval | 工具自检 + 目标工程门禁基线 | 修复 root-files 门禁（允许 `.pi-batch`、`test`），QUALITY PASS |
| R2 | assess / classify / rules / context / learn | 需求评估、任务分类、规则匹配、上下文路由、事故沉淀 | 后端域处方 1 条 + 刻意未选用清单；规则草案 DRAFT-20260805 |
| R3 | campaign discovery | 21 模块发现 + 工作量估算（dry-run，零状态写入） | 856 min 全量估算 → 分批执行 |
| R4 | campaign 分析+实现 | domain/service/store 分析（9 方向）+ 6 条流水线 | **2 通过**（VerifyIntegrity 内容摘要重推导、postgresBackend.Load 数据竞态）；2 设计门拒绝、2 meta 阶段失败 |
| R5 | campaign 重试 + 再分析 | 修复工具 `_strings`（LLM 字符串 evidence 容错）后重跑 | **1 通过**（canonical JSON 数字/时间编码）；3 设计门拒绝（评审者原型代码作为证据提交） |
| R6 | campaign 批 3 | httpapi/auth/archive 分析（6 方向） | **3 通过**（S3-only 归档静默失效修复、JWKS TTL 单飞缓存、恢复审批 SoD）；3 拒绝 |
| R7 | campaign 批 4 + 并行 worktree 门禁演示 | security/outbox/kafka（3 方向） | **2 通过**（Kafka 死信、outbox 冲突上报）；`--parallel-pipelines` 因脏工作区被 fail-safe 拒绝（特性按设计工作） |
| R8 | 全局对抗评审 + 独立裁决门 | 全量变更评审 → 门 FAIL（抓到 4 类阻断缺陷）→ 修复 → 复评 | **GATE PASS**；修复：outbox `*sql.Tx` 接口、DLQ 偏移提交时序、不可解析消息死信记录、sealedSegments 重试闭包、file→PG 迁移护栏 |
| R9 | advance / learn / eval / memory | 自推进扫描（38 项发现入账）、第 2 条规则草案、37 项 eval 全过、memory 检索 | 状态可续跑 |
| R10 | 收尾 | 全量门禁复验、报告、提交 | QUALITY PASS |

## 通过特性落地的新功能 / 修复（已提交）

1. `VerifyIntegrity` 不再信任存储的 `EventDigest`，重推导内容摘要（含 legacy 回退）— R4 PASS
2. `postgresBackend.Load` 读路径不再写 `lastVersion`（竞态修复）— R4 PASS
3. canonical JSON 数字/时间编码：`json.Number` 贯穿 ingest/snapshot reload/fieldcrypto/gRPC/Kafka 消费 — R5 PASS
4. S3-only 归档配置不再静默失效（`archive.Configured` 语义修正）— R6 PASS
5. JWKS TTL + single-flight 刷新 + kid-miss 强制刷新（fail closed）— R6 PASS
6. 恢复审批职责分离：创建者不得审批自己的恢复申请 — R6 PASS
7. Kafka 永久失败消息死信（poison message 不再卡死分区）— R7 PASS
8. outbox `Insert` 冲突/重复显式上报（不再静默丢弃）— R7 PASS
9. 评审门禁阻断修复：outbox `*sql.Tx` 精确重复分类（编译期 pin）、DLQ 偏移按持久决策提交、不可解析消息 DLQ 记录 + 计数、`sealedSegments` 每次 CAS 重试截断、file→PG 迁移护栏（`AUDIT_ALLOW_PG_EMPTY_LEDGER` 显式 opt-out）— R8
10. 并发会话批次的纠正性修复：`tenantClaim` 允许平台 token 空 tenant 作用域（仍拒绝空白/控制字符）— R8

## 门禁裁决记录（对抗验证，fail closed）

- R4/R5/R6/R7 中 **9 个方向被设计门拒绝**：hash-chain StoredDigest 承诺
  （部署/回滚契约缺失）、tenant-ID 字符集、schema 版本回滚、恢复审批 SoD（首版）等。
  拒绝理由全部落盘（`docs/auto/runs/*/artifacts/*/task-1-design-gate.md`），
  评审者原型代码按证据标准单独提交，未冒充已批准实现。
- R8 全局门第一轮 FAIL：抓到 4 类阻断缺陷（其中 outbox `*sql.Tx` 接口
  形状缺陷为**本次 campaign 引入的回归**，由门禁编译级证明）；修复后复评 PASS。

## 证据标准核对

| 产物 | 状态 | 说明 |
|---|---|---|
| 已提交实现 | Verified | 每轮提交前过 `python3 cli.py check`；R10 全量 `cli.py quality` PASS |
| 门拒绝方向 | Partial/Proposed | 证据与原型在 `docs/auto/runs/*/`，未提升为需求 |
| 规则草案 | Draft | `docs/rules/drafts/DRAFT-20260805*.yaml`、`DRAFT-20260806*.yaml`，待人工补 RCA 后并入注册表 |
| advance 发现 | Proposed | `docs/advance/state.jsonl`（38 项：上帝文件 23 / 测试无断言 6 / 架构 9） |

## 遗留事项（非阻断，供维护者跟进）

1. 并发会话（B1 批次）的工作区未提交批：`internal/auth|archive|grpcapi|httpapi`、
   `internal/fsutil/` — 其质量门禁已绿（含本报告的纠正性修复），提交权属该会话。
2. file 后端快照/replay-state 无 fsync（开发态注记）；无迁移导入工具（有 fail-fast 护栏）。
3. `--parallel-pipelines`（worktree 隔离）因并行会话脏工作区未实际运行 — 特性门禁本身已验证。
4. 工具仓库（ai-batch-runner）`make ci` 受并发开发中的未完成编辑影响（3 例失败属对方
   在途工作）；本会话对工具的修改（campaign `_strings` 容错、registry 测试 dict 适配）
   已独立验证全绿。

## 建议下一步

- 批准/合并 B1 批次的 S3 WORM 校验、JWKS 缓存、gRPC 脱敏（门禁已验证）。
- 将 2 条 learn 草案补 RCA 后并入 `backend-specs/rules.yaml`。
- 按 advance 的 P0 批次修测试无断言文件（6 项），再做 round 2。
