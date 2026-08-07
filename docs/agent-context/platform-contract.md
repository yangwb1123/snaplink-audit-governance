# 平台契约（本仓库视角：snaplink-audit-governance）

> 自动生成自 platform-map.yaml（组合模型）。本仓库是**独立可部署产品**，
> 通过标准协议与其他服务组合；核心代码只依赖端口，不依赖兄弟项目类名。

## 本仓库角色
`audit-governance-core`

## 拥有的领域资源（唯一写入源）
- `audit-event`
- `hash-chain`
- `legal-hold`
- `retention-policy`
- `export`
- `risk-finding`
- `governance-approval`

## 能力清单（capabilities）
- event-ingest
- audit-query
- integrity-verification
- legal-hold
- compliance-export
- risk-detection
- governance-workflow

## 集成端口（可选适配器，不硬编码）
- 无（本仓库是被集成方）

## 数据所有权（全局唯一写入源）
| 数据 | 权威源 |
|---|---|
| user-profile | aero-id |
| organization | aero-id |
| session | snaplink |
| role-binding | snaplink |
| permission-catalog | snaplink |
| file-metadata | aero-vault |
| share-link | aero-vault |
| message | aero-im |
| notification-preference | aero-im |
| audit-event | **本仓库** |
| legal-hold | **本仓库** |

## 同步依赖规则
- 同步链上限：3
- 允许：business-service -> snaplink (authorization-check)
- 允许：console-bff -> any service (management api)
- 允许：service -> audit-governance (event-ingest, 仅异步允许)
- 禁止：direct-database-access
- 禁止：bidirectional-sync
- 禁止：audit-callback-into-business-service
- 禁止：im-serial-fetch-context-on-send
- 禁止：token-validation-calls-aero-id-per-request
- 禁止：console-browser-bypasses-service-api

## 审计集成（L0/L1/L2 分级，full 组合部署用 L2）
- L0: local-log-or-db
- L1: standard-event-protocol
- L2: governance-adapter

## 命名规范（跨服务统一，防六套标准）
- 权限：`<domain>.<resource>.<action>`（如 vault.file.delete）
- 事件：`<domain>.<resource>.<action>@vN`（如 vault.file.deleted@1.1）
- 全平台使用不可变 UUID 作为跨系统主键；不依赖用户名/邮箱

## 变更纪律
- 单项目需求只改本仓库；跨项目变更必须同时改变双方契约并有 Change Manifest
- AI 产出是提案：按 Verified/Partial/Missing/Proposed 核对后提升