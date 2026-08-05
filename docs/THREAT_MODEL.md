# Snaplink Audit Governance 威胁模型

状态：Draft  
日期：2026-08-04  
范围：当前仓库参考实现 + 架构规划中的生产形态

## 1. 信任边界

```
业务系统 (Outbox) ──► 接入层 (REST/gRPC) ──► 账本/控制面 ──► 查询/导出 ──► 审计员
      │                    │                     │
  (受信业务事务)      (不可信网络输入)      (受信服务进程)
外部用户 (OIDC) ──► 管理面 ──► 控制面操作 ──► 自审计轨迹
```

- 边界 A：公共网络 → 接入层（Bearer Token、请求体、路径参数全部不可信）
- 边界 B：业务系统事务 → outbox 表（业务库写入受信，但 payload 内容不可信）
- 边界 C：服务进程 → 归档存储（进程不得拥有缩短保留期权限）
- 边界 D：租户 A 数据 ↔ 租户 B 数据（所有读写路径的强制隔离）

## 2. STRIDE 分析

| 威胁 | 场景 | 缓解（对应实现） |
|---|---|---|
| **Spoofing** | 伪造来源系统写入他人租户事件；伪造管理身份 | 签名 Token 的 `client_id`/`azp` 绑定来源白名单（`AllowedClientIDs`）；`sub` 永不当客户端身份；JWT 仅接受 EdDSA/ES256/384/512/RS256/PS256 且严格匹配 kid/alg/kty/crv/use/key_ops；HS256 仅限显式本机模式 |
| **Tampering** | 篡改账本事件、段清单、签名 | 分段哈希链（prev_hash/head_hash）、Merkle Root、HMAC 签名 checkpoint（生产 KMS/HSM 只签名不可导出）；WORM 归档（O_EXCL 只读 + fsync）；`VerifyIntegrity` 逐流校验 |
| **Repudiation** | 管理员否认执行过策略变更/导出/审批 | append-only 自审计轨迹（`AdminAction`），与变更同事务原子写入；审批记录审批人/时间 |
| **Information Disclosure** | 跨租户读取、导出他人数据；敏感字段泄漏到日志 | 服务端 `(client_id, source_system)` 租户解析（body tenant 忽略）；所有查询强制租户过滤 + 时间范围；导出/下载租户鉴权；字段级 AES-GCM 加密 + 搜索摘要；`rejectSensitive` 拒绝密码/Token/私钥等进入事件；日志不打印 payload；指标禁用 user_id/event_id 标签 |
| **Denial of Service** | 超大 payload、压缩炸弹、批量滥用、慢查询 | `MaxBytesReader` + 单事件 256KB 上限；`page_size ≤ 1000` 游标分页；每租户 QPS/突发配额（429）；请求超时（Read/Write/IdleTimeout）；panic 恢复中间件 |
| **Elevation of Privilege** | 普通租户执行平台/跨租户操作；平台角色读取租户明文 | RBAC 九权限（`audit:platform:cross_tenant` 等）；平台 token 读取需显式 `tenant_id`；跨租户运维需 break-glass + 双人审批 + 有限时授权（规划） |

## 3. 关键攻击面细节

### 3.1 租户解析（边界 D）
- 写入：`client_id` + `source_system` 在服务端唯一反查租户；签名 tenant
  claim 只收窄；**零命中与跨租户多命中都 403 失败关闭**；同一
  client/source 组合禁止跨租户复用。
- 查询：`require` 校验 `claims.TenantID` 非空（写事件除外），平台 token
  必须显式带 `tenant_id` 参数。
- 数据层：PostgreSQL RLS（`app.tenant_id` 会话变量，绝不来自请求头）。

### 3.2 请求体注入
- 重复 JSON 值拒绝（`decodeBody` 二次 Decode 校验）；`X-Tenant-ID` 请求头
  永不参与解析；`X-Request-ID` 只用于关联，不用于授权。

### 3.3 敏感数据
- 事件 payload 递归扫描禁止 `password/token/private_key/device_credential/
  card_number/secret` 字段名（含嵌套与数组）。
- Schema 可声明 `encrypted_fields`（AES-GCM + AAD 绑定 tenant/field/
  event_id）与 `searchable_fields`（HMAC 摘要，查询只走摘要）。
- before/after 大快照生产走加密对象引用（`payload_ref`），事件只留摘要。

### 3.4 供应链与部署
- 依赖锁定（go.sum）、工程门禁（gofmt/复杂度/架构方向/路由契约/竞态/
  构建）；生产：最小运行时镜像、非 root、SBOM、镜像签名、依赖扫描、
  GitOps 发布、Canary 验证。

## 4. 不在本机验证的攻击面

- 跨地域双写破坏链（tenant home region + 租约 + fencing）
- KMS/HSM 密钥轮换与销毁流程
- 归档账号权限分离的强制审计
- 大规模跨租户暴力枚举（需要真实流量与限流网关）
