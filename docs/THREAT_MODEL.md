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
| **Tampering** | 篡改账本事件、段清单、签名 | 分段哈希链（prev_hash/head_hash）、Merkle Root、HMAC 签名 checkpoint（生产 KMS/HSM 只签名不可导出）；WORM 归档（O_EXCL 只读 + fsync）；`VerifyIntegrity` 逐流校验，且逐事件把内容摘要与存储的 `SourceDigest` 比对——先反向重建保护前 payload（去掉 `*__search_digest`、用与写入相同的 AAD 解密 `encrypted_fields`），再从内容重新推导摘要；篡改内容而不一致地重推摘要（或复用旧摘要）必然报 `content digest mismatch`；缺失 `SourceDigest` 的事件按 fail-closed 报 `missing source_digest`（详见 §3.5） |
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

### 3.5 内容认证完整性校验（VerifyIntegrity）

- 链式校验（`prev_hash`/`hash`/段 Merkle/签名）绑定的是存储的 `SourceDigest`
  字段本身；自 2026-08 起 `VerifyIntegrity` 额外对每个事件做**内容认证**：
  用事件摄入时的 schema 版本（版本精确查找）把 payload 重建为保护前形态
  （深拷贝 → 删除 `field__search_digest` → 用 `tenant/field/event_id` AAD
  解密 `encrypted_fields`），再以 `EventContentDigest` 重新推导并与存储的
  `SourceDigest` 比对。不匹配报 `content digest mismatch`，字段解密失败报
  `cannot decrypt field …`，schema 版本缺失报 `schema … vN not found`，摘要
  为空报 `missing source_digest` —— 全部是新增错误串，`IntegrityResult` 的
  JSON 形状与 OpenAPI 契约不变。
- 运行不变式（违反会误报）：**已注册 schema 版本的**
  `encrypted_fields`/`searchable_fields` 列表不可原地修改（应注册新版本）；
  **加密密钥必须与摄入时一致**（轮换会导致敏感事件 `cannot decrypt` 假
  阴性，需带历史密钥验证或重加密）；数字编码的摄入/重建两侧必须一致
  （见 `TestVerifyIntegrityLargeIntSensitiveField` 耦合哨兵）。
- 全面重算型攻击者（同时持有签名密钥）仍无法被逐事件检查发现——密封段
  的签名清单与 WORM 归档（O_EXCL）是最终锚点。签名密钥由
  `AUDIT_SIGNING_SECRET`/`AUDIT_ENCRYPTION_KEY` 显式配置：公开默认值
  （`development-signing-key-change-me`/`development-encryption-key-change-me`）、
  空值与两值相同在非开发模式一律启动失败（`service.New` 校验 + 二进制
  非零退出，`-check-config` 可预检），开发模式必须显式
  `AUDIT_ALLOW_DEV_SECRETS=true`。曾以默认密钥运行过的部署必须把既有
  证据视为已泄露（签名可伪造、密文可解密），需要轮换密钥并重新密封；
  本参考实现不做多密钥历史验证。

### 3.6 供应链与部署
- 依赖锁定（go.sum）、工程门禁（gofmt/复杂度/架构方向/路由契约/竞态/
  构建）；生产：最小运行时镜像、非 root、SBOM、镜像签名、依赖扫描、
  GitOps 发布、Canary 验证。
- 部署前必须通过 `audit-api -check-config` / `audit-governance-worker
  -check-config` 预检（退出码 0 且无 `=well-known-default` 警告），API
  与 worker 必须使用相同的 `AUDIT_SIGNING_SECRET`/`AUDIT_ENCRYPTION_KEY`。
  预检同时校验认证配置（与启动同一规则）：无 JWT 信任源或仅凭
  `-allow-dev-auth` 的开发认证均失败关闭；开发认证白名单仅接受环境变量
  `AUDIT_ALLOW_DEV_AUTH=true`（见 ADR-0007）。

### 3.7 外部基础设施传输（B1/B2）
- **边界 B1（进程 → Vault Transit）：** `AUDIT_VAULT_ADDR` 必须为
  `https://…`，或为本机 loopback（`localhost`/`host.docker.internal`/
  `gateway.docker.internal`/loopback IP）+ 显式
  `AUDIT_ALLOW_INSECURE_VAULT_LOOPBACK=true`。非 loopback 明文 http
  无论该开关如何均启动/预检失败关闭（与 JWKS 同一规则，见
  `internal/auth/verifier.go` 与 `internal/runtimeconfig` 中镜像的
  `loopbackHost`）。scheme 缺失（`vault:8200`）、空 host（`https://:8200`）、
  带路径/query/userinfo/空白 的地址同样失败关闭，错误信息可操作且永不
  回显地址中的凭据（userinfo/query 会被剥离）。
- **边界 B2（进程 → S3 归档）：** `AUDIT_S3_USE_SSL=true` 时 S3 走 TLS
  （`transport_s3=tls`）；`https://` scheme 端点必须与该开关一致，否则
  失败关闭（绝不静默降级为明文）。**已记录的残余风险（决策，见
  design-rev2 §1）：** scheme 缺失的非 loopback 端点 + `AUDIT_S3_USE_SSL`
  默认 false 时允许明文 http（本机开发/验证栈 `localhost:19010`、
  `deploy/` `minio:9000` 兼容），静态密钥与归档证据在明文链路上可被
  MITM 读取——该不对称（与 Vault 侧分类禁止相反）是有意的兼容性权衡，
  由 `check_config=ok` 的 `transport_s3=http` 与 CI 断言
  `transport_s3=tls` 观察式强制；改变该姿势的硬化（默认 true 或新增
  S3 loopback 开关）超出当前需求范围，被
  `TestS3PlaintextNonLoopbackPermitted` 钉住。
- 两个二进制（audit-api / audit-governance-worker）的 `check_config=ok`
  逐腿报告 `transport_s3=`/`transport_vault=`（tls/http/local），格式串
  字节一致，CI 可对两腿分别断言（REQ-TLS-6/F3）。

## 4. 不在本机验证的攻击面

- 跨地域双写破坏链（tenant home region + 租约 + fencing）
- KMS/HSM 密钥轮换与销毁流程
- 归档账号权限分离的强制审计
- 大规模跨租户暴力枚举（需要真实流量与限流网关）
