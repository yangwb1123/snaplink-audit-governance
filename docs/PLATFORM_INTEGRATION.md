# Aero 平台联调（本机）

本页说明如何把本仓库放进真实本地平台栈（Snaplink + Aero ID/IM/Vault）做
集成验收。它验证**入站契约**：Snaplink 给身份，Aero 给审计事实，Snaplink
Console 读证据；本仓库不反向调用业务服务。

平台编排来自兄弟仓 `aero-im`（`deploy/platform/compose.yaml`），本仓库是其中的
`audit-governance` 服务（从当前源码构建）。**不要修改兄弟仓**；所有本地密钥放在
本仓库之外的私有 env 文件。

## 1. 启动平台栈

```bash
# 在私有目录准备 env（不要把真实密钥写进仓库）
# 参考 aero-im/deploy/platform/.env.example 生成全部 change/replace 值
docker compose --env-file /path/to/private/platform.env \
  -p audit-platform-int \
  -f ~/aero-im/deploy/platform/compose.yaml \
  up -d --build snaplink-init snaplink snaplink-config-sync \
  audit-governance-postgres audit-governance-init audit-governance-migrate \
  audit-governance-edge audit-governance audit-bootstrap
```

入口：Snaplink `http://localhost:28080`、Audit API `http://localhost:28089`。
`audit-bootstrap` 会自动创建租户 `platform-local`、三个 Aero 来源
（`aero-id.*`、`aero-im.source`、`aero-vault.*`）和对应 Schema。

> 用 `-p` 指定独立 project，避免覆盖已有 `aero-platform` 卷与网络。
> 只清理由本次 project 拥有的容器/卷；未知卷保留。

## 2. 协议级联调检查

```bash
export AUDIT_API_URL=http://127.0.0.1:28089
export SNAPLINK_TOKEN_URL=http://127.0.0.1:28080/token
export AUDIT_TENANT=platform-local
# 从私有 env 读取，绝不写入日志/argv
set -a; . /path/to/private/platform.env; set +a
export PLATFORM_BOOTSTRAP_CLIENT_SECRET="$SNAPLINK_CLIENT_SECRET_PLATFORM_AUDIT_BOOTSTRAP"
export AERO_ID_AUDIT_SECRET="$AERO_ID_AUDIT_CLIENT_SECRET_PLATFORM_LOCAL"
export AERO_IM_AUDIT_SECRET="$SNAPLINK_CLIENT_SECRET_AERO_IM_AUDIT"
export AERO_VAULT_AUDIT_SECRET="$SNAPLINK_CLIENT_SECRET_AERO_VAULT_AUDIT"
python3 test/e2e/platform_audit_integration.py
```

覆盖：平台 token/租户、三来源/三 Schema 契约、逐客户端摄入、重复幂等、
同 ID 异内容冲突、租户错绑、Schema 字段拒绝、跨来源拒绝、租户查询、
Snaplink Console 兼容读（含 `client_id` 过滤与 `no-store`）、回执元数据。

## 3. Web 联调

```bash
cd web
AUDIT_API_PROXY=http://localhost:28089 \
SNAPLINK_API_PROXY=http://localhost:28080 \
VITE_AUDIT_API_BASE_URL=/audit-api \
VITE_SNAPLINK_ISSUER=http://localhost:28080 \
VITE_SNAPLINK_DISCOVERY_URL=/snaplink-api/.well-known/openid-configuration \
VITE_SNAPLINK_API_BASE_URL=/snaplink-api \
VITE_SNAPLINK_HOSTED_LOGIN_URL=http://localhost:28080/auth/login \
VITE_SNAPLINK_CLIENT_ID=audit-governance-web \
VITE_SNAPLINK_RESOURCE=audit-governance \
VITE_SNAPLINK_SCOPE="openid profile audit:event:read audit:operation:read audit:export:create audit:integrity:verify audit:legal_hold:manage audit:policy:read audit:policy:write audit:platform:cross_tenant" \
pnpm dev
```

```bash
# 浏览器实测：真实 PKCE/JWT、目录接入状态、事件、详情回执；token 不落存储
AUDIT_WEB_URL=http://localhost:5178 \
AUDIT_API_URL=http://localhost:28089 \
SNAPLINK_IDP_URL=http://localhost:28080 \
SNAPLINK_HOSTED_LOGIN_URL=http://localhost:28080/auth/login \
AUDIT_WEB_PASSWORD="$SNAPLINK_ADMIN_PASSWORD" \
pnpm exec playwright test test/browser/platform.e2e.ts
```

浏览器测试走 Snaplink 的 **password provider 适配器**（真实 code/PKCE/JWT/JWKS），
不是兄弟仓的 Flutter Hosted Login UI；完整 Hosted Login UI 仍需单独验收。

## 已知边界

- Snaplink 的 discovery 端点按请求 Host 生成。必须统一使用 `localhost`（不要混用
  `127.0.0.1`），否则 issuer 与 endpoint 不同源，Web 会按设计失败关闭。
- Aero 服务的真实 relay（Vault/ID/IM 进程写入）需要额外构建对应镜像；本页的
  协议级检查证明契约与授权链路，Aero Vault/ID 实际 relay 另行验收。
- 本地栈不是生产：不声明容量、容灾、正式 WORM/COMPLIANCE 或安全发布通过。
