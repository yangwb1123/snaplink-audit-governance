# Audit Governance Web

本目录是 `snaplink-audit-governance` 的 Iris UI React 管理端。它直接调用本仓库
`/api/v1/*` 接口，并通过 Snaplink Hosted Login 使用 OIDC Authorization Code +
PKCE S256 登录。管理端覆盖跨服务高级事件检索（operation/correlation/trace/aggregate）、
完整时间窗口的结果/事件类型/来源 Facets 态势、操作回放、聚合对象版本历史、来源 Client 精确
绑定、事件 Schema 必填/允许/加密/可搜索字段策略、保留策略、完整性验证、法律保留、
合规导出和恢复审批。

法律保留和合规导出可分别设置时间范围、来源系统、事件类型、Operation、Actor、
Correlation 与结果条件；服务端使用同一查询契约完成读取自审计、保留冲突检查和证据选取。

治理目录还会按六项目平台部署契约检查 Aero ID、Aero IM、Aero Vault 的 Snaplink
M2M Client、租户来源与固定 Schema 版本。Aero ID/Vault 的 Source ID 由服务端按租户
派生，Web 只检查公开前缀，不接触派生密钥或 Client Secret。

## 本地运行

在仓库根目录执行：

```sh
corepack pnpm install --dir web
make web-dev
```

默认地址：

- Web：`http://localhost:5178`
- Audit API：`/audit-api` 代理到 `http://localhost:8089`
- Snaplink API：`/snaplink-api` 代理到 `http://localhost:18082`
- Snaplink Hosted Login：`http://localhost:4444/login/`

验证栈将 Audit API 暴露在宿主机 `19089` 时，使用：

```sh
AUDIT_API_PROXY=http://localhost:19089 make web-dev
```

复制 `.env.example` 可覆盖浏览器配置。生产部署还可以在主脚本之前通过
`public/config.js` 注入 `window.__AUDIT_GOVERNANCE_CONFIG__`；容器部署时可直接挂载
新的 `/usr/share/nginx/html/config.js`，无需重新构建前端。

## Snaplink 客户端契约

在 Snaplink 注册 public browser client：

- `client_id`: `audit-governance-web`
- `token_endpoint_auth_method`: `none`
- `redirect_uri`: `http://localhost:5178/auth/callback`，生产环境改为精确 HTTPS 地址
- `post_logout_redirect_uri`: `http://localhost:5178/`
- PKCE：仅 `S256`
- scopes：`openid profile` 加 Audit API 所需的 `audit:*` 最小权限集合
- resource / access-token audience：`audit-governance`

前端通过 discovery 的 `jwks_uri` 校验 ID token 和 access token 的签名、issuer、
audience 与有效期，并校验 state 和 nonce。令牌仅保存在 React 内存中；
sessionStorage 只保存一次性的 state、nonce 和 PKCE verifier。Audit API 仍是最终授权方。

## 检查与构建

```sh
make web-check
make web-build
docker build -f web/Dockerfile -t snaplink-audit-governance-web:local web
```

容器默认监听 `8080`，并将同源路径代理到：

- `AUDIT_API_UPSTREAM=http://audit-api:8089`
- `SNAPLINK_API_UPSTREAM=http://snaplink:8082`

可通过同名环境变量覆盖。应用使用固定版本的 `@iris-ui-kit/react` 及其基础包，
页面源码保持在本仓库内，构建不依赖 `/home` 下的绝对路径。

## 完整联调栈

下面的命令会先运行真实令牌 full-stack，引导 PostgreSQL、Kafka、WORM 归档和
Snaplink IdP，再启动本 Web 与 Snaplink Console Hosted Login：

```sh
make web-stack-up
```

完成后访问：

- Audit Governance Web：`http://localhost:15178`
- Snaplink Hosted Login：`http://localhost:4444/login/`
- 本机申请账号：`audit-operator` / `audit-demo-password`
- 本机审批账号：`audit-approver` / `audit-demo-password`

恢复申请强制职责分离：申请人不能批准或拒绝自己的申请。申请后退出 Snaplink，再使用
审批账号登录并按 restore run id 查询，即可完成第二主体决策。Web 在创建审批单前还会
强制完成恢复预演，并将预演绑定到当前租户、Operation ID 与原因；修改任一参数都必须
重新预演，提交前还需二次确认。服务端创建时会重新回放操作，前端预演不替代服务端校验。

该命令还会执行真实的 Authorization Code + PKCE S256 流程，校验 JWT audience、
issuer、tenant、ID token nonce 和 `audit:*` 授权 scopes，并携带该 token 调用事件查询、
Facets 聚合、聚合历史与来源 Client 绑定接口。验证链还会使用浏览器主体写入恢复测试事件，
完成预演、创建审批单、同主体 403 拒绝和第二个 Snaplink 用户批准。角色分配
仍保留给 Snaplink 控制台展示，Audit API 的最终授权以 access token scopes 为准。已有完整栈时
可用 `AUDIT_WEB_REUSE_STACK=true make web-stack-up` 跳过基础设施重建；单独重跑认证
链使用 `make web-auth-e2e`。停止本机验证栈使用 `make web-stack-down`。
