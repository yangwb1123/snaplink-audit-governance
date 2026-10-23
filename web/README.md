# Audit Governance Web

本目录是 `snaplink-audit-governance` 的 Iris UI Svelte 5 管理端。它直接调用本仓库
`/api/v1/*` 接口，并通过 Snaplink Hosted Login 使用 OIDC Authorization Code +
PKCE S256 登录。管理端覆盖跨服务高级事件检索（operation/correlation/trace/aggregate）、
完整时间窗口的结果/事件类型/来源 Facets 态势、操作回放、聚合对象版本历史、来源 Client 精确
绑定、事件 Schema 必填/允许/加密/可搜索字段策略、保留策略、完整性验证、法律保留、
合规导出和恢复审批。

法律保留和合规导出可分别设置时间范围、来源系统、事件类型、Operation、Actor、
Correlation 与结果条件；服务端使用同一查询契约完成读取自审计、保留冲突检查和证据选取。
平台会话必须先从成功加载的租户列表中选择有效租户，Web 才会挂载租户级页面并发出
scoped API 请求；没有可用租户时仍可通过治理目录创建租户，但不会降级为全租户审计查询。

治理目录还会按六项目平台部署契约检查 Aero ID、Aero IM、Aero Vault 的 Snaplink
M2M Client、租户来源与固定 Schema 版本。Aero ID/Vault 的 Source ID 由服务端按租户
派生，Web 只检查公开前缀，不接触派生密钥或 Client Secret。

目录的租户、来源、Schema、自审计与保留策略分别显示加载、成功、空态及错误；
单项失败不会吞掉其他项，读取错误可以显式重试。来源或 Schema 的刷新 / 重试会
同时启动两项独立读取，并立即撤销上次契约结论；只有当前租户、当前会话、同一
刷新代次的两项均成功（包括成功的空列表）才重新评估，否则明确显示“契约状态未知”。
保留策略仅在 API 明确返回 `404 not_found` 时显示未配置，不把失败当作默认策略。

## 管理操作自审计

菜单及静态直接路由 `#admin-actions` 提供独立的只读自审计页面；治理目录保留原有
最近记录预览，并链接到此页，二者都不是全量历史入口。权限沿用
`can(session, 'audit:policy:read')`，平台会话沿用 `can` 的平台语义，但必须先由 Shell
确认可用租户；未确认有效租户时不请求自审计，也不退化为跨租户读取。

页面复用 `AdminAction`、`listAdminActions` 和 `RequestScope`，每次显式为当前租户请求
最近最多 100 条（`limit=100`），保持服务端返回顺序；混入其他租户或缺少租户标识的响应
整体拒绝。明确显示已载入数和筛选命中数，忽略响应 `count`，不提供分页、全局总数或
完整性证明；不足 100 条也不表示全历史完整。
动作、操作者和目标类型 / ID 采用不区分大小写的文本包含匹配，多个条件同时满足，
只筛选已载入的这批记录，不追加请求或搜索更早记录。

筛选仅保存在该页面实例的内存中，不进入 URL、浏览器持久存储或日志。加载、读取失败
及只读重试、空列表与零命中分别呈现；所有返回字段按文本渲染，不将目标或详情当作
HTML / 链接。主体、访问 / ID 令牌、权限或租户变化会清空数据与筛选、取消旧读取，
即使传输忽略 `AbortSignal`，旧成功和旧失败也不能覆盖新页面。刷新与重试仅重新读取，
不会执行管理动作。

## 快速跳转与事件详情

全局快速跳转须先明确选择事件、操作或导出类型，再输入 ID；不猜测类型，
也不使用当前列表作为详情入口的前置条件。支持以下直接链接：

- 事件：`#events/event/<event_id>`
- 操作：`#operations/op/<operation_id>`（沿用操作时间线）
- 导出：`#evidence/exports/<export-id>`（沿用导出工作台）

构建、解析及一次性登录回跳共用 `src/pages/navigation.ts` 的白名单。事件 / 操作 ID
最多 256 个 UTF-16 单元，聚合类型最多 64；导出 / 恢复 ID 沿用服务端形态
`export-` / `restore-` 加 16–64 位小写十六进制。拒绝非法编码、重复编码、控制 / 格式
字符、路径分隔符、URL / query 注入及超长值。链接不携带或修改租户；仍须当前有效
租户与相应权限，API 始终是最终授权方。

事件详情独立读取认证事件和回执，分别处理加载、错误及重试；回执失败不隐藏已成功
读取的事件。`schema_id`、`schema_version`、`retention_class`、`data_classification`
只展示事件返回的历史值，不从列表快照或最新 Schema 推断，不增加策略读取权限。
切换租户、事件、会话或令牌后丢弃旧响应，即使传输忽略 `AbortSignal`；不会自动重发写入。

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
audience 与有效期，并校验 state 和 nonce。令牌仅保存在 Svelte 应用内存中；
sessionStorage 只保存一次性的 state、nonce、PKCE verifier 和经白名单校验的返回路由；不保存完整 URL、查询载荷或令牌，登录后消费删除。Audit API 仍是最终授权方。

## 治理任务工作台

证据区拆为导出任务、恢复审批、法律保留、完整性四个独立授权子页。
导出入口要求 `audit:export:create`，恢复读取要求 `audit:operation:read`，
保留管理要求 `audit:legal_hold:manage`，完整性要求 `audit:integrity:verify`；
恢复创建/决策暂沿用保留管理权限，服务端始终再次鉴权并禁止同主体自批。

`GET /api/v1/exports`、`GET /api/v1/restores` 支持实时游标分页、状态、申请人、
Operation 和创建时间筛选；`view=mine` 查看本人任务，恢复的 `view=actionable`
还要求决策权限，并只返回其他已知主体创建的待审批单。`count` 是本次请求筛选的
总匹配数量，不是剩余页数量。列表是运行状态，不是不可变证据快照。
导出列表不包含证据范围、对象路径、摘要、事件数量及原始失败诊断；可显示固定失败类别与原任务关联，详情与下载继续通过
Legal Hold 门禁，阻断响应兼容 `code=forbidden` 并增加 `reason=legal_hold`。

任务支持 `#evidence/exports/<id>` 和 `#evidence/restores/<id>` 深链接。
令牌不持久化，因此刷新需要重新登录；链接仍按当前有效租户解析，平台用户应重新
选择任务所属租户，链接自身不能取得租户权限。切换身份、令牌或租户会取消请求、
清理弹窗/预演/轮询，并通过请求代次丢弃晚到结果。进度未知只显示不定进度。

本人任务的列表/详情提供确认取消和关联新任务重试；平台权限不能代其他申请人操作。
取消只撤销控制面运行权，不保证远端写入停止或回滚，也不删除 WORM。重试保留原
终态、清旧 cursor、重新选择当前证据并检查 Hold/配额；同一原任务重复请求返回
同一后继，需要再试时指定失败后继。Hold 阻断详情时列表仍只提供安全摘要/本人控制，
不能借操作读取证据。超时/未知提交先只读刷新核对，不自动重发；切换令牌/权限/
租户/任务会清理确认并丢弃晚响应，操作结果须匹配当前 tenant/申请人/任务关联。

`started_at` 是 CAS 领取时的真实开始时间，旧任务可能缺失；新任务有私有 2 分钟
租约/30 秒心跳与过期 fencing（不在浏览器暴露 owner）。这不是进度、检查点或强杀
证明；仍无检查点续跑、分块认证下载或业务恢复执行按钮。详见 ADR-0019/0020。

## 检查与构建

```sh
make web-check
make web-build
docker build -f web/Dockerfile -t snaplink-audit-governance-web:local web
```

容器默认监听 `8080`，并将同源路径代理到：

- `AUDIT_API_UPSTREAM=http://audit-api:8089`
- `SNAPLINK_API_UPSTREAM=http://snaplink:8082`

可通过同名环境变量覆盖。应用使用固定版本的 `@iris-ui-kit/svelte`、Svelte 5 及其基础包，
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

## 浏览器回归

已有本机隔离服务（上述栈或本机真实 JWT API + Snaplink IdP）且 Web 地址与
public client 注册的 redirect_uri 一致时执行：

```sh
corepack pnpm --dir web browser:test
```

默认 Web/API/IdP 端口为 15178/19089/18082，默认使用系统 Chrome；可设置
`AUDIT_BROWSER_CHANNEL=chromium` 并安装相应 Playwright 浏览器。测试只接受 loopback
服务。它覆盖最小权限入口、真实 API 导出/下载、后加 Hold 阻断、刷新后的任务链接、
双主体审批，以及忽略 AbortSignal 时 A→B→A 的陈旧响应隔离；不录制令牌/载荷 trace、
视频或截图。为了自动化，授权步骤通过真实 Snaplink password provider 完成，
浏览器仍执行真实 PKCE、签名/nonce 校验和 API 授权；**不覆盖兄弟仓库 Flutter
Hosted Login 的布局/交互，也不替代 PostgreSQL/Kafka/ClickHouse/WORM 完整栈验收**。
