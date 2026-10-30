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

> `-p` 只改变 Compose project 名称，不保证网络隔离：若编排中的网络显式设置
> `name: aero-platform`，不同 project 仍可共享该网络；显式命名的卷也不因 `-p` 自动隔离。
> 启动前须核对解析后的资源归属、端口与已有栈，不能把换 project 当作隔离证明。
> 只清理由本次 project 独占且获授权的容器/卷；共享或未知资源保留。

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
- 协议级检查不代替真实 relay。第 5 节仅提供 Vault 的受控验收入口；当前真实 relay
  仍为 **blocked / 未验收**，合成夹具通过不解除阻塞。ID/IM 的真实业务 relay 不在
  该脚本范围内，仍须另行验收。
- 本地栈不是生产：不声明容量、容灾、正式 WORM/COMPLIANCE 或安全发布通过。

## 4. 外部操作事实的只读核对（platform_fact_probe.py）

当控制器已从真实 Aero 操作取得独立外证（含既有 `event_id`、精确来源/Schema 契约和
本 run 授权的服务与凭据）时，可用本工具从审计侧观察同一事件及回执，再把输出的事件
ID 交给现有 Web 详情页复查：

```bash
# Bearer 只从专用环境变量读取；从私有 env 注入，不写日志、argv 或报告
set -a; . /path/to/private/platform.env; set +a
export AUDIT_FACT_PROBE_BEARER="$PLATFORM_AUDIT_EVENT_READ_TOKEN"
# 可选：追加 --operation-id "$OPERATION_ID" --correlation-id "$CORRELATION_ID"
python3 test/e2e/platform_fact_probe.py \
  --audit-url http://127.0.0.1:28089 \
  --tenant platform-local \
  --event-id "$EXTERNAL_EVENT_ID" \
  --source-system aero-vault.security \
  --event-type aero-vault.security \
  --schema-id aero-vault.security \
  --schema-version 1
```

工具只做两次显式带 `tenant_id` 的 GET：单事件与 receipt；不调用 token 端点、不摄入或
合成事件、不访问数据库或业务 API。它要求 loopback Audit URL，拒绝带 userinfo、query
或 fragment 的基地址，限制读取超时和正文大小，禁用环境代理继承与重定向，任一读取失败
都不降级到其他租户查询。成功还要求事件 tenant/event/source_system/event_type/schema_id/
schema_version 精确匹配，receipt 状态属于账本已入账的 `ledgered`/`indexed`/`archived`，
`ledgered_at` 是带时区的严格 RFC3339 时间（接受服务端实际输出的 `Z`、合法数字偏移和
至多纳秒小数，拒绝缺失时区、非法日期、越界偏移、缺失值和 Go 零时间），且事件与回执的
tenant/event/stream/sequence/hash 一致，两个响应都为 `Cache-Control: no-store`。

退出码：`0` 证据核对通过；`1` 授权/身份/证据/读取失败；`2` 未执行（缺输入/凭据或
服务不可用）。stdout 只输出最小 JSON，成功含 `event_id`、`web_event_hash` 和链坐标，
不含 payload、actor 详情、原始响应或后端错误。把 `event_id` 或 `web_event_hash` 交给
现有 Web 即可跳转到 `#events/event/<urlencoded-event-id>` 查看同一事件/回执。

**边界**：这是审计侧的只读观察，不是业务操作执行证明。真实业务操作本身以及 outbox/relay
来源仍须由控制器保留独立外证；本工具通过不能单独宣称 relay、Kafka、ClickHouse、投影
确认、完整性或端到端全链通过。HTTP 夹具测试只证明读取与核对逻辑，不冒充真实 Aero
relay 联调。没有真实服务、凭据及外部事件证据时必须明确记为真实联调未执行
（`not_executed`），不得用夹具结果代替。

## 5. 真实 Vault relay 验收（仅 Vault）

仅供控制器对**已运行且本 run 获明确授权的本机平台栈**执行；当前真实 relay
仍为 **blocked / 未验收**，下列命令不是本阶段的执行授权。不启动/重配服务、不扩权、
不修改兄弟仓或身份配置，不为排障删除历史对象或反复上传：

```bash
# 仅传路径；私有文件原位只读，不 source、不复制、不打印
AUDIT_VAULT_RELAY_AUTHORIZED=true PLATFORM_ENV_FILE=/path/to/private/platform.env \
  make e2e-platform-vault-relay
# 非默认端口可直接传非敏感参数（所有地址仅接受 loopback HTTP(S)）
AUDIT_VAULT_RELAY_AUTHORIZED=true PLATFORM_ENV_FILE=/path/to/private/platform.env \
  python3 test/e2e/platform_vault_relay.py \
  --vault-url http://127.0.0.1:28081 \
  --audit-url http://127.0.0.1:28089 \
  --token-url http://127.0.0.1:28080/token --tenant platform-local
```

入口首先要求环境变量 `AUDIT_VAULT_RELAY_AUTHORIZED` **精确等于字符串 `true`**；
未设置、空值、大小写变体、`1` 或带空白值均退出 `2`（`not_authorized`），不读取私有
文件、不构造 HTTP 客户端、不联网。该开关是本次操作许可确认，不是授予平台权限。

脚本仅依赖 Python 标准库，私有 env 须为当前用户所有的普通单链接文件、无
组/其他用户权限，拒绝叶 symlink，最多 1 MiB。只保留以下白名单键，支持字面量
赋值、单/双引号和 `export` 前缀；不执行 shell、变量/命令展开，不导出到环境：

- `SNAPLINK_CLIENT_SECRET_AERO_IM_VAULT`
- `SNAPLINK_CLIENT_SECRET_PLATFORM_AUDIT_BOOTSTRAP`
- `AERO_VAULT_AUDIT_HMAC_KEY`（Vault 已配置的独立 32–4096 字节密钥）

两个 token 均通过 Basic `client_credentials` 获取，密钥/token 不进入 argv、日志或
报告。业务使用既有 `aero-im-vault` 客户端、`resource=aero-vault`、
`scope=read write vault.file.delete`；这只是复用 Vault 客户端，不调用 IM API。
审计读取使用 `platform-audit-bootstrap`、`resource=audit-governance`、
`scope=audit:platform:cross_tenant`，**每次审计请求显式选择 tenant_id**。该平台 scope
依现有契约包含读取权限，但脚本从不调用管理写入或事件摄入 API，也不使用 relay 写入凭据。

**token 前置**：业务客户端必须已获准 `vault.file.delete`；只有 `read write` 不足以
执行当前 Vault 删除契约。仅**本次业务 token 响应 HTTP 400，且解析后 JSON 顶层
`error` 严格等于 `invalid_scope`**，才退出 `2`（`missing_delete_scope`）；描述中的
子串、嵌套字段、畸形正文、其他状态或其他端点均不得这样跳过。其余颁发失败退出 `1`。
两个成功 token 响应都必须显式返回字符串 `scope` 并覆盖各自全部申请权限；缺失、null、
空值、类型错误或不足均退出 `1`，在任何 PUT 前停止。不改平台授权、不换身份绕过、
不伪造成功。申请完整 scope 在上传前完成；实际删除仍以服务端授权为准。

流程：先按两个独立随机请求 ID 的 HMAC 查询确认无既有事实，再使用随机 UUID key 和
`If-None-Match: *` 上传几十字节对象，逐字节回读，然后显式 `hard=0` 软删，并确认普通
读取为 404。上传获成功应答后，即使 JSON、metadata 或回读失败，也恰好尝试一次
同对象 `hard=0` 软删；未知 PUT 结果不猜测回滚、不补发删除，删除失败也不重试。
可能留下本次小对象时必须记为失败，不能声称清理完成；不得反复上传以碰出成功证据。
不硬删、不恢复对象、不清理历史对象、不删除审计事实。

关联完全在内存中复用 Vault 既有契约（参见兄弟仓 `docs/snaplink-audit-governance.md`）：
`HMAC-SHA256(key, domain NUL tenant NUL field NUL value NUL)`，
`domain=aero-vault/audit-governance/v1`，输出 base64url 无 padding；来源用
`source-system/tenant`，请求用 `request/request_id`，目标用
`file-target/(default NUL key)`，并遵循现有 `hmac-sha256:` → aggregate 的
`hmac-sha256.` 分隔符转换。原始请求 ID、路径、内容、HMAC 和响应体不输出或落盘。

业务操作结束后最多轮询 60 秒/60 轮，每次 HTTP 请求上限 5 秒、响应最多 1 MiB，禁用
环境代理和重定向。每次轮询发起**新** `POST /api/v1/events:query` 只读查询，HMAC
筛选放 JSON 正文，不复用冻结成员游标；拒绝未处理的分页/计数异常，不把截断当完整。
必须读到不同 event_id 的新 `file.created` / `file.deleted`：tenant、租户派生来源、
`aero.vault.security@1`、请求 HMAC、目标、file 类型、success、classification/retention
及发生时间均核对。再读取各自 receipt，要求 no-store、账本状态、有效且本次新入账时间，
以及 tenant/event/stream/sequence/hash 与事件一致。缺失事实、超时、403、坏证据或读
自审计失败均退出 `1`（`failed`），业务开始后不改记 SKIP / `not_executed`；不降级为
别租户/来源查询，不注入或合成审计事件。

stdout 只输出最小 JSON：通过时包含三项业务断言、两个事件 ID、receipt 状态及现有 Web
详情锚点；失败只含固定 reason/check。退出 `0` = 本次 Vault 操作及两条关联账本事实已
观察；`1` = 操作/授权/证据失败；`2` = 未显式授权、缺失/非法配置或上述精确的业务
token `invalid_scope` 前置失败。**不证明** relay 本地投递标记、
Kafka、ClickHouse、投影确认、完整性/生产 COMPLIANCE 或 ID/IM relay。

离线回归（HTTP 夹具只证明脚本逻辑，不是上面真实验收的替代）：

```bash
python3 -m unittest discover -s test/e2e -p 'test_platform_*.py'
```
