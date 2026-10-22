# 隔离 demo（core / advanced）

本页是 `deploy/docker-compose.demo.verify.yml` 与 `scripts/demo*` 的一页说明。
它用于本机功能演示与回归，不替代生产部署、容量验收、正式 WORM/COMPLIANCE 认证或
`test/e2e` 全栈验证。旧的 verify Compose、旧 Make 目标和平台联调文档保持不变。

## 快速开始

```sh
make demo-up
make demo-seed
make demo-prove
make demo-down
```

默认 `core` profile：只有一个 `audit-api` 写者，使用 Docker 命名卷中的真实
`state.json`、真实热/冷文件、真实本地归档目录和 API 内置导出队列；不启动
PostgreSQL、Kafka、ClickHouse、IdP，也不读取或构建兄弟仓库。

```sh
DEMO_PROFILE=advanced make demo-up
DEMO_PROFILE=advanced make demo-seed
DEMO_PROFILE=advanced make demo-prove
DEMO_PROFILE=advanced make demo-down
```

`advanced` 使用既有 PostgreSQL 快照后端、MinIO Object Lock 归档和
`audit-governance-worker`。`demo-up` 会先启动依赖、按文件名顺序应用
`migrations/*.sql`，再创建/验证启用 Object Lock 的 bucket、开启 versioning，并设置
`COMPLIANCE 365d` 默认留存；两个应用都使用每次 Put 的显式 `COMPLIANCE` 留存
（`AUDIT_ARCHIVE_RETENTION_DAYS=365`）。worker 必须观测到 `archive_ready=ok` 才算
advanced 启动成功。advanced 仍使用 PostgreSQL 的兼容快照布局；本 demo 不执行热/冷
显式 cutover，也不加入 Kafka/ClickHouse 投影。

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `DEMO_PROFILE` | `core` | `core` 或 `advanced` |
| `DEMO_PROJECT` | `snaplink-audit-demo` | Compose project；容器、网络、卷均限定在此项目 |
| `DEMO_API_PORT` | `18089` | API 仅发布到 `127.0.0.1:<port>` |
| `DEMO_RUN_DIR` | `$XDG_STATE_HOME/snaplink-audit-demo/<project>` 或 `~/.local/state/...` | 仓库外私有运行目录，权限 `0700`，内部文件 `0600` |

同一 project 一次只运行一个 profile（两者默认都占用 `DEMO_API_PORT`）。需要并行时
使用不同的 `DEMO_PROJECT` 和 `DEMO_API_PORT`。

## 认证与密钥

- Compose 显式设置 `AUDIT_ALLOW_DEV_AUTH=false` 和 `AUDIT_ALLOW_DEV_SECRETS=false`。
- 使用仓库既有的显式本地 HS256 模式：`AUDIT_ALLOW_LOCAL_HS256=true` 加真实签名 JWT。
  `demo-seed` / `demo-prove` 从私有运行文件读取 JWT secret 并在内存中签 token，
  **不会回退到 `dev:` token**；`demo-prove` 还会断言 `dev:` token 被 401 拒绝。
- `demo-up` 首次生成彼此独立的 JWT、checkpoint 签名、字段/导出加密密钥，以及
  PostgreSQL/MinIO 随机口令。它们只写入仓库外的 `runtime.env`，脚本不回显文件内容或
  token。重复 `demo-up` 复用同一私有文件，保证已有卷可继续使用。
- 若私有运行文件丢失但卷仍在，旧状态无法用新密钥解密/验签；这是预期失败关闭。
  先停止并显式决定是否重建该项目的数据，不要用弱密钥或开发认证绕过。

## 稳定种子

`demo-seed` 通过真实 API 完成：

1. 确认 `DEMO_TENANT`（默认 `demo`）存在；
2. 注册精确来源 `demo.crm`，`allowed_client_ids=["demo-writer"]`；
3. 注册精确 Schema `demo.order@v1`（固定 required/allowed 字段）；
4. 以归档等待方式写入 4 个固定事件：3 个属于 `demo-op-order-0001` 的订单事件，
   1 个用于精确查询对照的噪声事件；
5. 在 `DEMO_RUN_DIR/seed.manifest.json` 持久记录完整种子与事件字节 SHA-256。

重复 `demo-seed` 会先比较 manifest；若字节变化则拒绝覆盖。相同事件再次提交必须得到
`duplicate=true` 且 `stream_id`/`sequence`/`hash` 不变，因此重复 seed 不会新增账本事实。

## prove 断言

`make demo-prove` 直接调用真实 HTTP API，断言：

- `/healthz`、`/readyz` 健康；
- `dev:` token 被拒绝；
- 入账回执为 `archived`，含真实 `ledgered_at`、`archived_at`、链坐标和 hash；
- 同内容重放为 `duplicate=true`，链坐标和原回执不变；
- 同 event_id、异内容返回 `409 conflict`，原回执不被改写；
- 固定成员精确查询（operation/source/type/actor/时间窗）返回且仅返回 3 个目标成员，
  噪声 operation 只返回自己的 1 个成员；
- 导出任务完成，JSONL 成员集合精确匹配种子，下载字节 SHA-256 等于任务 `digest`；
- 后加 Legal Hold 对新建导出、既有导出详情和下载都返回 403 `reason=legal_hold`；
  释放后详情/下载恢复，且新导出重新可创建并完成；
- 租户完整性校验为 `valid=true`、包含真实签名段；advanced 还检查治理 worker 正在
  运行。

### 真实磁盘篡改（仅 core）

`core` 的 prove 在完成健康断言后：

1. 停止唯一的 `audit-api-core` 写者并确认没有运行中的同卷写者；
2. 用一次性 helper 把原卷复制到项目自有的 `demo_tamper_state` 卷，原卷仍以只读挂载；
3. 只在副本的冷账本 JSONL 中篡改一条 checkpoint 签名；
4. 在副本上启动独立 `audit-api-tamper`，调用真实完整性 API，要求 HTTP 200、
   `valid=false` 且错误包含 `signature mismatch`——连接失败或任意 5xx 都不算检测成功；
5. 停止副本 API，重启原写者，再次验证原卷 `valid=true`。

原卷、原容器和原 archive 从未被修改；健康原卷就是对照。advanced 使用 S3/MinIO WORM，
不做本地卷篡改，只做健康、导出、Hold 和 worker 冒烟。

## down 与失败现场

`make demo-down` 使用同一 `DEMO_PROJECT` 执行 Compose `down --remove-orphans`，
包含所有 profile，**不带 `-v`**：命名卷默认保留，且只操作本 demo 项目。
脚本失败时不会自动 down、删卷、删容器、删除 Hold/导出或改写数据；失败现场保留
用于检查。prove 的 core 篡改阶段若失败，会明确保留已停止的原写者和 tamper 容器。

## 结果记录

脚本只输出阶段、项目、profile、端口、种子 SHA-256、真实 receipt/export 坐标和断言
结果；不输出密钥、token、数据库/MinIO 口令或完整运行文件。需要复核时查看
`DEMO_RUN_DIR/seed.manifest.json`，以及 Compose 项目自己的日志和卷。
