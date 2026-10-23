# `internal/service` 耗时诊断

`make service-timing` 用 Python 标准库运行并汇总真实的 `internal/service`
全包测试。它是一个**观察工具**，不是性能门禁，也不替代 `python3 cli.py quality`、
race 门禁、基准或容量验证。入口只来自 `make help` 和本文；本工具不修改
`README.md`。

## 用法

```sh
make service-timing

# 额外跑一轮完整 race 全包（不是只跑某个测试）
SERVICE_TIMING_RACE=1 make service-timing

# 指定仓库外私有输出基目录；脚本会在其下创建每次运行独立子目录
SERVICE_TIMING_OUT=/var/tmp/service-timing make service-timing

# 沿用 Makefile 的 GO 变量
make service-timing GO=/path/to/go
```

也可以直接运行脚本：

```sh
GO=/path/to/go python3 scripts/service_timing.py
python3 scripts/service_timing.py --race
python3 scripts/service_timing.py --out /var/tmp/service-timing
```

脚本实际执行的命令固定为：

```text
go test -json -count=1 -timeout=10m ./internal/service
go test -json -race -count=1 -timeout=10m ./internal/service   # --race
```

没有 `-run` 缩减全集，也不接受修改包、测试集合或 10m 超时的参数。普通与 race
都是独立的全包运行。脚本在输出基目录的 `.service-timing.lock` 上持有排他
`flock`，因此两个诊断进程会串行等待，不会并发争用测试资源。

测试自测（不运行 Go 全包）：

```sh
python3 -m unittest discover -s scripts -p 'test_service_timing.py' -v
```

## Go 启动器与有效工具链

`GO` 指向的是**启动器**；在 `GOTOOLCHAIN` 作用下，启动器可能切换到另一套
实际工具链。摘要和 `metadata.json` 分别记录，不能混称：

- `go_launcher`：解析后的启动器路径；
- `go_version`：该启动器实际选择的工具链 `go version` 输出；
- `go_toolchain` / `go_goroot`：`go env` 报告的有效 `GOTOOLCHAIN` 和 `GOROOT`；
- `go_flags` / `go_maxprocs`：本次运行的 `GOFLAGS` / `GOMAXPROCS`；
- `declared_go_version`：仓库 `go.mod` 声明的版本。

例如宿主 `/usr/bin/go` 可能指向较新的启动器，但在固定
`GOTOOLCHAIN=go1.26.10` 下有效版本仍是 `go1.26.10`。历史证据属于哪个工具链
只按原始元数据陈述，不把旧版本结果改称新版本通过。脚本只记录这些白名单字段，
不保存或回显完整环境。

## 输出与隐私

默认输出基目录：

```text
$XDG_STATE_HOME/snaplink-audit-service-timing
# 或 ~/.local/state/snaplink-audit-service-timing
```

`SERVICE_TIMING_OUT` 或 `--out` 指定的目录必须在仓库之外。脚本会解析真实路径
（包括既有符号链接），拒绝仓库本身、仓库内路径以及解析后指回仓库的路径。
每次运行创建独立子目录（权限 `0700`），其中：

| 文件 | 内容 |
|---|---|
| `go-test.jsonl` | 原始 `go test -json` 标准输出，包含失败/跳过的原始测试输出 |
| `stderr.log` | 原始 Go 标准错误 |
| `metadata.json` | 实际 Go 启动器/有效工具链、`go.mod` 声明版本、完整命令参数、退出码与聚合诊断计数 |

原始文件权限为 `0600`。终端摘要只输出白名单字段：测试/子测试名、状态、
Go 报告的 `Elapsed`、包 `Elapsed`、脚本观察到的墙钟、已解析的计时字段和运行元数据。
**不输出**测试载荷、凭据、owner、对象 key、任意日志正文或完整环境。`metadata.json`
也不保存环境快照。

## 指标口径

### 测试与包

- 逐测试/子测试状态直接来自 JSON 的 `pass` / `fail` / `skip` 事件；只有
  `--- PASS` 语义的测试显示 `PASS`，SKIP 不会被改写成 PASS。
- `Elapsed` 是 Go 对**该条测试记录**报告的单值。父测试的 `Elapsed` 已包含其子测试，
  因此摘要明确标注 `not additive`，绝不把父子 `Elapsed` 相加冒充包 wall time。
  并行/交错的子测试也只保留各自上报值，不重排、不累计。
- 包耗时取包级 `pass` / `fail` / `skip` 事件的 `Elapsed`。脚本另报运行墙钟，
  它包含启动/回收 Go 进程，但**不**用作包 `Elapsed`。
- 最慢项按单条测试的 `Elapsed` 降序展示，并标记 `top` / `subtest` / `parent`；
  这些值同样不相加。
- 没有终态的测试或包显示 `INCOMPLETE`，不会被当作 PASS。

### `file_phase`

只解析现有测试已经输出的：

```text
file_phase stage=<name> ... elapsed_ms=<累计毫秒>
```

脚本按同一测试内的 `elapsed_ms` 从小到大排列阶段，再计算相邻阶段的差值。
第一个观测阶段没有前驱，显示 `delta_ms=unobserved`。阶段均值/耗时**不**从日志
到达时间、缓冲区刷新时间或测试总耗时反推。

当前有 `file_phase` 埋点的测试：

- `TestExportMatchesQueryEventsOrder`：`fixture_ready`、`query_ready`、`created`、
  `terminal`、`executor_exit`；
- `TestIngestCollapsesCraftedStreamIDs`：`domain_ready`、`ingested`。

其他测试没有该埋点，摘要显示 `unobserved`，不补造时间。

### `file_io`

每一行是一条**单次操作**：

```text
file_io operation=<get|put> kind=<events|segments|exports> success=<bool> elapsed_ms=<单次毫秒>
```

脚本逐条保留 `elapsed_ms`，不会把它当作累计时间，也不会对 `since_start_ms` 做阶段差分。
当前覆盖 `TestExportMatchesQueryEventsOrder` 通过 `observeFileProgress` 观察到的实际
Get/Put。

### `archive_boundary`

保留测试显式输出的分段，不重新计算或归并：

```text
archive_boundary ingest_ms=<总> before_put_ms=<段> put_ms=<段> after_put_ms=<段>
```

当前覆盖 `TestIngestArchivePutDetachedContextCancelsBlockingPut` 与
`TestDetachedPutMeasurementExcludesPostPutLedgerCommit`（两者复用同一真实边界观察）。
摘要原样列出四个字段，便于区分总时长与显式边界段。

## 失败、跳过与未完成

- **build 失败**：识别 `build-output` / `build-fail`，摘要只报失败计数，编译器正文仅在
  私有原始 JSON 中；整体退出非零。
- **SKIP**：保留 `SKIP` 状态与测试名。没有外部 PostgreSQL 时，既有
  `t.Skip("set AUDIT_TEST_POSTGRES_DSN ...")` /
  `t.Skip("set AUDIT_TEST_POSTGRES_EXPORT_LEASE_DSN ...")` 原样生效；摘要额外显示
  `postgres_skips` 计数。脚本不会设置、猜测或连接共享 DSN，也不会启动 Docker。
- **panic / timeout**：识别 Go 的 `panic:`、`fatal error:` 与
  `test timed out after ...` 标记，保留所有已开始但没有终态的测试为
  `INCOMPLETE`，整体失败。超时仍由 Go 的 `-timeout=10m` 产生，脚本不提高或重置。
- **截断 JSON / 损坏行**：保留已写出的原始 JSON 和已解析项；损坏行、未换行末行、
  缺失测试或包终态都会使整体非零，并保留 `INCOMPLETE` 项。
- **原始文件写入失败**：`go-test.jsonl` 或 `stderr.log` 写入/刷新失败时，脚本继续
  排空子进程管道，但把捕获失败记入 `capture_errors` 和私有元数据。即使 Go 子进程
  `exit 0`、所有已解析测试都是 PASS，摘要也必须是 `FAIL`、退出码非零，绝不显示整体
  `PASS`。
- **SIGINT / SIGTERM**：保留已写出的原始 JSON、stderr、元数据和已解析项。脚本在
  独立 session/process group 中启动 Go，只向本次启动的进程组发送 TERM/KILL；
  不向自己的进程组、父进程或其他任务广播信号。中断后等待管道排空并回收测试子进程，
  再输出带 `interrupted: true` / `signal: <n>` 的 `FAIL` 摘要和信号退出码。
- 只要 build/测试失败、仍有未完成项、JSON 损坏、捕获写入失败、panic/timeout、
  包终态缺失或收到中断，脚本都不会打印 `PASS`。摘要与退出码共用同一判定函数。
- `--race` 同样运行全包；如果既有测试在 race 下失败，脚本只保留真实的 `FAIL` 和原始
  证据，不会跳过、缩小测试集合或改写阈值来换取绿色。

### 历史 race 证据回放

旧失败产物中的 race 原始 JSON 只读保留，不重跑碰绿、不修改旧结论。脚本对相同语义
提供永久脱敏 fixture 自测：`TestRestoreReadControlFileAllocationDoesNotScaleWithUnrelatedEvidence`
和 `TestRetentionReadControlFileAllocationDoesNotScaleWithUnrelatedEvidence` 在旧 race
证据中是 allocation 断言失败，fixture 也只包含这两个公开测试名、失败状态和最小计时
字段。fixture 不依赖仓库外历史路径，不包含载荷、owner、对象 key、DSN 或环境；它验证
解析器遇到这两项失败时仍报 `FAIL` 且退出非零，不声称性能问题已修复。

## 覆盖范围与非性能修复边界

- 本工具不修改生产 Go、既有测试、`cli.py`、`checks/`、门禁阈值、生成代码或依赖；
  也不新增 service 埋点。
- 它只描述**当前工作树源码**在本机的一次观测，不证明历史 running、累计耗时或任何
  性能问题的唯一根因已闭环。
- 局部阶段差值、单次 `file_io` 或某次包 `Elapsed` 下降，不等于整包 wall 下降、
  生产 SLO、容量、数据库/网络/磁盘归因或跨环境结论。
- 没有外部 PostgreSQL 时，PG 用例按既有契约跳过；跳过不等于验证通过。脚本不会为了
  “跑满”而自动连接共享 DSN 或启动容器。
- 本工具不是性能修复，也不改变业务正确性、认证/安全校验、审计事实、至少一次投递、
  幂等或任何 deadline/等待预算。
- 完整 `python3 cli.py quality` 仍是独立门禁；quality 失败即 BLOCKED，不能用本工具的
  `PASS` 或计时摘要替代。
