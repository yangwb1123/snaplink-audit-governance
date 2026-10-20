# 工程门禁（quality gate）

`python3 cli.py quality` 是本仓库改动的强制门禁（见 `AGENTS.md`）。它串行执行约 28 个
阶段，任何一个失败即返回非零。失败行会打印阶段标签，**该标签就是可单独重跑的命令**：

```sh
python3 cli.py quality          # 完整门禁
python3 cli.py quality          # 输出示例
#   PASS: fmt (0.1s)
#   PASS: filesize (0.0s)
#   ...
#   PASS: race (88.6s)
#   QUALITY PASS

python3 cli.py <label>          # 只重跑失败的那个阶段，例如：
python3 cli.py proto-sync
python3 cli.py tenant-scope
python3 cli.py filesize
```

## 阶段清单

| 阶段标签 | 检查什么 |
|---|---|
| `fmt` | `gofmt -l` 干净 |
| `filesize` | 生产 Go 文件行数 ≤ `engineering.yaml: max_go_lines` |
| `complexity` | 函数长度 ≤ `max_function_lines`、分支数 ≤ `max_decisions` |
| `architecture` | domain/security/auth/store 不得反向依赖 service/httpapi/grpcapi |
| `directory-fanout` | 单目录子目录数 ≤ `max_subdirs` |
| `root-files` | 仓库根目录白名单（拒绝 vendored 工具链、容忍 gitignored 运行态） |
| `root-business-code` | 根目录不得出现 `handler.go`/`service.go`/`store.go` |
| `invariants` | 租户上下文赋值、敏感字段拒绝、哈希链、请求体上限；开发密钥单一来源 |
| `exemptions` | `engineering-exemptions.yaml` 存在时必须有 owner 与 reason |
| `adr-compliance` | 架构/验证计划与 `engineering.yaml` 存在 |
| `make-help` | Makefile 目标与其 help 一致 |
| `route-contract` | Go 路由 ↔ OpenAPI 双向一致，含权限与状态码映射 |
| `openapi-contract` | 用钉住的 Go 校验器验证 `api/openapi/openapi.yaml` |
| `contract-fields` | domain 字段与 OpenAPI/AsyncAPI/Proto 对齐 |
| `asyncapi-channels` | AsyncAPI 声明的 topic ↔ Go 常量双向一致 |
| `proto-sync` | `audit.proto` ↔ 检入生成代码（描述符 + 版本钉 + 字节重放） |
| `dev-auth-manifest` | 非 verify 部署清单不得开启开发认证 |
| `tenant-consistency` | DS-08：租户不一致检查必须先于服务端打标 |
| `tenant-scope` | 41 个操作的作用域矩阵（跨租户/查询/请求体/管理面） |
| `stream-consistency` | 客户端 stream_id 剥离 → 派生 → 服务端打标的顺序 |
| `replay-round-reset` | 重放每轮必须先重置 accepted 读取位置再扫描 |
| `prometheus-rules` | DLQ 告警集被钉死（永久丢失/发布失败/认证阻塞/积压） |
| `sensitive-logging` | 禁止记录敏感字段 |
| `vet` / `test` / `race` / `build` | go vet、单元测试、race detector、7 个发布二进制 |

## 设计约束

- **策略只有一份实现**：`checks/<policy>.py` 是唯一实现，`cli.py` 只是启动器。
  `checks/test_quality_checks.py` 断言 `cli.py` 不重新实现任何策略，且每个
  `checks/` 模块都有可单独运行的入口。
- **门禁的自检是静态的**：门禁运行时才跑单元测试，而递归跑全量门禁的测试永远执行不到，
  所以门禁的接线（CI 确实跑门禁、每个阶段标签都是命令、`check` 不强于 `quality`、
  递归守卫仍在）由 `checks/test_gate_wiring.py` 断言，而不是再跑一次门禁。
- **度量必须可信**：`complexity` 按函数的闭合花括号界定函数体，不把后续 type 声明算进去
  （旧实现把 16 行函数量成 258 行）；`engineering.yaml` 里的阈值来自实测分布。

## 常用组合

```sh
python3 cli.py check      # 快速子集：fmt、filesize、routes、openapi、vet、test
python3 cli.py race       # 仅竞态
python3 cli.py build      # 仅构建
python3 cli.py bench      # 本机基准，结果见 docs/BENCHMARKS.md
python3 cli.py lint       # golangci-lint（未安装时退出码 2）
python3 cli.py security-scan  # govulncheck / gosec（未安装时退出码 2）
python3 cli.py predeploy  # 构建后比较 API 与 worker 的一致性键（无网络）
```

等价的 `make` 目标：`make check`、`make quality`、`make test`、`make race`、`make build`、
`make ci`。
