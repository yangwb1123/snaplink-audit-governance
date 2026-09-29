# 契约同步（OpenAPI / AsyncAPI / Proto）

三份线面契约与运行时必须一起改：

| 契约 | 文件 | 守卫 |
|---|---|---|
| REST | `api/openapi/openapi.yaml` | `route-contract`、`openapi-contract`、`contract-fields` |
| 事件 | `api/asyncapi/asyncapi.yaml` | `asyncapi-channels`、`contract-fields` |
| gRPC | `api/proto/audit.proto` + 检入的 `audit.pb.go` / `audit_grpc.pb.go` | `proto-sync`、`contract-fields` |

## Proto 生成代码同步（api/proto）

`api/proto/audit.proto` 与检入的生成代码（`audit.pb.go`/`audit_grpc.pb.go`，即 gRPC
运行时实际编译的线面）由三道防线守护，全部挂在 `python3 cli.py quality` 中：

1. **描述符解析（零外部工具，始终执行）**：`checks/proto_sync.py` 直接解码
   `audit.pb.go` 内嵌的 `file_audit_proto_rawDesc`（纯标准库 wire walker），逐消息逐字段
   与 `audit.proto` 比对——`.proto` 新增字段但未重新生成会立即失败并点名该字段/消息。
2. **生成器版本钉（始终执行）**：`engineering.yaml` 的 `proto:` 块是唯一 pin 清单；
   生成头注释中的版本与 pin 不一致（或 `protoc-gen-go` pin 与 `go.mod` 的
   `google.golang.org/protobuf` 版本不一致）即失败。
3. **字节级重放（工具链存在时）**：`scripts/proto-gen.py --check` 用钉住的工具链在临时
   目录重新生成并与检入文件逐字节比对，注释/格式级漂移也能捕获；工具链缺失时显式打印
   跳过说明，描述符与版本检查仍强制执行（失败关闭，绝不静默通过）。

重新生成（首次运行会下载钉住的 protoc 到 `bin/protoc-<pin>/` 并用 `go install @<pin>`
安装两个生成器到被 gitignore 的 `bin/`，不触碰 `go.mod`/`go.sum`）：

```sh
make proto
```

CI 无差异断言（本地可复现，`make proto` 在未漂移树上幂等）：

```sh
make proto && test -z "$(git status --porcelain api/proto)"
```

漂移模拟（在 `git archive HEAD` 副本中注入字段、跑完整门禁、恢复后再跑一次，绝不改动
工作区）：

```sh
bash scripts/drift-simulate.sh
```

门禁级漂移测试的等价命令：`AUDIT_DRIFT_SIM=1 python3 -m unittest checks.test_proto_sync`
（默认跳过以避免完整门禁耗时翻倍）。

## 守卫为什么这样切分

- `route-contract` 同时扫两个方向：OpenAPI 声明但运行时没有的路由，以及运行时注册但契约
  缺失的路由；它还检查 operationId 唯一、每个操作的权限、以及 `statusForError` 的哨兵→
  HTTP 状态映射是否覆盖 OpenAPI 的 `x-error-mappings`。
- `asyncapi-channels` 保证 spec 里声明的每个 topic 都有 Go 常量，反之亦然（避免"声明了
  没人发"和"发了没人知道"两类静默漂移）。
- `contract-fields` 把 domain 模型的 35 个字段与三份契约逐一对齐。
- 这些守卫按**包**扫描而不是按文件：`internal/httpapi` 拆成 server / handlers_*.go /
  authz.go 之后，钉死单个文件名的守卫会随代码移动而失效。

## 改契约时的最小动作

1. 改 `api/**` 的契约文件。
2. 若 proto 变了：`make proto`，并确认 `git status --porcelain api/proto` 为空。
3. 改运行时实现。
4. `python3 cli.py quality` —— 契约类阶段（`route-contract`、`openapi-contract`、
   `contract-fields`、`asyncapi-channels`、`proto-sync`）任一失败都会在十几秒内报出。
