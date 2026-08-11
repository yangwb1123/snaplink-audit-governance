# pbatch — pi-batch 核心包

> 知识维护（maintenance-intelligence 06）：模块现状 + 决策记录，新人可接手。

## 职责

| 模块 | 职责 |
|---|---|
| `cli.py` | 命令行入口：参数解析、main、轮循环、单批/流水线分发 |
| `config.py` | 声明式配置（pi-batch.yaml）、agent 默认值、会话标志、验证器注册表 |
| `models.py` | 数据模型：Task / TaskResult / Stage / Pipeline |
| `runner.py` | 任务执行：硬超时、失败签名拒绝、重试、验证门禁、摘要 |
| `pipeline.py` | 流水线：阶段构建、meta 角色编排、gate 裁决、决策日志、归档 |
| `pipeline_schema.py` / `evidence.py` | fail-closed schema 与有界证据注入 |
| `relevance.py` | meta 角色关键词评分、命中证据、去重与数量上限 |
| `classifier.py` | 任务类型判定（前端→UI 流水线/后端→后端流水线）+ **system_type 系统分类学** |
| `rule_matcher.py` | 规则匹配：scale × page_type × risk 三轴 + LLM 双向校验 |
| `advance.py` | 自推进迭代引擎（扫描→分批→state.jsonl 续跑） |
| `assessor.py` | 需求评估：8 维检查、规模处方、产品化分级、工作流风险分级 |
| `product.py` | 产品思维层：productization_level、场景推演链、商业/开源要素 |
| `learn.py` | 自演进闭环：事故/RCA → Rule Schema 草案 → 人工批准并入注册表 |
| `ratelimit.py` | F 线 provider 令牌桶限速 + jitter 重试 |
| `context.py` | 上下文路由：glob + 关键词加载项目文档 |
| `eval.py` | 规则系统回归套件 |
| `selfcheck.py` | `pi-batch check` 自检（quality + schema + eval） |
| `memory*.py` / `index_io.py` | 渐进式 message memory：有界索引、按需 recent/find/read |
| `campaign*.py` | 项目级模块发现、方向选择、worktree 隔离流水线 |
| `registry.py` | 规则注册表（learn 产物并入） |
| `retro.py` | 复盘记录 |
| `pipeline_status.py` | **结果状态常量集**（PASSED/GATE_REJECTED/VALIDATION_FAILED/PIPELINE_FAILED）——集中定义防拼写漂移 |

## 设计决策（ADR 摘要）

- **fail-closed**：坏 YAML/JSON/负数/NaN 在创建日志或子进程前以退出码 2 拒绝
- **失败零产物**：拒绝/验证失败的结果不留文件；被拒产物保留有界摘录到 `.pi-batch/rejected/`
- **证据有界注入**：`evidence.max_bytes`（64KiB）+ `max_sources`（64）
- **门禁守护**：`make ci`（502 测试）+ `quality.py --strict` + 3 个检查器自扫描
- **检查器适用域**：designintelligence/backendexperience/knowledge 面向业务服务代码；
  CLI 工具本体（本包）用 quality + 测试门禁守护（backendexperience 的
  long-op/audit 规则对同步 CLI 工具不适用）

## 常用命令

```bash
python -m pbatch --help          # 入口（pip install . 后 pi-batch 同）
python -m pbatch check           # 自检
python -m pbatch eval            # 回归
python -m pbatch classify "订单审批流" --json   # 含 system_type
```
