# 批次门禁与 campaign 运行态

`implementation-gate.md` 是本仓的平台批次门禁矩阵（G0–G8 / B1–B6），属于项目资产，
保留在本仓。

Campaign 的**配置与执行**不再存放于本仓：批次运行器（ai-batch-runner）在自己的检出中
提供引擎（`pi-batch.py` + `pbatch/`）、validator 注册表和 campaign 配置。本仓不再
vendored 任何运行器代码——`checks/root_files.py` 的根目录策略会拒绝它们被重新放回。

执行一个 campaign（配置与 pipeline 模板只同步到本仓 gitignored 的 `.pi-batch/`）：

```sh
bash ~/ai-batch-runner/projects/audit-governance/run-campaign.sh --dry-run
```

配置 source of truth：`~/ai-batch-runner/projects/audit-governance/`
（`campaign.yaml`、`campaign-pipeline.yaml`、`tasks-*.yaml`）。
