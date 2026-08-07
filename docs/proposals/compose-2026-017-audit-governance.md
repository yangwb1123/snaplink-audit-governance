评估完成，提案文档已输出至 `docs/proposals/compose-2026-017-audit-governance.md`（169 行，未修改任何代码；`check-no-refusal` 与密钥扫描均通过，纯文档变更不触发 `cli.py quality` 门禁）。

## 评估结论：**Partial**（代码层 Verified / 接入就绪需 4 类适配）

**代码/契约零修改（Verified）**——证据链：
- 事件信封全通用：AsyncAPI `EventEnvelope` 的 `payload` 为 `additionalProperties: true` 的任意对象；`event_type`/`schema_id`/`schema_version` 是自由字符串/整型，无硬编码事件枚举（`api/asyncapi/asyncapi.yaml`、`internal/domain/models.go`）
- 校验全部 schema 数据驱动：`validateEvent` 按 `(tenant, schema_id, version)` 精确查找、`allowed_fields` 白名单、`rejectSensitive` 块名单、加密/摘要声明（`internal/service/service.go`）
- HTTP/gRPC/batch/outbox relay/Kafka 五条写入路径均按通用 `domain.Event` 处理，哈希链/投影/归档不感知事件类型

**必须的适配（全部为控制面配置 + 生产者约定，非代码）**：
1. **Schema 注册**：`POST /api/v1/schemas` 注册 2 个 schema + `POST /api/v1/sources` 注册 `snaplink.vault` 来源（未注册写入即 fail-closed）。关键契约决策：信封 `schema_version` 是**整型**，无法直接表示 `@1.1`，建议方案 A——`event_type="vault.file.deleted"`、`schema_version=1`（major），`1.1` 放 payload `contract_version` 字段（需与 aero-vault 对齐）
2. **幂等键约定**：确定性 `event_id`（UUIDv5 同源派生，重试不得重新随机）；`idempotency_key` 租户内逐事件唯一；重试字节必须稳定——`EventContentDigest` 覆盖 `trace_id`/`correlation_id`/`reason` 等，漂移会触发 409 `event_id_content_conflict`
3. **留存分级**：deleted → `compliance`（长留存归档 7 年级），notify → `operations`（短留存），按 `(tenant, retention_class)` 配置策略；平台只归档、永不删除不可变账本
4. **敏感字段脱敏**：`file_path`/`reason_detail` 放入 payload 并声明 `encrypted_fields`（AES-GCM + AAD，存储为 `enc:v1:` 密文，查询/导出即脱敏形态）+ 可选 `searchable_fields`；**重要发现**：信封 `reason`/`targets[].name` 明文存储且平台无查询解密能力（`DecryptJSON` 仅用于完整性验证），因此删除原因必须由 aero-vault 侧清洗后再放信封

**待决风险**：@1.1 版本映射方案需与 aero-vault 冻结；审计员可读性（密文 vs 明文）需产品确认；notify 若含所有者邮箱 PII 应单独按 `PII` 分类加密。
