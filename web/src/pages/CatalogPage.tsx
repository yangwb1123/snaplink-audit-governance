import * as React from 'react'
import {
  IrisButton,
  IrisCard,
  IrisDescriptions,
  IrisFormField,
  IrisInput,
  IrisSelect,
  IrisSwitch,
  IrisTable,
  type IrisTableColumn,
} from '@iris-ui-kit/react'
import { useAuditApi } from '../api/useAuditApi'
import type {
  AdminAction,
  EventSchema,
  RetentionPolicy,
  RetentionReport,
  SourceSystem,
  Tenant,
} from '../api/types'
import { can } from '../auth/claims'
import { useAuth } from '../auth/AuthProvider'
import { ErrorNotice, PageHeader, formatDate } from '../components/Page'
import { StatusBadge } from '../components/StatusBadge'
import {
  assessPlatformAuditContracts,
  type PlatformAuditStatus,
} from './platformAuditContracts'
import { parseSchemaFieldList, validateSchemaFieldPolicy } from './schemaPolicy'

const sourceColumns: IrisTableColumn<SourceSystem>[] = [
  { key: 'id', title: 'Source ID', width: 150 },
  { key: 'name', title: '名称', minWidth: 160 },
  {
    key: 'allowed_client_ids',
    title: '允许的 Snaplink Client',
    minWidth: 230,
    render: (value) => (Array.isArray(value) ? value.join(', ') : '默认同 Source ID'),
  },
  {
    key: 'active',
    title: '状态',
    width: 100,
    render: (value) => <StatusBadge value={Boolean(value)} />,
  },
]

type SchemaRow = EventSchema & { row_key: string }

const schemaColumns: IrisTableColumn<SchemaRow>[] = [
  { key: 'schema_id', title: 'Schema ID', width: 180 },
  { key: 'version', title: '版本', width: 80 },
  { key: 'event_type', title: '事件类型', minWidth: 190 },
  { key: 'classification', title: '分级', width: 120 },
  {
    key: 'active',
    title: '状态',
    width: 100,
    render: (value) => <StatusBadge value={Boolean(value)} />,
  },
]

const classificationItems = [
  { value: 'public', label: '公开（public）' },
  { value: 'internal', label: '内部（internal）' },
  { value: 'personal', label: '个人数据（personal）' },
  { value: 'confidential', label: '机密（confidential）' },
  { value: 'restricted', label: '受限（restricted）' },
]

function fieldSummary(fields?: string[]): string {
  return fields?.length ? fields.join(', ') : '未配置'
}

const tenantColumns: IrisTableColumn<Tenant>[] = [
  { key: 'id', title: 'Tenant ID', width: 150 },
  { key: 'name', title: '名称', minWidth: 160 },
  { key: 'home_region', title: '主区域', width: 120 },
  { key: 'events_per_second', title: 'EPS', width: 80 },
  {
    key: 'active',
    title: '状态',
    width: 100,
    render: (value) => <StatusBadge value={Boolean(value)} />,
  },
]

const actionColumns: IrisTableColumn<AdminAction>[] = [
  { key: 'created_at', title: '时间', width: 180, render: (value) => formatDate(String(value)) },
  { key: 'action', title: '动作', width: 190 },
  { key: 'actor', title: '操作者', width: 140 },
  { key: 'target_id', title: '目标', minWidth: 180 },
  { key: 'detail', title: '详情', minWidth: 220 },
]

const platformAuditColumns: IrisTableColumn<PlatformAuditStatus>[] = [
  { key: 'service', title: '平台服务', minWidth: 190 },
  { key: 'client_id', title: 'Snaplink Client', minWidth: 170 },
  { key: 'source_id', title: 'Source ID', minWidth: 210 },
  { key: 'schema_ref', title: 'Schema', minWidth: 210 },
  {
    key: 'ready',
    title: '状态',
    width: 100,
    render: (value) => <StatusBadge value={value ? 'valid' : 'invalid'} />,
  },
  { key: 'detail', title: '契约检查', minWidth: 280 },
]

// Page-level orchestration branches by platform/tenant capability; mutation
// logic stays in the focused sections below.
// eslint-disable-next-line complexity
export function CatalogPage({ tenantId }: { tenantId?: string }) {
  const api = useAuditApi()
  const { session } = useAuth()
  const writable = Boolean(session && can(session, 'audit:policy:write'))
  const [tenants, setTenants] = React.useState<Tenant[]>([])
  const [sources, setSources] = React.useState<SourceSystem[]>([])
  const [schemas, setSchemas] = React.useState<EventSchema[]>([])
  const [actions, setActions] = React.useState<AdminAction[]>([])
  const [policy, setPolicy] = React.useState<RetentionPolicy | null>(null)
  const [error, setError] = React.useState<unknown>(null)
  const [revision, setRevision] = React.useState(0)

  React.useEffect(() => {
    let active = true
    const load = async () => {
      setError(null)
      const scoped = Boolean(tenantId || !session?.platform)
      try {
        const [tenantResult, sourceResult, schemaResult, actionResult, policyResult] =
          await Promise.all([
            session?.platform ? api.listTenants() : Promise.resolve({ items: [] as Tenant[] }),
            scoped ? api.listSources(tenantId) : Promise.resolve({ items: [] as SourceSystem[] }),
            scoped ? api.listSchemas(tenantId) : Promise.resolve({ items: [] as EventSchema[] }),
            api.listAdminActions(tenantId, 100),
            scoped ? api.getRetention(tenantId).catch(() => null) : Promise.resolve(null),
          ])
        if (!active) return
        setTenants(tenantResult.items)
        setSources(sourceResult.items)
        setSchemas(schemaResult.items)
        setActions(actionResult.items)
        setPolicy(policyResult)
      } catch (cause) {
        if (active) setError(cause)
      }
    }
    void load()
    return () => {
      active = false
    }
  }, [api, revision, session?.platform, tenantId])

  const refresh = () => setRevision((value) => value + 1)
  return (
    <section>
      <PageHeader
        title="治理目录"
        description="来源绑定、事件 Schema、租户配额与保留策略；每次变更都会进入管理自审计。"
        actions={
          <IrisButton variant="outline" onClick={refresh}>
            刷新
          </IrisButton>
        }
      />
      <ErrorNotice error={error} onRetry={refresh} />
      {session?.platform ? (
        <TenantSection items={tenants} writable={writable} onChanged={refresh} />
      ) : null}
      {session?.platform && !tenantId ? (
        <p className="muted-copy">选择租户后可管理该租户的来源、Schema 与保留策略。</p>
      ) : (
        <>
          <PlatformIntegrationSection sources={sources} schemas={schemas} />
          <SourceSection
            items={sources}
            tenantId={tenantId ?? session?.tenantId ?? ''}
            writable={writable}
            onChanged={refresh}
          />
          <SchemaSection
            items={schemas}
            tenantId={tenantId ?? session?.tenantId ?? ''}
            writable={writable}
            onChanged={refresh}
          />
          <RetentionSection
            policy={policy}
            tenantId={tenantId ?? session?.tenantId ?? ''}
            writable={writable}
            onChanged={refresh}
          />
        </>
      )}
      <IrisCard variant="outline" padding="none" header="管理操作自审计">
        <IrisTable rowKey="id" columns={actionColumns} data={actions} size="small" />
      </IrisCard>
    </section>
  )
}

function PlatformIntegrationSection({
  sources,
  schemas,
}: {
  sources: SourceSystem[]
  schemas: EventSchema[]
}) {
  const statuses = React.useMemo(
    () => assessPlatformAuditContracts(sources, schemas),
    [schemas, sources],
  )
  return (
    <IrisCard variant="outline" header="平台审计接入状态" className="section-card">
      <p className="card-copy">
        Snaplink 与 Console 承担认证和审计读平面；下表按六项目部署契约检查 Aero ID、Aero IM 与
        Aero Vault 的 M2M Client、租户来源和固定 Schema 版本。租户派生 Source ID 仅校验公开前缀，
        不在浏览器中处理派生密钥。
      </p>
      <IrisTable
        rowKey="key"
        columns={platformAuditColumns}
        data={statuses}
        size="small"
        striped
      />
    </IrisCard>
  )
}

function TenantSection({
  items,
  writable,
  onChanged,
}: {
  items: Tenant[]
  writable: boolean
  onChanged: () => void
}) {
  const api = useAuditApi()
  const [id, setId] = React.useState('')
  const [name, setName] = React.useState('')
  const [error, setError] = React.useState<unknown>(null)
  const create = async () => {
    setError(null)
    try {
      await api.createTenant({
        id: id.trim(),
        name: name.trim(),
        home_region: 'local',
        data_region: 'local',
        active: true,
        events_per_second: 1000,
        burst: 2000,
      })
      setId('')
      setName('')
      onChanged()
    } catch (cause) {
      setError(cause)
    }
  }
  return (
    <IrisCard variant="outline" header="租户与配额" className="section-card">
      <div className="three-field-form">
        <IrisFormField label="Tenant ID">
          <IrisInput value={id} onChange={(event) => setId(event.target.value)} />
        </IrisFormField>
        <IrisFormField label="名称">
          <IrisInput value={name} onChange={(event) => setName(event.target.value)} />
        </IrisFormField>
        <IrisButton
          variant="solid"
          disabled={!writable || !id.trim() || !name.trim()}
          onClick={() => void create()}
        >
          创建租户
        </IrisButton>
      </div>
      <ErrorNotice error={error} />
      <IrisTable rowKey="id" columns={tenantColumns} data={items} size="small" />
    </IrisCard>
  )
}

function SourceSection({
  items,
  tenantId,
  writable,
  onChanged,
}: {
  items: SourceSystem[]
  tenantId: string
  writable: boolean
  onChanged: () => void
}) {
  const api = useAuditApi()
  const [id, setId] = React.useState('')
  const [name, setName] = React.useState('')
  const [clients, setClients] = React.useState('')
  const [editing, setEditing] = React.useState<SourceSystem | null>(null)
  const [editName, setEditName] = React.useState('')
  const [editClients, setEditClients] = React.useState('')
  const [editActive, setEditActive] = React.useState(true)
  const [saving, setSaving] = React.useState(false)
  const [error, setError] = React.useState<unknown>(null)

  React.useEffect(() => {
    setEditing(null)
  }, [tenantId])

  const clientIDs = (value: string) => [
    ...new Set(
      value
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean),
    ),
  ]

  const selectSource = (source: SourceSystem) => {
    if (!writable) return
    setEditing(source)
    setEditName(source.name)
    setEditClients((source.allowed_client_ids ?? []).join(', '))
    setEditActive(source.active)
    setError(null)
  }

  const create = async () => {
    setError(null)
    try {
      await api.createSource(
        {
          id: id.trim(),
          tenant_id: tenantId,
          name: name.trim(),
          allowed_client_ids: clientIDs(clients),
          active: true,
        },
        tenantId,
      )
      setId('')
      setName('')
      setClients('')
      onChanged()
    } catch (cause) {
      setError(cause)
    }
  }

  const update = async () => {
    if (!editing) return
    setSaving(true)
    setError(null)
    try {
      await api.updateSource(
        {
          id: editing.id,
          name: editName.trim(),
          allowed_client_ids: clientIDs(editClients),
          active: editActive,
        },
        tenantId,
      )
      setEditing(null)
      onChanged()
    } catch (cause) {
      setError(cause)
    } finally {
      setSaving(false)
    }
  }
  return (
    <IrisCard variant="outline" header="来源系统与 Client 绑定" className="section-card">
      <div className="four-field-form">
        <IrisFormField label="Source ID">
          <IrisInput value={id} onChange={(event) => setId(event.target.value)} />
        </IrisFormField>
        <IrisFormField label="名称">
          <IrisInput value={name} onChange={(event) => setName(event.target.value)} />
        </IrisFormField>
        <IrisFormField label="允许的 client_id（逗号分隔）">
          <IrisInput value={clients} onChange={(event) => setClients(event.target.value)} />
        </IrisFormField>
        <IrisButton
          variant="solid"
          disabled={!writable || !id.trim() || !name.trim()}
          onClick={() => void create()}
        >
          添加来源
        </IrisButton>
      </div>
      <ErrorNotice error={error} />
      <p className="table-hint">
        {writable ? '选择来源行可编辑精确 Client 绑定与启用状态。' : '当前会话仅可查看来源。'}
      </p>
      <IrisTable
        rowKey="id"
        columns={sourceColumns}
        data={items}
        size="small"
        currentRowKey={editing?.id}
        onRowClick={writable ? selectSource : undefined}
      />
      {editing ? (
        <div className="source-editor">
          <strong>编辑来源：{editing.id}</strong>
          <div className="source-editor-form">
            <IrisFormField label="名称">
              <IrisInput
                value={editName}
                onChange={(event) => setEditName(event.target.value)}
              />
            </IrisFormField>
            <IrisFormField label="允许的 client_id（逗号分隔）">
              <IrisInput
                value={editClients}
                onChange={(event) => setEditClients(event.target.value)}
              />
            </IrisFormField>
            <label className="switch-field">
              <span>允许写入</span>
              <IrisSwitch checked={editActive} onChange={setEditActive} />
            </label>
            <div className="button-row">
              <IrisButton
                variant="solid"
                disabled={saving || !editName.trim()}
                onClick={() => void update()}
              >
                {saving ? '保存中…' : '保存绑定'}
              </IrisButton>
              <IrisButton variant="outline" disabled={saving} onClick={() => setEditing(null)}>
                取消
              </IrisButton>
            </div>
          </div>
        </div>
      ) : null}
    </IrisCard>
  )
}

function SchemaSection({
  items,
  tenantId,
  writable,
  onChanged,
}: {
  items: EventSchema[]
  tenantId: string
  writable: boolean
  onChanged: () => void
}) {
  const api = useAuditApi()
  const [schemaId, setSchemaId] = React.useState('')
  const [eventType, setEventType] = React.useState('')
  const [version, setVersion] = React.useState(1)
  const [classification, setClassification] = React.useState('internal')
  const [requiredFields, setRequiredFields] = React.useState('')
  const [allowedFields, setAllowedFields] = React.useState('')
  const [encryptedFields, setEncryptedFields] = React.useState('')
  const [searchableFields, setSearchableFields] = React.useState('')
  const [selectedKey, setSelectedKey] = React.useState<string>()
  const [saving, setSaving] = React.useState(false)
  const [error, setError] = React.useState<unknown>(null)
  const rows = React.useMemo<SchemaRow[]>(
    () => items.map((schema) => ({ ...schema, row_key: `${schema.schema_id}:${schema.version}` })),
    [items],
  )
  const selectedSchema = rows.find((schema) => schema.row_key === selectedKey)

  React.useEffect(() => {
    setSelectedKey(undefined)
  }, [tenantId])

  const create = async () => {
    setError(null)
    const policy = {
      requiredFields: parseSchemaFieldList(requiredFields),
      allowedFields: parseSchemaFieldList(allowedFields),
      encryptedFields: parseSchemaFieldList(encryptedFields),
      searchableFields: parseSchemaFieldList(searchableFields),
    }
    const policyError = validateSchemaFieldPolicy(policy)
    if (policyError) {
      setError(new Error(policyError))
      return
    }
    setSaving(true)
    try {
      await api.createSchema(
        {
          tenant_id: tenantId,
          schema_id: schemaId.trim(),
          version,
          event_type: eventType.trim(),
          required_fields: policy.requiredFields,
          allowed_fields: policy.allowedFields,
          encrypted_fields: policy.encryptedFields,
          searchable_fields: policy.searchableFields,
          classification,
          active: true,
        },
        tenantId,
      )
      setSchemaId('')
      setEventType('')
      setVersion(1)
      setClassification('internal')
      setRequiredFields('')
      setAllowedFields('')
      setEncryptedFields('')
      setSearchableFields('')
      onChanged()
    } catch (cause) {
      setError(cause)
    } finally {
      setSaving(false)
    }
  }
  return (
    <IrisCard variant="outline" header="事件 Schema" className="section-card">
      <div className="schema-core-form">
        <IrisFormField label="Schema ID">
          <IrisInput value={schemaId} onChange={(event) => setSchemaId(event.target.value)} />
        </IrisFormField>
        <IrisFormField label="事件类型">
          <IrisInput value={eventType} onChange={(event) => setEventType(event.target.value)} />
        </IrisFormField>
        <IrisFormField label="版本">
          <IrisInput
            type="number"
            min={1}
            value={version}
            onChange={(event) => setVersion(Number(event.target.value))}
          />
        </IrisFormField>
        <IrisFormField label="数据分级">
          <IrisSelect
            items={classificationItems}
            value={classification}
            onValueChange={(value) => setClassification(String(value))}
          />
        </IrisFormField>
        <IrisButton
          variant="solid"
          disabled={
            saving ||
            !writable ||
            !schemaId.trim() ||
            !eventType.trim() ||
            !Number.isInteger(version) ||
            version < 1
          }
          onClick={() => void create()}
        >
          {saving ? '注册中…' : '注册 Schema'}
        </IrisButton>
      </div>
      <div className="schema-policy-form">
        <IrisFormField label="必填字段（逗号分隔）">
          <IrisInput
            value={requiredFields}
            placeholder="event_id, actor.id, resource"
            onChange={(event) => setRequiredFields(event.target.value)}
          />
        </IrisFormField>
        <IrisFormField label="允许的 payload 字段">
          <IrisInput
            value={allowedFields}
            placeholder="resource, object_id, email"
            onChange={(event) => setAllowedFields(event.target.value)}
          />
        </IrisFormField>
        <IrisFormField label="加密字段">
          <IrisInput
            value={encryptedFields}
            placeholder="email"
            onChange={(event) => setEncryptedFields(event.target.value)}
          />
        </IrisFormField>
        <IrisFormField label="可搜索字段">
          <IrisInput
            value={searchableFields}
            placeholder="email"
            onChange={(event) => setSearchableFields(event.target.value)}
          />
        </IrisFormField>
      </div>
      <p className="table-hint">
        留空允许字段表示不限制 payload 键；可搜索字段生成租户隔离摘要，不要求同时加密。
      </p>
      <ErrorNotice error={error} />
      <p className="table-hint">选择 Schema 版本可检查其完整字段保护策略。</p>
      <IrisTable
        rowKey="row_key"
        columns={schemaColumns}
        data={rows}
        size="small"
        currentRowKey={selectedKey}
        onRowClick={(schema) => setSelectedKey(schema.row_key)}
      />
      {selectedSchema ? (
        <div className="schema-policy-view">
          <strong>
            策略详情：{selectedSchema.schema_id} v{selectedSchema.version}
          </strong>
          <IrisDescriptions
            bordered
            columns={2}
            items={[
              { label: '必填字段', value: fieldSummary(selectedSchema.required_fields) },
              { label: '允许字段', value: fieldSummary(selectedSchema.allowed_fields) },
              { label: '加密字段', value: fieldSummary(selectedSchema.encrypted_fields) },
              { label: '可搜索字段', value: fieldSummary(selectedSchema.searchable_fields) },
              { label: '数据分级', value: selectedSchema.classification || '未限制' },
              { label: '创建时间', value: formatDate(selectedSchema.created_at) },
            ]}
          />
        </div>
      ) : null}
    </IrisCard>
  )
}

function RetentionSection({
  policy,
  tenantId,
  writable,
  onChanged,
}: {
  policy: RetentionPolicy | null
  tenantId: string
  writable: boolean
  onChanged: () => void
}) {
  const api = useAuditApi()
  const [draft, setDraft] = React.useState<RetentionPolicy>({
    tenant_id: tenantId,
    hot_days: 30,
    warm_days: 90,
    archive_days: 365,
    retention_class: 'default',
  })
  const [error, setError] = React.useState<unknown>(null)
  const [report, setReport] = React.useState<RetentionReport | null>(null)
  const [busy, setBusy] = React.useState(false)
  React.useEffect(() => {
    setDraft(
      policy ?? {
        tenant_id: tenantId,
        hot_days: 30,
        warm_days: 90,
        archive_days: 365,
        retention_class: 'default',
      },
    )
    setReport(null)
  }, [policy, tenantId])
  const setNumber = (key: 'hot_days' | 'warm_days' | 'archive_days', value: string) =>
    setDraft((current) => ({ ...current, [key]: Number(value) }))
  const save = async () => {
    setBusy(true)
    setError(null)
    try {
      await api.setRetention({ ...draft, tenant_id: tenantId }, tenantId)
      onChanged()
    } catch (cause) {
      setError(cause)
    } finally {
      setBusy(false)
    }
  }
  const evaluate = async () => {
    setBusy(true)
    setError(null)
    try {
      setReport(await api.evaluateRetention(tenantId))
    } catch (cause) {
      setError(cause)
    } finally {
      setBusy(false)
    }
  }
  return (
    <IrisCard variant="outline" header="保留策略" className="section-card">
      <div className="retention-form">
        <IrisFormField label="Retention class">
          <IrisInput
            value={draft.retention_class}
            onChange={(event) =>
              setDraft((current) => ({ ...current, retention_class: event.target.value }))
            }
          />
        </IrisFormField>
        <IrisFormField label="热存储天数">
          <IrisInput
            type="number"
            min={0}
            value={draft.hot_days}
            onChange={(event) => setNumber('hot_days', event.target.value)}
          />
        </IrisFormField>
        <IrisFormField label="温存储天数">
          <IrisInput
            type="number"
            min={0}
            value={draft.warm_days}
            onChange={(event) => setNumber('warm_days', event.target.value)}
          />
        </IrisFormField>
        <IrisFormField label="归档天数">
          <IrisInput
            type="number"
            min={0}
            value={draft.archive_days}
            onChange={(event) => setNumber('archive_days', event.target.value)}
          />
        </IrisFormField>
        <div className="button-row">
          <IrisButton variant="solid" disabled={!writable || busy} onClick={() => void save()}>
            {busy ? '处理中…' : '保存策略'}
          </IrisButton>
          <IrisButton variant="outline" disabled={busy} onClick={() => void evaluate()}>
            评估
          </IrisButton>
        </div>
      </div>
      <ErrorNotice error={error} />
      {report ? (
        <div className="result-panel">
          <StatusBadge value={report.action} />
          <span>可归档 {report.eligible_events} 条</span>
          <span>法律保留保护 {report.protected_events} 条</span>
          <span>保留规则 {report.hold_ids?.join(', ') || '无'}</span>
        </div>
      ) : null}
    </IrisCard>
  )
}
