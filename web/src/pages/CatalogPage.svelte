<script lang="ts">
  import {
    IrisButton,
    IrisCard,
    IrisDescriptions,
    IrisFormField,
    IrisInput,
    IrisSelect,
    IrisSwitch,
    IrisTable,
  } from '@iris-ui-kit/svelte'
  import { useAuditApi } from '../api/useAuditApi'
  import type { AdminAction, EventSchema, RetentionPolicy, RetentionReport, SourceSystem, Tenant } from '../api/types'
  import { can } from '../auth/claims'
  import { authState } from '../auth/store'
  import ErrorNotice from '../components/ErrorNotice.svelte'
  import PageHeader from '../components/PageHeader.svelte'
  import StatusBadge from '../components/StatusBadge.svelte'
  import { formatDate } from '../components/format'
  import { assessPlatformAuditContracts } from './platformAuditContracts'
  import {
    canEvaluateRetention,
    canSaveRetention,
    defaultRetentionPolicy,
    retentionReadFailureState,
    type RetentionState,
  } from './retentionState'
  import { parseSchemaFieldList, validateSchemaFieldPolicy } from './schemaPolicy'

  let { tenantId }: { tenantId?: string } = $props()
  const api = useAuditApi()
  const session = $derived($authState.session)
  const writable = $derived(Boolean(session && can(session, 'audit:policy:write')))
  let tenants = $state<Tenant[]>([])
  let sources = $state<SourceSystem[]>([])
  let schemas = $state<EventSchema[]>([])
  let actions = $state<AdminAction[]>([])
  let error = $state<unknown>(null)
  let revision = $state(0)
  let retentionScope = ''
  let loadGeneration = 0
  let retentionOperationGeneration = 0

  const classificationItems = [
    { value: 'public', label: '公开（public）' },
    { value: 'internal', label: '内部（internal）' },
    { value: 'personal', label: '个人数据（personal）' },
    { value: 'confidential', label: '机密（confidential）' },
    { value: 'restricted', label: '受限（restricted）' },
  ]

  const tenantColumns = [
    { key: 'id', title: 'Tenant ID', width: 150 },
    { key: 'name', title: '名称', minWidth: 160 },
    { key: 'home_region', title: '主区域', width: 120 },
    { key: 'events_per_second', title: 'EPS', width: 80 },
    { key: 'active', title: '状态', width: 100 },
  ]
  const platformColumns = [
    { key: 'service', title: '平台服务', minWidth: 190 },
    { key: 'client_id', title: 'Snaplink Client', minWidth: 170 },
    { key: 'source_id', title: 'Source ID', minWidth: 210 },
    { key: 'schema_ref', title: 'Schema', minWidth: 210 },
    { key: 'ready_text', title: '状态', width: 100 },
    { key: 'detail', title: '契约检查', minWidth: 280 },
  ]
  const actionColumns = [
    { key: 'created_at', title: '时间', width: 180 },
    { key: 'action', title: '动作', width: 190 },
    { key: 'actor', title: '操作者', width: 140 },
    { key: 'target_id', title: '目标', minWidth: 180 },
    { key: 'detail', title: '详情', minWidth: 220 },
  ]

  async function load(): Promise<void> {
    const generation = ++loadGeneration
    const scoped = Boolean(tenantId || !session?.platform)
    const policyTenantId = tenantId ?? session?.tenantId ?? ''
    error = null

    if (scoped) {
      if (retentionScope !== policyTenantId) {
        retentionScope = policyTenantId
        draft = { tenant_id: policyTenantId, hot_days: 0, warm_days: 0, archive_days: 0, retention_class: '' }
        retentionBusy = false
        retentionOperationGeneration += 1
      }
      retentionState = 'loading'
      retentionError = null
      retentionReport = null
      void api.getRetention(tenantId).then((loadedPolicy) => {
        if (generation !== loadGeneration) return
        draft = { ...loadedPolicy }
        retentionState = 'configured'
        retentionError = null
      }).catch((cause: unknown) => {
        if (generation !== loadGeneration) return
        const failedState = retentionReadFailureState(cause)
        retentionState = failedState
        retentionError = failedState === 'error' ? cause : null
        retentionReport = null
      })
    } else {
      retentionScope = ''
      retentionState = 'unscoped'
      retentionError = null
      retentionReport = null
      retentionBusy = false
      retentionOperationGeneration += 1
    }

    try {
      const [tenantResult, sourceResult, schemaResult, actionResult] = await Promise.all([
        session?.platform ? api.listTenants() : Promise.resolve({ items: [] as Tenant[], count: 0 }),
        scoped ? api.listSources(tenantId) : Promise.resolve({ items: [] as SourceSystem[], count: 0 }),
        scoped ? api.listSchemas(tenantId) : Promise.resolve({ items: [] as EventSchema[], count: 0 }),
        api.listAdminActions(tenantId, 100),
      ])
      if (generation !== loadGeneration) return
      tenants = tenantResult.items
      sources = sourceResult.items
      schemas = schemaResult.items
      actions = actionResult.items
    } catch (cause) {
      if (generation === loadGeneration) error = cause
    }
  }

  $effect(() => {
    void tenantId
    void session?.platform
    void session?.tenantId
    void revision
    void load()
  })

  function refresh(): void {
    revision += 1
  }

  function tableRows<T extends Record<string, unknown>>(items: T[]): T[] {
    return items
  }
  const tenantRows = $derived(tableRows(tenants as unknown as Record<string, unknown>[]))
  const platformRows = $derived(
    assessPlatformAuditContracts(sources, schemas).map((status) => ({
      ...status,
      ready_text: status.ready ? 'valid' : 'invalid',
    })),
  )
  const actionRows = $derived(
    actions.map((action) => ({ ...action, created_at: formatDate(action.created_at) })),
  )

  // Tenant management
  let tenantDraftId = $state('')
  let tenantDraftName = $state('')
  let tenantError = $state<unknown>(null)
  async function createTenant(): Promise<void> {
    tenantError = null
    try {
      await api.createTenant({
        id: tenantDraftId.trim(),
        name: tenantDraftName.trim(),
        home_region: 'local',
        data_region: 'local',
        active: true,
        events_per_second: 1000,
        burst: 2000,
      })
      tenantDraftId = ''
      tenantDraftName = ''
      refresh()
    } catch (cause) {
      tenantError = cause
    }
  }

  // Source management
  let sourceId = $state('')
  let sourceName = $state('')
  let sourceClients = $state('')
  let editingSource = $state<SourceSystem | null>(null)
  let editSourceName = $state('')
  let editSourceClients = $state('')
  let editSourceActive = $state(true)
  let sourceBusy = $state(false)
  let sourceError = $state<unknown>(null)

  function clientIds(value: string): string[] {
    return [...new Set(value.split(',').map((item) => item.trim()).filter(Boolean))]
  }
  function selectSource(source: SourceSystem): void {
    if (!writable) return
    editingSource = source
    editSourceName = source.name
    editSourceClients = (source.allowed_client_ids ?? []).join(', ')
    editSourceActive = source.active
    sourceError = null
  }
  async function createSource(): Promise<void> {
    sourceError = null
    try {
      await api.createSource({ id: sourceId.trim(), tenant_id: tenantId ?? session?.tenantId ?? '', name: sourceName.trim(), allowed_client_ids: clientIds(sourceClients), active: true }, tenantId)
      sourceId = ''
      sourceName = ''
      sourceClients = ''
      refresh()
    } catch (cause) {
      sourceError = cause
    }
  }
  async function updateSource(): Promise<void> {
    if (!editingSource) return
    sourceBusy = true
    sourceError = null
    try {
      await api.updateSource({ id: editingSource.id, name: editSourceName.trim(), allowed_client_ids: clientIds(editSourceClients), active: editSourceActive }, tenantId)
      editingSource = null
      refresh()
    } catch (cause) {
      sourceError = cause
    } finally {
      sourceBusy = false
    }
  }

  // Schema management
  let schemaId = $state('')
  let eventType = $state('')
  let schemaVersion = $state(1)
  let classification = $state('internal')
  let requiredFields = $state('')
  let allowedFields = $state('')
  let encryptedFields = $state('')
  let searchableFields = $state('')
  let selectedSchemaKey = $state('')
  let schemaBusy = $state(false)
  let schemaError = $state<unknown>(null)
  const schemaRows = $derived(schemas.map((schema) => ({ ...schema, row_key: `${schema.schema_id}:${schema.version}` })))
  const selectedSchema = $derived(schemaRows.find((schema) => schema.row_key === selectedSchemaKey))

  function fieldSummary(fields?: string[]): string {
    return fields?.length ? fields.join(', ') : '未配置'
  }
  async function createSchema(): Promise<void> {
    schemaError = null
    const fieldPolicy = {
      requiredFields: parseSchemaFieldList(requiredFields),
      allowedFields: parseSchemaFieldList(allowedFields),
      encryptedFields: parseSchemaFieldList(encryptedFields),
      searchableFields: parseSchemaFieldList(searchableFields),
    }
    const policyError = validateSchemaFieldPolicy(fieldPolicy)
    if (policyError) {
      schemaError = new Error(policyError)
      return
    }
    schemaBusy = true
    try {
      await api.createSchema({
        tenant_id: tenantId ?? session?.tenantId ?? '',
        schema_id: schemaId.trim(),
        version: schemaVersion,
        event_type: eventType.trim(),
        required_fields: fieldPolicy.requiredFields,
        allowed_fields: fieldPolicy.allowedFields,
        encrypted_fields: fieldPolicy.encryptedFields,
        searchable_fields: fieldPolicy.searchableFields,
        classification,
        active: true,
      }, tenantId)
      schemaId = ''
      eventType = ''
      schemaVersion = 1
      classification = 'internal'
      requiredFields = ''
      allowedFields = ''
      encryptedFields = ''
      searchableFields = ''
      refresh()
    } catch (cause) {
      schemaError = cause
    } finally {
      schemaBusy = false
    }
  }

  // Retention management
  let draft = $state<RetentionPolicy>({ tenant_id: '', hot_days: 0, warm_days: 0, archive_days: 0, retention_class: '' })
  let retentionState = $state<RetentionState>('unscoped')
  let retentionError = $state<unknown>(null)
  let retentionReport = $state<RetentionReport | null>(null)
  let retentionBusy = $state(false)

  function beginRetentionCreation(): void {
    if (retentionState !== 'unconfigured' || !writable) return
    draft = defaultRetentionPolicy(tenantId ?? session?.tenantId ?? '')
    retentionState = 'creating'
    retentionError = null
    retentionReport = null
  }

  async function saveRetention(): Promise<void> {
    if (!canSaveRetention(retentionState, writable, retentionBusy)) return
    const generation = loadGeneration
    const operationGeneration = ++retentionOperationGeneration
    const targetTenantId = tenantId ?? session?.tenantId ?? ''
    const stateAtSubmit = retentionState
    retentionBusy = true
    retentionError = null
    try {
      const savedPolicy = await api.setRetention({ ...draft, tenant_id: targetTenantId }, tenantId)
      if (generation !== loadGeneration || operationGeneration !== retentionOperationGeneration) return
      draft = { ...savedPolicy }
      retentionState = 'configured'
      retentionReport = null
      refresh()
    } catch (cause) {
      if (generation !== loadGeneration || operationGeneration !== retentionOperationGeneration) return
      retentionState = stateAtSubmit
      retentionError = cause
    } finally {
      if (operationGeneration === retentionOperationGeneration) retentionBusy = false
    }
  }

  async function evaluateRetention(): Promise<void> {
    if (!canEvaluateRetention(retentionState, retentionBusy)) return
    const generation = loadGeneration
    const operationGeneration = ++retentionOperationGeneration
    retentionBusy = true
    retentionError = null
    try {
      const report = await api.evaluateRetention(tenantId)
      if (generation !== loadGeneration || operationGeneration !== retentionOperationGeneration) return
      retentionReport = report
    } catch (cause) {
      if (generation !== loadGeneration || operationGeneration !== retentionOperationGeneration) return
      retentionError = cause
    } finally {
      if (operationGeneration === retentionOperationGeneration) retentionBusy = false
    }
  }
</script>

<section>
  {#snippet headerActions()}<IrisButton variant="outline" onclick={refresh}>刷新</IrisButton>{/snippet}
  <PageHeader title="治理目录" description="来源绑定、事件 Schema、租户配额与保留策略；每次变更都会进入管理自审计。" actions={headerActions} />
  <ErrorNotice {error} onRetry={refresh} />

  {#if session?.platform}
    <IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
      <h3 class="card-heading">租户与配额</h3>
      <div class="three-field-form">
        <IrisFormField label="Tenant ID"><IrisInput value={tenantDraftId} oninput={(event) => (tenantDraftId = event.currentTarget.value)} /></IrisFormField>
        <IrisFormField label="名称"><IrisInput value={tenantDraftName} oninput={(event) => (tenantDraftName = event.currentTarget.value)} /></IrisFormField>
        <IrisButton variant="solid" disabled={!writable || !tenantDraftId.trim() || !tenantDraftName.trim()} onclick={() => void createTenant()}>创建租户</IrisButton>
      </div>
      <ErrorNotice error={tenantError} />
      <IrisTable rowKey="id" columns={tenantColumns} data={tenantRows} density="compact" />
    </IrisCard>
  {/if}

  {#if session?.platform && !tenantId}
    <p class="muted-copy">选择租户后可管理该租户的来源、Schema 与保留策略。</p>
  {:else}
    <IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
      <h3 class="card-heading">平台审计接入状态</h3>
      <p class="card-copy">按六项目部署契约检查 Aero ID、Aero IM 与 Aero Vault 的 M2M Client、租户来源和固定 Schema 版本。租户派生 Source ID 仅校验公开前缀，不在浏览器中处理派生密钥。</p>
      <IrisTable rowKey="key" columns={platformColumns} data={platformRows} density="compact" striped />
    </IrisCard>

    <IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
      <h3 class="card-heading">来源系统与 Client 绑定</h3>
      <div class="four-field-form">
        <IrisFormField label="Source ID"><IrisInput value={sourceId} oninput={(event) => (sourceId = event.currentTarget.value)} /></IrisFormField>
        <IrisFormField label="名称"><IrisInput value={sourceName} oninput={(event) => (sourceName = event.currentTarget.value)} /></IrisFormField>
        <IrisFormField label="允许的 client_id（逗号分隔）"><IrisInput value={sourceClients} oninput={(event) => (sourceClients = event.currentTarget.value)} /></IrisFormField>
        <IrisButton variant="solid" disabled={!writable || !sourceId.trim() || !sourceName.trim()} onclick={() => void createSource()}>添加来源</IrisButton>
      </div>
      <ErrorNotice error={sourceError} />
      <p class="table-hint">{writable ? '选择来源行可编辑精确 Client 绑定与启用状态。' : '当前会话仅可查看来源。'}</p>
      <div class="data-table-wrap"><table class="data-table"><thead><tr><th>Source ID</th><th>名称</th><th>允许的 Snaplink Client</th><th>状态</th><th></th></tr></thead><tbody>
        {#each sources as source (source.id)}
          <tr><td>{source.id}</td><td>{source.name}</td><td>{source.allowed_client_ids?.join(', ') || '默认同 Source ID'}</td><td><StatusBadge value={source.active} /></td><td><IrisButton size="sm" variant="outline" disabled={!writable} onclick={() => selectSource(source)}>编辑</IrisButton></td></tr>
        {:else}<tr><td colspan="5" class="empty-cell">暂无来源</td></tr>{/each}
      </tbody></table></div>
      {#if editingSource}
        <div class="source-editor"><strong>编辑来源：{editingSource.id}</strong><div class="source-editor-form">
          <IrisFormField label="名称"><IrisInput value={editSourceName} oninput={(event) => (editSourceName = event.currentTarget.value)} /></IrisFormField>
          <IrisFormField label="允许的 client_id（逗号分隔）"><IrisInput value={editSourceClients} oninput={(event) => (editSourceClients = event.currentTarget.value)} /></IrisFormField>
          <label class="switch-field"><span>允许写入</span><IrisSwitch checked={editSourceActive} onChange={(next) => (editSourceActive = next)} /></label>
          <div class="button-row"><IrisButton variant="solid" disabled={sourceBusy || !editSourceName.trim()} onclick={() => void updateSource()}>{sourceBusy ? '保存中…' : '保存绑定'}</IrisButton><IrisButton variant="outline" disabled={sourceBusy} onclick={() => (editingSource = null)}>取消</IrisButton></div>
        </div></div>
      {/if}
    </IrisCard>

    <IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
      <h3 class="card-heading">事件 Schema</h3>
      <div class="schema-core-form">
        <IrisFormField label="Schema ID"><IrisInput value={schemaId} oninput={(event) => (schemaId = event.currentTarget.value)} /></IrisFormField>
        <IrisFormField label="事件类型"><IrisInput value={eventType} oninput={(event) => (eventType = event.currentTarget.value)} /></IrisFormField>
        <IrisFormField label="版本"><IrisInput type="number" min={1} value={schemaVersion} oninput={(event) => (schemaVersion = Number(event.currentTarget.value))} /></IrisFormField>
        <IrisFormField label="数据分级"><IrisSelect items={classificationItems} value={classification} onValueChange={(value) => (classification = String(value))} /></IrisFormField>
        <IrisButton variant="solid" disabled={schemaBusy || !writable || !schemaId.trim() || !eventType.trim() || !Number.isInteger(schemaVersion) || schemaVersion < 1} onclick={() => void createSchema()}>{schemaBusy ? '注册中…' : '注册 Schema'}</IrisButton>
      </div>
      <div class="schema-policy-form">
        <IrisFormField label="必填字段（逗号分隔）"><IrisInput value={requiredFields} placeholder="event_id, actor.id, resource" oninput={(event) => (requiredFields = event.currentTarget.value)} /></IrisFormField>
        <IrisFormField label="允许的 payload 字段"><IrisInput value={allowedFields} placeholder="resource, object_id, email" oninput={(event) => (allowedFields = event.currentTarget.value)} /></IrisFormField>
        <IrisFormField label="加密字段"><IrisInput value={encryptedFields} placeholder="email" oninput={(event) => (encryptedFields = event.currentTarget.value)} /></IrisFormField>
        <IrisFormField label="可搜索字段"><IrisInput value={searchableFields} placeholder="email" oninput={(event) => (searchableFields = event.currentTarget.value)} /></IrisFormField>
      </div>
      <p class="table-hint">留空允许字段表示不限制 payload 键；可搜索字段生成租户隔离摘要，不要求同时加密。</p>
      <ErrorNotice error={schemaError} />
      <div class="data-table-wrap"><table class="data-table"><thead><tr><th>Schema ID</th><th>版本</th><th>事件类型</th><th>分级</th><th>状态</th></tr></thead><tbody>
        {#each schemaRows as schema (schema.row_key)}
          <tr class:selected-row={selectedSchemaKey === schema.row_key} onclick={() => (selectedSchemaKey = schema.row_key)}><td>{schema.schema_id}</td><td>{schema.version}</td><td>{schema.event_type}</td><td>{schema.classification}</td><td><StatusBadge value={schema.active} /></td></tr>
        {:else}<tr><td colspan="5" class="empty-cell">暂无 Schema</td></tr>{/each}
      </tbody></table></div>
      {#if selectedSchema}
        <div class="schema-policy-view"><strong>策略详情：{selectedSchema.schema_id} v{selectedSchema.version}</strong><IrisDescriptions style="" bordered columns={2} items={[{ label: '必填字段', value: fieldSummary(selectedSchema.required_fields) }, { label: '允许字段', value: fieldSummary(selectedSchema.allowed_fields) }, { label: '加密字段', value: fieldSummary(selectedSchema.encrypted_fields) }, { label: '可搜索字段', value: fieldSummary(selectedSchema.searchable_fields) }, { label: '数据分级', value: selectedSchema.classification || '未限制' }, { label: '创建时间', value: formatDate(selectedSchema.created_at) }]} /></div>
      {/if}
    </IrisCard>

    <IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
      <h3 class="card-heading">保留策略</h3>
      {#if retentionState === 'loading'}
        <p class="muted-copy" role="status" aria-live="polite">正在读取当前租户的保留策略…</p>
      {:else if retentionState === 'error'}
        <ErrorNotice error={retentionError} onRetry={refresh} />
      {:else if retentionState === 'unconfigured'}
        <div class="retention-empty-state">
          <p class="muted-copy">当前租户尚未配置保留策略。此状态仅在 API 明确返回 404 时显示；不会将读取错误当作空策略，也不会自动写入默认值。</p>
          <IrisButton variant="solid" disabled={!writable} onclick={beginRetentionCreation}>新建保留策略（默认草稿）</IrisButton>
        </div>
      {:else if retentionState === 'creating' || retentionState === 'configured'}
        {#if retentionState === 'creating'}
          <p class="table-hint">这是尚未保存的默认策略草稿；确认后显式保存才会创建策略。</p>
        {/if}
        <div class="retention-form">
          <IrisFormField label="Retention class"><IrisInput value={draft.retention_class} oninput={(event) => (draft.retention_class = event.currentTarget.value)} /></IrisFormField>
          <IrisFormField label="热存储天数"><IrisInput type="number" min={0} value={draft.hot_days} oninput={(event) => (draft.hot_days = Number(event.currentTarget.value))} /></IrisFormField>
          <IrisFormField label="温存储天数"><IrisInput type="number" min={0} value={draft.warm_days} oninput={(event) => (draft.warm_days = Number(event.currentTarget.value))} /></IrisFormField>
          <IrisFormField label="归档天数"><IrisInput type="number" min={0} value={draft.archive_days} oninput={(event) => (draft.archive_days = Number(event.currentTarget.value))} /></IrisFormField>
          <div class="button-row"><IrisButton variant="solid" disabled={!canSaveRetention(retentionState, writable, retentionBusy)} onclick={() => void saveRetention()}>{retentionBusy ? '处理中…' : '保存策略'}</IrisButton><IrisButton variant="outline" disabled={!canEvaluateRetention(retentionState, retentionBusy)} onclick={() => void evaluateRetention()}>评估</IrisButton></div>
        </div>
        <ErrorNotice error={retentionError} />
        {#if retentionReport}<div class="result-panel"><StatusBadge value={retentionReport.action} /><span>可归档 {retentionReport.eligible_events} 条</span><span>法律保留保护 {retentionReport.protected_events} 条</span><span>保留规则 {retentionReport.hold_ids?.join(', ') || '无'}</span></div>{/if}
      {/if}
    </IrisCard>
  {/if}

  <IrisCard variant="outline" class="section-card" padding="none" style="" header={undefined} footer={undefined}>
    <h3 class="card-heading card-heading-padded">管理操作自审计</h3>
    <IrisTable rowKey="id" columns={actionColumns} data={actionRows} density="compact" />
  </IrisCard>
</section>
