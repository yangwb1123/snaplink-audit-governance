<script lang="ts">
  import { onDestroy, untrack } from 'svelte'
  import { RequestScope } from '../api/requestScope'
  import ReadState from '../components/ReadState.svelte'
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
  import { TENANTS_CHANGED_EVENT } from '../tenantScope'
  import { assessPlatformAuditContracts } from './platformAuditContracts'
  import { ADMIN_ACTION_LIMIT, adminActionsContext } from './adminActions'
  import {
    canEvaluateRetention,
    canSaveRetention,
    defaultRetentionPolicy,
    retentionReadFailureState,
    type RetentionState,
  } from './retentionState'
  import { parseSchemaFieldList, validateSchemaFieldPolicy } from './schemaPolicy'

  let { tenantId }: { tenantId?: string } = $props()
  const session = $derived($authState.session)
  const readable = $derived(Boolean($authState.session && can($authState.session, 'audit:policy:read')))
  const writable = $derived(Boolean(readable && $authState.session && can($authState.session, 'audit:policy:write')))
  const scoped = $derived(Boolean(readable && $authState.session && ($authState.session.platform ? tenantId : $authState.session.tenantId && (!tenantId || tenantId === $authState.session.tenantId))))
  type ListRead<T> = { status: 'idle' | 'loading' | 'success' | 'error'; items: T[]; error: unknown; generation: number }
  function emptyRead<T>(): ListRead<T> { return { status: 'idle', items: [], error: null, generation: 0 } }
  const tenantRead = $state(emptyRead<Tenant>())
  const sourceRead = $state(emptyRead<SourceSystem>())
  const schemaRead = $state(emptyRead<EventSchema>())
  const actionRead = $state(emptyRead<AdminAction>())
  const sources = $derived(sourceRead.items)
  const schemas = $derived(schemaRead.items)
  const reads = { tenants: new RequestScope(), sources: new RequestScope(), schemas: new RequestScope(), actions: new RequestScope(), retention: new RequestScope() }
  const writes = { tenants: new RequestScope(), sources: new RequestScope(), schemas: new RequestScope(), retention: new RequestScope() }
  const allScopes = [...Object.values(reads), ...Object.values(writes)]
  let contractGeneration = $state(0)
  const context = $derived(adminActionsContext($authState.session, tenantId))
  let loadedContext = $state('')
  const currentContext = $derived(loadedContext === context)
  const actions = $derived(currentContext ? actionRead.items : [])
  const contractKnown = $derived(scoped && currentContext && sourceRead.status === 'success' && schemaRead.status === 'success' && sourceRead.generation === contractGeneration && schemaRead.generation === contractGeneration)
  onDestroy(() => allScopes.forEach((scope) => scope.dispose()))

  function begin(scope: RequestScope) {
    const request = scope.next()
    const expectedContext = context
    return { ...request, current: () => request.current() && adminActionsContext($authState.session, tenantId) === expectedContext }
  }

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

  async function readList<T>(state: ListRead<T>, scope: RequestScope, read: (signal: AbortSignal) => Promise<{ items: T[] }>, generation = 0): Promise<void> {
    const request = begin(scope)
    state.status = 'loading'; state.error = null; state.items = []; state.generation = generation
    try {
      const result = await read(request.signal)
      if (!request.current()) return
      state.items = result.items
      state.status = 'success'
    } catch (cause) {
      if (request.current()) { state.error = cause; state.status = 'error' }
    } finally { request.finish() }
  }

  function refreshTenants(): void {
    if (!readable || !session?.platform) return
    void readList(tenantRead, reads.tenants, (signal) => useAuditApi().listTenants(signal))
  }

  // Both dependencies start in the same epoch, but settle independently. A
  // retry never waits for, or hides, unrelated tenant/action/retention reads.
  function refreshContracts(): void {
    contractGeneration += 1 // revoke the previous verdict before starting I/O
    if (!scoped) return
    const tenant = tenantId ?? session?.tenantId
    const api = useAuditApi()
    const checkTenant = <T extends { tenant_id: string }>(result: { items: T[] }) => {
      if (result.items.some((item) => item.tenant_id !== tenant)) throw new Error('目录记录与当前租户不匹配')
      return result
    }
    editingSource = null; selectedSchemaKey = ''
    void readList(sourceRead, reads.sources, async (signal) => checkTenant(await api.listSources(tenantId, signal)), contractGeneration)
    void readList(schemaRead, reads.schemas, async (signal) => checkTenant(await api.listSchemas(tenantId, signal)), contractGeneration)
  }

  function refreshActions(): void {
    if (!scoped || !currentContext) return
    const tenant = session?.platform ? tenantId : session?.tenantId
    if (!tenant) return
    void readList(actionRead, reads.actions, async (signal) => {
      const result = await useAuditApi().listAdminActions(tenant, ADMIN_ACTION_LIMIT, signal)
      if (result.items.some((item) => item.tenant_id !== tenant)) throw new Error('管理自审计记录与当前租户不匹配')
      return { items: result.items.slice(0, ADMIN_ACTION_LIMIT) }
    })
  }

  async function refreshRetention(): Promise<void> {
    if (!scoped) return
    const request = begin(reads.retention)
    writes.retention.invalidate()
    retentionBusy = false; retentionState = 'loading'; retentionError = null; retentionReport = null
    try {
      const policy = await useAuditApi().getRetention(tenantId, request.signal)
      if (!request.current()) return
      if (policy.tenant_id !== (tenantId ?? session?.tenantId)) throw new Error('保留策略与当前租户不匹配')
      draft = { ...policy }; retentionState = 'configured'
    } catch (cause) {
      if (!request.current()) return
      retentionState = retentionReadFailureState(cause)
      retentionError = retentionState === 'error' ? cause : null
    } finally { request.finish() }
  }

  function refresh(): void {
    refreshTenants(); refreshContracts(); refreshActions(); void refreshRetention()
  }

  $effect(() => {
    const key = context
    untrack(() => {
      const expectedSession = session
      const expectedTenant = tenantId
      loadedContext = key
      allScopes.forEach((scope) => scope.invalidate())
      for (const state of [tenantRead, sourceRead, schemaRead, actionRead]) { state.status = 'idle'; state.items = []; state.error = null }
      editingSource = null; selectedSchemaKey = ''; sourceError = null; schemaError = null; tenantError = null
      sourceBusy = false; schemaBusy = false; tenantBusy = false
      sourceId = ''; sourceName = ''; sourceClients = ''; schemaId = ''; eventType = ''; schemaVersion = 1; classification = 'internal'
      editSourceName = ''; editSourceClients = ''; editSourceActive = true
      tenantDraftId = ''; tenantDraftName = ''; requiredFields = ''; allowedFields = ''; encryptedFields = ''; searchableFields = ''
      retentionState = 'unscoped'; retentionError = null; retentionReport = null; retentionBusy = false
      draft = { tenant_id: expectedTenant ?? expectedSession?.tenantId ?? '', hot_days: 0, warm_days: 0, archive_days: 0, retention_class: '' }
      refresh()
    })
    return () => allScopes.forEach((scope) => scope.invalidate())
  })

  function tableRows<T extends Record<string, unknown>>(items: T[]): T[] {
    return items
  }
  const tenantRows = $derived(tableRows(tenantRead.items as unknown as Record<string, unknown>[]))
  const platformRows = $derived(
    (contractKnown ? assessPlatformAuditContracts(sources, schemas) : []).map((status) => ({
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
  let tenantBusy = $state(false)
  async function createTenant(): Promise<void> {
    if (!writable || !session?.platform || tenantBusy) return
    const request = begin(writes.tenants)
    tenantError = null; tenantBusy = true
    try {
      await useAuditApi().createTenant({
        id: tenantDraftId.trim(),
        name: tenantDraftName.trim(),
        home_region: 'local',
        data_region: 'local',
        active: true,
        events_per_second: 1000,
        burst: 2000,
      }, request.signal)
      if (!request.current()) return
      tenantDraftId = ''
      tenantDraftName = ''
      window.dispatchEvent(new Event(TENANTS_CHANGED_EVENT))
      refreshTenants(); refreshActions()
    } catch (cause) {
      if (request.current()) tenantError = cause
    } finally { if (request.current()) tenantBusy = false; request.finish() }
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
    if (!writable || !scoped || sourceBusy) return
    const request = begin(writes.sources)
    sourceError = null; sourceBusy = true
    try {
      await useAuditApi().createSource({ id: sourceId.trim(), tenant_id: tenantId ?? session?.tenantId ?? '', name: sourceName.trim(), allowed_client_ids: clientIds(sourceClients), active: true }, tenantId, request.signal)
      if (!request.current()) return
      sourceId = ''; sourceName = ''; sourceClients = ''
      refreshContracts(); refreshActions()
    } catch (cause) {
      if (request.current()) sourceError = cause
    } finally { if (request.current()) sourceBusy = false; request.finish() }
  }
  async function updateSource(): Promise<void> {
    if (!editingSource || !writable || !scoped || sourceBusy) return
    const request = begin(writes.sources)
    sourceBusy = true; sourceError = null
    try {
      await useAuditApi().updateSource({ id: editingSource.id, name: editSourceName.trim(), allowed_client_ids: clientIds(editSourceClients), active: editSourceActive }, tenantId, request.signal)
      if (!request.current()) return
      editingSource = null
      refreshContracts(); refreshActions()
    } catch (cause) {
      if (request.current()) sourceError = cause
    } finally { if (request.current()) sourceBusy = false; request.finish() }
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

  function selectSchema(rowKey: string): void {
    selectedSchemaKey = rowKey
  }

  function handleSchemaKeydown(event: KeyboardEvent, rowKey: string): void {
    if (event.key !== 'Enter' && event.key !== ' ') return
    event.preventDefault()
    selectSchema(rowKey)
  }

  function fieldSummary(fields?: string[]): string {
    return fields?.length ? fields.join(', ') : '未配置'
  }
  async function createSchema(): Promise<void> {
    if (!writable || !scoped || schemaBusy) return
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
    const request = begin(writes.schemas)
    schemaBusy = true
    try {
      await useAuditApi().createSchema({
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
      }, tenantId, request.signal)
      if (!request.current()) return
      schemaId = ''
      eventType = ''
      schemaVersion = 1
      classification = 'internal'
      requiredFields = ''
      allowedFields = ''
      encryptedFields = ''
      searchableFields = ''
      refreshContracts(); refreshActions()
    } catch (cause) {
      if (request.current()) schemaError = cause
    } finally { if (request.current()) schemaBusy = false; request.finish() }
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
    if (!scoped) return
    const request = begin(writes.retention)
    const targetTenantId = tenantId ?? session?.tenantId ?? ''
    const stateAtSubmit = retentionState
    retentionBusy = true
    retentionError = null
    try {
      const savedPolicy = await useAuditApi().setRetention({ ...draft, tenant_id: targetTenantId }, tenantId, request.signal)
      if (!request.current()) return
      if (savedPolicy.tenant_id !== targetTenantId) throw new Error('保留策略与当前租户不匹配')
      draft = { ...savedPolicy }; retentionState = 'configured'; retentionReport = null
      refreshActions()
    } catch (cause) {
      if (!request.current()) return
      retentionState = stateAtSubmit; retentionError = cause
    } finally { if (request.current()) retentionBusy = false; request.finish() }
  }

  async function evaluateRetention(): Promise<void> {
    if (!scoped || !canEvaluateRetention(retentionState, retentionBusy)) return
    const request = begin(writes.retention)
    retentionBusy = true
    retentionError = null
    try {
      const report = await useAuditApi().evaluateRetention(tenantId, request.signal)
      if (!request.current()) return
      if (report.tenant_id !== (tenantId ?? session?.tenantId)) throw new Error('留存评估与当前租户不匹配')
      retentionReport = report
    } catch (cause) {
      if (request.current()) retentionError = cause
    } finally { if (request.current()) retentionBusy = false; request.finish() }
  }
</script>

<section>
  {#snippet headerActions()}<IrisButton variant="outline" onclick={refresh}>刷新</IrisButton>{/snippet}
  <PageHeader title="治理目录" description="来源绑定、事件 Schema、租户配额与保留策略；每次变更都会进入管理自审计。" actions={headerActions} />
  {#if !readable}<p role="alert">当前会话没有治理目录读取权限，不会发出请求。</p>{/if}

  {#if readable && session?.platform}
    <IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
      <h3 class="card-heading">租户与配额</h3>
      <IrisButton size="sm" variant="outline" onclick={refreshTenants}>刷新租户目录</IrisButton>
      <div class="three-field-form">
        <IrisFormField label="Tenant ID"><IrisInput value={tenantDraftId} oninput={(event) => (tenantDraftId = event.currentTarget.value)} /></IrisFormField>
        <IrisFormField label="名称"><IrisInput value={tenantDraftName} oninput={(event) => (tenantDraftName = event.currentTarget.value)} /></IrisFormField>
        <IrisButton variant="solid" disabled={!writable || tenantBusy || !tenantDraftId.trim() || !tenantDraftName.trim()} onclick={() => void createTenant()}>创建租户</IrisButton>
      </div>
      <ErrorNotice error={tenantError} />
      <ReadState loading={tenantRead.status === 'loading'} error={tenantRead.error} empty={tenantRead.status === 'success' && tenantRead.items.length === 0} loadingLabel="正在读取租户目录…" emptyLabel="暂无租户" onRetry={refreshTenants}>
        <IrisTable rowKey="id" columns={tenantColumns} data={tenantRows} density="compact" />
      </ReadState>
    </IrisCard>
  {/if}

  {#if session?.platform && !tenantId}
    <p class="muted-copy">选择租户后可管理该租户的来源、Schema 与保留策略。</p>
  {:else if readable && scoped}
    <IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
      <h3 class="card-heading">平台审计接入状态</h3>
      <p class="card-copy">按六项目部署契约检查 Aero ID、Aero IM 与 Aero Vault 的 M2M Client、租户来源和固定 Schema 版本。租户派生 Source ID 仅校验公开前缀，不在浏览器中处理派生密钥。</p>
      {#if contractKnown}
        <IrisTable rowKey="key" columns={platformColumns} data={platformRows} density="compact" striped />
      {:else}<p class="muted-copy" role="status">契约状态未知：须当前租户与会话下，同一刷新代次的来源和 Schema 均读取成功后才能评估；加载或读取失败不表示契约无效。</p>{/if}
    </IrisCard>

    <IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
      <h3 class="card-heading">来源系统与 Client 绑定</h3>
      <IrisButton size="sm" variant="outline" onclick={refreshContracts}>刷新来源</IrisButton>
      <p class="table-hint">刷新或重试来源 / Schema 会同步启动两项独立读取，以重新确认同代契约。</p>
      <div class="four-field-form">
        <IrisFormField label="Source ID"><IrisInput value={sourceId} oninput={(event) => (sourceId = event.currentTarget.value)} /></IrisFormField>
        <IrisFormField label="名称"><IrisInput value={sourceName} oninput={(event) => (sourceName = event.currentTarget.value)} /></IrisFormField>
        <IrisFormField label="允许的 client_id（逗号分隔）"><IrisInput value={sourceClients} oninput={(event) => (sourceClients = event.currentTarget.value)} /></IrisFormField>
        <IrisButton variant="solid" disabled={!writable || sourceBusy || !sourceId.trim() || !sourceName.trim()} onclick={() => void createSource()}>添加来源</IrisButton>
      </div>
      <ErrorNotice error={sourceError} />
      <p class="table-hint">{writable ? '选择来源行可编辑精确 Client 绑定与启用状态。' : '当前会话仅可查看来源。'}</p>
      <ReadState loading={sourceRead.status === 'loading'} error={sourceRead.error} empty={sourceRead.status === 'success' && sources.length === 0} loadingLabel="正在读取来源…" emptyLabel="暂无来源" onRetry={refreshContracts}>
      <div class="data-table-wrap"><table class="data-table"><thead><tr><th>Source ID</th><th>名称</th><th>允许的 Snaplink Client</th><th>状态</th><th></th></tr></thead><tbody>
        {#each sources as source (source.id)}
          <tr><td>{source.id}</td><td>{source.name}</td><td>{source.allowed_client_ids?.join(', ') || '默认同 Source ID'}</td><td><StatusBadge value={source.active} /></td><td><IrisButton size="sm" variant="outline" disabled={!writable} onclick={() => selectSource(source)}>编辑</IrisButton></td></tr>
        {:else}<tr><td colspan="5" class="empty-cell">暂无来源</td></tr>{/each}
      </tbody></table></div>
      </ReadState>
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
      <IrisButton size="sm" variant="outline" onclick={refreshContracts}>刷新 Schema</IrisButton>
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
      <ReadState loading={schemaRead.status === 'loading'} error={schemaRead.error} empty={schemaRead.status === 'success' && schemas.length === 0} loadingLabel="正在读取 Schema…" emptyLabel="暂无 Schema" onRetry={refreshContracts}>
      <div class="data-table-wrap"><table class="data-table"><thead><tr><th>Schema ID</th><th>版本</th><th>事件类型</th><th>分级</th><th>状态</th></tr></thead><tbody>
        {#each schemaRows as schema (schema.row_key)}
          <tr tabindex="0" aria-selected={selectedSchemaKey === schema.row_key} class:selected-row={selectedSchemaKey === schema.row_key} onclick={() => selectSchema(schema.row_key)} onkeydown={(event) => handleSchemaKeydown(event, schema.row_key)}><td>{schema.schema_id}</td><td>{schema.version}</td><td>{schema.event_type}</td><td>{schema.classification}</td><td><StatusBadge value={schema.active} /></td></tr>
        {:else}<tr><td colspan="5" class="empty-cell">暂无 Schema</td></tr>{/each}
      </tbody></table></div>
      </ReadState>
      {#if selectedSchema}
        <div class="schema-policy-view"><strong>策略详情：{selectedSchema.schema_id} v{selectedSchema.version}</strong><IrisDescriptions style="" bordered columns={2} items={[{ label: '必填字段', value: fieldSummary(selectedSchema.required_fields) }, { label: '允许字段', value: fieldSummary(selectedSchema.allowed_fields) }, { label: '加密字段', value: fieldSummary(selectedSchema.encrypted_fields) }, { label: '可搜索字段', value: fieldSummary(selectedSchema.searchable_fields) }, { label: '数据分级', value: selectedSchema.classification || '未限制' }, { label: '创建时间', value: formatDate(selectedSchema.created_at) }]} /></div>
      {/if}
    </IrisCard>

    <IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
      <h3 class="card-heading">保留策略</h3>
      <IrisButton size="sm" variant="outline" onclick={() => void refreshRetention()}>刷新保留策略</IrisButton>
      <ReadState loading={retentionState === 'loading'} error={retentionState === 'error' ? retentionError : null} loadingLabel="正在读取当前租户的保留策略…" onRetry={() => void refreshRetention()}>
      {#if retentionState === 'unconfigured'}
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
        <ErrorNotice error={retentionError} onRetry={() => void refreshRetention()} />
        {#if retentionReport}<div class="result-panel"><StatusBadge value={retentionReport.action} /><span>可归档 {retentionReport.eligible_events} 条</span><span>法律保留保护 {retentionReport.protected_events} 条</span><span>保留规则 {retentionReport.hold_ids?.join(', ') || '无'}</span></div>{/if}
      {/if}
      </ReadState>
    </IrisCard>
  {/if}

  {#if readable}
  <IrisCard variant="outline" class="section-card" padding="none" style="" header={undefined} footer={undefined}>
    <h3 class="card-heading card-heading-padded">管理操作自审计</h3>
    <p class="table-hint">当前租户最近最多 {ADMIN_ACTION_LIMIT} 条的预览，非全量历史；不足 {ADMIN_ACTION_LIMIT} 条也不代表完整。<a href="#admin-actions">打开自审计页面（本地筛选）</a></p>
    <IrisButton size="sm" variant="outline" disabled={!scoped} onclick={refreshActions}>刷新自审计</IrisButton>
    {#if scoped}
      <p class="table-hint">已载入 {actions.length} 条（仅最近记录）</p>
      <ReadState loading={!currentContext || actionRead.status === 'loading'} error={currentContext ? actionRead.error : null} empty={actionRead.status === 'success' && actions.length === 0} loadingLabel="正在读取管理自审计…" emptyLabel="暂无管理自审计记录" onRetry={refreshActions}>
        <IrisTable rowKey="id" columns={actionColumns} data={actionRows} density="compact" />
      </ReadState>
    {:else}<p class="muted-copy">请选择有效租户后读取自审计预览。</p>{/if}
  </IrisCard>
  {/if}
</section>
