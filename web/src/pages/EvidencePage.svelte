<script lang="ts">
  import {
    IrisAlert,
    IrisButton,
    IrisCard,
    IrisDescriptions,
    IrisFormField,
    IrisInput,
    IrisProgress,
    IrisTable,
  } from '@iris-ui-kit/svelte'
  import { useAuditApi } from '../api/useAuditApi'
  import type { ExportJob, IntegrityResult, LegalHold, RestoreRun } from '../api/types'
  import { can } from '../auth/claims'
  import { authState } from '../auth/store'
  import ConfirmDialog from '../components/ConfirmDialog.svelte'
  import ErrorNotice from '../components/ErrorNotice.svelte'
  import PageHeader from '../components/PageHeader.svelte'
  import StatusBadge from '../components/StatusBadge.svelte'
  import { formatDate } from '../components/format'
  import {
    buildEvidenceScopeQuery,
    defaultEvidenceRange,
    emptyEvidenceScope,
    evidenceScopeSummary,
    type EvidenceScopeDraft,
  } from './evidenceScope'
  import {
    canAutoPollExport,
    EXPORT_POLL_INTERVAL_MS,
    EXPORT_POLL_MAX_ATTEMPTS,
    restoreDecisionBlockReason,
    shouldPollExport,
  } from './governanceState'
  import { bindRestorePreview, restorePreviewBlockReason, type BoundRestorePreview } from './restoreWorkflow'
  import EvidenceScopeFields, { type DateRange } from './EvidenceScopeFields.svelte'

  let { tenantId }: { tenantId?: string } = $props()
  const api = useAuditApi()
  const session = $derived($authState.session)
  const integrity = $derived(Boolean(session && can(session, 'audit:integrity:verify')))
  const holds = $derived(Boolean(session && can(session, 'audit:legal_hold:manage')))
  const exports = $derived(Boolean(session && can(session, 'audit:export:create')))
  const disabled = $derived(Boolean(session?.platform && !tenantId))

  // Integrity verification
  let streamId = $state('')
  let integrityResult = $state<IntegrityResult | null>(null)
  let integrityError = $state<unknown>(null)
  let integrityBusy = $state(false)

  async function verifyIntegrity(): Promise<void> {
    integrityBusy = true
    integrityError = null
    try {
      integrityResult = await api.verifyIntegrity(streamId.trim(), tenantId)
    } catch (cause) {
      integrityError = cause
    } finally {
      integrityBusy = false
    }
  }

  // Export jobs
  let exportRange = $state<DateRange>(defaultEvidenceRange(1))
  let exportScope = $state<EvidenceScopeDraft>({ ...emptyEvidenceScope })
  let jobId = $state('')
  let job = $state<ExportJob | null>(null)
  let exportError = $state<unknown>(null)
  let exportPollingError = $state<unknown>(null)
  let exportPollingAttempts = $state(0)
  let exportPollingPaused = $state(false)
  let exportBusy = $state(false)
  let exportDownloadBusy = $state(false)
  let exportContextGeneration = 0
  let exportTenantScope: string | undefined
  let hasExportTenantScope = false

  $effect(() => {
    const scopeTenantId = tenantId
    if (!hasExportTenantScope) {
      exportTenantScope = scopeTenantId
      hasExportTenantScope = true
      return
    }
    if (scopeTenantId === exportTenantScope) return
    exportTenantScope = scopeTenantId
    exportContextGeneration += 1
    job = null
    jobId = ''
    exportError = null
    exportPollingError = null
    exportPollingAttempts = 0
    exportPollingPaused = false
    exportBusy = false
    exportDownloadBusy = false
  })

  $effect(() => {
    const current = job
    const attempts = exportPollingAttempts
    const paused = exportPollingPaused
    const scopeTenantId = tenantId
    const generation = exportContextGeneration
    if (
      !current ||
      (scopeTenantId && current.tenant_id !== scopeTenantId) ||
      !canAutoPollExport(current.status, attempts, paused)
    ) return
    let active = true
    const timer = window.setTimeout(() => {
      void api.getExport(current.id, scopeTenantId).then((next) => {
        if (!active || generation !== exportContextGeneration || scopeTenantId !== tenantId) return
        if (scopeTenantId && next.tenant_id !== scopeTenantId) {
          exportPollingError = new Error('导出任务的租户与当前选择不匹配')
          exportPollingPaused = true
          return
        }
        job = next
        exportError = null
        exportPollingAttempts = attempts + 1
      }).catch((cause: unknown) => {
        if (!active || generation !== exportContextGeneration || scopeTenantId !== tenantId) return
        exportPollingError = cause
        exportPollingPaused = true
      })
    }, EXPORT_POLL_INTERVAL_MS)
    return () => {
      active = false
      window.clearTimeout(timer)
    }
  })

  function updateScope(scope: EvidenceScopeDraft, field: keyof EvidenceScopeDraft, value: string): EvidenceScopeDraft {
    return { ...scope, [field]: value }
  }

  function selectExportJob(next: ExportJob, scopeTenantId?: string): void {
    if (scopeTenantId && next.tenant_id !== scopeTenantId) {
      job = null
      exportError = new Error('导出任务的租户与当前选择不匹配')
      return
    }
    job = next
    jobId = next.id
    exportError = null
    exportPollingAttempts = 0
    exportPollingPaused = false
    exportPollingError = null
  }

  function retryExportPolling(): void {
    if (!job || !shouldPollExport(job.status)) return
    exportPollingAttempts = 0
    exportPollingPaused = false
    exportPollingError = null
    exportError = null
  }

  async function createExport(): Promise<void> {
    const query = buildEvidenceScopeQuery(exportRange, exportScope)
    if (!query) {
      exportError = new Error('请选择有效且非空的导出时间范围')
      return
    }
    const generation = exportContextGeneration
    const scopeTenantId = tenantId
    exportBusy = true
    exportError = null
    try {
      const next = await api.createExport(query, scopeTenantId)
      if (generation !== exportContextGeneration || scopeTenantId !== tenantId) return
      selectExportJob(next, scopeTenantId)
    } catch (cause) {
      if (generation === exportContextGeneration && scopeTenantId === tenantId) exportError = cause
    } finally {
      if (generation === exportContextGeneration && scopeTenantId === tenantId) exportBusy = false
    }
  }

  async function lookupExport(): Promise<void> {
    const requestedId = jobId.trim()
    if (!requestedId) return
    const generation = exportContextGeneration
    const scopeTenantId = tenantId
    exportBusy = true
    exportError = null
    exportPollingPaused = true
    exportPollingError = null
    try {
      const next = await api.getExport(requestedId, scopeTenantId)
      if (generation !== exportContextGeneration || scopeTenantId !== tenantId) return
      selectExportJob(next, scopeTenantId)
    } catch (cause) {
      if (generation === exportContextGeneration && scopeTenantId === tenantId) exportError = cause
    } finally {
      if (generation === exportContextGeneration && scopeTenantId === tenantId) exportBusy = false
    }
  }

  async function downloadExport(): Promise<void> {
    const current = job
    if (!current || current.status !== 'completed' || exportDownloadBusy) return
    const generation = exportContextGeneration
    const scopeTenantId = tenantId
    exportDownloadBusy = true
    exportError = null
    try {
      const blob = await api.downloadExport(current.id, scopeTenantId)
      if (generation !== exportContextGeneration || scopeTenantId !== tenantId || job?.id !== current.id) return
      const url = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = `${current.id}.jsonl`
      link.click()
      window.setTimeout(() => URL.revokeObjectURL(url), 1000)
    } catch (cause) {
      if (generation === exportContextGeneration && scopeTenantId === tenantId) exportError = cause
    } finally {
      if (generation === exportContextGeneration && scopeTenantId === tenantId) exportDownloadBusy = false
    }
  }

  // Legal holds
  let holdRange = $state<DateRange>(defaultEvidenceRange(30))
  let holdScope = $state<EvidenceScopeDraft>({ ...emptyEvidenceScope })
  let legalHolds = $state<LegalHold[]>([])
  let holdName = $state('')
  let holdReason = $state('')
  let holdError = $state<unknown>(null)
  let pendingRelease = $state<LegalHold | null>(null)
  let holdBusy = $state(false)
  let holdRevision = $state(0)

  async function loadHolds(): Promise<void> {
    if (disabled) {
      legalHolds = []
      return
    }
    try {
      const result = await api.listLegalHolds(tenantId)
      legalHolds = result.items
    } catch (cause) {
      holdError = cause
    }
  }

  $effect(() => {
    void tenantId
    void disabled
    void holdRevision
    void loadHolds()
  })

  async function createLegalHold(): Promise<void> {
    const query = buildEvidenceScopeQuery(holdRange, holdScope)
    if (!query) {
      holdError = new Error('请选择有效且非空的法律保留时间范围')
      return
    }
    holdBusy = true
    holdError = null
    try {
      await api.createLegalHold({ name: holdName.trim(), reason: holdReason.trim(), filter: query }, tenantId)
      holdName = ''
      holdReason = ''
      holdRevision += 1
    } catch (cause) {
      holdError = cause
    } finally {
      holdBusy = false
    }
  }

  async function releaseLegalHold(): Promise<void> {
    if (!pendingRelease) return
    holdBusy = true
    holdError = null
    try {
      await api.releaseLegalHold(pendingRelease.id, tenantId)
      pendingRelease = null
      holdRevision += 1
    } catch (cause) {
      holdError = cause
    } finally {
      holdBusy = false
    }
  }

  function holdRows(): Record<string, unknown>[] {
    return legalHolds.map((hold) => ({
      ...hold,
      created_at: formatDate(hold.created_at),
      state: hold.released_at ? 'released' : 'active',
      filter_summary: evidenceScopeSummary(hold.filter),
    }))
  }
  const holdTableRows = $derived(holdRows())

  // Restore preview and approval workflow
  let operationId = $state('')
  let restoreReason = $state('')
  let runId = $state('')
  let boundPreview = $state<BoundRestorePreview | null>(null)
  let restoreRun = $state<RestoreRun | null>(null)
  let restoreError = $state<unknown>(null)
  let createConfirmation = $state(false)
  let decision = $state<'approve' | 'reject' | null>(null)
  let restoreBusy = $state(false)
  const restoreRequest = $derived({ operation_id: operationId.trim(), reason: restoreReason.trim() })
  const preview = $derived(boundPreview?.preview ?? null)
  const previewBlockReason = $derived(restorePreviewBlockReason(boundPreview, restoreRequest, tenantId))
  const decisionBlockReason = $derived(
    restoreRun ? restoreDecisionBlockReason(restoreRun, session?.subject ?? '', holds) : null,
  )

  async function previewRestore(): Promise<void> {
    const request = { operation_id: operationId.trim(), reason: restoreReason.trim() }
    restoreBusy = true
    restoreError = null
    try {
      const next = await api.previewRestore(request, tenantId)
      boundPreview = bindRestorePreview(next, request)
    } catch (cause) {
      restoreError = cause
    } finally {
      restoreBusy = false
    }
  }

  async function createRestore(): Promise<void> {
    if (previewBlockReason) {
      createConfirmation = false
      restoreError = new Error(previewBlockReason)
      return
    }
    restoreBusy = true
    restoreError = null
    try {
      const next = await api.createRestore(restoreRequest, tenantId)
      restoreRun = next
      runId = next.id
      boundPreview = null
      createConfirmation = false
    } catch (cause) {
      restoreError = cause
    } finally {
      restoreBusy = false
    }
  }

  async function lookupRestore(): Promise<void> {
    if (!runId.trim()) return
    restoreBusy = true
    restoreError = null
    try {
      restoreRun = await api.getRestore(runId.trim(), tenantId)
      boundPreview = null
    } catch (cause) {
      restoreError = cause
    } finally {
      restoreBusy = false
    }
  }

  async function decideRestore(): Promise<void> {
    if (!restoreRun || !decision || decisionBlockReason) return
    restoreBusy = true
    restoreError = null
    try {
      restoreRun = await api.decideRestore(restoreRun.id, decision, tenantId)
      decision = null
    } catch (cause) {
      restoreError = cause
    } finally {
      restoreBusy = false
    }
  }
</script>

{#snippet releaseCell(row: Record<string, unknown>)}
  {#if String(row.state) === 'active'}
    <IrisButton size="sm" variant="outline" onclick={() => (pendingRelease = row as unknown as LegalHold)}>释放</IrisButton>
  {:else}—{/if}
{/snippet}

<section>
  <PageHeader title="证据与治理" description="完整性证明、法律保留、合规导出和双人审批恢复共用同一租户边界。" />
  {#if session?.platform && !tenantId}
    <IrisAlert tone="warning" title="请选择租户" onclose={undefined} style="" icon={undefined} titleSlot={undefined}>创建导出、法律保留或恢复操作前，需要在顶栏选择明确的 tenant_id。</IrisAlert>
  {/if}
  <div class="evidence-grid">
    {#if integrity}
      <IrisCard variant="outline" style="" header={undefined} footer={undefined}>
        <h3 class="card-heading">账本完整性验证</h3>
        <p class="card-copy">留空验证整个租户，或填写 stream_id 验证一条哈希链。</p>
        <div class="stack-form">
          <IrisFormField label="Stream ID"><IrisInput value={streamId} placeholder="可选" oninput={(event) => (streamId = event.currentTarget.value)} /></IrisFormField>
          <IrisButton variant="solid" disabled={disabled || integrityBusy} onclick={() => void verifyIntegrity()}>{integrityBusy ? '验证中…' : '开始验证'}</IrisButton>
        </div>
        <ErrorNotice error={integrityError} />
        {#if integrityResult}
          <div class="result-panel"><StatusBadge value={integrityResult.valid} /><span>事件 {integrityResult.event_count ?? 0} · 分段 {integrityResult.segment_count ?? 0}</span></div>
          {#if integrityResult.errors?.length}<ul>{#each integrityResult.errors as item (item)}<li>{item}</li>{/each}</ul>{/if}
        {/if}
      </IrisCard>
    {/if}

    {#if exports}
      <IrisCard variant="outline" class="wide-card" style="" header={undefined} footer={undefined}>
        <h3 class="card-heading">合规导出</h3>
        <p class="card-copy">按时间与跨服务关联条件创建封存 JSONL；完成后服务端在下载前再次校验摘要和法律保留。</p>
        <EvidenceScopeFields
          range={exportRange}
          scope={exportScope}
          disabled={disabled || exportBusy}
          onRangeChange={(next) => (exportRange = next)}
          onScopeChange={(field, value) => (exportScope = updateScope(exportScope, field, value))}
        />
        <IrisButton variant="solid" disabled={disabled || exportBusy} onclick={() => void createExport()}>{exportBusy ? '提交中…' : '创建范围导出'}</IrisButton>
        <div class="inline-form">
          <IrisInput value={jobId} placeholder="export job id" oninput={(event) => (jobId = event.currentTarget.value)} />
          <IrisButton variant="outline" disabled={exportBusy || !jobId.trim()} onclick={() => void lookupExport()}>查询</IrisButton>
        </div>
        <ErrorNotice error={exportError} />
        {#if job}
          <div class="result-panel">
            <StatusBadge value={job.status} /><span>{job.id} · {job.event_count} 条事件</span>
            {#if job.status === 'completed'}<IrisButton size="sm" variant="outline" disabled={exportDownloadBusy} onclick={() => void downloadExport()}>{exportDownloadBusy ? '准备下载…' : '下载 JSONL'}</IrisButton>{/if}
            {#if canAutoPollExport(job.status, exportPollingAttempts, exportPollingPaused)}<IrisProgress indeterminate size="sm" />{/if}
            {#if job.error}<span class="error-copy">{job.error}</span>{/if}
          </div>
        {/if}
        <ErrorNotice error={exportPollingError} onRetry={retryExportPolling} />
        {#if job && shouldPollExport(job.status) && exportPollingPaused && !exportPollingError}
          <div class="result-panel"><span>自动状态查询已暂停。</span><IrisButton size="sm" variant="outline" disabled={exportBusy} onclick={() => void lookupExport()}>重新查询</IrisButton></div>
        {/if}
        {#if job && shouldPollExport(job.status) && !exportPollingPaused && exportPollingAttempts >= EXPORT_POLL_MAX_ATTEMPTS}
          <div class="result-panel"><span>已达到自动查询上限（{EXPORT_POLL_MAX_ATTEMPTS} 次）。任务仍在处理中，可手动继续查询。</span><IrisButton size="sm" variant="outline" onclick={retryExportPolling}>继续查询</IrisButton></div>
        {/if}
      </IrisCard>
    {/if}

    {#if holds}
      <IrisCard variant="outline" class="wide-card" style="" header={undefined} footer={undefined}>
        <h3 class="card-heading">法律保留</h3>
        <div class="three-field-form">
          <IrisFormField label="名称"><IrisInput value={holdName} oninput={(event) => (holdName = event.currentTarget.value)} /></IrisFormField>
          <IrisFormField label="原因"><IrisInput value={holdReason} oninput={(event) => (holdReason = event.currentTarget.value)} /></IrisFormField>
          <IrisButton variant="solid" disabled={disabled || holdBusy || !holdName.trim() || !holdReason.trim()} onclick={() => void createLegalHold()}>{holdBusy ? '处理中…' : '创建范围保留'}</IrisButton>
        </div>
        <EvidenceScopeFields
          range={holdRange}
          scope={holdScope}
          disabled={disabled || holdBusy}
          onRangeChange={(next) => (holdRange = next)}
          onScopeChange={(field, value) => (holdScope = updateScope(holdScope, field, value))}
        />
        <ErrorNotice error={holdError} />
        <IrisTable
          rowKey="id"
          columns={[
            { key: 'name', title: '保留名称', minWidth: 160 },
            { key: 'reason', title: '原因', minWidth: 220 },
            { key: 'created_by', title: '创建者', width: 140 },
            { key: 'created_at', title: '创建时间', width: 180 },
            { key: 'state', title: '状态', width: 110 },
            { key: 'filter_summary', title: '保留范围', minWidth: 320 },
            { key: 'actions', title: '操作', width: 90, render: () => releaseCell },
          ]}
          data={holdTableRows}
          density="compact"
        />
        <ConfirmDialog
          open={pendingRelease !== null}
          title="确认释放法律保留"
          description={`释放 ${pendingRelease?.name ?? ''} 后，新归档评估将不再受该保留保护；操作会写入治理自审计。`}
          confirmLabel="确认释放"
          busy={holdBusy}
          onOpenChange={(open) => !open && (pendingRelease = null)}
          onConfirm={releaseLegalHold}
        />
      </IrisCard>
    {/if}

    {#if session && can(session, 'audit:operation:read')}
      <IrisCard variant="outline" class="wide-card" style="" header={undefined} footer={undefined}>
        <h3 class="card-heading">恢复预演与审批</h3>
        <div class="three-field-form">
          <IrisFormField label="Operation ID"><IrisInput value={operationId} oninput={(event) => (operationId = event.currentTarget.value)} /></IrisFormField>
          <IrisFormField label="恢复原因"><IrisInput value={restoreReason} oninput={(event) => (restoreReason = event.currentTarget.value)} /></IrisFormField>
          <div class="button-row">
            <IrisButton variant="outline" disabled={disabled || restoreBusy || !operationId.trim() || !restoreReason.trim()} onclick={() => void previewRestore()}>预演</IrisButton>
            {#if holds}<IrisButton variant="solid" disabled={disabled || restoreBusy || Boolean(previewBlockReason)} onclick={() => (createConfirmation = true)}>创建审批单</IrisButton>{/if}
          </div>
        </div>
        <div class="inline-form">
          <IrisInput value={runId} placeholder="restore run id" oninput={(event) => (runId = event.currentTarget.value)} />
          <IrisButton variant="outline" disabled={restoreBusy || !runId.trim()} onclick={() => void lookupRestore()}>查询运行</IrisButton>
        </div>
        <ErrorNotice error={restoreError} />
        {#if holds && operationId.trim() && restoreReason.trim()}
          {#if previewBlockReason}<IrisAlert tone="warning" title="恢复预演门禁" onclose={undefined} style="" icon={undefined} titleSlot={undefined}>{previewBlockReason}</IrisAlert>
          {:else}<IrisAlert tone="success" title="当前参数预演有效" onclose={undefined} style="" icon={undefined} titleSlot={undefined}>创建审批单时服务端仍会重新回放操作，并进入双人审批流程。</IrisAlert>{/if}
        {/if}
        {#if preview}
          <div class="restore-preview">
            <IrisDescriptions
        style=""
              bordered
              columns={2}
              items={[
                { label: 'Preview ID', value: preview.id },
                { label: 'Operation ID', value: preview.operation_id },
                { label: '租户', value: preview.tenant_id },
                { label: '需要审批', value: preview.requires_approval ? '是' : '否' },
                { label: '外部调用声明', value: preview.external_calls.length ? preview.external_calls.join('、') : '无（当前预演只产生恢复提案）' },
                { label: '状态字段数', value: Object.keys(preview.proposed_state).length },
              ]}
            />
            <h3>拟恢复状态</h3><pre class="json-view">{JSON.stringify(preview.proposed_state, null, 2)}</pre>
          </div>
        {/if}
        {#if restoreRun?.status === 'pending_approval' && decisionBlockReason}<IrisAlert tone="warning" title="双人审批门禁" onclose={undefined} style="" icon={undefined} titleSlot={undefined}>{decisionBlockReason}</IrisAlert>{/if}
        {#if restoreRun}
          <div class="result-panel">
            <StatusBadge value={restoreRun.status} /><span>{restoreRun.id} · {restoreRun.operation_id}</span><span>申请人 {restoreRun.created_by || '—'} · 决策人 {restoreRun.approved_by || restoreRun.rejected_by || '待定'}</span>
            {#if restoreRun.status === 'pending_approval'}
              <span class="button-row"><IrisButton size="sm" variant="solid" disabled={restoreBusy || Boolean(decisionBlockReason)} onclick={() => (decision = 'approve')}>批准</IrisButton><IrisButton size="sm" variant="outline" disabled={restoreBusy || Boolean(decisionBlockReason)} onclick={() => (decision = 'reject')}>拒绝</IrisButton></span>
            {/if}
          </div>
        {/if}
        <ConfirmDialog
          open={createConfirmation}
          title="确认创建恢复审批单"
          description={`预演 ${preview?.id ?? ''} 已绑定 Operation ${operationId.trim()}。${session.subject} 将以“${restoreReason.trim()}”为原因创建双人审批申请；服务端会再次回放。`}
          confirmLabel="确认创建"
          busy={restoreBusy}
          onOpenChange={(open) => (createConfirmation = open)}
          onConfirm={createRestore}
        />
        <ConfirmDialog
          open={decision !== null}
          title={decision === 'approve' ? '确认批准恢复申请' : '确认拒绝恢复申请'}
          description={`${restoreRun?.id ?? ''} 将记录由 ${session.subject} 作出的${decision === 'approve' ? '批准' : '拒绝'}决定；该决定不可重复提交。`}
          confirmLabel={decision === 'approve' ? '确认批准' : '确认拒绝'}
          busy={restoreBusy}
          onOpenChange={(open) => !open && (decision = null)}
          onConfirm={decideRestore}
        />
      </IrisCard>
    {/if}
  </div>
</section>
