<script lang="ts">
  import { onDestroy, untrack } from 'svelte'
  import { IrisAlert, IrisButton, IrisCard, IrisDescriptions, IrisFormField, IrisInput } from '@iris-ui-kit/svelte'
  import { useAuditApi } from '../../api/useAuditApi'
  import { RequestScope } from '../../api/requestScope'
  import { ApiError } from '../../api/client'
  import type { RestoreRun } from '../../api/types'
  import { authState } from '../../auth/store'
  import { can } from '../../auth/claims'
  import ConfirmDialog from '../../components/ConfirmDialog.svelte'
  import ErrorNotice from '../../components/ErrorNotice.svelte'
  import JobList from '../../components/JobList.svelte'
  import StatusBadge from '../../components/StatusBadge.svelte'
  import { formatDate } from '../../components/format'
  import { restoreDecisionBlockReason } from '../../pages/governanceState'
  import { bindRestorePreview, restorePreviewBlockReason, type BoundRestorePreview } from '../../pages/restoreWorkflow'
  import { navigateEvidence } from '../../pages/evidenceNavigation'

  let { tenantId, initialId = '' }: { tenantId?: string; initialId?: string } = $props()
  const api = useAuditApi()
  const reads = new RequestScope()
  const writes = new RequestScope()
  const session = $derived($authState.session)
  const scopedTenant = $derived(session?.platform ? tenantId : session?.tenantId)
  const scopeReady = $derived(Boolean(session && scopedTenant && (!tenantId || tenantId === scopedTenant)))
  // Memory-only binding; request epochs also fence A -> B -> A late results.
  const identity = $derived(JSON.stringify([tenantId, scopedTenant, $authState.status, session?.subject,
    session?.platform, session?.tenantId, session?.accessToken, session?.idToken, session?.expiresAt,
    session ? [...session.permissions].sort() : []]))
  let lastIdentity = untrack(() => identity)
  let lastSelection = untrack(() => initialId)
  const manage = $derived(Boolean(session && can(session, 'audit:legal_hold:manage')))
  let operationId = $state('')
  let reason = $state('')
  let runId = $state('')
  let boundPreview = $state<BoundRestorePreview | null>(null)
  let run = $state<RestoreRun | null>(null)
  let error = $state<unknown>(null)
  let busy = $state(false)
  let lookupBusy = $state(false)
  let confirmation = $state(false)
  let decision = $state<'approve' | 'reject' | null>(null)
  let revision = $state(0)
  let decisionGeneration = 0
  type PendingDecision = { id: string; tenant: string; identity: string; generation: number }
  // Navigating to B must not erase an unresolved decision for A.
  let pendingDecisions = $state<Record<string, PendingDecision>>({})
  let terminalObservations = $state<Record<string, RestoreRun>>({})
  const selectedId = $derived(initialId || run?.id || '')
  const pendingReconciliation = $derived(pendingDecisions[selectedId])
  const needsReconciliation = $derived(Boolean(pendingReconciliation &&
    pendingReconciliation.tenant === scopedTenant && pendingReconciliation.identity === identity))
  const request = $derived({ operation_id: operationId.trim(), reason: reason.trim() })
  const preview = $derived(boundPreview?.preview ?? null)
  const previewBlock = $derived(restorePreviewBlockReason(boundPreview, request, tenantId))
  const decisionBlock = $derived(run ? (!scopeReady || run.tenant_id !== scopedTenant
    ? '恢复申请与当前会话租户不匹配'
    : restoreDecisionBlockReason(run, session?.subject ?? '', manage)) : null)

  $effect(() => {
    const currentIdentity = identity
    // Initial values are already clean. Do not erase a first-frame edit.
    if (currentIdentity === lastIdentity) return () => { reads.invalidate(); writes.invalidate() }
    lastIdentity = currentIdentity
    reads.invalidate(); writes.invalidate()
    operationId = ''; reason = ''; runId = ''
    boundPreview = null; run = null; error = null; busy = false; lookupBusy = false
    confirmation = false; decision = null; pendingDecisions = {}; terminalObservations = {}
    return () => { reads.invalidate(); writes.invalidate() }
  })
  $effect(() => {
    const id = initialId
    const changed = id !== lastSelection
    lastSelection = id
    void identity
    const ready = scopeReady
    untrack(() => {
      if (id && ready) { runId = id; void lookup(id) }
      else if (!id && changed) {
        reads.invalidate(); writes.invalidate()
        run = null; runId = ''; decision = null; confirmation = false
        busy = false; lookupBusy = false; error = null
      }
    })
  })
  onDestroy(() => { reads.dispose(); writes.dispose() })

  function changeRequest(field: 'operation' | 'reason', value: string): void {
    writes.invalidate()
    boundPreview = null
    confirmation = false
    error = null
    busy = false
    if (field === 'operation') operationId = value
    else reason = value
  }

  async function previewRestore(): Promise<void> {
    const ticket = writes.next()
    const requested = { ...request }
    const scopeTenant = tenantId
    busy = true; error = null; boundPreview = null
    try {
      const next = await api.previewRestore(requested, scopeTenant, ticket.signal)
      if (!ticket.current()) return
      if (scopeTenant && next.tenant_id !== scopeTenant) throw new Error('预演结果与当前租户不匹配')
      boundPreview = bindRestorePreview(next, requested)
    } catch (cause) {
      if (ticket.current()) error = cause
    } finally {
      if (ticket.current()) busy = false
      ticket.finish()
    }
  }

  async function create(): Promise<void> {
    if (!manage || previewBlock) { confirmation = false; error = new Error(previewBlock ?? '没有恢复申请权限'); return }
    const ticket = writes.next()
    const scopeTenant = tenantId
    busy = true; error = null
    try {
      const next = await api.createRestore({ ...request }, scopeTenant, ticket.signal)
      if (!ticket.current()) return
      if (scopeTenant && next.tenant_id !== scopeTenant) throw new Error('恢复申请与当前租户不匹配')
      run = next; runId = next.id; boundPreview = null; confirmation = false
      revision += 1
      navigateEvidence('restores', next.id)
    } catch (cause) {
      if (ticket.current()) error = cause
    } finally {
      if (ticket.current()) busy = false
      ticket.finish()
    }
  }

  function observe(next: RestoreRun): void {
    run = next; runId = next.id
    if (next.status === 'approved' || next.status === 'rejected') terminalObservations[next.id] = next
  }

  async function lookup(id = runId.trim()): Promise<void> {
    if (!id || !scopeReady) return
    const ticket = reads.next()
    const scopeTenant = scopedTenant
    const scopeIdentity = identity
    const selection = initialId
    const pending = pendingDecisions[id]
    const reconciling = pending?.tenant === scopeTenant && pending.identity === scopeIdentity
    const current = () => ticket.current() && scopeIdentity === identity && selection === initialId
    const sameRun = run?.id === id && run.tenant_id === scopeTenant
    const terminal = terminalObservations[id]
    writes.invalidate()
    busy = false; lookupBusy = true; boundPreview = null
    // Failed/foreign observations never grant permission to replay a command.
    run = terminal ?? (reconciling && sameRun ? run : null)
    error = null; confirmation = false; decision = null
    try {
      const next = await useAuditApi().getRestore(id, scopeTenant, ticket.signal)
      if (!current()) return
      if (next.tenant_id !== scopeTenant || next.id !== id) throw new Error('恢复申请与当前租户或申请 ID 不匹配')
      if (terminal && (next.status !== terminal.status || next.approved_by !== terminal.approved_by ||
          next.approved_at !== terminal.approved_at || next.rejected_by !== terminal.rejected_by ||
          next.rejected_at !== terminal.rejected_at)) throw new Error('恢复申请终态不应回退或改变，请只读核对')
      observe(next)
      if (reconciling && pendingDecisions[id]?.generation === pending.generation) delete pendingDecisions[id]
    } catch (cause) {
      if (current()) error = cause
    } finally {
      if (current()) lookupBusy = false
      ticket.finish()
    }
  }

  function reconcile(): void {
    const pending = pendingReconciliation
    if (!needsReconciliation || !pending || busy || lookupBusy) return
    // Never use the editable lookup draft or replay a decision POST.
    void lookup(pending.id)
  }

  function select(): void {
    if (needsReconciliation) { reconcile(); return }
    const id = runId.trim()
    if (!id) return
    if (id === initialId) void lookup(id)
    else navigateEvidence('restores', id)
  }

  async function decide(): Promise<void> {
    // Auth stores update synchronously, before effects close an old dialog.
    // Never let that same-frame window dispatch under a different binding.
    if (identity !== lastIdentity || initialId !== lastSelection) { decision = null; return }
    const current = run
    const target = decision
    if (!current || !target || decisionBlock || busy || lookupBusy || needsReconciliation || !scopedTenant) return
    const ticket = writes.next()
    reads.invalidate()
    const scopeTenant = scopedTenant
    const scopeIdentity = identity
    const selection = initialId
    const subject = session?.subject
    const generation = ++decisionGeneration
    const active = () => ticket.current() && scopeIdentity === identity && selection === initialId &&
      pendingDecisions[current.id]?.generation === generation
    pendingDecisions[current.id] = { id: current.id, tenant: scopeTenant, identity: scopeIdentity, generation }
    busy = true; error = null; decision = null
    try {
      const next = await useAuditApi().decideRestore(current.id, target, scopeTenant, ticket.signal)
      if (!active()) return
      if (next.tenant_id !== scopeTenant || next.id !== current.id ||
          next.status !== (target === 'approve' ? 'approved' : 'rejected') ||
          (target === 'approve' ? next.approved_by : next.rejected_by) !== subject) {
        throw new Error('决策结果与当前申请或决策不匹配，请只读刷新核对')
      }
      observe(next); busy = false; delete pendingDecisions[current.id]; revision += 1
    } catch (cause) {
      if (!active()) return
      error = cause; busy = false
      // 409 can be another durable decision; 499/5xx/network errors can follow
      // a committed Save. Neither case authorizes this non-idempotent replay.
      if (cause instanceof ApiError && cause.status < 500 && cause.status !== 499 && cause.status !== 409) {
        delete pendingDecisions[current.id]
      }
    } finally {
      if (active()) busy = false
      ticket.finish()
    }
  }
</script>

<JobList kind="restores" {tenantId} {revision} />
<IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
  <h2 class="card-heading">恢复预演与申请</h2>
  <p class="card-copy">回放推导审计记录中的状态，不等于反向回滚。审批只记录治理决策，不调用业务系统。</p>
  <div class="three-field-form">
    <IrisFormField label="Operation ID"><IrisInput value={operationId} disabled={busy} oninput={(event) => changeRequest('operation', event.currentTarget.value)} /></IrisFormField>
    <IrisFormField label="恢复原因"><IrisInput value={reason} disabled={busy} oninput={(event) => changeRequest('reason', event.currentTarget.value)} /></IrisFormField>
    <div class="button-row"><IrisButton variant="outline" disabled={busy || !operationId.trim() || !reason.trim()} onclick={() => void previewRestore()}>预演</IrisButton>{#if manage}<IrisButton variant="solid" disabled={busy || Boolean(previewBlock)} onclick={() => (confirmation = true)}>创建审批单</IrisButton>{/if}</div>
  </div>
  {#if manage && operationId.trim() && reason.trim() && previewBlock}<p role="status" class="muted-copy">{previewBlock}</p>{/if}
  {#if preview}
    <IrisDescriptions style="" bordered columns={2} items={[{ label: 'Preview ID', value: preview.id }, { label: 'Operation ID', value: preview.operation_id }, { label: '租户', value: preview.tenant_id }, { label: '外部调用', value: preview.external_calls.length ? preview.external_calls.join('、') : '无' }]} />
    <h3>审计推导的拟恢复状态</h3><pre class="json-view">{JSON.stringify(preview.proposed_state, null, 2)}</pre>
  {/if}
  <ConfirmDialog open={confirmation} title="确认创建恢复审批单" description={`预演已绑定 ${operationId.trim()} 与当前原因；服务端仍会重新回放，并进入双人审批。`} confirmLabel="确认创建" {busy} onOpenChange={(open) => (confirmation = open)} onConfirm={create} />
</IrisCard>
<IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
  <h2 class="card-heading">申请详情与决策</h2>
  <div class="inline-form"><IrisInput value={runId} placeholder="restore run id" disabled={lookupBusy || busy} oninput={(event) => (runId = event.currentTarget.value)} /><IrisButton variant="outline" disabled={lookupBusy || busy || !runId.trim()} onclick={select}>查询运行</IrisButton></div>
  <ErrorNotice {error} onRetry={needsReconciliation ? undefined : () => void lookup()} />
  {#if needsReconciliation}
    <p role="status" class="muted-copy">决策可能已提交；上次读取的状态不能证明尚未提交。请先刷新核对，不要重复批准或拒绝。</p>
    <IrisButton variant="outline" disabled={lookupBusy || busy} onclick={reconcile}>刷新核对</IrisButton>
  {/if}
  {#if run}
    <div class="result-panel"><StatusBadge value={run.status} /><span>{run.id} · {run.operation_id}</span></div>
    <p>申请人：{run.created_by || '未知'} · 决策人：{run.approved_by || run.rejected_by || '待定'}</p>
    {#if run.approved_at || run.rejected_at}<p>决策时间：<time datetime={run.approved_at || run.rejected_at}>{formatDate(run.approved_at || run.rejected_at)}</time></p>{/if}
    <p>原因：{run.reason}</p>
    <IrisAlert tone="info" title="审批与执行分离" style="" onclose={undefined} icon={undefined} titleSlot={undefined}>当前只记录审批事实，不代表业务数据已经恢复。执行状态：本平台尚未接入。</IrisAlert>
    {#if run.status === 'pending_approval'}
      {#if decisionBlock}<p class="muted-copy" role="status">{decisionBlock}</p>{/if}
      <div class="button-row"><IrisButton variant="solid" disabled={busy || lookupBusy || needsReconciliation || Boolean(decisionBlock)} onclick={() => (decision = 'approve')}>批准</IrisButton><IrisButton variant="outline" disabled={busy || lookupBusy || needsReconciliation || Boolean(decisionBlock)} onclick={() => (decision = 'reject')}>拒绝</IrisButton></div>
    {/if}
  {:else if !lookupBusy && !error}<p class="muted-copy">从申请列表选择一项，或输入申请 ID。</p>{/if}
  <ConfirmDialog open={decision !== null} title={decision === 'approve' ? '确认批准恢复申请' : '确认拒绝恢复申请'} description={`${run?.id ?? ''} 将记录 ${session?.subject ?? ''} 的决策；这不会触发业务补偿执行。`} confirmLabel={decision === 'approve' ? '确认批准' : '确认拒绝'} {busy} onOpenChange={(open) => !open && (decision = null)} onConfirm={decide} />
</IrisCard>
