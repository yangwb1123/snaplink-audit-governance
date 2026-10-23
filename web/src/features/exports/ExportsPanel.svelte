<script lang="ts">
  import { onDestroy, onMount, untrack } from 'svelte'
  import { IrisButton, IrisCard, IrisInput, IrisProgress } from '@iris-ui-kit/svelte'
  import { useAuditApi } from '../../api/useAuditApi'
  import { authState } from '../../auth/store'
  import { can } from '../../auth/claims'
  import { RequestScope } from '../../api/requestScope'
  import type { ExportJob, ExportSummary } from '../../api/types'
  import ExportActions from './ExportActions.svelte'
  import { exportFailureLabel } from './exportWorkflow'
  import { ApiError } from '../../api/client'
  import ReadState from '../../components/ReadState.svelte'
  import JobList from '../../components/JobList.svelte'
  import StatusBadge from '../../components/StatusBadge.svelte'
  import { formatDate } from '../../components/format'
  import EvidenceScopeFields, { type DateRange } from '../../pages/EvidenceScopeFields.svelte'
  import { buildEvidenceScopeQuery, defaultEvidenceRange, emptyEvidenceScope, evidenceScopeSummary, type EvidenceScopeDraft } from '../../pages/evidenceScope'
  import { canAutoPollExport, shouldPollExport, EXPORT_POLL_INTERVAL_MS, EXPORT_POLL_MAX_ATTEMPTS } from '../../pages/governanceState'
  import { evidenceHash, navigateEvidence } from '../../pages/evidenceNavigation'

  let { tenantId, initialId = '' }: { tenantId?: string; initialId?: string } = $props()
  const session = $derived($authState.session)
  const scopedTenant = $derived(session?.platform ? tenantId : session?.tenantId)
  const scopeReady = $derived(Boolean($authState.status === 'authenticated' && session &&
    can(session, 'audit:export:create') && scopedTenant && (!tenantId || tenantId === scopedTenant)))
  const reads = new RequestScope()
  const writes = new RequestScope()
  const downloads = new RequestScope()
  let lastTenant = untrack(() => tenantId)
  let lastSession = untrack(() => session)
  let range = $state<DateRange>(defaultEvidenceRange(1))
  let scope = $state<EvidenceScopeDraft>({ ...emptyEvidenceScope })
  let jobId = $state('')
  let job = $state<ExportJob | null>(null)
  // Retain only the safe denied task binding, never the old evidence details.
  let denied = $state<{ id: string; tenantId: string } | null>(null)
  let notFound = $state(false)
  let error = $state<unknown>(null)
  let pollError = $state<unknown>(null)
  let attempts = $state(0)
  let paused = $state(false)
  let busy = $state(false)
  let creating = $state(false)
  let downloading = $state(false)
  let revision = $state(0)
  let visible = $state(!document.hidden)

  $effect(() => {
    const currentTenant = tenantId
    const currentSession = session
    void $authState.status
    const id = initialId
    untrack(() => {
      reads.invalidate(); writes.invalidate(); downloads.invalidate()
      if (currentTenant !== lastTenant || currentSession !== lastSession) {
        range = defaultEvidenceRange(1)
        scope = { ...emptyEvidenceScope }
      }
      lastTenant = currentTenant; lastSession = currentSession
      jobId = id; job = null; denied = null; notFound = false; error = null; pollError = null
      attempts = 0; paused = false; busy = false; creating = false; downloading = false
      if (id) void lookup(id)
    })
    return () => { reads.invalidate(); writes.invalidate(); downloads.invalidate() }
  })
  $effect(() => {
    const current = job
    if (!current || !visible || !canAutoPollExport(current.status, attempts, paused)) return
    const previousAttempts = attempts
    let active = true
    const timer = window.setTimeout(() => {
      if (!active || !scopeReady) return
      const ticket = scopedTicket(reads, false)
      void useAuditApi().getExport(current.id, ticket.tenantId, ticket.signal).then((next) => {
        if (!active || !ticket.current() || job?.id !== current.id) return
        if (next.id !== current.id || next.tenant_id !== ticket.tenantId) throw new Error('导出任务与当前租户或 ID 不匹配')
        job = next
        attempts = previousAttempts + 1
      }).catch((cause: unknown) => {
        if (!active || !ticket.current()) return
        pollError = cause
        paused = true
        // A denied lookup cannot leave old evidence metadata/download controls visible.
        job = null
      }).finally(ticket.finish)
    }, EXPORT_POLL_INTERVAL_MS)
    return () => { active = false; window.clearTimeout(timer) }
  })
  onMount(() => {
    const update = () => (visible = !document.hidden)
    document.addEventListener('visibilitychange', update)
    return () => document.removeEventListener('visibilitychange', update)
  })
  onDestroy(() => { reads.dispose(); writes.dispose(); downloads.dispose() })

  // Check identity as well as epochs: even a transport ignoring AbortSignal
  // must not act on a response from another route, tenant or session.
  function scopedTicket(requests: RequestScope, replace = true) {
    const ticket = replace ? requests.next() : requests.capture()
    const identity = $authState
    const tenant = scopedTenant
    const id = initialId
    return { ...ticket, tenantId: tenant, current: () => ticket.current() &&
      identity === $authState && tenant === scopedTenant && id === initialId && scopeReady }
  }

  async function create(): Promise<void> {
    if (!scopeReady) return
    const query = buildEvidenceScopeQuery(range, scope)
    if (!query) { error = new Error('请选择有效且非空的导出时间范围'); return }
    const ticket = scopedTicket(writes)
    creating = true
    error = null
    try {
      const next = await useAuditApi().createExport(query, ticket.tenantId, ticket.signal)
      if (!ticket.current()) return
      if (next.tenant_id !== ticket.tenantId) throw new Error('导出任务与当前租户不匹配')
      reads.invalidate(); downloads.invalidate(); busy = false; downloading = false
      job = next; jobId = next.id; denied = null; notFound = false
      attempts = 0; paused = false; pollError = null
      revision += 1
      navigateEvidence('exports', next.id)
    } catch (cause) {
      if (ticket.current()) error = cause
    } finally {
      if (ticket.current()) creating = false
      ticket.finish()
    }
  }

  async function lookup(id = jobId.trim(), expectedTenant = scopedTenant): Promise<void> {
    if (!id || !scopeReady || expectedTenant !== scopedTenant) return
    const ticket = scopedTicket(reads)
    writes.invalidate()
    creating = false
    downloads.invalidate()
    downloading = false
    busy = true; job = null; notFound = false; error = null; pollError = null; paused = true
    try {
      const next = await useAuditApi().getExport(id, ticket.tenantId, ticket.signal)
      if (!ticket.current()) return
      if (next.id !== id || next.tenant_id !== ticket.tenantId) throw new Error('导出任务与当前租户或 ID 不匹配')
      job = next; jobId = next.id; denied = null; attempts = 0; paused = false
    } catch (cause) {
      if (ticket.current()) {
        notFound = cause instanceof ApiError && cause.status === 404 && cause.code === 'not_found'
        error = notFound ? null : cause
      }
    } finally {
      if (ticket.current()) busy = false
      ticket.finish()
    }
  }

  function refreshDetail(): void {
    // Never use the editable query ID to reconcile a denied download.
    if (denied) void lookup(denied.id, denied.tenantId)
    else void lookup()
  }

  function select(): void {
    const id = jobId.trim()
    if (!id) return
    if (id === initialId) void lookup(id)
    else navigateEvidence('exports', id)
  }

  function afterAction(summary: ExportSummary): void {
    reads.invalidate(); writes.invalidate(); downloads.invalidate()
    job = null; jobId = summary.id; denied = null; notFound = false; error = null; pollError = null
    busy = false; creating = false; downloading = false; attempts = 0; paused = true
    revision += 1
    if (summary.id === initialId) void lookup(summary.id)
    else navigateEvidence('exports', summary.id)
  }

  async function refreshAction(id: string): Promise<void> {
    revision += 1
    await lookup(id)
  }

  async function download(): Promise<void> {
    const current = job
    if (!current || current.status !== 'completed' || downloading || !scopeReady) return
    const ticket = scopedTicket(downloads)
    downloading = true
    error = null
    try {
      const blob = await useAuditApi().downloadExport(current.id, ticket.tenantId, ticket.signal)
      if (!ticket.current() || job?.id !== current.id || job.tenant_id !== current.tenant_id) return
      const url = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = `${current.id}.jsonl`
      link.click()
      window.setTimeout(() => URL.revokeObjectURL(url), 1000)
    } catch (cause) {
      if (!ticket.current() || job?.id !== current.id || job.tenant_id !== current.tenant_id) return
      if (cause instanceof ApiError && cause.status === 403 && cause.reason === 'legal_hold') {
        denied = { id: current.id, tenantId: current.tenant_id }
        reads.invalidate(); downloads.invalidate()
        job = null; paused = true; pollError = null; busy = false; downloading = false; notFound = false
        error = cause
      } else {
        // A reason on a non-403 response must not masquerade as a Hold denial.
        error = cause instanceof ApiError
          ? new ApiError(cause.status, cause.code, '下载失败，请刷新任务后重试', cause.requestId)
          : new Error('下载失败，请检查连接后重试')
      }
    } finally {
      if (ticket.current()) downloading = false
      ticket.finish()
    }
  }
</script>

<JobList kind="exports" {tenantId} {revision}>
  {#snippet actions(item)}
    {#if 'requested_by' in item}
      <ExportActions job={item as ExportSummary} {tenantId} onCommitted={afterAction} onRefresh={refreshAction} />
    {/if}
  {/snippet}
</JobList>
<IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
  <h2 class="card-heading">创建合规导出</h2>
  <p class="card-copy">按时间及关联条件封存 JSONL。下载前服务端重新校验摘要、租户和法律保留。取消仅撤销运行权，不证明外部写入停止；重试创建关联新任务，不延续旧查询快照。</p>
  <EvidenceScopeFields {range} {scope} disabled={creating} onRangeChange={(next) => (range = next)} onScopeChange={(field, value) => (scope = { ...scope, [field]: value })} />
  <IrisButton variant="solid" disabled={creating} onclick={() => void create()}>{creating ? '提交中…' : '创建范围导出'}</IrisButton>
</IrisCard>
<IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
  <h2 class="card-heading">导出详情</h2>
  <div class="inline-form"><IrisInput value={jobId} placeholder="export job id" disabled={busy} oninput={(event) => (jobId = event.currentTarget.value)} /><IrisButton variant="outline" disabled={busy || !jobId.trim()} onclick={select}>查询导出</IrisButton></div>
  {#if denied}
    <p role="status">已撤下旧证据详情。任务 {denied.id} · 租户 {denied.tenantId}。刷新只读取此任务，不会自动下载。</p>
    <IrisButton variant="outline" disabled={busy} onclick={refreshDetail}>刷新导出详情</IrisButton>
  {/if}
  <ReadState loading={busy} error={error || pollError} empty={notFound} loadingLabel="正在读取导出详情…" emptyLabel="未找到当前租户的导出任务" onRetry={refreshDetail}>
  {#if job}
    <div class="result-panel"><StatusBadge value={job.status} /><span>{job.id} · {job.event_count} 条事件</span>{#if canAutoPollExport(job.status, attempts, paused)}<IrisProgress indeterminate size="sm" />{/if}</div>
    <p>申请人：{job.requested_by} · 创建：{formatDate(job.created_at)}</p>
    <p>开始执行：{job.started_at ? formatDate(job.started_at) : job.status === 'pending' ? '等待启动' : '未上报（旧任务）'}</p>
    <p>范围：{evidenceScopeSummary(job.query)}</p>
    {#if job.digest}<p>内容摘要：<code class="hash-value">{job.digest}</code></p>{/if}
    {#if job.retry_of}<p>重试自：<a href={evidenceHash('exports', job.retry_of)}>{job.retry_of}</a>。原任务终态不变。</p>{/if}
    {#if job.failure_code}<p class="muted-copy">{exportFailureLabel(job.failure_code)}</p>{/if}
    {#if job.error}<p class="error-copy">{job.error}</p>{/if}
    <ExportActions {job} {tenantId} onCommitted={afterAction} onRefresh={refreshAction} />
    {#if job.status === 'completed'}<IrisButton variant="outline" disabled={downloading} onclick={() => void download()}>{downloading ? '准备下载…' : '下载 JSONL'}</IrisButton>{/if}
    {#if shouldPollExport(job.status) && attempts >= EXPORT_POLL_MAX_ATTEMPTS}<p>已达到自动查询上限。任务可能仍在执行，可手动刷新。</p><IrisButton variant="outline" onclick={() => void lookup()}>继续查询</IrisButton>{/if}
  {:else}<p class="muted-copy">从任务列表选择一项，或输入任务 ID。</p>{/if}
  </ReadState>
</IrisCard>
