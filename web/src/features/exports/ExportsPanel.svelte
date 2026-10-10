<script lang="ts">
  import { onDestroy, onMount, untrack } from 'svelte'
  import { IrisButton, IrisCard, IrisInput, IrisProgress } from '@iris-ui-kit/svelte'
  import { useAuditApi } from '../../api/useAuditApi'
  import { RequestScope } from '../../api/requestScope'
  import type { ExportJob } from '../../api/types'
  import ErrorNotice from '../../components/ErrorNotice.svelte'
  import JobList from '../../components/JobList.svelte'
  import StatusBadge from '../../components/StatusBadge.svelte'
  import { formatDate } from '../../components/format'
  import EvidenceScopeFields, { type DateRange } from '../../pages/EvidenceScopeFields.svelte'
  import { buildEvidenceScopeQuery, defaultEvidenceRange, emptyEvidenceScope, evidenceScopeSummary, type EvidenceScopeDraft } from '../../pages/evidenceScope'
  import { canAutoPollExport, shouldPollExport, EXPORT_POLL_INTERVAL_MS, EXPORT_POLL_MAX_ATTEMPTS } from '../../pages/governanceState'
  import { navigateEvidence } from '../../pages/evidenceNavigation'

  let { tenantId, initialId = '' }: { tenantId?: string; initialId?: string } = $props()
  const api = useAuditApi()
  const reads = new RequestScope()
  const writes = new RequestScope()
  const downloads = new RequestScope()
  let lastTenant = untrack(() => tenantId)
  let range = $state<DateRange>(defaultEvidenceRange(1))
  let scope = $state<EvidenceScopeDraft>({ ...emptyEvidenceScope })
  let jobId = $state('')
  let job = $state<ExportJob | null>(null)
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
    if (currentTenant === lastTenant) return () => { reads.invalidate(); writes.invalidate(); downloads.invalidate() }
    lastTenant = currentTenant
    reads.invalidate(); writes.invalidate(); downloads.invalidate()
    range = defaultEvidenceRange(1)
    scope = { ...emptyEvidenceScope }
    jobId = ''; job = null; error = null; pollError = null
    attempts = 0; paused = false; busy = false; creating = false; downloading = false
    return () => { reads.invalidate(); writes.invalidate(); downloads.invalidate() }
  })
  $effect(() => {
    const id = initialId
    void tenantId
    if (id) { jobId = id; void lookup(id) }
  })
  $effect(() => {
    const current = job
    if (!current || !visible || !canAutoPollExport(current.status, attempts, paused)) return
    const previousAttempts = attempts
    const scopeTenant = tenantId
    let active = true
    const timer = window.setTimeout(() => {
      const ticket = reads.capture()
      void api.getExport(current.id, scopeTenant, ticket.signal).then((next) => {
        if (!active || !ticket.current() || job?.id !== current.id) return
        if (scopeTenant && next.tenant_id !== scopeTenant) throw new Error('导出任务与当前租户不匹配')
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

  async function create(): Promise<void> {
    const query = buildEvidenceScopeQuery(range, scope)
    if (!query) { error = new Error('请选择有效且非空的导出时间范围'); return }
    const ticket = writes.next()
    const scopeTenant = tenantId
    creating = true
    error = null
    try {
      const next = await api.createExport(query, scopeTenant, ticket.signal)
      if (!ticket.current()) return
      if (scopeTenant && next.tenant_id !== scopeTenant) throw new Error('导出任务与当前租户不匹配')
      job = next; jobId = next.id
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

  async function lookup(id = jobId.trim()): Promise<void> {
    if (!id) return
    const ticket = reads.next()
    const scopeTenant = tenantId
    downloads.invalidate()
    downloading = false
    busy = true; job = null; error = null; pollError = null; paused = true
    try {
      const next = await api.getExport(id, scopeTenant, ticket.signal)
      if (!ticket.current()) return
      if (scopeTenant && next.tenant_id !== scopeTenant) throw new Error('导出任务与当前租户不匹配')
      job = next; jobId = next.id; attempts = 0; paused = false
    } catch (cause) {
      if (ticket.current()) error = cause
    } finally {
      if (ticket.current()) busy = false
      ticket.finish()
    }
  }

  function select(): void {
    const id = jobId.trim()
    if (!id) return
    if (id === initialId) void lookup(id)
    else navigateEvidence('exports', id)
  }

  async function download(): Promise<void> {
    const current = job
    if (!current || current.status !== 'completed' || downloading) return
    const ticket = downloads.next()
    downloading = true
    error = null
    try {
      const blob = await api.downloadExport(current.id, tenantId, ticket.signal)
      if (!ticket.current() || job?.id !== current.id) return
      const url = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = `${current.id}.jsonl`
      link.click()
      window.setTimeout(() => URL.revokeObjectURL(url), 1000)
    } catch (cause) {
      if (ticket.current()) error = cause
    } finally {
      if (ticket.current()) downloading = false
      ticket.finish()
    }
  }
</script>

<JobList kind="exports" {tenantId} {revision} />
<IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
  <h2 class="card-heading">创建合规导出</h2>
  <p class="card-copy">按时间及关联条件封存 JSONL。下载前服务端重新校验摘要、租户和法律保留。</p>
  <EvidenceScopeFields {range} {scope} disabled={creating} onRangeChange={(next) => (range = next)} onScopeChange={(field, value) => (scope = { ...scope, [field]: value })} />
  <IrisButton variant="solid" disabled={creating} onclick={() => void create()}>{creating ? '提交中…' : '创建范围导出'}</IrisButton>
</IrisCard>
<IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
  <h2 class="card-heading">导出详情</h2>
  <div class="inline-form"><IrisInput value={jobId} placeholder="export job id" disabled={busy} oninput={(event) => (jobId = event.currentTarget.value)} /><IrisButton variant="outline" disabled={busy || !jobId.trim()} onclick={select}>查询导出</IrisButton></div>
  <ErrorNotice {error} onRetry={() => void lookup()} />
  <ErrorNotice error={pollError} onRetry={() => void lookup()} />
  {#if job}
    <div class="result-panel"><StatusBadge value={job.status} /><span>{job.id} · {job.event_count} 条事件</span>{#if canAutoPollExport(job.status, attempts, paused)}<IrisProgress indeterminate size="sm" />{/if}</div>
    <p>申请人：{job.requested_by} · 创建：{formatDate(job.created_at)}</p>
    <p>开始执行：{job.started_at ? formatDate(job.started_at) : job.status === 'pending' ? '等待启动' : '未上报（旧任务）'}</p>
    <p>范围：{evidenceScopeSummary(job.query)}</p>
    {#if job.digest}<p>内容摘要：<code class="hash-value">{job.digest}</code></p>{/if}
    {#if job.error}<p class="error-copy">{job.error}</p>{/if}
    {#if job.status === 'completed'}<IrisButton variant="outline" disabled={downloading} onclick={() => void download()}>{downloading ? '准备下载…' : '下载 JSONL'}</IrisButton>{/if}
    {#if shouldPollExport(job.status) && attempts >= EXPORT_POLL_MAX_ATTEMPTS}<p>已达到自动查询上限。任务可能仍在执行，可手动刷新。</p><IrisButton variant="outline" onclick={() => void lookup()}>继续查询</IrisButton>{/if}
  {:else if !busy && !error && !pollError}<p class="muted-copy">从任务列表选择一项，或输入任务 ID。</p>{/if}
</IrisCard>
