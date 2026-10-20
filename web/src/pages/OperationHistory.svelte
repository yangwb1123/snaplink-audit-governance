<script lang="ts">
  import { onMount } from 'svelte'
  import { IrisButton, IrisCard, IrisDescriptions, IrisFormField, IrisInput } from '@iris-ui-kit/svelte'
  import type { OperationSummary, QueryResult, ReplayResult } from '../api/types'
  import { useAuditApi } from '../api/useAuditApi'
  import ErrorNotice from '../components/ErrorNotice.svelte'
  import LoadingBlock from '../components/LoadingBlock.svelte'
  import { formatDate } from '../components/format'
  import HistoryTimeline from './HistoryTimeline.svelte'
  import { LatestLookup } from './lookupState'

  let { tenantId, initialId = '' }: { tenantId?: string; initialId?: string } = $props()
  const api = useAuditApi()
  const lookupState = new LatestLookup()
  let operationId = $state('')
  let attempted = $state(false)
  let summary = $state<OperationSummary | null>(null)
  let timeline = $state<QueryResult | null>(null)
  let replay = $state<ReplayResult | null>(null)
  let summaryError = $state<unknown>(null)
  let timelineError = $state<unknown>(null)
  let replayError = $state<unknown>(null)
  let loadingSummary = $state(false)
  let loadingTimeline = $state(false)
  let loadingReplay = $state(false)
  let loading = $state(false)

  onMount(() => {
    const id = initialId.trim()
    if (!id) return
    operationId = id
    void lookup()
  })

  function lookupKey(id = operationId.trim()): string {
    return JSON.stringify([tenantId ?? '', id])
  }

  function resetResults(): void {
    summary = null
    timeline = null
    replay = null
    summaryError = null
    timelineError = null
    replayError = null
    loadingSummary = false
    loadingTimeline = false
    loadingReplay = false
  }

  function updateOperationId(value: string): void {
    operationId = value
    lookupState.invalidate()
    attempted = false
    resetResults()
    loading = false
  }

  // The three reads are independent observations. A failure in one section must
  // not hide the facts the other sections already returned; each failure stays
  // explicit and retryable instead of degrading into a blank page.
  async function lookup(): Promise<void> {
    const id = operationId.trim()
    if (!id || loading) return
    const key = lookupKey(id)
    const request = lookupState.begin(key)
    attempted = true
    loading = true
    resetResults()
    loadingSummary = true
    loadingTimeline = true
    loadingReplay = true
    const [summaryResult, timelineResult, replayResult] = await Promise.allSettled([
      api.getOperation(id, tenantId),
      api.getOperationTimeline(id, { pageSize: 200, tenantId }),
      api.replayOperation(id, tenantId),
    ])
    if (!lookupState.isCurrent(request, lookupKey())) return
    if (summaryResult.status === 'fulfilled') summary = summaryResult.value
    else summaryError = summaryResult.reason
    if (timelineResult.status === 'fulfilled') timeline = timelineResult.value
    else timelineError = timelineResult.reason
    if (replayResult.status === 'fulfilled') replay = replayResult.value
    else replayError = replayResult.reason
    loadingSummary = false
    loadingTimeline = false
    loadingReplay = false
    loading = false
  }

  async function retrySection(section: 'summary' | 'timeline' | 'replay'): Promise<void> {
    const id = operationId.trim()
    if (!id || loading) return
    const key = lookupKey(id)
    const request = lookupState.begin(key)
    loading = true
    if (section === 'summary') {
      loadingSummary = true
      summaryError = null
    } else if (section === 'timeline') {
      loadingTimeline = true
      timelineError = null
    } else {
      loadingReplay = true
      replayError = null
    }
    try {
      if (section === 'summary') {
        const value = await api.getOperation(id, tenantId)
        if (lookupState.isCurrent(request, lookupKey())) summary = value
      } else if (section === 'timeline') {
        const value = await api.getOperationTimeline(id, { pageSize: 200, tenantId })
        if (lookupState.isCurrent(request, lookupKey())) timeline = value
      } else {
        const value = await api.replayOperation(id, tenantId)
        if (lookupState.isCurrent(request, lookupKey())) replay = value
      }
    } catch (cause) {
      if (!lookupState.isCurrent(request, lookupKey())) return
      if (section === 'summary') summaryError = cause
      else if (section === 'timeline') timelineError = cause
      else replayError = cause
    } finally {
      if (lookupState.isCurrent(request, lookupKey())) {
        if (section === 'summary') loadingSummary = false
        else if (section === 'timeline') loadingTimeline = false
        else loadingReplay = false
        loading = false
      }
    }
  }

  const partial = $derived(
    Boolean(
      (summaryError || timelineError || replayError) && (summary || timeline || replay),
    ),
  )
</script>

<IrisCard variant="outline" class="lookup-card" style="" header={undefined} footer={undefined}>
  <IrisFormField label="Operation ID">
    <IrisInput
      value={operationId}
      placeholder="输入 operation_id"
      oninput={(event) => updateOperationId(event.currentTarget.value)}
      onkeydown={(event) => event.key === 'Enter' && void lookup()}
    />
  </IrisFormField>
  <IrisButton variant="solid" disabled={!operationId.trim() || loading} onclick={() => void lookup()}>
    {loading ? '读取中…' : '查询操作'}
  </IrisButton>
</IrisCard>
{#if attempted}
  {#if partial}
    <p class="muted-copy" role="status">
      部分区块读取失败；已显示的分区各自独立，失败分区可单独重试。本页是实时观察，不声明操作完整性或业务执行结果。
    </p>
  {/if}
  <div class="two-column-grid">
    <IrisCard variant="outline" style="" header={undefined} footer={undefined}>
      <h3 class="card-heading">操作摘要</h3>
      {#if summary}
        <IrisDescriptions
          style=""
          columns={1}
          items={[
            { label: 'Operation ID', value: summary.operation_id },
            { label: '租户', value: summary.tenant_id },
            { label: '事件数', value: summary.event_count },
            { label: '时间范围', value: `${formatDate(summary.first_at)} — ${formatDate(summary.last_at)}` },
            { label: '结果', value: summary.outcomes.join(', ') || '—' },
          ]}
        />
      {:else if summaryError}
        <ErrorNotice error={summaryError} onRetry={() => void retrySection('summary')} />
      {:else if loadingSummary}
        <LoadingBlock label="正在读取操作摘要…" />
      {:else}
        <p class="muted-copy">尚未读取操作摘要。</p>
      {/if}
    </IrisCard>
    <IrisCard variant="outline" style="" header={undefined} footer={undefined}>
      <h3 class="card-heading">状态回放</h3>
      {#if replay}
        <p class="muted-copy">共回放 {replay.event_count} 个事件，最后序号 {replay.last_sequence}；这是本地折叠观察，不是业务执行回执。</p>
        <pre class="json-view">{JSON.stringify(replay.state ?? {}, null, 2)}</pre>
      {:else if replayError}
        <ErrorNotice error={replayError} onRetry={() => void retrySection('replay')} />
      {:else if loadingReplay}
        <LoadingBlock label="正在读取状态回放…" />
      {:else}
        <p class="muted-copy">尚未读取状态回放。</p>
      {/if}
    </IrisCard>
    {#if timeline}
      <HistoryTimeline result={timeline} title="事件时间线" />
    {:else if timelineError}
      <IrisCard variant="outline" class="timeline-card" style="" header={undefined} footer={undefined}>
        <h3 class="card-heading">事件时间线</h3>
        <ErrorNotice error={timelineError} onRetry={() => void retrySection('timeline')} />
      </IrisCard>
    {:else}
      <IrisCard variant="outline" class="timeline-card" style="" header={undefined} footer={undefined}>
        <h3 class="card-heading">事件时间线</h3>
        {#if loadingTimeline}
          <LoadingBlock label="正在读取事件时间线…" />
        {:else}
          <p class="muted-copy">尚未读取事件时间线。</p>
        {/if}
      </IrisCard>
    {/if}
  </div>
{/if}
