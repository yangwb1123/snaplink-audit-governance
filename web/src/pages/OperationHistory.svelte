<script lang="ts">
  import { IrisButton, IrisCard, IrisDescriptions, IrisFormField, IrisInput } from '@iris-ui-kit/svelte'
  import type { OperationSummary, QueryResult, ReplayResult } from '../api/types'
  import { useAuditApi } from '../api/useAuditApi'
  import ErrorNotice from '../components/ErrorNotice.svelte'
  import { formatDate } from '../components/format'
  import HistoryTimeline from './HistoryTimeline.svelte'

  let { tenantId }: { tenantId?: string } = $props()
  const api = useAuditApi()
  let operationId = $state('')
  let summary = $state<OperationSummary | null>(null)
  let timeline = $state<QueryResult | null>(null)
  let replay = $state<ReplayResult | null>(null)
  let loading = $state(false)
  let error = $state<unknown>(null)

  async function lookup(): Promise<void> {
    if (!operationId.trim()) return
    loading = true
    error = null
    try {
      const id = operationId.trim()
      const [nextSummary, nextTimeline, nextReplay] = await Promise.all([
        api.getOperation(id, tenantId),
        api.getOperationTimeline(id, { pageSize: 200, tenantId }),
        api.replayOperation(id, tenantId),
      ])
      summary = nextSummary
      timeline = nextTimeline
      replay = nextReplay
    } catch (cause) {
      error = cause
    } finally {
      loading = false
    }
  }
</script>

<IrisCard variant="outline" class="lookup-card" style="" header={undefined} footer={undefined}>
  <IrisFormField label="Operation ID">
    <IrisInput
      value={operationId}
      placeholder="输入 operation_id"
      oninput={(event) => (operationId = event.currentTarget.value)}
      onkeydown={(event) => event.key === 'Enter' && void lookup()}
    />
  </IrisFormField>
  <IrisButton variant="solid" disabled={!operationId.trim() || loading} onclick={() => void lookup()}>
    {loading ? '读取中…' : '查询操作'}
  </IrisButton>
</IrisCard>
<ErrorNotice {error} onRetry={() => void lookup()} />
{#if summary}
  <div class="two-column-grid">
    <IrisCard variant="outline" style="" header={undefined} footer={undefined}>
      <h3 class="card-heading">操作摘要</h3>
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
    </IrisCard>
    <IrisCard variant="outline" style="" header={undefined} footer={undefined}>
      <h3 class="card-heading">状态回放</h3>
      <p class="muted-copy">共回放 {replay?.event_count ?? 0} 个事件，最后序号 {replay?.last_sequence ?? 0}</p>
      <pre class="json-view">{JSON.stringify(replay?.state ?? {}, null, 2)}</pre>
    </IrisCard>
    <HistoryTimeline result={timeline} title="事件时间线" />
  </div>
{/if}
