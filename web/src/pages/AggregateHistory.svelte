<script lang="ts">
  import { IrisButton, IrisCard, IrisDescriptions, IrisFormField, IrisInput } from '@iris-ui-kit/svelte'
  import type { QueryResult } from '../api/types'
  import { useAuditApi } from '../api/useAuditApi'
  import ErrorNotice from '../components/ErrorNotice.svelte'
  import HistoryTimeline from './HistoryTimeline.svelte'

  let { tenantId }: { tenantId?: string } = $props()
  const api = useAuditApi()
  let aggregateType = $state('')
  let aggregateId = $state('')
  let result = $state<QueryResult | null>(null)
  let loading = $state(false)
  let error = $state<unknown>(null)

  async function lookup(cursor = '', append = false): Promise<void> {
    if (!aggregateType.trim() || !aggregateId.trim()) return
    loading = true
    error = null
    try {
      const next = await api.getAggregateTimeline(aggregateType.trim(), aggregateId.trim(), {
        pageSize: 200,
        cursor,
        tenantId,
      })
      result = append && result ? { ...next, items: [...result.items, ...next.items] } : next
    } catch (cause) {
      error = cause
    } finally {
      loading = false
    }
  }
</script>

<IrisCard variant="outline" class="aggregate-lookup-card" style="" header={undefined} footer={undefined}>
  <IrisFormField label="Aggregate Type">
    <IrisInput value={aggregateType} placeholder="例如 file" oninput={(event) => (aggregateType = event.currentTarget.value)} />
  </IrisFormField>
  <IrisFormField label="Aggregate ID">
    <IrisInput
      value={aggregateId}
      placeholder="输入业务对象 ID"
      oninput={(event) => (aggregateId = event.currentTarget.value)}
      onkeydown={(event) => event.key === 'Enter' && void lookup()}
    />
  </IrisFormField>
  <IrisButton variant="solid" disabled={!aggregateType.trim() || !aggregateId.trim() || loading} onclick={() => void lookup()}>
    {loading ? '读取中…' : '查询对象历史'}
  </IrisButton>
</IrisCard>
<ErrorNotice {error} onRetry={() => void lookup()} />
{#if result}
  <div class="aggregate-result">
    <IrisCard variant="outline" style="" header={undefined} footer={undefined}>
      <IrisDescriptions
        style=""
        columns={3}
        items={[
          { label: '对象类型', value: aggregateType.trim() },
          { label: '对象 ID', value: aggregateId.trim() },
          { label: '事件总数', value: result.count },
        ]}
      />
    </IrisCard>
    <HistoryTimeline result={result} title="聚合版本历史" showVersion />
    {#if result.next_cursor}
      <div class="table-footer">
        <IrisButton variant="outline" disabled={loading} onclick={() => void lookup(result?.next_cursor ?? '', true)}>
          {loading ? '加载中…' : '加载下一页'}
        </IrisButton>
      </div>
    {/if}
  </div>
{/if}
