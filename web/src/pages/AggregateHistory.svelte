<script lang="ts">
  import { onDestroy, onMount } from 'svelte'
  import { IrisButton, IrisCard, IrisDescriptions, IrisFormField, IrisInput } from '@iris-ui-kit/svelte'
  import type { QueryResult } from '../api/types'
  import { useAuditApi } from '../api/useAuditApi'
  import ErrorNotice from '../components/ErrorNotice.svelte'
  import LoadingBlock from '../components/LoadingBlock.svelte'
  import HistoryTimeline from './HistoryTimeline.svelte'
  import { LatestLookup } from './lookupState'

  let { tenantId, initialType = '', initialId = '' }: { tenantId?: string; initialType?: string; initialId?: string } = $props()
  const api = useAuditApi()
  const lookupState = new LatestLookup()
  onDestroy(() => lookupState.invalidate())
  let aggregateType = $state('')
  let aggregateId = $state('')
  let result = $state<QueryResult | null>(null)
  let loading = $state(false)
  let error = $state<unknown>(null)
  let failedCursor = $state<string | null>(null)
  let activeSearchKey = ''

  onMount(() => {
    const type = initialType.trim()
    const id = initialId.trim()
    if (!type || !id) return
    aggregateType = type
    aggregateId = id
    void lookup()
  })

  function lookupKey(type = aggregateType.trim(), id = aggregateId.trim()): string {
    return JSON.stringify([tenantId ?? '', type, id])
  }

  function invalidateLookup(): void {
    lookupState.invalidate()
    activeSearchKey = ''
    result = null
    error = null
    failedCursor = null
    loading = false
  }

  function updateAggregateType(value: string): void {
    aggregateType = value
    invalidateLookup()
  }

  function updateAggregateId(value: string): void {
    aggregateId = value
    invalidateLookup()
  }

  async function lookup(cursor = '', append = false): Promise<void> {
    const type = aggregateType.trim()
    const id = aggregateId.trim()
    if (!type || !id || loading) return
    const key = lookupKey(type, id)
    const previousResult = result
    if (
      append &&
      (!previousResult || activeSearchKey !== key || !cursor || previousResult.next_cursor !== cursor)
    ) return

    const request = lookupState.begin(key)
    if (!append) {
      activeSearchKey = key
      result = null
      failedCursor = null
    } else {
      failedCursor = cursor
    }
    loading = true
    error = null
    try {
      const next = await api.getAggregateTimeline(type, id, {
        pageSize: 200,
        cursor,
        tenantId,
      })
      if (!lookupState.isCurrent(request, lookupKey()) || activeSearchKey !== key) return
      result = append && previousResult
        ? { ...next, items: [...previousResult.items, ...next.items] }
        : next
      failedCursor = null
    } catch (cause) {
      if (lookupState.isCurrent(request, lookupKey()) && activeSearchKey === key) {
        error = cause
        failedCursor = append ? cursor : null
      }
    } finally {
      if (lookupState.isCurrent(request, lookupKey())) loading = false
    }
  }

  function retryLookup(): void {
    const cursor = failedCursor
    void (cursor ? lookup(cursor, true) : lookup())
  }
</script>

<IrisCard variant="outline" class="aggregate-lookup-card" style="" header={undefined} footer={undefined}>
  <IrisFormField label="Aggregate Type">
    <IrisInput value={aggregateType} placeholder="例如 file" oninput={(event) => updateAggregateType(event.currentTarget.value)} />
  </IrisFormField>
  <IrisFormField label="Aggregate ID">
    <IrisInput
      value={aggregateId}
      placeholder="输入业务对象 ID"
      oninput={(event) => updateAggregateId(event.currentTarget.value)}
      onkeydown={(event) => event.key === 'Enter' && void lookup()}
    />
  </IrisFormField>
  <IrisButton variant="solid" disabled={!aggregateType.trim() || !aggregateId.trim() || loading} onclick={() => void lookup()}>
    {loading ? '读取中…' : '查询对象历史'}
  </IrisButton>
</IrisCard>
<ErrorNotice {error} onRetry={retryLookup} />
{#if loading && !result}<LoadingBlock label="正在读取聚合历史…" />{/if}
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
