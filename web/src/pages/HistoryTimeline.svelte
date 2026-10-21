<script lang="ts">
  import { IrisButton, IrisCard, IrisTimeline, type IrisTimelineItem } from '@iris-ui-kit/svelte'
  import type { QueryResult } from '../api/types'
  import ErrorNotice from '../components/ErrorNotice.svelte'
  import { formatDate } from '../components/format'

  let {
    result,
    title,
    showVersion = false,
    loadingMore = false,
    onLoadMore,
    moreError,
    onRetryMore,
  }: {
    result: QueryResult | null
    title: string
    showVersion?: boolean
    loadingMore?: boolean
    onLoadMore?: () => void
    moreError?: unknown
    onRetryMore?: () => void
  } = $props()
  const items = $derived<IrisTimelineItem[]>(
    (result?.items ?? []).map((event) => ({
      key: event.event_id,
      time: formatDate(event.occurred_at),
      title: `${event.source_system} · ${event.event_type}`,
      description: `${showVersion ? `v${String(event.aggregate_version ?? '—')} · ` : ''}${event.actor?.name || event.actor?.id || 'unknown'} · ${event.action} · ${event.outcome}`,
      variant: event.outcome === 'failed' ? 'danger' : 'success',
    })),
  )
</script>

<IrisCard variant="outline" class="timeline-card" style="" header={undefined} footer={undefined}>
  <h3 class="card-heading">{title}</h3>
  {#if result && items.length === 0}
    <p class="muted-copy" role="status">本次读取未返回事件；这不是历史不存在或完整性的证明。</p>
  {/if}
  <IrisTimeline {items} />
  {#if result}
    <div class="timeline-footer">
      <span class="muted-copy">已显示 {items.length} / 共 {result.count} 条；实时观察，翻页期间总数可能随提交变化。</span>
      {#if result.next_cursor && onLoadMore}
        <IrisButton size="sm" variant="outline" disabled={loadingMore} onclick={onLoadMore}>
          {loadingMore ? '加载中…' : '加载下一页'}
        </IrisButton>
      {/if}
    </div>
    {#if moreError}
      <ErrorNotice error={moreError} onRetry={onRetryMore} />
    {/if}
  {/if}
</IrisCard>
