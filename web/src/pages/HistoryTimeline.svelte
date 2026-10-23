<script lang="ts">
  import { IrisButton, IrisCard, IrisTimeline, type IrisTimelineItem } from '@iris-ui-kit/svelte'
  import type { AuditEvent, QueryResult } from '../api/types'
  import { can } from '../auth/claims'
  import { authState } from '../auth/store'
  import ErrorNotice from '../components/ErrorNotice.svelte'
  import { formatDate } from '../components/format'
  import { eventHash } from './navigation'

  let {
    result,
    title,
    tenantId,
    showVersion = false,
    loadingMore = false,
    onLoadMore,
    moreError,
    onRetryMore,
  }: {
    result: QueryResult | null
    title: string
    tenantId?: string
    showVersion?: boolean
    loadingMore?: boolean
    onLoadMore?: () => void
    moreError?: unknown
    onRetryMore?: () => void
  } = $props()
  const session = $derived($authState.session)
  // Shell supplies an established platform selection; tenant sessions may only
  // use their signed scope, just as the event detail page does.
  const effectiveTenant = $derived(session?.platform ? tenantId
    : session?.tenantId && (!tenantId || tenantId === session.tenantId) ? session.tenantId : undefined)
  const eventReadAllowed = $derived(Boolean(session && can(session, 'audit:event:read') && effectiveTenant))

  function eventHref(event: AuditEvent): string | null {
    if (!eventReadAllowed || event.tenant_id !== effectiveTenant) return null
    const href = eventHash(event.event_id)
    // The builder's safe list fallback is not an original-event destination.
    return href === '#events' ? null : href
  }

  const items = $derived<(IrisTimelineItem & { eventId: string; href: string | null })[]>(
    (result?.items ?? []).map((event) => ({
      eventId: event.event_id,
      href: eventHref(event),
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
  <IrisTimeline {items}>
    {#snippet itemSnippet({ item, index })}
      <div data-iris-timeline-time class="timeline-time">{item.time}</div>
      <div data-iris-timeline-title class="timeline-title">{item.title}</div>
      <div data-iris-timeline-desc class="timeline-description">{item.description}</div>
      {#if items[index].href}
        <a class="timeline-event-link" href={items[index].href}>查看原事件 {items[index].eventId}</a>
      {/if}
    {/snippet}
  </IrisTimeline>
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

<style>
  .timeline-time {
    font-size: var(--iris-font-size-xs, 12px);
    color: var(--iris-muted);
  }
  .timeline-title {
    font-weight: 600;
    color: var(--iris-foreground);
  }
  .timeline-description {
    font-size: var(--iris-font-size-md, 14px);
    color: var(--iris-foreground);
  }
  .timeline-event-link {
    color: var(--iris-primary);
    overflow-wrap: anywhere;
  }
</style>
