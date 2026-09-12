<script lang="ts">
  import { IrisCard, IrisTimeline, type IrisTimelineItem } from '@iris-ui-kit/svelte'
  import type { QueryResult } from '../api/types'
  import { formatDate } from '../components/format'

  let { result, title, showVersion = false }: { result: QueryResult | null; title: string; showVersion?: boolean } = $props()
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
  <IrisTimeline {items} />
</IrisCard>
