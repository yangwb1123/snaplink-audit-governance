<script lang="ts">
  import { IrisBadge, IrisCard, IrisProgress } from '@iris-ui-kit/svelte'
  import { topFacetEntries } from './auditFacets'

  let { title, values, total }: { title: string; values: Record<string, number>; total: number } = $props()
  const entries = $derived(topFacetEntries(values))
</script>

<IrisCard variant="outline" style="" header={undefined} footer={undefined}>
  <h3 class="card-heading">{title}</h3>
  {#if entries.length}
    <div class="facet-list">
      {#each entries as entry (entry.label)}
        <div class="facet-row">
          <div class="facet-label">
            <span title={entry.label}>{entry.label}</span>
            <IrisBadge tone="neutral" size="sm">{entry.count}</IrisBadge>
          </div>
          <IrisProgress
            value={entry.count}
            max={Math.max(total, 1)}
            size="sm"
            aria-label={`${entry.label} ${entry.count} / ${total}`}
          />
        </div>
      {/each}
    </div>
  {:else}
    <p class="muted-copy">当前窗口暂无数据</p>
  {/if}
</IrisCard>
