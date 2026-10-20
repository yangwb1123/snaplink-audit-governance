<script lang="ts">
  import { onMount } from 'svelte'
  import { IrisSegmented } from '@iris-ui-kit/svelte'
  import PageHeader from '../components/PageHeader.svelte'
  import AggregateHistory from './AggregateHistory.svelte'
  import OperationHistory from './OperationHistory.svelte'
  import { investigationLocation, type InvestigationTarget } from './investigationNavigation'

  type HistoryMode = 'operation' | 'aggregate'
  let { tenantId }: { tenantId?: string } = $props()
  const initial = investigationLocation(window.location.hash)
  let target = $state<InvestigationTarget | null>(initial)
  let mode = $state<HistoryMode>(initial?.mode ?? 'operation')

  const operationId = $derived(target?.mode === 'operation' ? target.operationId : '')
  const aggregate = $derived(target?.mode === 'aggregate' ? target : null)
  // Remount only when an investigation link supplies a new target, so each
  // mode keeps its own draft/result state across manual tab switches.
  const targetKey = $derived(
    operationId || (aggregate ? `${aggregate.aggregateType}/${aggregate.aggregateId}` : ''),
  )

  function chooseMode(value: HistoryMode): void {
    mode = value
    if (target) {
      target = null
      // Consume the one-shot cross-link without leaving a stale identifier in
      // history; the source page remains reachable with the back button.
      history.replaceState(null, '', '#operations')
    }
  }

  onMount(() => {
    const update = () => {
      const next = investigationLocation(window.location.hash)
      target = next
      if (next) mode = next.mode
    }
    window.addEventListener('hashchange', update)
    return () => window.removeEventListener('hashchange', update)
  })
</script>

<section>
  {#snippet headerActions()}
    <IrisSegmented
      ariaLabel="历史查询方式"
      value={mode}
      options={[{ label: '操作时间线', value: 'operation' }, { label: '聚合对象历史', value: 'aggregate' }]}
      onchange={(value) => chooseMode(value as HistoryMode)}
    />
  {/snippet}
  <PageHeader
    title="业务历史与回放"
    description="按 operation_id 聚合跨服务事实，或按 aggregate_type / aggregate_id 查看业务对象的版本历史。"
    actions={headerActions}
  />
  {#if mode === 'operation'}
    {#key targetKey}<OperationHistory {tenantId} initialId={operationId} />{/key}
  {:else}
    {#key targetKey}<AggregateHistory {tenantId} initialType={aggregate?.aggregateType ?? ''} initialId={aggregate?.aggregateId ?? ''} />{/key}
  {/if}
</section>
