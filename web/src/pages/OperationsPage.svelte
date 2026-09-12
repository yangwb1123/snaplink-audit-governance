<script lang="ts">
  import { IrisSegmented } from '@iris-ui-kit/svelte'
  import PageHeader from '../components/PageHeader.svelte'
  import AggregateHistory from './AggregateHistory.svelte'
  import OperationHistory from './OperationHistory.svelte'

  type HistoryMode = 'operation' | 'aggregate'
  let { tenantId }: { tenantId?: string } = $props()
  let mode = $state<HistoryMode>('operation')
</script>

<section>
  {#snippet headerActions()}
    <IrisSegmented
      ariaLabel="历史查询方式"
      value={mode}
      options={[{ label: '操作时间线', value: 'operation' }, { label: '聚合对象历史', value: 'aggregate' }]}
      onchange={(value) => (mode = value as HistoryMode)}
    />
  {/snippet}
  <PageHeader
    title="业务历史与回放"
    description="按 operation_id 聚合跨服务事实，或按 aggregate_type / aggregate_id 查看业务对象的版本历史。"
    actions={headerActions}
  />
  {#if mode === 'operation'}
    <OperationHistory {tenantId} />
  {:else}
    <AggregateHistory {tenantId} />
  {/if}
</section>
