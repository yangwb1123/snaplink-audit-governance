<script lang="ts">
  import { IrisButton, IrisCard, IrisStatistic, IrisTable, type IrisTableColumn } from '@iris-ui-kit/svelte'
  import { useAuditApi } from '../api/useAuditApi'
  import type { AdminAction, AuditEvent, EventFacets } from '../api/types'
  import { can } from '../auth/claims'
  import { authState } from '../auth/store'
  import ErrorNotice from '../components/ErrorNotice.svelte'
  import LoadingBlock from '../components/LoadingBlock.svelte'
  import PageHeader from '../components/PageHeader.svelte'
  import { formatDate } from '../components/format'
  import FacetBreakdown from './FacetBreakdown.svelte'
  import { failedFacetCount } from './auditFacets'

  let { tenantId }: { tenantId?: string } = $props()
  const api = useAuditApi()
  let events = $state<AuditEvent[]>([])
  let facets = $state<EventFacets | null>(null)
  let actions = $state<AdminAction[]>([])
  let loading = $state(true)
  let error = $state<unknown>(null)
  let revision = $state(0)
  const readable = $derived(Boolean($authState.session && can($authState.session, 'audit:event:read')))
  const policyReadable = $derived(Boolean($authState.session && can($authState.session, 'audit:policy:read')))

  const eventColumns: IrisTableColumn[] = [
    { key: 'occurred_at', title: '发生时间', width: 180 },
    { key: 'source_system', title: '来源', width: 130 },
    { key: 'event_type', title: '事件类型', minWidth: 180 },
    { key: 'actor_name', title: '操作者', width: 150 },
    { key: 'outcome', title: '结果', width: 110 },
  ]
  const actionColumns: IrisTableColumn[] = [
    { key: 'created_at', title: '记录时间', width: 180 },
    { key: 'action', title: '治理动作', width: 190 },
    { key: 'actor', title: '操作者', width: 140 },
    { key: 'target_id', title: '对象', minWidth: 170 },
  ]

  function recentWindow() {
    const to = new Date()
    const from = new Date(to.getTime() - 86_400_000)
    return { from: from.toISOString(), to: to.toISOString(), page_size: 25 }
  }

  async function load(): Promise<void> {
    if (!readable) {
      events = []
      facets = null
      actions = []
      loading = false
      return
    }
    loading = true
    error = null
    try {
      const range = recentWindow()
      const [eventResult, facetResult, actionResult] = await Promise.all([
        api.queryEvents(range, tenantId),
        api.getEventFacets({ since: range.from, until: range.to }, tenantId),
        policyReadable ? api.listAdminActions(tenantId, 25) : Promise.resolve({ items: [] as AdminAction[], count: 0 }),
      ])
      events = eventResult.items ?? []
      facets = facetResult.facets
      actions = actionResult.items ?? []
    } catch (cause) {
      error = cause
    } finally {
      loading = false
    }
  }

  $effect(() => {
    void tenantId
    void readable
    void policyReadable
    void revision
    void load()
  })

  function eventRows(): Record<string, unknown>[] {
    return events.slice(0, 10).map((event) => ({
      ...event,
      occurred_at: formatDate(event.occurred_at),
      actor_name: event.actor?.name || event.actor?.id || '—',
    }))
  }

  function actionRows(): Record<string, unknown>[] {
    return actions.slice(0, 10).map((action) => ({
      ...action,
      created_at: formatDate(action.created_at),
    }))
  }

  const eventTableRows = $derived(eventRows())
  const actionTableRows = $derived(actionRows())
  const eventCount = $derived(facets?.total ?? 0)
  const failed = $derived(failedFacetCount(facets?.outcomes ?? {}))
  const sourceCount = $derived(Object.keys(facets?.clients ?? {}).length)
  const failureRate = $derived(failed > 0 && eventCount > 0 ? `${((failed / eventCount) * 100).toFixed(1)}%` : '0%')
</script>

<section>
  {#snippet headerActions()}
    <IrisButton variant="outline" onclick={() => (revision += 1)}>刷新</IrisButton>
  {/snippet}
  <PageHeader
    title="审计态势"
    description="最近 24 小时的不可变审计事实与治理操作，数据直接来自 Audit Governance API。"
    actions={headerActions}
  />
  <ErrorNotice {error} onRetry={() => (revision += 1)} />
  {#if !readable}
    <IrisCard variant="outline" style="" header={undefined} footer={undefined}>当前 Snaplink 角色没有 audit:event:read，Audit API 不会返回审计事实。</IrisCard>
  {/if}
  <div class="metric-grid">
    <IrisCard variant="outline" style="" header={undefined} footer={undefined}><IrisStatistic label="近 24 小时事件" value={eventCount} style="" /></IrisCard>
    <IrisCard variant="outline" style="" header={undefined} footer={undefined}><IrisStatistic label="活跃来源系统" value={sourceCount} style="" /></IrisCard>
    <IrisCard variant="outline" style="" header={undefined} footer={undefined}>
      <IrisStatistic label="失败类结果" value={failed} trend={failed > 0 ? 'up' : 'neutral'} trendTone={failed > 0 ? 'danger' : 'neutral'} trendValue={failureRate} style="" />
    </IrisCard>
    <IrisCard variant="outline" style="" header={undefined} footer={undefined}><IrisStatistic label="最近治理记录" value={actions.length} description="最多展示 25 条" style="" /></IrisCard>
  </div>
  {#if loading}
    <LoadingBlock />
  {:else}
    <div class="facet-grid">
      <FacetBreakdown title="结果分布" values={facets?.outcomes ?? {}} total={eventCount} />
      <FacetBreakdown title="事件类型" values={facets?.types ?? {}} total={eventCount} />
      <FacetBreakdown title="来源系统" values={facets?.clients ?? {}} total={eventCount} />
    </div>
    <div class="dashboard-grid">
      <IrisCard variant="outline" padding="none" style="" header={undefined} footer={undefined}>
        <h3 class="card-heading card-heading-padded">最近审计事件</h3>
        <IrisTable rowKey="event_id" columns={eventColumns} data={eventTableRows} density="compact" />
      </IrisCard>
      {#if policyReadable}
        <IrisCard variant="outline" padding="none" style="" header={undefined} footer={undefined}>
          <h3 class="card-heading card-heading-padded">治理操作自审计</h3>
          <IrisTable rowKey="id" columns={actionColumns} data={actionTableRows} density="compact" />
        </IrisCard>
      {/if}
    </div>
  {/if}
</section>
