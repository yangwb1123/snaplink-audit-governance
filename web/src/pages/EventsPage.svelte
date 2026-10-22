<script lang="ts">
  import { onDestroy, onMount, untrack } from 'svelte'
  import { RequestScope } from '../api/requestScope'
  import { ApiError } from '../api/client'
  import {
    IrisButton,
    IrisCard,
    IrisFormField,
    IrisInput,
    IrisTable,
  } from '@iris-ui-kit/svelte'
  import { useAuditApi } from '../api/useAuditApi'
  import type { AuditEvent, EventQuery, QueryResult } from '../api/types'
  import { can } from '../auth/claims'
  import { authState } from '../auth/store'
  import ErrorNotice from '../components/ErrorNotice.svelte'
  import ReadState from '../components/ReadState.svelte'
  import EventDetail from './EventDetail.svelte'
  import PageHeader from '../components/PageHeader.svelte'
  import StatusBadge from '../components/StatusBadge.svelte'
  import { formatDate } from '../components/format'
  import { buildEventQuery, emptyEventFilters, type EventFilterDraft } from './eventQuery'
  import { eventLocation, navigateEvent } from './navigation'

  let { tenantId }: { tenantId?: string } = $props()
  const session = $derived($authState.session)
  const allowed = $derived(Boolean(session && can(session, 'audit:event:read') && (session.platform ? tenantId : session.tenantId && (!tenantId || tenantId === session.tenantId))))

  function dateInputValue(date: Date): string {
    const offset = date.getTimezoneOffset() * 60_000
    return new Date(date.getTime() - offset).toISOString().slice(0, 16)
  }
  function defaultRange(): { from: string; to: string } {
    const end = new Date()
    return { from: dateInputValue(new Date(end.getTime() - 86_400_000)), to: dateInputValue(end) }
  }

  const initial = defaultRange()
  let fromInput = $state(initial.from)
  let toInput = $state(initial.to)
  let filters = $state<EventFilterDraft>({ ...emptyEventFilters })
  let advanced = $state(false)
  let appliedQuery = $state<EventQuery>(buildEventQuery({ start: new Date(initial.from), end: new Date(initial.to) }, emptyEventFilters)!)
  let events = $state<AuditEvent[]>([])
  let total = $state(0)
  let nextCursor = $state('')
  let loading = $state(false)
  let error = $state<unknown>(null)
  let selectedId = $state(eventLocation(window.location.hash))
  let failedCursor = $state('')
  let snapshot = $state<QueryResult['snapshot']>()
  const queryScope = new RequestScope()
  onDestroy(() => queryScope.dispose())
  onMount(() => {
    const update = () => (selectedId = eventLocation(window.location.hash))
    window.addEventListener('hashchange', update)
    return () => window.removeEventListener('hashchange', update)
  })

  const advancedFields: Array<{ name: keyof EventFilterDraft; label: string; placeholder: string }> = [
    { name: 'actorId', label: 'Actor ID', placeholder: '用户或服务主体' },
    { name: 'targetId', label: 'Target ID', placeholder: '被操作对象' },
    { name: 'operationId', label: 'Operation ID', placeholder: '业务操作标识' },
    { name: 'causationId', label: 'Causation ID', placeholder: '直接因果事件' },
    { name: 'correlationId', label: 'Correlation ID', placeholder: '端到端关联标识' },
    { name: 'traceId', label: 'Trace ID', placeholder: 'W3C Trace ID' },
    { name: 'aggregateType', label: 'Aggregate Type', placeholder: '例如 file' },
    { name: 'aggregateId', label: 'Aggregate ID', placeholder: '业务对象 ID' },
    { name: 'streamId', label: 'Stream ID', placeholder: '账本流标识' },
    { name: 'payloadField', label: 'Payload Field', placeholder: '受治理的可搜索字段' },
    { name: 'payloadDigest', label: 'Payload Digest', placeholder: '字段搜索摘要' },
  ]

  function setFilter(name: keyof EventFilterDraft, value: string): void {
    filters[name] = value
  }

  function queryRange(): { start: Date; end: Date } | null {
    const start = new Date(fromInput)
    const end = new Date(toInput)
    if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return null
    return { start, end }
  }

  function submitQuery(): void {
    const range = queryRange()
    const next = range ? buildEventQuery(range, filters) : null
    if (!next) {
      error = new Error('请选择有效的开始和结束时间')
      return
    }
    appliedQuery = next
  }

  function resetQuery(): void {
    const range = defaultRange()
    fromInput = range.from
    toInput = range.to
    filters = { ...emptyEventFilters }
    const dates = queryRangeFor(range)
    appliedQuery = buildEventQuery(dates, filters)!
  }

  function queryRangeFor(range: { from: string; to: string }): { start: Date; end: Date } {
    return { start: new Date(range.from), end: new Date(range.to) }
  }

  async function runQuery(cursor = '', append = false): Promise<void> {
    if (!allowed) return
    const ticket = queryScope.next()
    const expectedSession = session, expectedTenant = tenantId
    const request = { ...ticket, current: () => ticket.current() && session === expectedSession && tenantId === expectedTenant && allowed }
    const query = { ...appliedQuery, cursor }
    if (!append) {
      snapshot = undefined
      events = []
      total = 0
      nextCursor = ''
      failedCursor = ''
    }
    loading = true
    error = null
    try {
      const result = await useAuditApi().queryEvents(query, tenantId, request.signal)
      if (!request.current()) return
      if (result.items.some((event) => event.tenant_id !== (tenantId ?? session?.tenantId))) throw new Error('事件列表与当前租户不匹配')
      events = append ? [...events, ...result.items] : result.items
      total = result.count
      snapshot = result.snapshot
      nextCursor = result.next_cursor ?? ''
      failedCursor = ''
    } catch (cause) {
      if (request.current()) {
        error = cause
        const restart = cause instanceof ApiError && cause.code === 'invalid_request'
        failedCursor = append && !restart ? cursor : ''
        if (restart) nextCursor = ''
      }
    } finally {
      if (request.current()) loading = false
      request.finish()
    }
  }

  function retryQuery(): void {
    const cursor = failedCursor
    void (cursor ? runQuery(cursor, true) : runQuery())
  }

  $effect(() => {
    void appliedQuery; void tenantId; void session
    untrack(() => {
      queryScope.invalidate()
      events = []; total = 0; nextCursor = ''; snapshot = undefined; error = null; loading = false
      void runQuery()
    })
    return () => queryScope.invalidate()
  })

  function tableRows(): Record<string, unknown>[] {
    return events.map((event) => ({
      ...event,
      actor_name: event.actor?.name || event.actor?.id || '—',
      occurred_at: formatDate(event.occurred_at),
    }))
  }
  const rows = $derived(tableRows())
</script>

{#snippet eventIdCell(row: Record<string, unknown>)}
  <button class="link-button" type="button" onclick={() => navigateEvent(String(row.event_id))}>
    {String(row.event_id)}
  </button>
{/snippet}
{#snippet resultCell(row: Record<string, unknown>)}
  <StatusBadge value={String(row.outcome)} />
{/snippet}

<section>
  {#snippet headerActions()}<IrisButton size="sm" variant="outline" disabled={loading || !allowed} onclick={() => void runQuery()}>重新查询最新结果</IrisButton>{/snippet}
  <PageHeader
    title="审计事件"
    description="跨 Snaplink、Aero ID、Aero Vault 与 Aero IM 检索账本事实；筛选仅在提交后执行，服务端游标用于稳定翻页。"
    actions={headerActions}
  />
  <IrisCard variant="outline" class="filter-card" style="" header={undefined} footer={undefined}>
    <div class="filter-grid">
      <IrisFormField label="开始时间">
        <input class="native-control" type="datetime-local" value={fromInput} oninput={(event) => (fromInput = event.currentTarget.value)} />
      </IrisFormField>
      <IrisFormField label="结束时间">
        <input class="native-control" type="datetime-local" value={toInput} oninput={(event) => (toInput = event.currentTarget.value)} />
      </IrisFormField>
      <IrisFormField label="事件类型">
        <IrisInput value={filters.eventType} placeholder="例如 file.downloaded" oninput={(event) => setFilter('eventType', event.currentTarget.value)} />
      </IrisFormField>
      <IrisFormField label="来源系统">
        <IrisInput value={filters.sourceSystem} placeholder="例如 aero-vault" oninput={(event) => setFilter('sourceSystem', event.currentTarget.value)} />
      </IrisFormField>
      <IrisFormField label="结果">
        <IrisInput value={filters.outcome} placeholder="success / failed" oninput={(event) => setFilter('outcome', event.currentTarget.value)} />
      </IrisFormField>
      <div class="filter-actions">
        <IrisButton variant="outline" onclick={() => (advanced = !advanced)}>{advanced ? '收起高级条件' : '高级筛选'}</IrisButton>
        <IrisButton variant="solid" disabled={loading} onclick={submitQuery}>查询</IrisButton>
      </div>
    </div>
    {#if advanced}
      <div class="advanced-filter-grid">
        {#each advancedFields as field (field.name)}
          <IrisFormField label={field.label}>
            <IrisInput value={filters[field.name]} placeholder={field.placeholder} oninput={(event) => setFilter(field.name, event.currentTarget.value)} />
          </IrisFormField>
        {/each}
      </div>
    {/if}
    <div class="filter-footer">
      <span>支持通过操作、因果、关联、Trace 和业务聚合标识还原跨服务调用链。</span>
      <IrisButton size="sm" variant="outline" disabled={loading} onclick={resetQuery}>重置为最近 24 小时</IrisButton>
    </div>
  </IrisCard>
  {#if !allowed}<p role="alert">当前会话没有事件读取权限或有效租户，不会发出请求。</p>{/if}
  {#if events.length > 0}<ErrorNotice {error} onRetry={retryQuery} />{/if}
  <ReadState loading={loading && events.length === 0} error={events.length === 0 ? error : null} empty={!loading && allowed && !error && events.length === 0 && total === 0} emptyLabel="当前条件下暂无事件" onRetry={retryQuery}>
  <IrisCard variant="outline" padding="none" style="" header={undefined} footer={undefined}>
    <div class="table-summary" role="status">
      <span>已加载 {events.length} / 共 {total} 条事件</span>
      {#if snapshot}
        <span>固定结果集：{formatDate(snapshot.observed_at)}；有效至 {formatDate(snapshot.expires_at)}。新到达事件需重新查询；不表示完整性已确认。</span>
      {/if}
    </div>
    <IrisTable
      rowKey="event_id"
      columns={[
        { key: 'event_id', title: '事件 ID', minWidth: 210, render: () => eventIdCell },
        { key: 'occurred_at', title: '发生时间', width: 180 },
        { key: 'source_system', title: '来源', width: 130 },
        { key: 'event_type', title: '事件类型', minWidth: 180 },
        { key: 'actor_name', title: '操作者', width: 150 },
        { key: 'action', title: '动作', width: 130 },
        { key: 'outcome', title: '结果', width: 110, render: () => resultCell },
      ]}
      data={rows}
      density="compact"
      striped
      resizableColumns
    />
    {#if nextCursor}
      <div class="table-footer">
        <IrisButton variant="outline" disabled={loading} onclick={() => void runQuery(nextCursor, true)}>{loading ? '加载中…' : '加载下一页'}</IrisButton>
      </div>
    {/if}
  </IrisCard>
  </ReadState>
  {#if selectedId}<EventDetail eventId={selectedId} {tenantId} onClose={() => (window.location.hash = '#events')} />{/if}
</section>
