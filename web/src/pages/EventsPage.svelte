<script lang="ts">
  import { onDestroy } from 'svelte'
  import { RequestScope } from '../api/requestScope'
  import { ApiError } from '../api/client'
  import {
    IrisButton,
    IrisCard,
    IrisDescriptions,
    IrisDrawer,
    IrisDrawerContent,
    IrisDrawerTitle,
    IrisFormField,
    IrisInput,
    IrisTable,
  } from '@iris-ui-kit/svelte'
  import { useAuditApi } from '../api/useAuditApi'
  import type { AuditEvent, EventQuery, EventReceipt, QueryResult } from '../api/types'
  import ErrorNotice from '../components/ErrorNotice.svelte'
  import FieldDiff from '../components/FieldDiff.svelte'
  import LoadingBlock from '../components/LoadingBlock.svelte'
  import PageHeader from '../components/PageHeader.svelte'
  import StatusBadge from '../components/StatusBadge.svelte'
  import { formatDate } from '../components/format'
  import { buildEventQuery, emptyEventFilters, type EventFilterDraft } from './eventQuery'
  import { navigateInvestigation } from './investigationNavigation'

  let { tenantId }: { tenantId?: string } = $props()
  const api = useAuditApi()

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
  let selected = $state<AuditEvent | null>(null)
  let receipt = $state<EventReceipt | null>(null)
  let detailError = $state<unknown>(null)
  let failedCursor = $state('')
  let snapshot = $state<QueryResult['snapshot']>()
  const queryScope = new RequestScope()
  const detailScope = new RequestScope()
  onDestroy(() => { queryScope.dispose(); detailScope.dispose() })

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
    const request = queryScope.next()
    const query = { ...appliedQuery, cursor }
    if (!append) {
      snapshot = undefined
      events = []
      total = 0
      nextCursor = ''
      failedCursor = ''
      closeDetail()
    }
    loading = true
    error = null
    try {
      const result = await api.queryEvents(query, tenantId, request.signal)
      if (!request.current()) return
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
    void appliedQuery
    void tenantId
    void runQuery()
  })

  function closeDetail(): void {
    detailScope.invalidate()
    selected = null
    receipt = null
    detailError = null
  }

  async function openDetail(event: AuditEvent): Promise<void> {
    const request = detailScope.next()
    const eventId = event.event_id
    selected = event
    receipt = null
    detailError = null
    try {
      const [full, eventReceipt] = await Promise.all([
        api.getEvent(eventId, tenantId, request.signal),
        api.getReceipt(eventId, tenantId, request.signal),
      ])
      if (!request.current() || selected?.event_id !== eventId) return
      selected = full
      receipt = eventReceipt
    } catch (cause) {
      if (request.current() && selected?.event_id === eventId) detailError = cause
    } finally { request.finish() }
  }

  function openOperationTimeline(): void {
    const id = selected?.operation_id
    if (id) navigateInvestigation({ mode: 'operation', operationId: id })
  }

  function openAggregateHistory(): void {
    const type = selected?.aggregate_type
    const id = selected?.aggregate_id
    if (type && id) navigateInvestigation({ mode: 'aggregate', aggregateType: type, aggregateId: id })
  }

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
  <button class="link-button" type="button" onclick={() => void openDetail(row as unknown as AuditEvent)}>
    {String(row.event_id)}
  </button>
{/snippet}
{#snippet resultCell(row: Record<string, unknown>)}
  <StatusBadge value={String(row.outcome)} />
{/snippet}

<section>
  <PageHeader
    title="审计事件"
    description="跨 Snaplink、Aero ID、Aero Vault 与 Aero IM 检索账本事实；筛选仅在提交后执行，服务端游标用于稳定翻页。"
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
  <ErrorNotice {error} onRetry={retryQuery} />
  {#if loading && events.length === 0}<LoadingBlock />{/if}
  <IrisCard variant="outline" padding="none" style="" header={undefined} footer={undefined}>
    <div class="table-summary" role="status">
      <span>已加载 {events.length} / 共 {total} 条事件</span>
      {#if snapshot}
        <span>固定结果集：{formatDate(snapshot.observed_at)}；有效至 {formatDate(snapshot.expires_at)}。新到达事件需重新查询；不表示完整性已确认。</span>
      {/if}
      <IrisButton size="sm" variant="outline" disabled={loading} onclick={() => void runQuery()}>重新查询最新结果</IrisButton>
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

  <IrisDrawer open={selected !== null} onOpenChange={(open) => !open && closeDetail()} size="min(720px, 92vw)">
    <IrisDrawerContent>
      <IrisDrawerTitle>审计事件详情</IrisDrawerTitle>
      {#if selected}
        <div class="drawer-body">
          <ErrorNotice error={detailError} />
          <IrisDescriptions
        style=""
            bordered
            columns={1}
            items={[
              { label: '事件 ID', value: selected.event_id },
              { label: '租户', value: selected.tenant_id },
              { label: '来源 / 类型', value: `${selected.source_system} / ${selected.event_type}` },
              { label: '操作 / 结果', value: `${selected.action} / ${selected.outcome}` },
              { label: '操作者', value: selected.actor?.name || selected.actor?.id || '—' },
              { label: '时间', value: formatDate(selected.occurred_at) },
              { label: '操作 / 因果 / 关联', value: `${selected.operation_id || '—'} / ${selected.causation_id || '—'} / ${selected.correlation_id || '—'}` },
              { label: 'Trace / Span', value: `${selected.trace_id || '—'} / ${selected.span_id || '—'}` },
              { label: '聚合对象 / 版本', value: `${selected.aggregate_type || '—'}:${selected.aggregate_id || '—'} / ${selected.aggregate_version ?? '—'}` },
              { label: '流 / 序号', value: `${selected.stream_id || '—'} / ${selected.sequence ?? '—'}` },
              { label: '回执状态', value: receipt?.status ?? (detailError ? '回执不可用' : '读取中…') },
            ]}
          />
          {#if receipt}
            <IrisDescriptions
              style=""
              bordered
              columns={1}
              items={[
                { label: '回执流 / 序号', value: `${receipt.stream_id ?? '—'} / ${receipt.sequence ?? '—'}` },
                { label: '账本 / 索引 / 归档', value: `${formatDate(receipt.ledgered_at)} / ${formatDate(receipt.indexed_at)} / ${formatDate(receipt.archived_at)}` },
              ]}
            />
          {/if}
          {#if selected.operation_id || (selected.aggregate_type && selected.aggregate_id)}
            <div class="button-row">
              {#if selected.operation_id}
                <IrisButton size="sm" variant="outline" onclick={openOperationTimeline}>查看操作时间线</IrisButton>
              {/if}
              {#if selected.aggregate_type && selected.aggregate_id}
                <IrisButton size="sm" variant="outline" onclick={openAggregateHistory}>查看聚合历史</IrisButton>
              {/if}
            </div>
          {/if}
          <h3>Payload</h3>
          <FieldDiff changes={selected.changed_fields} />
          <pre class="json-view">{JSON.stringify(selected.payload ?? {}, null, 2)}</pre>
          <h3>Hash</h3>
          <code class="hash-value">{selected.hash || receipt?.hash || '—'}</code>
        </div>
      {/if}
    </IrisDrawerContent>
  </IrisDrawer>
</section>
