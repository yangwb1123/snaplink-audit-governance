import * as React from 'react'
import {
  IrisButton,
  IrisCard,
  IrisDateRangePicker,
  IrisDescriptions,
  IrisDrawer,
  IrisDrawerContent,
  IrisDrawerTitle,
  IrisFormField,
  IrisInput,
  IrisTable,
  type IrisDateRange,
  type IrisTableColumn,
} from '@iris-ui-kit/react'
import { useAuditApi } from '../api/useAuditApi'
import type { AuditEvent, EventQuery, EventReceipt } from '../api/types'
import { ErrorNotice, LoadingBlock, PageHeader, formatDate } from '../components/Page'
import { StatusBadge } from '../components/StatusBadge'
import { buildEventQuery, emptyEventFilters, type EventFilterDraft } from './eventQuery'

function defaultRange(): IrisDateRange {
  const end = new Date()
  return { start: new Date(end.getTime() - 24 * 60 * 60 * 1000), end }
}

// Query, cursor and detail-drawer states converge in this route component.
// eslint-disable-next-line complexity
export function EventsPage({ tenantId }: { tenantId?: string }) {
  const api = useAuditApi()
  const [initialRange] = React.useState<IrisDateRange>(defaultRange)
  const [range, setRange] = React.useState<IrisDateRange>(initialRange)
  const [filters, setFilters] = React.useState<EventFilterDraft>(emptyEventFilters)
  const [advanced, setAdvanced] = React.useState(false)
  const [appliedQuery, setAppliedQuery] = React.useState<EventQuery>(
    () => buildEventQuery(initialRange, emptyEventFilters)!,
  )
  const [events, setEvents] = React.useState<AuditEvent[]>([])
  const [total, setTotal] = React.useState(0)
  const [nextCursor, setNextCursor] = React.useState('')
  const [loading, setLoading] = React.useState(false)
  const [error, setError] = React.useState<unknown>(null)
  const [selected, setSelected] = React.useState<AuditEvent | null>(null)
  const [receipt, setReceipt] = React.useState<EventReceipt | null>(null)
  const [detailError, setDetailError] = React.useState<unknown>(null)
  const requestSequence = React.useRef(0)

  const setFilter = (name: keyof EventFilterDraft, value: string) =>
    setFilters((current) => ({ ...current, [name]: value }))

  const runQuery = React.useCallback(
    async (cursor = '', append = false) => {
      const sequence = ++requestSequence.current
      setLoading(true)
      setError(null)
      try {
        const result = await api.queryEvents(
          {
            ...appliedQuery,
            cursor,
          },
          tenantId,
        )
        if (sequence !== requestSequence.current) return
        setEvents((current) => (append ? [...current, ...result.items] : result.items))
        setTotal(result.count)
        setNextCursor(result.next_cursor ?? '')
      } catch (cause) {
        if (sequence === requestSequence.current) setError(cause)
      } finally {
        if (sequence === requestSequence.current) setLoading(false)
      }
    },
    [api, appliedQuery, tenantId],
  )

  React.useEffect(() => {
    void runQuery()
    return () => {
      requestSequence.current += 1
    }
  }, [runQuery])

  React.useEffect(() => {
    setSelected(null)
    setReceipt(null)
  }, [tenantId])

  const submitQuery = () => {
    const next = buildEventQuery(range, filters)
    if (!next) {
      setError(new Error('请选择有效的开始和结束时间'))
      return
    }
    setAppliedQuery(next)
  }

  const resetQuery = () => {
    const nextRange = defaultRange()
    setRange(nextRange)
    setFilters(emptyEventFilters)
    setAppliedQuery(buildEventQuery(nextRange, emptyEventFilters)!)
  }

  const openDetail = React.useCallback(
    async (event: AuditEvent) => {
      setSelected(event)
      setReceipt(null)
      setDetailError(null)
      try {
        const [full, eventReceipt] = await Promise.all([
          api.getEvent(event.event_id, tenantId),
          api.getReceipt(event.event_id, tenantId),
        ])
        setSelected(full)
        setReceipt(eventReceipt)
      } catch (cause) {
        setDetailError(cause)
      }
    },
    [api, tenantId],
  )

  const columns = React.useMemo<IrisTableColumn<AuditEvent>[]>(
    () => [
      {
        key: 'event_id',
        title: '事件 ID',
        minWidth: 210,
        render: (value, row) => (
          <button className="link-button" type="button" onClick={() => void openDetail(row)}>
            {String(value)}
          </button>
        ),
      },
      {
        key: 'occurred_at',
        title: '发生时间',
        width: 180,
        render: (value) => formatDate(String(value)),
      },
      { key: 'source_system', title: '来源', width: 130 },
      { key: 'event_type', title: '事件类型', minWidth: 180 },
      {
        key: 'actor',
        title: '操作者',
        width: 150,
        render: (_value, row) => row.actor?.name || row.actor?.id || '—',
      },
      { key: 'action', title: '动作', width: 130 },
      {
        key: 'outcome',
        title: '结果',
        width: 110,
        render: (value) => <StatusBadge value={String(value)} />,
      },
    ],
    [openDetail],
  )

  return (
    <section>
      <PageHeader
        title="审计事件"
        description="跨 Snaplink、Aero ID、Aero Vault 与 Aero IM 检索账本事实；筛选仅在提交后执行，服务端游标用于稳定翻页。"
      />
      <IrisCard variant="outline" className="filter-card">
        <div className="filter-grid">
          <IrisFormField label="时间范围">
            <IrisDateRangePicker value={range} onValueChange={setRange} />
          </IrisFormField>
          <IrisFormField label="事件类型">
            <IrisInput
              value={filters.eventType}
              onChange={(event) => setFilter('eventType', event.target.value)}
              placeholder="例如 file.downloaded"
            />
          </IrisFormField>
          <IrisFormField label="来源系统">
            <IrisInput
              value={filters.sourceSystem}
              onChange={(event) => setFilter('sourceSystem', event.target.value)}
              placeholder="例如 aero-vault"
            />
          </IrisFormField>
          <IrisFormField label="结果">
            <IrisInput
              value={filters.outcome}
              onChange={(event) => setFilter('outcome', event.target.value)}
              placeholder="success / failed"
            />
          </IrisFormField>
          <div className="filter-actions">
            <IrisButton variant="outline" onClick={() => setAdvanced((value) => !value)}>
              {advanced ? '收起高级条件' : '高级筛选'}
            </IrisButton>
            <IrisButton
              variant="solid"
              disabled={!range.start || !range.end || loading}
              onClick={submitQuery}
            >
              查询
            </IrisButton>
          </div>
        </div>
        {advanced ? <AdvancedFilters filters={filters} onChange={setFilter} /> : null}
        <div className="filter-footer">
          <span>支持通过操作、因果、关联、Trace 和业务聚合标识还原跨服务调用链。</span>
          <IrisButton size="sm" variant="outline" disabled={loading} onClick={resetQuery}>
            重置为最近 24 小时
          </IrisButton>
        </div>
      </IrisCard>
      <ErrorNotice error={error} onRetry={() => void runQuery()} />
      {loading && events.length === 0 ? <LoadingBlock /> : null}
      <IrisCard variant="outline" padding="none">
        <div className="table-summary" role="status">
          已加载 {events.length} / 共 {total} 条事件
        </div>
        <IrisTable
          rowKey="event_id"
          columns={columns}
          data={events}
          size="small"
          striped
          resizableColumns
        />
        {nextCursor ? (
          <div className="table-footer">
            <IrisButton
              variant="outline"
              disabled={loading}
              onClick={() => void runQuery(nextCursor, true)}
            >
              {loading ? '加载中…' : '加载下一页'}
            </IrisButton>
          </div>
        ) : null}
      </IrisCard>
      <IrisDrawer
        open={selected !== null}
        onOpenChange={(open) => !open && setSelected(null)}
        size="min(720px, 92vw)"
      >
        <IrisDrawerContent>
          <IrisDrawerTitle>审计事件详情</IrisDrawerTitle>
          {selected ? (
            <div className="drawer-body">
              <ErrorNotice error={detailError} />
              <IrisDescriptions
                bordered
                columns={1}
                items={[
                  { label: '事件 ID', value: selected.event_id },
                  { label: '租户', value: selected.tenant_id },
                  {
                    label: '来源 / 类型',
                    value: `${selected.source_system} / ${selected.event_type}`,
                  },
                  {
                    label: '操作 / 结果',
                    value: (
                      <span className="inline-pair">
                        {selected.action}
                        <StatusBadge value={selected.outcome} />
                      </span>
                    ),
                  },
                  { label: '操作者', value: selected.actor?.name || selected.actor?.id || '—' },
                  { label: '时间', value: formatDate(selected.occurred_at) },
                  {
                    label: '操作 / 因果 / 关联',
                    value: `${selected.operation_id || '—'} / ${selected.causation_id || '—'} / ${selected.correlation_id || '—'}`,
                  },
                  {
                    label: 'Trace / Span',
                    value: `${selected.trace_id || '—'} / ${selected.span_id || '—'}`,
                  },
                  {
                    label: '聚合对象 / 版本',
                    value: `${selected.aggregate_type || '—'}:${selected.aggregate_id || '—'} / ${selected.aggregate_version ?? '—'}`,
                  },
                  {
                    label: '流 / 序号',
                    value: `${selected.stream_id || '—'} / ${selected.sequence ?? '—'}`,
                  },
                  {
                    label: '回执状态',
                    value: receipt ? <StatusBadge value={receipt.status} /> : '读取中…',
                  },
                ]}
              />
              <h3>Payload</h3>
              <pre className="json-view">{JSON.stringify(selected.payload ?? {}, null, 2)}</pre>
              <h3>Hash</h3>
              <code className="hash-value">{selected.hash || receipt?.hash || '—'}</code>
            </div>
          ) : null}
        </IrisDrawerContent>
      </IrisDrawer>
    </section>
  )
}

function AdvancedFilters({
  filters,
  onChange,
}: {
  filters: EventFilterDraft
  onChange: (name: keyof EventFilterDraft, value: string) => void
}) {
  const fields: Array<{
    name: keyof EventFilterDraft
    label: string
    placeholder: string
  }> = [
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
  return (
    <div className="advanced-filter-grid">
      {fields.map((field) => (
        <IrisFormField key={field.name} label={field.label}>
          <IrisInput
            value={filters[field.name]}
            placeholder={field.placeholder}
            onChange={(event) => onChange(field.name, event.target.value)}
          />
        </IrisFormField>
      ))}
    </div>
  )
}
