import * as React from 'react'
import {
  IrisButton,
  IrisCard,
  IrisDescriptions,
  IrisFormField,
  IrisInput,
  IrisSegmented,
  IrisTimeline,
} from '@iris-ui-kit/react'
import { useAuditApi } from '../api/useAuditApi'
import type { OperationSummary, QueryResult, ReplayResult } from '../api/types'
import { ErrorNotice, PageHeader, formatDate } from '../components/Page'
import { StatusBadge } from '../components/StatusBadge'

type HistoryMode = 'operation' | 'aggregate'

export function OperationsPage({ tenantId }: { tenantId?: string }) {
  const [mode, setMode] = React.useState<HistoryMode>('operation')
  return (
    <section>
      <PageHeader
        title="业务历史与回放"
        description="按 operation_id 聚合跨服务事实，或按 aggregate_type / aggregate_id 查看业务对象的版本历史。"
        actions={
          <IrisSegmented
            ariaLabel="历史查询方式"
            value={mode}
            options={[
              { label: '操作时间线', value: 'operation' },
              { label: '聚合对象历史', value: 'aggregate' },
            ]}
            onValueChange={(value) => setMode(value as HistoryMode)}
          />
        }
      />
      {mode === 'operation' ? (
        <OperationHistory tenantId={tenantId} />
      ) : (
        <AggregateHistory tenantId={tenantId} />
      )}
    </section>
  )
}

function OperationHistory({ tenantId }: { tenantId?: string }) {
  const api = useAuditApi()
  const [operationId, setOperationId] = React.useState('')
  const [summary, setSummary] = React.useState<OperationSummary | null>(null)
  const [timeline, setTimeline] = React.useState<QueryResult | null>(null)
  const [replay, setReplay] = React.useState<ReplayResult | null>(null)
  const [loading, setLoading] = React.useState(false)
  const [error, setError] = React.useState<unknown>(null)

  const lookup = async () => {
    if (!operationId.trim()) return
    setLoading(true)
    setError(null)
    try {
      const [nextSummary, nextTimeline, nextReplay] = await Promise.all([
        api.getOperation(operationId.trim(), tenantId),
        api.getOperationTimeline(operationId.trim(), { pageSize: 200, tenantId }),
        api.replayOperation(operationId.trim(), tenantId),
      ])
      setSummary(nextSummary)
      setTimeline(nextTimeline)
      setReplay(nextReplay)
    } catch (cause) {
      setError(cause)
    } finally {
      setLoading(false)
    }
  }

  return (
    <>
      <IrisCard variant="outline" className="lookup-card">
        <IrisFormField label="Operation ID">
          <IrisInput
            value={operationId}
            onChange={(event) => setOperationId(event.target.value)}
            placeholder="输入 operation_id"
            onKeyDown={(event) => event.key === 'Enter' && void lookup()}
          />
        </IrisFormField>
        <IrisButton
          variant="solid"
          disabled={!operationId.trim() || loading}
          onClick={() => void lookup()}
        >
          {loading ? '读取中…' : '查询操作'}
        </IrisButton>
      </IrisCard>
      <ErrorNotice error={error} onRetry={() => void lookup()} />
      {summary ? (
        <div className="two-column-grid">
          <IrisCard variant="outline" header="操作摘要">
            <IrisDescriptions
              columns={1}
              items={[
                { label: 'Operation ID', value: summary.operation_id },
                { label: '租户', value: summary.tenant_id },
                { label: '事件数', value: summary.event_count },
                {
                  label: '时间范围',
                  value: `${formatDate(summary.first_at)} — ${formatDate(summary.last_at)}`,
                },
                {
                  label: '结果',
                  value: (
                    <span className="badge-list">
                      {summary.outcomes.map((value) => (
                        <StatusBadge key={value} value={value} />
                      ))}
                    </span>
                  ),
                },
              ]}
            />
          </IrisCard>
          <IrisCard variant="outline" header="状态回放">
            <p className="muted-copy">
              共回放 {replay?.event_count ?? 0} 个事件，最后序号 {replay?.last_sequence ?? 0}
            </p>
            <pre className="json-view">{JSON.stringify(replay?.state ?? {}, null, 2)}</pre>
          </IrisCard>
          <HistoryTimeline result={timeline} title="事件时间线" />
        </div>
      ) : null}
    </>
  )
}

function AggregateHistory({ tenantId }: { tenantId?: string }) {
  const api = useAuditApi()
  const [aggregateType, setAggregateType] = React.useState('')
  const [aggregateId, setAggregateId] = React.useState('')
  const [result, setResult] = React.useState<QueryResult | null>(null)
  const [loading, setLoading] = React.useState(false)
  const [error, setError] = React.useState<unknown>(null)

  const lookup = async (cursor = '', append = false) => {
    if (!aggregateType.trim() || !aggregateId.trim()) return
    setLoading(true)
    setError(null)
    try {
      const next = await api.getAggregateTimeline(aggregateType.trim(), aggregateId.trim(), {
        pageSize: 200,
        cursor,
        tenantId,
      })
      setResult((current) =>
        append && current ? { ...next, items: [...current.items, ...next.items] } : next,
      )
    } catch (cause) {
      setError(cause)
    } finally {
      setLoading(false)
    }
  }

  return (
    <>
      <IrisCard variant="outline" className="aggregate-lookup-card">
        <IrisFormField label="Aggregate Type">
          <IrisInput
            value={aggregateType}
            onChange={(event) => setAggregateType(event.target.value)}
            placeholder="例如 file"
          />
        </IrisFormField>
        <IrisFormField label="Aggregate ID">
          <IrisInput
            value={aggregateId}
            onChange={(event) => setAggregateId(event.target.value)}
            placeholder="输入业务对象 ID"
            onKeyDown={(event) => event.key === 'Enter' && void lookup()}
          />
        </IrisFormField>
        <IrisButton
          variant="solid"
          disabled={!aggregateType.trim() || !aggregateId.trim() || loading}
          onClick={() => void lookup()}
        >
          {loading ? '读取中…' : '查询对象历史'}
        </IrisButton>
      </IrisCard>
      <ErrorNotice error={error} onRetry={() => void lookup()} />
      {result ? (
        <div className="aggregate-result">
          <IrisCard variant="outline">
            <IrisDescriptions
              columns={3}
              items={[
                { label: '对象类型', value: aggregateType.trim() },
                { label: '对象 ID', value: aggregateId.trim() },
                { label: '事件总数', value: result.count },
              ]}
            />
          </IrisCard>
          <HistoryTimeline result={result} title="聚合版本历史" showVersion />
          {result.next_cursor ? (
            <div className="table-footer">
              <IrisButton
                variant="outline"
                disabled={loading}
                onClick={() => void lookup(result.next_cursor, true)}
              >
                {loading ? '加载中…' : '加载下一页'}
              </IrisButton>
            </div>
          ) : null}
        </div>
      ) : null}
    </>
  )
}

function HistoryTimeline({
  result,
  title,
  showVersion = false,
}: {
  result: QueryResult | null
  title: string
  showVersion?: boolean
}) {
  return (
    <IrisCard variant="outline" header={title} className="timeline-card">
      <IrisTimeline
        items={(result?.items ?? []).map((event) => ({
          key: event.event_id,
          time: formatDate(event.occurred_at),
          title: `${event.source_system} · ${event.event_type}`,
          description: `${showVersion ? `v${String(event.aggregate_version ?? '—')} · ` : ''}${
            event.actor?.name || event.actor?.id || 'unknown'
          } · ${event.action} · ${event.outcome}`,
          variant: event.outcome === 'failed' ? 'danger' : 'success',
        }))}
      />
    </IrisCard>
  )
}
