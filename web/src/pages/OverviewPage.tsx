import * as React from 'react'
import {
  IrisBadge,
  IrisButton,
  IrisCard,
  IrisProgress,
  IrisStatistic,
  IrisTable,
  type IrisTableColumn,
} from '@iris-ui-kit/react'
import { useAuditApi } from '../api/useAuditApi'
import type { AdminAction, AuditEvent, EventFacets } from '../api/types'
import { can } from '../auth/claims'
import { useAuth } from '../auth/AuthProvider'
import { ErrorNotice, LoadingBlock, PageHeader, formatDate } from '../components/Page'
import { StatusBadge } from '../components/StatusBadge'
import { failedFacetCount, topFacetEntries } from './auditFacets'

function recentWindow() {
  const to = new Date()
  const from = new Date(to.getTime() - 24 * 60 * 60 * 1000)
  return { from: from.toISOString(), to: to.toISOString(), page_size: 25 }
}

const eventColumns: IrisTableColumn<AuditEvent>[] = [
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
  {
    key: 'outcome',
    title: '结果',
    width: 110,
    render: (value) => <StatusBadge value={String(value)} />,
  },
]

const actionColumns: IrisTableColumn<AdminAction>[] = [
  {
    key: 'created_at',
    title: '记录时间',
    width: 180,
    render: (value) => formatDate(String(value)),
  },
  { key: 'action', title: '治理动作', width: 190 },
  { key: 'actor', title: '操作者', width: 140 },
  { key: 'target_id', title: '对象', minWidth: 170 },
]

export function OverviewPage({ tenantId }: { tenantId?: string }) {
  const api = useAuditApi()
  const { session } = useAuth()
  const readable = Boolean(session && can(session, 'audit:event:read'))
  const policyReadable = Boolean(session && can(session, 'audit:policy:read'))
  const [events, setEvents] = React.useState<AuditEvent[]>([])
  const [facets, setFacets] = React.useState<EventFacets | null>(null)
  const [actions, setActions] = React.useState<AdminAction[]>([])
  const [loading, setLoading] = React.useState(true)
  const [error, setError] = React.useState<unknown>(null)
  const [revision, setRevision] = React.useState(0)

  React.useEffect(() => {
    let active = true
    const load = async () => {
      if (!readable) {
        setEvents([])
        setFacets(null)
        setActions([])
        setLoading(false)
        return
      }
      setLoading(true)
      setError(null)
      setEvents([])
      setFacets(null)
      setActions([])
      try {
        const window = recentWindow()
        const [eventResult, facetResult, actionResult] = await Promise.all([
          api.queryEvents(window, tenantId),
          api.getEventFacets({ since: window.from, until: window.to }, tenantId),
          policyReadable
            ? api.listAdminActions(tenantId, 25)
            : Promise.resolve({ items: [] as AdminAction[], count: 0 }),
        ])
        if (!active) return
        setEvents(eventResult.items ?? [])
        setFacets(facetResult.facets)
        setActions(actionResult.items ?? [])
      } catch (cause) {
        if (active) setError(cause)
      } finally {
        if (active) setLoading(false)
      }
    }
    void load()
    return () => {
      active = false
    }
  }, [api, policyReadable, readable, revision, tenantId])

  return (
    <section>
      <PageHeader
        title="审计态势"
        description="最近 24 小时的不可变审计事实与治理操作，数据直接来自 Audit Governance API。"
        actions={
          <IrisButton variant="outline" onClick={() => setRevision((value) => value + 1)}>
            刷新
          </IrisButton>
        }
      />
      <ErrorNotice error={error} onRetry={() => setRevision((value) => value + 1)} />
      {!readable ? (
        <IrisCard variant="outline">
          当前 Snaplink 角色没有 audit:event:read，Audit API 不会返回审计事实。
        </IrisCard>
      ) : null}
      <OverviewMetrics facets={facets} actionCount={actions.length} />
      {loading ? (
        <LoadingBlock />
      ) : (
        <OverviewData
          facets={facets}
          events={events}
          actions={actions}
          showActions={policyReadable}
        />
      )}
    </section>
  )
}

function OverviewMetrics({
  facets,
  actionCount,
}: {
  facets: EventFacets | null
  actionCount: number
}) {
  const eventCount = facets?.total ?? 0
  const failed = failedFacetCount(facets?.outcomes ?? {})
  const sources = Object.keys(facets?.clients ?? {}).length
  const hasFailures = failed > 0
  const failureRate = hasFailures && eventCount > 0 ? `${((failed / eventCount) * 100).toFixed(1)}%` : '0%'
  return (
    <div className="metric-grid">
      <IrisCard variant="outline">
        <IrisStatistic label="近 24 小时事件" value={eventCount} />
      </IrisCard>
      <IrisCard variant="outline">
        <IrisStatistic label="活跃来源系统" value={sources} />
      </IrisCard>
      <IrisCard variant="outline">
        <IrisStatistic
          label="失败类结果"
          value={failed}
          trend={hasFailures ? 'up' : 'neutral'}
          trendTone={hasFailures ? 'danger' : 'neutral'}
          trendValue={failureRate}
        />
      </IrisCard>
      <IrisCard variant="outline">
        <IrisStatistic label="最近治理记录" value={actionCount} description="最多展示 25 条" />
      </IrisCard>
    </div>
  )
}

function OverviewData({
  facets,
  events,
  actions,
  showActions,
}: {
  facets: EventFacets | null
  events: AuditEvent[]
  actions: AdminAction[]
  showActions: boolean
}) {
  const total = facets?.total ?? 0
  return (
    <>
      <div className="facet-grid">
        <FacetBreakdown title="结果分布" values={facets?.outcomes ?? {}} total={total} />
        <FacetBreakdown title="事件类型" values={facets?.types ?? {}} total={total} />
        <FacetBreakdown title="来源系统" values={facets?.clients ?? {}} total={total} />
      </div>
      <div className="dashboard-grid">
        <IrisCard variant="outline" padding="none" header="最近审计事件">
          <IrisTable
            rowKey="event_id"
            size="small"
            columns={eventColumns}
            data={events.slice(0, 10)}
          />
        </IrisCard>
        {showActions ? (
          <IrisCard variant="outline" padding="none" header="治理操作自审计">
            <IrisTable
              rowKey="id"
              size="small"
              columns={actionColumns}
              data={actions.slice(0, 10)}
            />
          </IrisCard>
        ) : null}
      </div>
    </>
  )
}

function FacetBreakdown({
  title,
  values,
  total,
}: {
  title: string
  values: Record<string, number>
  total: number
}) {
  const entries = topFacetEntries(values)
  return (
    <IrisCard variant="outline" header={title}>
      {entries.length ? (
        <div className="facet-list">
          {entries.map((entry) => (
            <div className="facet-row" key={entry.label}>
              <div className="facet-label">
                <span title={entry.label}>{entry.label}</span>
                <IrisBadge tone="neutral" size="sm">
                  {entry.count}
                </IrisBadge>
              </div>
              <IrisProgress
                value={entry.count}
                max={Math.max(total, 1)}
                size="sm"
                aria-label={`${entry.label} ${entry.count} / ${total}`}
              />
            </div>
          ))}
        </div>
      ) : (
        <p className="muted-copy">当前窗口暂无数据</p>
      )}
    </IrisCard>
  )
}
