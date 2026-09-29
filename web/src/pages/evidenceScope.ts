import type { EventQuery } from '../api/types'
import { buildCalendarEventQuery, emptyEventFilters } from './eventQuery'

export interface EvidenceScopeDraft {
  sourceSystem: string
  eventType: string
  operationId: string
  actorId: string
  correlationId: string
  outcome: string
}

export const emptyEvidenceScope: EvidenceScopeDraft = {
  sourceSystem: '',
  eventType: '',
  operationId: '',
  actorId: '',
  correlationId: '',
  outcome: '',
}

export function defaultEvidenceRange(days: number, now = new Date()) {
  return {
    start: new Date(now.getTime() - days * 86_400_000),
    end: new Date(now.getTime()),
  }
}

export function buildEvidenceScopeQuery(
  range: { start: Date | null; end: Date | null },
  scope: EvidenceScopeDraft,
): EventQuery | null {
  return buildCalendarEventQuery(
    range,
    {
      ...emptyEventFilters,
      sourceSystem: scope.sourceSystem,
      eventType: scope.eventType,
      operationId: scope.operationId,
      actorId: scope.actorId,
      correlationId: scope.correlationId,
      outcome: scope.outcome,
    },
    1000,
  )
}

function localDateLabel(value: Date): string {
  const year = String(value.getFullYear()).padStart(4, '0')
  const month = String(value.getMonth() + 1).padStart(2, '0')
  const day = String(value.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

function isStartOfLocalDay(value: Date): boolean {
  return value.getHours() === 0 && value.getMinutes() === 0 && value.getSeconds() === 0 && value.getMilliseconds() === 0
}

export function evidenceScopeSummary(query: EventQuery): string {
  const unbounded = query.from.startsWith('0001-') || query.to.startsWith('0001-')
  const from = new Date(query.from)
  const to = new Date(query.to)
  if (!unbounded && isStartOfLocalDay(to)) to.setDate(to.getDate() - 1)
  const values = [
    unbounded ? '全部时间' : `${localDateLabel(from)} — ${localDateLabel(to)}`,
    query.source_system && `来源 ${query.source_system}`,
    query.event_type && `类型 ${query.event_type}`,
    query.operation_id && `操作 ${query.operation_id}`,
    query.actor_id && `主体 ${query.actor_id}`,
    query.correlation_id && `关联 ${query.correlation_id}`,
    query.outcome && `结果 ${query.outcome}`,
  ]
  return values.filter(Boolean).join(' · ')
}
