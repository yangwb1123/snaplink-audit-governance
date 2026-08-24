import type { EventQuery } from '../api/types'
import { buildEventQuery, emptyEventFilters } from './eventQuery'

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
  return buildEventQuery(
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

export function evidenceScopeSummary(query: EventQuery): string {
  const unbounded = query.from.startsWith('0001-') || query.to.startsWith('0001-')
  const values = [
    unbounded ? '全部时间' : `${query.from.slice(0, 10)} — ${query.to.slice(0, 10)}`,
    query.source_system && `来源 ${query.source_system}`,
    query.event_type && `类型 ${query.event_type}`,
    query.operation_id && `操作 ${query.operation_id}`,
    query.actor_id && `主体 ${query.actor_id}`,
    query.correlation_id && `关联 ${query.correlation_id}`,
    query.outcome && `结果 ${query.outcome}`,
  ]
  return values.filter(Boolean).join(' · ')
}
