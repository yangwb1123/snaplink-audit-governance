import type { EventQuery } from '../api/types'

export interface EventFilterDraft {
  eventType: string
  sourceSystem: string
  outcome: string
  actorId: string
  targetId: string
  operationId: string
  causationId: string
  correlationId: string
  traceId: string
  aggregateType: string
  aggregateId: string
  streamId: string
  payloadField: string
  payloadDigest: string
}

export const emptyEventFilters: EventFilterDraft = {
  eventType: '',
  sourceSystem: '',
  outcome: '',
  actorId: '',
  targetId: '',
  operationId: '',
  causationId: '',
  correlationId: '',
  traceId: '',
  aggregateType: '',
  aggregateId: '',
  streamId: '',
  payloadField: '',
  payloadDigest: '',
}

export function buildEventQuery(
  range: { start: Date | null; end: Date | null },
  filters: EventFilterDraft,
  pageSize = 100,
): EventQuery | null {
  if (!range.start || !range.end || range.start >= range.end) return null
  return {
    from: range.start.toISOString(),
    to: range.end.toISOString(),
    event_type: filters.eventType.trim(),
    source_system: filters.sourceSystem.trim(),
    outcome: filters.outcome.trim(),
    actor_id: filters.actorId.trim(),
    target_id: filters.targetId.trim(),
    operation_id: filters.operationId.trim(),
    causation_id: filters.causationId.trim(),
    correlation_id: filters.correlationId.trim(),
    trace_id: filters.traceId.trim(),
    aggregate_type: filters.aggregateType.trim(),
    aggregate_id: filters.aggregateId.trim(),
    stream_id: filters.streamId.trim(),
    payload_field: filters.payloadField.trim(),
    payload_digest: filters.payloadDigest.trim(),
    page_size: pageSize,
  }
}
