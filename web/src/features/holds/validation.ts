import type { EventQuery, LegalHold } from '../../api/types'

export function record(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
function date(value: unknown): value is string {
  return typeof value === 'string' && Number.isFinite(Date.parse(value))
}
const queryStrings = ['event_type', 'source_system', 'actor_id', 'target_id', 'aggregate_type', 'aggregate_id',
  'operation_id', 'causation_id', 'correlation_id', 'trace_id', 'outcome', 'payload_field', 'payload_digest', 'stream_id', 'cursor'] as const

export function validHold(value: unknown, tenant: string): value is LegalHold {
  if (!record(value) || typeof value.id !== 'string' || !value.id || value.tenant_id !== tenant ||
      typeof value.name !== 'string' || typeof value.reason !== 'string' || typeof value.created_by !== 'string' ||
      !date(value.created_at) || !record(value.filter) || !date(value.filter.from) || !date(value.filter.to)) return false
  const filter = value.filter
  return queryStrings.every((key) => filter[key] === undefined || typeof filter[key] === 'string') &&
    (filter.page_size === undefined || (Number.isInteger(filter.page_size) && Number(filter.page_size) >= 0)) &&
    (value.released_at === undefined || date(value.released_at)) &&
    (value.released_by === undefined || typeof value.released_by === 'string')
}
export function sameQuery(left: EventQuery, right: EventQuery): boolean {
  return Date.parse(left.from) === Date.parse(right.from) && Date.parse(left.to) === Date.parse(right.to) &&
    queryStrings.every((key) => (left[key] ?? '') === (right[key] ?? '')) && (left.page_size ?? 0) === (right.page_size ?? 0)
}
