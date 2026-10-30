import type { IntegrityResult } from '../../api/types'

// RFC3339 date-time used by the OpenAPI contract. Fractional seconds are
// intentionally unbounded and offsets are accepted; the contract does not
// declare an era window, a zero-time rule or a precision ceiling.
const dateTime = /^(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:[Zz]|([+-])(\d{2}):(\d{2}))$/

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function nonNegativeSafeInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0
}

function isDateTime(value: unknown): value is string {
  if (typeof value !== 'string') return false
  const match = dateTime.exec(value)
  if (!match) return false
  const year = Number(match[1])
  const month = Number(match[2])
  const day = Number(match[3])
  const hour = Number(match[4])
  const minute = Number(match[5])
  // RFC3339 permits a leap second. Go's encoder does not emit one, but the
  // shared OpenAPI date-time contract does not declare it invalid.
  const second = Number(match[6])
  if (month < 1 || month > 12 || hour > 23 || minute > 59 || second > 60) return false
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0)
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1]
  if (day < 1 || day > days) return false
  if (match[8] !== undefined && (Number(match[8]) > 23 || Number(match[9]) > 59)) return false
  if (second === 60) {
    // RFC3339's leap-second clock is UTC 23:59, not any local minute.
    // This checks the clock/offset grammar, not the IERS announcement schedule.
    const offset = (match[7] === '-' ? -1 : 1) * (Number(match[8] ?? 0) * 60 + Number(match[9] ?? 0))
    const utcMinute = ((hour * 60 + minute - offset) % 1440 + 1440) % 1440
    if (utcMinute !== 1439) return false
  }
  return true
}

/** Validate a runtime integrity result against the request that produced it.
 * Required fields are never defaulted; a stream-scoped request requires an
 * exact returned stream and a whole-tenant request accepts only an omitted or
 * empty stream. Diagnostics are preserved verbatim, including duplicates. */
export function isIntegrityResult(
  value: unknown,
  tenantId: string | undefined,
  streamId: string,
): value is IntegrityResult {
  if (!record(value)) return false
  if (typeof tenantId !== 'string' || value.tenant_id !== tenantId) return false
  if (typeof value.valid !== 'boolean') return false
  if (!nonNegativeSafeInteger(value.event_count) || !nonNegativeSafeInteger(value.segment_count)) return false
  if (!isDateTime(value.checked_at)) return false
  if (value.errors !== undefined &&
      (!Array.isArray(value.errors) || !value.errors.every((item) => typeof item === 'string'))) return false
  if (streamId !== '') return value.stream_id === streamId
  return value.stream_id === undefined || value.stream_id === ''
}
