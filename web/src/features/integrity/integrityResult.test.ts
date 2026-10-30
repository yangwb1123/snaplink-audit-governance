import { describe, expect, it } from 'vitest'
import { isIntegrityResult } from './integrityResult'

const base: Record<string, unknown> = {
  tenant_id: 'tenant-a',
  valid: true,
  event_count: 12,
  segment_count: 2,
  checked_at: '2026-10-25T00:00:00Z',
}

function candidate(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return { ...base, ...overrides }
}

function valid(overrides: Record<string, unknown> = {}, tenant = 'tenant-a', stream = ''): boolean {
  return isIntegrityResult(candidate(overrides), tenant, stream)
}

function without(key: string): Record<string, unknown> {
  const copy = candidate()
  delete copy[key]
  return copy
}

describe('integrity result runtime validation', () => {
  it.each([
    ['an omitted stream for the whole tenant', {}],
    ['an empty stream for the whole tenant', { stream_id: '' }],
    ['a completed finding without diagnostics', { valid: false, event_count: 0, segment_count: 0 }],
    ['the maximum safe counts', { event_count: Number.MAX_SAFE_INTEGER, segment_count: Number.MAX_SAFE_INTEGER }],
  ])('accepts %s', (_label, overrides) => {
    expect(valid(overrides)).toBe(true)
  })

  it('requires an exact stream for a non-empty request', () => {
    expect(valid({ stream_id: 'stream-a' }, 'tenant-a', 'stream-a')).toBe(true)
    expect(valid({}, 'tenant-a', 'stream-a')).toBe(false)
    expect(valid({ stream_id: '' }, 'tenant-a', 'stream-a')).toBe(false)
    expect(valid({ stream_id: 'stream-b' }, 'tenant-a', 'stream-a')).toBe(false)
    expect(valid({ stream_id: 'stream-a ' }, 'tenant-a', 'stream-a')).toBe(false)
    expect(valid({ stream_id: null }, 'tenant-a', 'stream-a')).toBe(false)
  })

  it('accepts only an omitted or empty stream for a whole-tenant request', () => {
    expect(valid({}, 'tenant-a', '')).toBe(true)
    expect(valid({ stream_id: '' }, 'tenant-a', '')).toBe(true)
    expect(valid({ stream_id: 'stream-a' }, 'tenant-a', '')).toBe(false)
    expect(valid({ stream_id: null }, 'tenant-a', '')).toBe(false)
    expect(valid({ stream_id: 0 }, 'tenant-a', '')).toBe(false)
  })

  it.each([null, undefined, [], [base], 'result', 42, true])('rejects a non-object or array input: %s', (value) => {
    expect(isIntegrityResult(value, 'tenant-a', '')).toBe(false)
  })

  it.each(['tenant_id', 'valid', 'event_count', 'segment_count', 'checked_at'])(
    'rejects a result without required %s',
    (key) => {
      expect(isIntegrityResult(without(key), 'tenant-a', '')).toBe(false)
    },
  )

  it('requires the exact request tenant', () => {
    expect(valid()).toBe(true)
    expect(valid({ tenant_id: 'tenant-b' }, 'tenant-a', '')).toBe(false)
    expect(valid({}, 'tenant-b', '')).toBe(false)
    expect(isIntegrityResult(candidate(), undefined, '')).toBe(false)
    expect(isIntegrityResult(candidate({ tenant_id: null }), 'tenant-a', '')).toBe(false)
  })

  it.each([true, false])('accepts boolean valid=%s', (value) => {
    expect(valid({ valid: value })).toBe(true)
  })

  it.each(['false', 'true', 0, 1, null, undefined, {}, []])('rejects non-boolean valid=%s', (value) => {
    expect(valid({ valid: value })).toBe(false)
  })

  it.each([0, -0, 1, Number.MAX_SAFE_INTEGER - 1, Number.MAX_SAFE_INTEGER])(
    'accepts a non-negative safe count of %s',
    (value) => {
      expect(valid({ event_count: value })).toBe(true)
      expect(valid({ segment_count: value })).toBe(true)
    },
  )

  it.each([-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY,
    Number.MAX_SAFE_INTEGER + 1, '12', null, true])('rejects an unsafe or negative count of %s', (value) => {
    expect(valid({ event_count: value })).toBe(false)
    expect(valid({ segment_count: value })).toBe(false)
  })

  it.each([
    '2026-10-25T00:00:00Z',
    '2026-10-25T00:00:00.1Z',
    '2026-10-25T00:00:00.123456789Z',
    '2026-10-25T12:34:56.123456789+08:00',
    '2026-10-25T12:34:56-12:00',
    '2026-10-25T12:34:56+14:00',
    '2026-10-25t12:34:56z',
    '0000-01-01T00:00:00Z',
    '0001-01-01T00:00:00Z',
    '2024-02-29T23:59:59.999999999+05:30',
    '9999-12-31T23:59:59Z',
    '2026-12-31T23:59:60Z',
  ])('accepts RFC3339 date-time %s', (checked_at) => {
    expect(valid({ checked_at })).toBe(true)
  })

  it.each([
    '',
    '2026-10-25',
    '2026-10-25 00:00:00Z',
    '2026-10-25T00:00:00',
    '2026-10-25T00:00:00.Z',
    '2026-10-25T00:00:00,5Z',
    '2026-10-25T00:00:00Zjunk',
    '2026-13-01T00:00:00Z',
    '2026-00-01T00:00:00Z',
    '2026-02-30T00:00:00Z',
    '2026-02-29T00:00:00Z',
    '2026-04-31T00:00:00Z',
    '2026-10-25T24:00:00Z',
    '2026-10-25T00:60:00Z',
    '2026-10-25T00:00:61Z',
    '2026-10-25T00:00:00+24:00',
    '2026-10-25T00:00:00+08:60',
    0,
    null,
  ])('rejects invalid date-time %s', (checked_at) => {
    expect(valid({ checked_at })).toBe(false)
  })

  it.each([
    '2016-12-31T23:59:60Z',
    '2016-12-31T15:59:60.123456789-08:00',
    '2017-01-01T00:59:60+01:00',
  ])('accepts a leap-second clock at the offset-adjusted UTC minute boundary: %s', (checked_at) => {
    expect(valid({ checked_at })).toBe(true)
  })

  it.each([
    '2026-10-25T00:00:60Z',
    '2026-12-31T23:58:60Z',
    '2017-01-01T00:59:60+02:00',
  ])('rejects a leap-second clock outside UTC 23:59: %s', (checked_at) => {
    expect(valid({ checked_at })).toBe(false)
  })

  it('preserves duplicate and long diagnostics without deduplicating or capping them', () => {
    const errors = ['same diagnostic', 'same diagnostic', '<b>not markup</b>', 'x'.repeat(4096)]
    const result = candidate({ valid: false, errors })
    expect(isIntegrityResult(result, 'tenant-a', '')).toBe(true)
    expect(result.errors).toBe(errors)
    expect(result.errors).toEqual(['same diagnostic', 'same diagnostic', '<b>not markup</b>', 'x'.repeat(4096)])
  })

  it.each([null, 'error', {}, [1], ['ok', null]])('rejects malformed errors=%s', (errors) => {
    expect(valid({ errors })).toBe(false)
  })

  it('accepts an empty or omitted optional errors field', () => {
    expect(valid({ errors: [] })).toBe(true)
    expect(valid()).toBe(true)
  })
})
