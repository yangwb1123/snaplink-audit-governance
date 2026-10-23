import { describe, expect, it } from 'vitest'
import { evidenceHash, evidenceLocation } from './evidenceNavigation'
import { investigationHash, investigationLocation } from './investigationNavigation'
import { eventHash, eventLocation, navigationHash, parseNavigationHash, quickJumpHash, safeNavigationHash, type NavigationTarget } from './navigation'

const targets: NavigationTarget[] = [
  { page: 'overview' }, { page: 'catalog' }, { page: 'events' }, { page: 'admin-actions' },
  { page: 'events', eventId: 'event-0123' },
  { page: 'events', eventId: '事件:7' },
  { page: 'operations', operationId: 'order:7' },
  { page: 'operations', aggregateType: 'audit.v1:order', aggregateId: 'order-7' },
  { page: 'evidence', tab: 'exports', id: 'export-0123456789abcdef' },
  { page: 'evidence', tab: 'restores', id: 'restore-0123456789abcdef' },
  { page: 'evidence', tab: 'holds' }, { page: 'evidence', tab: 'integrity' },
]

describe('one navigation allowlist', () => {
  it.each(targets)('builds, parses and restores the same target: %j', (target) => {
    const hash = navigationHash(target)
    expect(parseNavigationHash(hash)).toEqual(target)
    expect(safeNavigationHash(hash)).toBe(hash)
    expect(safeNavigationHash(hash.replace('#', '#/'))).toBe(hash)
  })

  it('uses explicit event, operation and export types, never guesses from a list or ID prefix', () => {
    expect(quickJumpHash('event', 'op-1')).toBe('#events/event/op-1')
    expect(quickJumpHash('operation', 'op-1')).toBe(investigationHash({ mode: 'operation', operationId: 'op-1' }))
    expect(quickJumpHash('export', 'export-0123456789abcdef')).toBe(evidenceHash('exports', 'export-0123456789abcdef'))
    expect(quickJumpHash('export', 'op-1')).toBeNull()
  })

  const unsafeValues = [
    '', ' ', 'a b', 'a/b', 'a\\b', 'x?tenant_id=other', 'x#token', 'x&token', 'x=y',
    'x\u0000y', 'x\u001fy', 'x\u007fy', 'x\u0085y', 'x\u200by', 'x\u202ey',
    '%', '%2f', '%252f', 'https://evil.test', 'https:evil', 'javascript:alert(1)', 'data:text',
    '\uD800', '\uDC00', '.', '..', 'x'.repeat(257),
  ]
  it.each(unsafeValues)('rejects unsafe raw values in every builder: %j', (value) => {
    expect(eventHash(value)).toBe('#events')
    expect(investigationHash({ mode: 'operation', operationId: value })).toBe('#operations')
    expect(evidenceHash('exports', value)).toBe('#evidence/exports')
    expect(quickJumpHash('event', value)).toBeNull()
  })

  const unsafeSegments = [
    ...unsafeValues.map(value => { try { return encodeURIComponent(value) } catch { return value } }),
    '%', '%GG', '%C0%AF', '%ED%A0%80', '%FF', '%252F', 'x/y', 'x?tenant_id=other',
  ]
  it.each(unsafeSegments)('rejects unsafe segments in live parsing and login restoration: %j', (segment) => {
    for (const hash of [`#events/event/${segment}`, `#operations/op/${segment}`, `#evidence/exports/${segment}`]) {
      expect(parseNavigationHash(hash)).toBeNull()
      expect(safeNavigationHash(hash)).toBe('#overview')
      expect(eventLocation(hash)).toBe('')
      expect(investigationLocation(hash)).toBeNull()
      expect(evidenceLocation(hash).id).toBe('')
    }
  })

  it.each([
    '#events/event/id/extra', '#events/event/id?tenant_id=other', '#events/event/id#tail',
    '#evidence/exports/export-0123456789abcdef/extra', '#evidence/holds/export-0123456789abcdef',
    '#evidence/exports/export-0123456789abcdef?access_token=secret', '#events?payload_digest=secret',
    '#events/event/' + 'a'.repeat(5000), 'https://evil.test/#events/event/id',
    '#admin-actions/extra', '#admin-actions?actor=secret', '#admin-actions#secret',
    '#admin-actions?tenant_id=other', '#admin-actions%2Fextra', '#admin%2Dactions',
  ])('rejects extra route components and URL/query injection: %s', (hash) => {
    expect(parseNavigationHash(hash)).toBeNull()
    expect(safeNavigationHash(hash)).toBe('#overview')
  })
})
