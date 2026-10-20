import { describe, expect, it } from 'vitest'
import { investigationHash, investigationLocation } from './investigationNavigation'

describe('investigation cross-links', () => {
  it('parses bounded operation and aggregate targets', () => {
    expect(investigationLocation('#operations/op/op-0123')).toEqual({ mode: 'operation', operationId: 'op-0123' })
    expect(investigationLocation('#/operations/aggregate/file/file-9')).toEqual({
      mode: 'aggregate',
      aggregateType: 'file',
      aggregateId: 'file-9',
    })
    expect(investigationLocation('#operations/aggregate/audit.v1:order/order%3A7')).toEqual({
      mode: 'aggregate',
      aggregateType: 'audit.v1:order',
      aggregateId: 'order:7',
    })
  })

  it('builds shareable links and round-trips them', () => {
    const operation = { mode: 'operation' as const, operationId: 'op-0123' }
    const aggregate = { mode: 'aggregate' as const, aggregateType: 'file', aggregateId: 'file-9' }
    expect(investigationHash(operation)).toBe('#operations/op/op-0123')
    expect(investigationHash(aggregate)).toBe('#operations/aggregate/file/file-9')
    expect(investigationLocation(investigationHash(operation))).toEqual(operation)
    expect(investigationLocation(investigationHash(aggregate))).toEqual(aggregate)
  })

  it('rejects untrusted or malformed links instead of embedding them', () => {
    for (const hash of [
      '#operations',
      '#operations/op/',
      '#operations/op/bad%2Fid',
      '#operations/op/a b',
      '#operations/op/x%00y',
      '#operations/op/%',
      '#operations/unknown/x',
      '#operations/aggregate/file',
      '#operations/aggregate/bad%20type/id',
      '#operations/aggregate/file/id%0A',
      '#events/op/op-1',
      `#operations/op/${'x'.repeat(300)}`,
      '#operations/op/%ED%A0%80',
    ]) {
      expect(investigationLocation(hash)).toBeNull()
    }
    expect(investigationHash({ mode: 'operation', operationId: 'a/b' })).toBe('#operations')
    expect(investigationHash({ mode: 'aggregate', aggregateType: 'bad type', aggregateId: 'id' })).toBe('#operations')
    expect(investigationHash({ mode: 'aggregate', aggregateType: 'file', aggregateId: 'id?x=1' })).toBe('#operations')
    expect(investigationHash({ mode: 'operation', operationId: '\uD800' })).toBe('#operations')
  })
})
