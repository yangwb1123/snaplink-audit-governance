import { describe, expect, it } from 'vitest'
import { LatestLookup } from './lookupState'

describe('latest lookup guard', () => {
  it('ignores stale requests and requests for edited query keys', () => {
    const lookup = new LatestLookup()
    const first = lookup.begin('tenant-a:op-1')
    const second = lookup.begin('tenant-a:op-2')

    expect(lookup.isCurrent(first, first.key)).toBe(false)
    expect(lookup.isCurrent(second, 'tenant-a:op-1')).toBe(false)
    expect(lookup.isCurrent(second, second.key)).toBe(true)
  })

  it('invalidates in-flight work when the user edits a lookup', () => {
    const lookup = new LatestLookup()
    const request = lookup.begin('tenant-a:aggregate-1')
    lookup.invalidate()

    expect(lookup.isCurrent(request, request.key)).toBe(false)
  })
})
