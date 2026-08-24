import { describe, expect, it } from 'vitest'
import { failedFacetCount, topFacetEntries } from './auditFacets'

describe('audit facet presentation', () => {
  it('orders positive finite counts and applies the display limit', () => {
    expect(
      topFacetEntries({ vault: 4, snaplink: 9, empty: 0, invalid: Number.NaN, identity: 4 }, 3),
    ).toEqual([
      { label: 'snaplink', count: 9 },
      { label: 'identity', count: 4 },
      { label: 'vault', count: 4 },
    ])
  })

  it('combines failure outcome spellings without treating success as failure', () => {
    expect(
      failedFacetCount({ success: 20, failed: 2, FAILURE: 3, error: 1, denied: 4 }),
    ).toBe(10)
  })
})
