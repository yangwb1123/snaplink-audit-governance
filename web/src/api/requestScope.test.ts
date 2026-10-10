import { describe, expect, it } from 'vitest'
import { RequestScope } from './requestScope'

describe('request scope', () => {
  it('rejects late responses even if a fetcher ignores cancellation and A returns after A -> B -> A', () => {
    const scope = new RequestScope()
    const firstA = scope.capture()
    scope.invalidate()
    const b = scope.capture()
    scope.invalidate()
    const secondA = scope.capture()
    expect(firstA.current()).toBe(false)
    expect(firstA.signal.aborted).toBe(true)
    expect(b.current()).toBe(false)
    expect(secondA.current()).toBe(true)
  })

  it('isolates superseded selections and disposes requests on unmount', () => {
    const scope = new RequestScope()
    const old = scope.next()
    const current = scope.next()
    expect(old.current()).toBe(false)
    expect(current.current()).toBe(true)
    scope.dispose()
    expect(current.current()).toBe(false)
    expect(current.signal.aborted).toBe(true)
    const after = scope.capture()
    expect(after.current()).toBe(false)
    expect(after.signal.aborted).toBe(true)
  })
})
