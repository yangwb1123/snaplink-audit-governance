// @vitest-environment happy-dom
import { afterEach, describe, expect, it } from 'vitest'
import { get } from 'svelte/store'
import { authState } from '../../auth/store'
import { HoldCreateOwner, holdCreateSession } from './createOwner.svelte'

function login(): void {
  authState.set({ status: 'authenticated', error: null, session: {
    subject: 'actor', displayName: 'Actor', tenantId: 'tenant-a', platform: true,
    accessToken: 'token-a', idToken: 'id-a', roles: [], permissions: new Set(['audit:legal_hold:manage']), expiresAt: Date.now() + 100000,
  } })
}
afterEach(() => authState.set({ status: 'anonymous', error: null, session: null }))

describe('minimal Hold creation owner', () => {
  it('registers synchronously, isolates tenants and never reuses an attempt', () => {
    const owner = new HoldCreateOwner()
    const a = owner.begin('tenant-a')!
    expect(owner.begin('tenant-a')).toBeUndefined()
    expect(owner.forTenant('tenant-a')).toEqual({ attempt: a, phase: 'sending' })
    const b = owner.begin('tenant-b')!
    expect(b).not.toBe(a)
    owner.uncertain('tenant-a', a)
    expect(owner.begin('tenant-a')).toBeUndefined()
    expect(owner.clear('tenant-a', b)).toBe(false)
    expect(owner.clear('tenant-a', a)).toBe(true)
    const next = owner.begin('tenant-a')!
    expect(next).not.toBe(a)
    owner.uncertain('tenant-a', a)
    expect(owner.clear('tenant-a', a)).toBe(false)
    expect(owner.forTenant('tenant-a')).toEqual({ attempt: next, phase: 'sending' })
    expect(owner.forTenant('tenant-b')).toEqual({ attempt: b, phase: 'sending' })
    owner.retire()
    expect(owner.active).toBe(false)
    expect(owner.begin('tenant-a')).toBeUndefined()
    expect(owner.clear('tenant-b', b)).toBe(false)
  })

  it.each(['subject', 'accessToken', 'idToken', 'permissions', 'status', 'tenantId', 'platform'] as const)(
    'auth %s A -> B -> A retires synchronously, not only after a render effect', (change) => {
      login()
      const session = holdCreateSession()
      try {
        const original = get(authState)
        const owner = session.owner
        const attempt = owner.begin('tenant-a')!
        if (change === 'status') authState.set({ ...original, status: 'checking' })
        else authState.set({ ...original, session: { ...original.session!,
          ...(change === 'permissions' ? { permissions: new Set<string>() } : change === 'platform' ? { platform: false } : { [change]: 'changed' }),
        } })
        expect(owner.active).toBe(false)
        const intermediate = session.owner
        authState.set(original)
        expect(intermediate.active).toBe(false)
        expect(session.owner).not.toBe(owner)
        expect(session.owner.forTenant('tenant-a')).toBeUndefined()
        expect(owner.clear('tenant-a', attempt)).toBe(false)
      } finally { session.dispose() }
    },
  )

  it('non-authorizing decoration updates do not silently discard same-session constraints', () => {
    login()
    const session = holdCreateSession()
    try {
      const owner = session.owner
      const attempt = owner.begin('tenant-a')!
      owner.uncertain('tenant-a', attempt)
      authState.update((state) => ({ ...state, error: 'decoration', session: { ...state.session!, displayName: 'New label', permissions: new Set(state.session!.permissions) } }))
      expect(session.owner).toBe(owner)
      expect(session.owner.forTenant('tenant-a')).toEqual({ attempt, phase: 'uncertain' })
    } finally { session.dispose() }
  })
})
