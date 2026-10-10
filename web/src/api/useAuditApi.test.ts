// @vitest-environment happy-dom
import { get } from 'svelte/store'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { authState } from '../auth/store'
import { useAuditApi } from './useAuditApi'

function session(token: string) {
  return { accessToken: token, idToken: 'id', expiresAt: Date.now() + 100000, subject: 'actor', displayName: 'actor', tenantId: 'tenant-a', roles: [], permissions: new Set(['audit:event:read']), platform: false }
}

afterEach(() => {
  vi.unstubAllGlobals()
  authState.set({ status: 'anonymous', session: null, error: null })
})

describe('late authentication errors', () => {
  it('never lets an old token invalidate a newer login with the same subject and tenant', async () => {
    let resolve!: (response: Response) => void
    vi.stubGlobal('fetch', () => new Promise<Response>((complete) => (resolve = complete)))
    authState.set({ status: 'authenticated', session: session('old'), error: null })
    const api = useAuditApi()
    const request = api.getEvent('event', 'tenant-a')
    authState.set({ status: 'authenticated', session: session('new'), error: null })
    resolve(new Response('{"error":{"code":"unauthorized"}}', { status: 401 }))
    await expect(request).rejects.toMatchObject({ status: 401 })
    expect(get(authState).session?.accessToken).toBe('new')
  })

  it('still invalidates the token that received the denial', async () => {
    vi.stubGlobal('fetch', async () => new Response('{"error":{"code":"unauthorized"}}', { status: 401 }))
    authState.set({ status: 'authenticated', session: session('old'), error: null })
    await expect(useAuditApi().getEvent('event', 'tenant-a')).rejects.toMatchObject({ status: 401 })
    expect(get(authState).status).toBe('anonymous')
  })
})
