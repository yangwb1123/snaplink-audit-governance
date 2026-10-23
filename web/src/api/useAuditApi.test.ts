// @vitest-environment happy-dom
import { get } from 'svelte/store'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { authState, type AuthState } from '../auth/store'
import type { UserSession } from '../auth/types'
import { ApiError } from './client'
import { useAuditApi } from './useAuditApi'

function session(token: string) {
  return { accessToken: token, idToken: 'id', expiresAt: Date.now() + 100000, subject: 'actor', displayName: 'actor', tenantId: 'tenant-a', roles: [], permissions: new Set(['audit:event:read']), platform: false }
}

afterEach(() => {
  vi.unstubAllGlobals()
  authState.set({ status: 'anonymous', session: null, error: null })
})

type ExportRead = 'JSON detail' | 'blob download'
const exportReads: ExportRead[] = ['JSON detail', 'blob download']

function exportRequest(api: ReturnType<typeof useAuditApi>, path: ExportRead, signal: AbortSignal) {
  return path === 'JSON detail'
    ? api.getExport('export-a', 'tenant-a', signal)
    : api.downloadExport('export-a', 'tenant-a', signal)
}

function controlledFetch() {
  let resolve!: (response: Response) => void
  let reject!: (cause: unknown) => void
  // Deliberately ignore AbortSignal: aborting the page is not a side-effect fence.
  const fetcher = vi.fn<typeof fetch>(() => new Promise<Response>((done, fail) => { resolve = done; reject = fail }))
  vi.stubGlobal('fetch', fetcher)
  return { fetcher, resolve: (response: Response) => resolve(response), reject: (cause: unknown) => reject(cause) }
}

function authenticated(): AuthState {
  return { status: 'authenticated', session: session('reused'), error: null }
}

function denial(status = 401): Response {
  return Response.json({ error: { code: status === 401 ? 'unauthorized' : 'forbidden', request_id: 'fixture-request' } }, { status })
}

const sessionChanges: Array<[string, Partial<UserSession>]> = [
  ['access token', { accessToken: 'different' }],
  ['ID token', { idToken: 'different' }],
  ['subject', { subject: 'other-actor' }],
  ['tenant', { tenantId: 'tenant-b' }],
  ['permissions', { permissions: new Set(['audit:export:create']) }],
  ['platform', { platform: true }],
  ['roles', { roles: ['different-role'] }],
  ['expiry', { expiresAt: 1 }],
]

async function expectUnauthorized(request: Promise<unknown>): Promise<void> {
  const failure: unknown = await request.catch((cause: unknown) => cause)
  expect(failure instanceof ApiError).toBe(true)
  if (failure instanceof ApiError) {
    expect(failure.status).toBe(401)
    expect(failure.code).toBe('unauthorized')
    expect(failure.requestId).toBe('fixture-request')
  }
}

describe.each(exportReads)('%s authentication generation fence through the real adapter', (path) => {
  for (const restoration of ['no restoration', 'equal values', 'original objects'] as const) {
    it.each(sessionChanges)(`retires %s changes (${restoration}) before an ignored-abort 401 side effect`, async (_name, change) => {
      const transport = controlledFetch()
      const original = authenticated()
      authState.set(original)
      const api = useAuditApi()
      const controller = new AbortController()
      const request = exportRequest(api, path, controller.signal)
      const changed: AuthState = { ...original, session: { ...original.session!, ...change } }
      authState.set(changed)
      if (restoration === 'equal values') authState.set({ ...original, session: { ...original.session! } })
      else if (restoration === 'original objects') authState.set(original)
      // No tick/await between A -> B -> A, including reuse of the entire state/session.
      const current = get(authState)
      controller.abort()
      transport.resolve(denial())
      await expectUnauthorized(request)
      expect(get(authState).status).toBe('authenticated')
      expect(get(authState) === current).toBe(true)
      expect(transport.fetcher).toHaveBeenCalledTimes(1)
    })
  }

  for (const status of ['checking', 'anonymous', 'error'] as const) {
    it.each([false, true])(`retires the authenticated generation through ${status} (restore=%s)`, async (restore) => {
      const transport = controlledFetch()
      const original = authenticated()
      authState.set(original)
      const request = exportRequest(useAuditApi(), path, new AbortController().signal)
      authState.set({ ...original, status })
      if (restore) authState.set(original)
      const current = get(authState)
      transport.resolve(denial())
      await expectUnauthorized(request)
      expect(get(authState).status).toBe(restore ? 'authenticated' : status)
      expect(get(authState) === current).toBe(true)
    })
  }

  it.each(['access token', 'subject', 'tenant', 'permissions'] as const)('observes in-place %s ABA notifications on the original session object', async (field) => {
    const transport = controlledFetch()
    const original = authenticated()
    authState.set(original)
    const currentSession = original.session!
    const request = exportRequest(useAuditApi(), path, new AbortController().signal)
    const saved = { ...currentSession }
    if (field === 'access token') currentSession.accessToken = 'different'
    else if (field === 'subject') currentSession.subject = 'other-actor'
    else if (field === 'tenant') currentSession.tenantId = 'tenant-b'
    else currentSession.permissions.add('audit:export:create')
    authState.set(original)
    if (field === 'permissions') currentSession.permissions.delete('audit:export:create')
    else Object.assign(currentSession, saved)
    authState.set(original)
    transport.resolve(denial())
    await expectUnauthorized(request)
    expect(get(authState).status).toBe('authenticated')
    expect(get(authState) === original).toBe(true)
  })

  it('retires a replaced session even if all authentication values are equal', async () => {
    const transport = controlledFetch()
    const original = authenticated()
    authState.set(original)
    const request = exportRequest(useAuditApi(), path, new AbortController().signal)
    authState.set({ ...original, session: { ...original.session! } })
    const current = get(authState)
    transport.resolve(denial())
    await expectUnauthorized(request)
    expect(get(authState).status).toBe('authenticated')
    expect(get(authState) === current).toBe(true)
  })

  it('keeps a retired adapter retired, but a fresh adapter with the reused token still invalidates', async () => {
    const transport = controlledFetch()
    const original = authenticated()
    authState.set(original)
    const oldApi = useAuditApi()
    authState.set({ ...original, status: 'anonymous', session: null })
    authState.set(original)
    const stale = exportRequest(oldApi, path, new AbortController().signal)
    transport.resolve(denial())
    await expectUnauthorized(stale)
    expect(get(authState).status).toBe('authenticated')
    const current = exportRequest(useAuditApi(), path, new AbortController().signal)
    transport.resolve(denial())
    await expectUnauthorized(current)
    expect(get(authState).status).toBe('anonymous')
    expect(get(authState).session === null).toBe(true)
    expect(transport.fetcher).toHaveBeenCalledTimes(2)
  })

  it('still invalidates a current generation on 401 without replacing the ApiError rejection', async () => {
    const transport = controlledFetch()
    authState.set(authenticated())
    const request = exportRequest(useAuditApi(), path, new AbortController().signal)
    transport.resolve(denial())
    await expectUnauthorized(request)
    expect(get(authState).status).toBe('anonymous')
    expect(get(authState).session === null).toBe(true)
    expect(transport.fetcher).toHaveBeenCalledTimes(1)
  })

  it('does not retire a generation for an error-only update on the same session', async () => {
    const transport = controlledFetch()
    const original = authenticated()
    authState.set(original)
    const request = exportRequest(useAuditApi(), path, new AbortController().signal)
    authState.set({ ...original, error: 'fixture notice' })
    transport.resolve(denial())
    await expectUnauthorized(request)
    expect(get(authState).status).toBe('anonymous')
  })

  it.each(['403', 'network'] as const)('never invalidates or retries on %s', async (failure) => {
    const transport = controlledFetch()
    const original = authenticated()
    authState.set(original)
    const request = exportRequest(useAuditApi(), path, new AbortController().signal)
    const network = new TypeError('fixture network failure')
    if (failure === '403') transport.resolve(denial(403))
    else transport.reject(network)
    const rejection: unknown = await request.catch((cause: unknown) => cause)
    expect(failure === '403' ? rejection instanceof ApiError && rejection.status === 403 : rejection === network).toBe(true)
    expect(get(authState) === original).toBe(true)
    expect(transport.fetcher).toHaveBeenCalledTimes(1)
  })
})

it('does not add permanent auth subscriptions for each adapter/request', async () => {
  const transport = controlledFetch()
  authState.set(authenticated())
  const originalSubscribe = authState.subscribe
  let active = 0
  const subscription = vi.spyOn(authState, 'subscribe').mockImplementation((...args) => {
    active += 1
    const stop = originalSubscribe(...args)
    return () => { active -= 1; stop() }
  })
  try {
    useAuditApi()
    const initial = active
    for (let index = 0; index < 8; index += 1) {
      const request = useAuditApi().getExport('export-a', 'tenant-a')
      transport.resolve(denial(403))
      const failure: unknown = await request.catch((cause: unknown) => cause)
      expect(failure instanceof ApiError && failure.status === 403).toBe(true)
      expect(active).toBe(initial)
    }
    expect(initial <= 1).toBe(true)
  } finally { subscription.mockRestore() }
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
