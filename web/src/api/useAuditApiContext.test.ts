// @vitest-environment happy-dom
import { get } from 'svelte/store'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { authState, type AuthState } from '../auth/store'
import type { UserSession } from '../auth/types'
import { ApiError } from './client'
import { useAuditApi } from './useAuditApi'

interface FetchJob {
  url: string
  init: RequestInit
  resolve: (response: Response) => void
  reject: (cause: unknown) => void
}

function session(token = 'current-token', tenantId = 'tenant-a'): UserSession {
  return {
    accessToken: token,
    idToken: 'id-token',
    expiresAt: Date.now() + 100_000,
    subject: 'actor',
    displayName: 'actor',
    tenantId,
    roles: [],
    permissions: new Set(['audit:event:read']),
    platform: false,
  }
}

function authenticated(current = session()): AuthState {
  return { status: 'authenticated', session: current, error: null }
}

function controlledFetch() {
  const jobs: FetchJob[] = []
  const fetcher = vi.fn<typeof fetch>((input, init = {}) => new Promise<Response>((resolve, reject) => {
    const normalized = init ?? {}
    jobs.push({ url: String(input), init: normalized, resolve, reject })
  }))
  vi.stubGlobal('fetch', fetcher)
  return { fetcher, jobs }
}

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

afterEach(() => {
  vi.unstubAllGlobals()
  authState.set({ status: 'anonymous', session: null, error: null })
})

describe('useAuditApi request context through the real adapter', () => {
  const operationReads = [
    {
      name: 'summary',
      path: '/api/v1/operations/op-a?tenant_id=tenant-a',
      read: (api: ReturnType<typeof useAuditApi>) => api.getOperation('op-a', 'tenant-a'),
      body: { operation_id: 'op-a', tenant_id: 'tenant-a', event_count: 1 },
    },
    {
      name: 'timeline',
      path: '/api/v1/operations/op-a/timeline?page_size=200&cursor=cursor-a&tenant_id=tenant-a',
      read: (api: ReturnType<typeof useAuditApi>) =>
        api.getOperationTimeline('op-a', { pageSize: 200, cursor: 'cursor-a', tenantId: 'tenant-a' }),
      body: { items: [], count: 0 },
    },
    {
      name: 'replay',
      path: '/api/v1/operations/op-a/replay?tenant_id=tenant-a',
      read: (api: ReturnType<typeof useAuditApi>) => api.replayOperation('op-a', 'tenant-a'),
      body: { operation_id: 'op-a', tenant_id: 'tenant-a', event_count: 1, state: {}, last_sequence: 1 },
    },
  ] as const

  for (const read of operationReads) {
    it(`passes the context signal to the ${read.name} JSON fetch`, async () => {
      authState.set(authenticated())
      const transport = controlledFetch()
      const controller = new AbortController()
      const api = useAuditApi({ signal: controller.signal })

      const pending = read.read(api)

      expect(transport.jobs).toHaveLength(1)
      expect(transport.jobs[0].url).toBe(`/audit-api${read.path}`)
      expect(transport.jobs[0].init.signal).toBe(controller.signal)
      transport.jobs[0].resolve(json(read.body))
      await expect(pending).resolves.toEqual(read.body)
    })
  }

  it('passes the context signal to a blob download fetch', async () => {
    authState.set(authenticated())
    const transport = controlledFetch()
    const controller = new AbortController()
    const api = useAuditApi({ signal: controller.signal })

    const pending = api.downloadExport('export-a', 'tenant-a')

    expect(transport.jobs[0].url).toBe('/audit-api/api/v1/exports/export-a/download?tenant_id=tenant-a')
    expect(transport.jobs[0].init.signal).toBe(controller.signal)
    transport.jobs[0].resolve(new Response(new Blob(['fixture'])))
    await expect(pending).resolves.toBeInstanceOf(Blob)
  })

  it('lets an explicit method signal override the context signal and falls back when undefined', async () => {
    authState.set(authenticated())
    const transport = controlledFetch()
    const fallback = new AbortController()
    const explicit = new AbortController()
    const api = useAuditApi({ signal: fallback.signal })

    const first = api.getEvent('event-a', 'tenant-a', explicit.signal)
    expect(transport.jobs[0].init.signal).toBe(explicit.signal)
    expect(transport.jobs[0].init.signal).not.toBe(fallback.signal)
    transport.jobs[0].resolve(json({ event_id: 'event-a' }))
    await first

    const second = api.getEvent('event-b', 'tenant-a', undefined)
    expect(transport.jobs[1].init.signal).toBe(fallback.signal)
    transport.jobs[1].resolve(json({ event_id: 'event-b' }))
    await second
  })

  it('keeps the old fetch shape when useAuditApi is called without a context', async () => {
    authState.set(authenticated())
    const transport = controlledFetch()

    const pending = useAuditApi().getOperation('op-a', 'tenant-a')

    expect(Object.prototype.hasOwnProperty.call(transport.jobs[0].init, 'signal')).toBe(false)
    transport.jobs[0].resolve(json({ operation_id: 'op-a', tenant_id: 'tenant-a', event_count: 1 }))
    await pending
  })
})

describe('401 fencing with request context', () => {
  function unauthorized(): Response {
    return json({ error: { code: 'unauthorized', request_id: 'request-a' } }, 401)
  }

  it('rejects late 401 with the original ApiError but does not invalidate a scoped aborted read', async () => {
    const original = authenticated()
    authState.set(original)
    const transport = controlledFetch()
    const controller = new AbortController()
    controller.abort()
    const api = useAuditApi({ signal: controller.signal })

    const pending = api.getOperation('op-a', 'tenant-a')
    transport.jobs[0].resolve(unauthorized())

    const failure: unknown = await pending.catch((cause: unknown) => cause)
    expect(failure).toBeInstanceOf(ApiError)
    if (failure instanceof ApiError) {
      expect(failure.status).toBe(401)
      expect(failure.code).toBe('unauthorized')
      expect(failure.requestId).toBe('request-a')
    }
    expect(get(authState)).toBe(original)
    expect(get(authState).status).toBe('authenticated')
    expect(transport.fetcher).toHaveBeenCalledTimes(1)
  })

  it('still invalidates on 401 when an active explicit signal overrides an aborted context signal', async () => {
    authState.set(authenticated())
    const transport = controlledFetch()
    const fallback = new AbortController()
    const explicit = new AbortController()
    fallback.abort()
    const api = useAuditApi({ signal: fallback.signal })

    const pending = api.getEvent('event-a', 'tenant-a', explicit.signal)
    transport.jobs[0].resolve(unauthorized())

    await expect(pending).rejects.toMatchObject({ status: 401, code: 'unauthorized' })
    expect(get(authState).status).toBe('anonymous')
    expect(get(authState).session).toBeNull()
  })

  it('retires an old adapter generation on synchronous A → B → original A before a late 401', async () => {
    const original = authenticated(session('token-a'))
    authState.set(original)
    const transport = controlledFetch()
    const controller = new AbortController()
    const api = useAuditApi({ signal: controller.signal })

    const pending = api.getOperation('op-a', 'tenant-a')
    authState.set(authenticated(session('token-b', 'tenant-b')))
    authState.set(original)
    const current = get(authState)
    expect(current).toBe(original)

    transport.jobs[0].resolve(unauthorized())

    await expect(pending).rejects.toMatchObject({ status: 401, code: 'unauthorized' })
    expect(get(authState)).toBe(current)
    expect(get(authState).status).toBe('authenticated')
    expect(transport.fetcher).toHaveBeenCalledTimes(1)
  })
})
