import { describe, expect, it, vi } from 'vitest'
import { ApiClient } from './client'

interface FetchJob {
  url: string
  init: RequestInit
  resolve: (response: Response) => void
  reject: (cause: unknown) => void
}

// Deliberately ignores AbortSignal: request generations and the 401 side-effect
// fence must remain separate from browser I/O cancellation.
function controlledFetch() {
  const jobs: FetchJob[] = []
  const fetcher = vi.fn<typeof fetch>((input, init = {}) => new Promise<Response>((resolve, reject) => {
    const normalized = init ?? {}
    jobs.push({ url: String(input), init: normalized, resolve, reject })
  }))
  return { fetcher, jobs }
}

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

function client(options: { signal?: AbortSignal; onUnauthorized?: () => void; fetcher: typeof fetch }) {
  return new ApiClient({
    baseUrl: 'https://audit.example.test',
    accessToken: 'fixture-token',
    fetcher: options.fetcher,
    requestSignal: options.signal,
    onUnauthorized: options.onUnauthorized,
  })
}

describe('ApiClient default request signal', () => {
  it('applies the default signal to JSON reads', async () => {
    const transport = controlledFetch()
    const controller = new AbortController()
    const api = client({ fetcher: transport.fetcher, signal: controller.signal })

    const pending = api.request<{ ok: boolean }>('/api/v1/operations/op-a')

    expect(transport.jobs).toHaveLength(1)
    expect(transport.jobs[0].url).toBe('https://audit.example.test/api/v1/operations/op-a')
    expect(transport.jobs[0].init.signal).toBe(controller.signal)
    transport.jobs[0].resolve(json({ ok: true }))
    await expect(pending).resolves.toEqual({ ok: true })
  })

  it('applies the default signal to blob reads', async () => {
    const transport = controlledFetch()
    const controller = new AbortController()
    const api = client({ fetcher: transport.fetcher, signal: controller.signal })

    const pending = api.blob('/api/v1/exports/export-a/download')

    expect(transport.jobs).toHaveLength(1)
    expect(transport.jobs[0].init.signal).toBe(controller.signal)
    transport.jobs[0].resolve(new Response(new Blob(['fixture'])))
    await expect(pending).resolves.toBeInstanceOf(Blob)
  })

  it('lets an explicit init signal override the default', async () => {
    const transport = controlledFetch()
    const fallback = new AbortController()
    const explicit = new AbortController()
    const api = client({ fetcher: transport.fetcher, signal: fallback.signal })

    const pending = api.request('/api/v1/events/event-a', { signal: explicit.signal })

    expect(transport.jobs[0].init.signal).toBe(explicit.signal)
    expect(transport.jobs[0].init.signal).not.toBe(fallback.signal)
    transport.jobs[0].resolve(json({ event_id: 'event-a' }))
    await pending
  })

  it('falls back to the default when the init signal is explicitly undefined', async () => {
    const transport = controlledFetch()
    const fallback = new AbortController()
    const api = client({ fetcher: transport.fetcher, signal: fallback.signal })

    const pending = api.request('/api/v1/events/event-a', { signal: undefined })

    expect(transport.jobs[0].init.signal).toBe(fallback.signal)
    transport.jobs[0].resolve(json({ event_id: 'event-a' }))
    await pending
  })

  it('keeps an explicit null init signal as the standard way to disable the default', async () => {
    const transport = controlledFetch()
    const fallback = new AbortController()
    const api = client({ fetcher: transport.fetcher, signal: fallback.signal })

    const pending = api.request('/api/v1/events/event-a', { signal: null })

    expect(transport.jobs[0].init.signal).toBeNull()
    transport.jobs[0].resolve(json({ event_id: 'event-a' }))
    await pending
  })

  it('lets an explicit blob signal override the default and null disable it', async () => {
    const transport = controlledFetch()
    const fallback = new AbortController()
    const explicit = new AbortController()
    const api = client({ fetcher: transport.fetcher, signal: fallback.signal })

    const first = api.blob('/api/v1/exports/export-a/download', explicit.signal)
    expect(transport.jobs[0].init.signal).toBe(explicit.signal)
    transport.jobs[0].resolve(new Response(new Blob(['fixture'])))
    await first

    const second = api.blob('/api/v1/exports/export-a/download', null)
    expect(transport.jobs[1].init.signal).toBeNull()
    transport.jobs[1].resolve(new Response(new Blob(['fixture'])))
    await second
  })

  it('does not add a signal property when no request context is supplied', async () => {
    const transport = controlledFetch()
    const api = client({ fetcher: transport.fetcher })

    const pending = api.request('/api/v1/operations/op-a')

    expect(Object.prototype.hasOwnProperty.call(transport.jobs[0].init, 'signal')).toBe(false)
    transport.jobs[0].resolve(json({ operation_id: 'op-a' }))
    await pending
  })
})

describe('ApiClient 401 side-effect fence uses the effective request signal', () => {
  it.each(['JSON', 'blob'] as const)('preserves legacy %s unauthorized notification after an ignored explicit abort without a bound context', async (kind) => {
    const transport = controlledFetch()
    const explicit = new AbortController()
    const onUnauthorized = vi.fn()
    const api = client({ fetcher: transport.fetcher, onUnauthorized })
    const pending = kind === 'JSON'
      ? api.request('/api/v1/events/event-a', { signal: explicit.signal })
      : api.blob('/api/v1/exports/export-a/download', explicit.signal)
    expect(transport.jobs[0].init.signal).toBe(explicit.signal)
    explicit.abort()
    transport.jobs[0].resolve(unauthorized())
    await expect(pending).rejects.toMatchObject({ status: 401, code: 'unauthorized', requestId: 'request-a' })
    expect(onUnauthorized).toHaveBeenCalledTimes(1)
  })

  function unauthorized(): Response {
    return json({ error: { code: 'unauthorized', request_id: 'request-a' } }, 401)
  }

  it('suppresses a late 401 side effect for an actually aborted scoped request', async () => {
    const transport = controlledFetch()
    const controller = new AbortController()
    const onUnauthorized = vi.fn()
    const api = client({ fetcher: transport.fetcher, signal: controller.signal, onUnauthorized })
    controller.abort()

    const pending = api.request('/api/v1/operations/op-a')
    transport.jobs[0].resolve(unauthorized())

    await expect(pending).rejects.toMatchObject({
      status: 401, code: 'unauthorized', requestId: 'request-a',
    })
    expect(onUnauthorized).not.toHaveBeenCalled()
  })

  it('still handles the current 401 when an active explicit signal overrides an aborted default', async () => {
    const transport = controlledFetch()
    const fallback = new AbortController()
    const explicit = new AbortController()
    const onUnauthorized = vi.fn()
    const api = client({ fetcher: transport.fetcher, signal: fallback.signal, onUnauthorized })
    fallback.abort()

    const pending = api.request('/api/v1/events/event-a', { signal: explicit.signal })
    transport.jobs[0].resolve(unauthorized())

    await expect(pending).rejects.toMatchObject({ status: 401, code: 'unauthorized' })
    expect(onUnauthorized).toHaveBeenCalledTimes(1)
  })

  it('still handles the current 401 when RequestInit null overrides an aborted default', async () => {
    const transport = controlledFetch()
    const fallback = new AbortController()
    const onUnauthorized = vi.fn()
    const api = client({ fetcher: transport.fetcher, signal: fallback.signal, onUnauthorized })
    fallback.abort()

    const pending = api.request('/api/v1/events/event-a', { signal: null })
    transport.jobs[0].resolve(unauthorized())

    await expect(pending).rejects.toMatchObject({ status: 401, code: 'unauthorized' })
    expect(onUnauthorized).toHaveBeenCalledTimes(1)
  })

  it('suppresses the side effect when the explicit signal is the aborted effective signal', async () => {
    const transport = controlledFetch()
    const fallback = new AbortController()
    const explicit = new AbortController()
    const onUnauthorized = vi.fn()
    const api = client({ fetcher: transport.fetcher, signal: fallback.signal, onUnauthorized })
    explicit.abort()

    const pending = api.request('/api/v1/events/event-a', { signal: explicit.signal })
    transport.jobs[0].resolve(unauthorized())

    await expect(pending).rejects.toMatchObject({ status: 401, code: 'unauthorized' })
    expect(onUnauthorized).not.toHaveBeenCalled()
  })

  it('keeps the original 401 behavior when no request context is supplied', async () => {
    const transport = controlledFetch()
    const onUnauthorized = vi.fn()
    const api = client({ fetcher: transport.fetcher, onUnauthorized })

    const pending = api.request('/api/v1/events/event-a')
    transport.jobs[0].resolve(unauthorized())

    await expect(pending).rejects.toMatchObject({ status: 401 })
    expect(onUnauthorized).toHaveBeenCalledTimes(1)
  })
})
