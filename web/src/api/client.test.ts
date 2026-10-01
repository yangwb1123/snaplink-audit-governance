import { describe, expect, it, vi } from 'vitest'
import { AuditApi } from './audit'
import { ApiClient, ApiError } from './client'

const ndjson = '{"event_id":"e-1"}\n{"event_id":"e-2"}\n'

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

describe('ApiClient', () => {
  it('sends the tenant-scoped download envelope and returns the raw evidence bytes', async () => {
    const fetcher = vi.fn<typeof fetch>(
      async () =>
        new Response(ndjson, {
          status: 200,
          headers: { 'Content-Type': 'application/x-ndjson' },
        }),
    )
    const api = new AuditApi({ baseUrl: '/audit-api', accessToken: 'token', fetcher })

    const blob = await api.downloadExport('job/1', 'tenant/a')

    const [url, init] = fetcher.mock.calls[0]!
    expect(String(url)).toBe('/audit-api/api/v1/exports/job%2F1/download?tenant_id=tenant%2Fa')
    expect(init?.credentials).toBe('omit')
    // The download path hand-builds its headers: no ambient cookie, no Accept.
    expect([...new Headers(init?.headers).entries()]).toEqual([['authorization', 'Bearer token']])
    expect(blob).toBeInstanceOf(Blob)
    expect(blob.type).toBe('application/x-ndjson')
    expect(await blob.text()).toBe(ndjson)
  })

  it('refuses to emit an unscoped platform download before any bytes are read', () => {
    const fetcher = vi.fn<typeof fetch>(async () => new Response(ndjson, { status: 200 }))
    const api = new AuditApi({
      baseUrl: '/audit-api',
      accessToken: 'token',
      platform: true,
      fetcher,
    })

    expect(() => api.downloadExport('job-1')).toThrow('tenant_id is required')
    expect(fetcher).not.toHaveBeenCalled()
  })

  it('invalidates the session exactly once on a 401 download and throws the error contract', async () => {
    const onUnauthorized = vi.fn()
    const fetcher = vi.fn<typeof fetch>(
      async () =>
        jsonResponse(401, {
          error: { code: 'unauthorized', message: '会话已过期', request_id: 'req-7' },
        }),
    )
    const client = new ApiClient({
      baseUrl: '/audit-api',
      accessToken: 'expired',
      onUnauthorized,
      fetcher,
    })

    const failure = client.blob('/api/v1/exports/job-1/download?tenant_id=tenant-a')

    await expect(failure).rejects.toBeInstanceOf(ApiError)
    await expect(failure).rejects.toMatchObject({
      name: 'ApiError',
      status: 401,
      code: 'unauthorized',
      message: '会话已过期',
      requestId: 'req-7',
    })
    expect(onUnauthorized).toHaveBeenCalledTimes(1)
  })

  it('keeps a permission-scoped rejection from invalidating the session', async () => {
    const onUnauthorized = vi.fn()
    const fetcher = vi.fn<typeof fetch>(
      async () =>
        jsonResponse(403, {
          error: { code: 'forbidden', message: '缺少导出权限', request_id: 'req-8' },
        }),
    )
    const client = new ApiClient({
      baseUrl: '/audit-api',
      accessToken: 'token',
      onUnauthorized,
      fetcher,
    })

    await expect(
      client.blob('/api/v1/exports/job-1/download?tenant_id=tenant-a'),
    ).rejects.toMatchObject({
      status: 403,
      code: 'forbidden',
      message: '缺少导出权限',
      requestId: 'req-8',
    })
    // The JSON path shares the same 401-only boundary: 403 must not tear the session down.
    await expect(client.request('/api/v1/events/event-1')).rejects.toMatchObject({
      status: 403,
      code: 'forbidden',
    })
    expect(onUnauthorized).not.toHaveBeenCalled()
  })

  it('collapses a non-JSON proxy body to the HTTP status without leaking the parse error', async () => {
    const response = new Response(
      '<html><head><title>502 Bad Gateway</title></head></html>',
      { status: 502, headers: { 'Content-Type': 'text/html' } },
    )
    const json = vi.spyOn(response, 'json')
    const client = new ApiClient({
      baseUrl: '/audit-api',
      accessToken: 'token',
      fetcher: vi.fn<typeof fetch>(async () => response),
    })

    await expect(client.blob('/api/v1/exports/job-1/download')).rejects.toMatchObject({
      name: 'ApiError',
      status: 502,
      code: 'http_502',
      message: 'Audit API request failed (502)',
      requestId: undefined,
    })
    // The body is read and the parse failure is swallowed, not skipped.
    expect(json).toHaveBeenCalledTimes(1)
  })

  it('short-circuits a 204 without reading the body', async () => {
    const response = new Response(null, { status: 204 })
    const json = vi.spyOn(response, 'json')
    const client = new ApiClient({
      baseUrl: '/audit-api',
      accessToken: 'token',
      fetcher: vi.fn<typeof fetch>(async () => response),
    })

    await expect(client.request('/api/v1/events/event-1/receipt')).resolves.toBeUndefined()
    expect(json).not.toHaveBeenCalled()
  })

  it('sends the JSON envelope with forced Accept and body-derived Content-Type', async () => {
    const fetcher = vi.fn<typeof fetch>(
      async () =>
        jsonResponse(201, {
          legal_hold_id: 'hold-1',
          tenant_id: 'tenant-a',
          name: 'case-1',
        }),
    )
    const client = new ApiClient({ baseUrl: '/audit-api/', accessToken: 'token', fetcher })

    const created = await client.request<{ legal_hold_id: string }>('/api/v1/legal-holds', {
      method: 'POST',
      headers: { 'X-Trace': 'trace-1' },
      body: '{"name":"case-1"}',
    })

    const [url, init] = fetcher.mock.calls[0]!
    const headers = new Headers(init?.headers)
    expect(String(url)).toBe('/audit-api/api/v1/legal-holds')
    expect(init?.credentials).toBe('omit')
    expect(headers.get('Accept')).toBe('application/json')
    expect(headers.get('Authorization')).toBe('Bearer token')
    expect(headers.get('Content-Type')).toBe('application/json')
    expect(headers.get('X-Trace')).toBe('trace-1')
    expect(created.legal_hold_id).toBe('hold-1')
  })
})
