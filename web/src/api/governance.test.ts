import { describe, expect, it, vi } from 'vitest'
import { AuditApi } from './audit'
import { ApiError } from './client'

describe('governance work API', () => {
  it('encodes views, filters and authenticated tenant selection and forwards cancellation', async () => {
    const fetcher = vi.fn<typeof fetch>(async () => new Response('{"items":[],"count":0}', { status: 200 }))
    const api = new AuditApi({ baseUrl: '/audit-api', accessToken: 'token', platform: true, fetcher })
    const signal = new AbortController().signal
    await api.listExports({ view: 'mine', status: 'failed', requested_by: 'a/b', cursor: 'cursor+next', page_size: 50 }, 'tenant-a', signal)
    await api.listRestores({ view: 'actionable', operation_id: 'op/1' }, 'tenant-a', signal)
    expect(String(fetcher.mock.calls[0]?.[0])).toContain('view=mine')
    expect(String(fetcher.mock.calls[0]?.[0])).toContain('requested_by=a%2Fb')
    expect(String(fetcher.mock.calls[0]?.[0])).toContain('cursor=cursor%2Bnext')
    expect(String(fetcher.mock.calls[1]?.[0])).toContain('/restores?view=actionable&operation_id=op%2F1&tenant_id=tenant-a')
    expect(fetcher.mock.calls[0]?.[1]?.signal).toBe(signal)
    expect(() => api.listExports()).toThrow('tenant_id is required')
    expect(() => api.listRestores()).toThrow('tenant_id is required')
  })

  it('reads scoped governance metadata without adding sensitive query parameters and forwards cancellation', async () => {
    const fetcher = vi.fn<typeof fetch>(async () => new Response('{"tenant_id":"tenant/a","observed_at":"2026-10-08T00:00:00Z","source":"control","completeness":"not_asserted"}', { status: 200 }))
    const api = new AuditApi({ baseUrl: '/audit-api', accessToken: 'token', platform: true, fetcher })
    const signal = new AbortController().signal
    await api.getGovernanceStatus('tenant/a', signal)
    expect(String(fetcher.mock.calls[0]?.[0])).toBe('/audit-api/api/v1/governance/status?tenant_id=tenant%2Fa')
    expect(fetcher.mock.calls[0]?.[1]?.signal).toBe(signal)
    expect(() => api.getGovernanceStatus()).toThrow('tenant_id is required')
  })

  it('sends owner commands with no identity/filter body or URL data and requires platform scope', async () => {
    const fetcher = vi.fn<typeof fetch>(async () => new Response('{"id":"child","tenant_id":"tenant-a","requested_by":"alice","status":"pending","created_at":"2026-10-14T00:00:00Z"}', { status: 202 }))
    const api = new AuditApi({ baseUrl: '/audit-api', accessToken: 'token', platform: true, fetcher })
    const signal = new AbortController().signal
    await api.cancelExport('job/a', 'tenant-a', signal)
    await api.retryExport('job/a', 'tenant-a', signal)
    expect(String(fetcher.mock.calls[0]?.[0])).toBe('/audit-api/api/v1/exports/job%2Fa/cancel?tenant_id=tenant-a')
    expect(String(fetcher.mock.calls[1]?.[0])).toBe('/audit-api/api/v1/exports/job%2Fa/retry?tenant_id=tenant-a')
    for (const [, options] of fetcher.mock.calls) {
      expect(options?.method).toBe('POST')
      expect(options?.body).toBeUndefined()
      expect(options?.signal).toBe(signal)
    }
    expect(() => api.cancelExport('job')).toThrow('tenant_id is required')
    expect(() => api.retryExport('job')).toThrow('tenant_id is required')
  })

  it('calls fetch with its global Web API receiver, not with the API client as this', async () => {
    const fetcher = vi.fn<typeof fetch>(function (this: unknown) {
      if (this !== globalThis) throw new TypeError('Illegal invocation')
      return Promise.resolve(new Response('{"items":[],"count":0}', { status: 200 }))
    })
    const api = new AuditApi({ baseUrl: '/audit-api', accessToken: 'token', fetcher })
    await expect(api.listExports({}, 'tenant-a')).resolves.toMatchObject({ count: 0 })
  })

  it('forwards independent catalog read cancellation without changing tenant or permissions', async () => {
    const fetcher = vi.fn<typeof fetch>(async () => new Response('{"items":[],"count":0}', { status: 200 }))
    const api = new AuditApi({ baseUrl: '/audit-api', accessToken: 'memory-only-token', platform: true, fetcher })
    const signal = new AbortController().signal
    await api.listTenants(signal)
    await api.listSources('tenant-a', signal)
    await api.listSchemas('tenant-a', signal)
    await api.listAdminActions('tenant-a', 100, signal)
    await api.getRetention('tenant-a', signal)
    expect(fetcher.mock.calls.map(([url]) => String(url))).toEqual([
      '/audit-api/api/v1/tenants', '/audit-api/api/v1/sources?tenant_id=tenant-a',
      '/audit-api/api/v1/schemas?tenant_id=tenant-a', '/audit-api/api/v1/admin/actions?tenant_id=tenant-a&limit=100',
      '/audit-api/api/v1/policies/retention?tenant_id=tenant-a',
    ])
    for (const [url, options] of fetcher.mock.calls) {
      expect(options?.signal).toBe(signal)
      expect(new Headers(options?.headers).get('Authorization')).toBe('Bearer memory-only-token')
      expect(String(url)).not.toContain('memory-only-token')
    }
    expect(() => api.listSources()).toThrow('tenant_id is required')
    expect(() => api.listSchemas()).toThrow('tenant_id is required')
    expect(() => api.getRetention()).toThrow('tenant_id is required')
  })

  it('retains structured Hold denials for JSON and download requests', async () => {
    const fetcher = vi.fn<typeof fetch>(async () => new Response(JSON.stringify({ error: { code: 'forbidden', message: 'blocked', request_id: 'req-1', reason: 'legal_hold', hold_id: 'hold-1' } }), { status: 403 }))
    const api = new AuditApi({ baseUrl: '/audit-api', accessToken: 'token', fetcher })
    await expect(api.getExport('job', 'tenant-a')).rejects.toMatchObject({ status: 403, reason: 'legal_hold', holdId: 'hold-1', requestId: 'req-1' })
    await expect(api.downloadExport('job', 'tenant-a')).rejects.toBeInstanceOf(ApiError)
  })
})
