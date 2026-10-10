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

  it('calls fetch with its global Web API receiver, not with the API client as this', async () => {
    const fetcher = vi.fn<typeof fetch>(function (this: unknown) {
      if (this !== globalThis) throw new TypeError('Illegal invocation')
      return Promise.resolve(new Response('{"items":[],"count":0}', { status: 200 }))
    })
    const api = new AuditApi({ baseUrl: '/audit-api', accessToken: 'token', fetcher })
    await expect(api.listExports({}, 'tenant-a')).resolves.toMatchObject({ count: 0 })
  })

  it('retains structured Hold denials for JSON and download requests', async () => {
    const fetcher = vi.fn<typeof fetch>(async () => new Response(JSON.stringify({ error: { code: 'forbidden', message: 'blocked', request_id: 'req-1', reason: 'legal_hold', hold_id: 'hold-1' } }), { status: 403 }))
    const api = new AuditApi({ baseUrl: '/audit-api', accessToken: 'token', fetcher })
    await expect(api.getExport('job', 'tenant-a')).rejects.toMatchObject({ status: 403, reason: 'legal_hold', holdId: 'hold-1', requestId: 'req-1' })
    await expect(api.downloadExport('job', 'tenant-a')).rejects.toBeInstanceOf(ApiError)
  })
})
