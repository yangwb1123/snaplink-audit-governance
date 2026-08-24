import { describe, expect, it, vi } from 'vitest'
import { AuditApi } from './audit'

describe('AuditApi', () => {
  it('encodes event filters and platform tenant scope', async () => {
    const fetcher = vi.fn<typeof fetch>(
      async () =>
        new Response(JSON.stringify({ items: [], count: 0 }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        }),
    )
    const api = new AuditApi({ baseUrl: '/audit-api', accessToken: 'token', fetcher })
    await api.queryEvents(
      {
        from: '2026-08-21T00:00:00.000Z',
        to: '2026-08-22T00:00:00.000Z',
        event_type: 'file.downloaded',
        correlation_id: 'flow/100',
        payload_field: 'object_id',
        payload_digest: 'digest+value',
        page_size: 50,
      },
      'tenant/a',
    )
    const [url, init] = fetcher.mock.calls[0]!
    expect(String(url)).toContain('/api/v1/events?')
    expect(String(url)).toContain('event_type=file.downloaded')
    expect(String(url)).toContain('correlation_id=flow%2F100')
    expect(String(url)).toContain('payload_field=object_id')
    expect(String(url)).toContain('payload_digest=digest%2Bvalue')
    expect(String(url)).toContain('tenant_id=tenant%2Fa')
    expect(new Headers(init?.headers).get('Authorization')).toBe('Bearer token')
  })

  it('queries full-window Snaplink-compatible facets with tenant scope', async () => {
    const fetcher = vi.fn<typeof fetch>(
      async () =>
        new Response(
          JSON.stringify({
            facets: {
              total: 7,
              outcomes: { success: 6, failed: 1 },
              types: { 'file.downloaded': 7 },
              clients: { 'aero-vault': 7 },
              providers: {},
            },
          }),
          { status: 200, headers: { 'Content-Type': 'application/json' } },
        ),
    )
    const api = new AuditApi({ baseUrl: '/audit-api', accessToken: 'token', fetcher })
    const response = await api.getEventFacets(
      {
        since: '2026-08-22T00:00:00.000Z',
        until: '2026-08-23T00:00:00.000Z',
        client_id: 'aero-vault',
      },
      'tenant/a',
    )

    const [url, init] = fetcher.mock.calls[0]!
    expect(String(url)).toContain('/api/v1/compat/snaplink/audit/facets?')
    expect(String(url)).toContain('client_id=aero-vault')
    expect(String(url)).toContain('tenant_id=tenant%2Fa')
    expect(new Headers(init?.headers).get('Authorization')).toBe('Bearer token')
    expect(response.facets.total).toBe(7)
  })

  it('preserves scoped evidence filters for exports and legal holds', async () => {
    const fetcher = vi.fn<typeof fetch>(
      async () =>
        new Response('{}', {
          status: 202,
          headers: { 'Content-Type': 'application/json' },
        }),
    )
    const api = new AuditApi({ baseUrl: '/audit-api', accessToken: 'token', fetcher })
    const query = {
      from: '2026-08-01T00:00:00.000Z',
      to: '2026-08-23T00:00:00.000Z',
      source_system: 'aero-im.source',
      event_type: 'aero.im.security',
      operation_id: 'op-100',
      correlation_id: 'flow-100',
      page_size: 1000,
    }

    await api.createExport(query, 'platform-local')
    await api.createLegalHold(
      { name: 'case-100', reason: 'investigation', filter: query },
      'platform-local',
    )

    expect(String(fetcher.mock.calls[0]?.[0])).toBe(
      '/audit-api/api/v1/exports?tenant_id=platform-local',
    )
    expect(JSON.parse(String(fetcher.mock.calls[0]?.[1]?.body))).toEqual(query)
    expect(String(fetcher.mock.calls[1]?.[0])).toBe(
      '/audit-api/api/v1/legal-holds?tenant_id=platform-local',
    )
    expect(JSON.parse(String(fetcher.mock.calls[1]?.[1]?.body))).toEqual({
      name: 'case-100',
      reason: 'investigation',
      filter: query,
    })
  })

  it('maps the stable server error contract and invalidates on 401', async () => {
    const invalidate = vi.fn()
    const fetcher = vi.fn<typeof fetch>(
      async () =>
        new Response(
          JSON.stringify({
            error: { code: 'unauthorized', message: 'unauthorized', request_id: 'req-1' },
          }),
          { status: 401, headers: { 'Content-Type': 'application/json' } },
        ),
    )
    const api = new AuditApi({
      baseUrl: '/audit-api',
      accessToken: 'expired',
      fetcher,
      onUnauthorized: invalidate,
    })
    await expect(api.getEvent('event-1')).rejects.toMatchObject({
      status: 401,
      code: 'unauthorized',
      requestId: 'req-1',
    })
    expect(invalidate).toHaveBeenCalledOnce()
  })

  it('encodes aggregate history cursors and tenant scope', async () => {
    const fetcher = vi.fn<typeof fetch>(
      async () =>
        new Response(JSON.stringify({ items: [], count: 0 }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        }),
    )
    const api = new AuditApi({ baseUrl: '/audit-api', accessToken: 'token', fetcher })
    await api.getAggregateTimeline('invoice/type', 'inv 100', {
      pageSize: 200,
      cursor: 'next/page',
      tenantId: 'tenant-a',
    })

    expect(String(fetcher.mock.calls[0]?.[0])).toBe(
      '/audit-api/api/v1/aggregates/invoice%2Ftype/inv%20100/timeline?page_size=200&cursor=next%2Fpage&tenant_id=tenant-a',
    )
  })

  it('sends only the OpenAPI SourceUpdate fields', async () => {
    const fetcher = vi.fn<typeof fetch>(
      async () =>
        new Response(
          JSON.stringify({
            id: 'aero-vault',
            tenant_id: 'demo',
            name: 'Aero Vault',
            allowed_client_ids: ['vault-worker'],
            active: false,
          }),
          { status: 200, headers: { 'Content-Type': 'application/json' } },
        ),
    )
    const api = new AuditApi({ baseUrl: '/audit-api', accessToken: 'token', fetcher })
    await api.updateSource(
      {
        id: 'aero-vault',
        name: 'Aero Vault',
        allowed_client_ids: ['vault-worker'],
        active: false,
      },
      'demo',
    )

    const [url, init] = fetcher.mock.calls[0]!
    expect(String(url)).toBe('/audit-api/api/v1/sources/aero-vault?tenant_id=demo')
    expect(JSON.parse(String(init?.body))).toEqual({
      name: 'Aero Vault',
      allowed_client_ids: ['vault-worker'],
      active: false,
    })
  })

  it('sends the complete OpenAPI event schema policy', async () => {
    const schema = {
      tenant_id: 'demo',
      schema_id: 'vault.object.downloaded',
      version: 2,
      event_type: 'vault.object.downloaded',
      required_fields: ['event_id', 'object_id'],
      allowed_fields: ['object_id', 'email'],
      encrypted_fields: ['email'],
      searchable_fields: ['email'],
      classification: 'confidential',
      active: true,
    }
    const fetcher = vi.fn<typeof fetch>(
      async () =>
        new Response(JSON.stringify(schema), {
          status: 201,
          headers: { 'Content-Type': 'application/json' },
        }),
    )
    const api = new AuditApi({ baseUrl: '/audit-api', accessToken: 'token', fetcher })
    await api.createSchema(schema, 'demo')

    const [url, init] = fetcher.mock.calls[0]!
    expect(String(url)).toBe('/audit-api/api/v1/schemas?tenant_id=demo')
    expect(init?.method).toBe('POST')
    expect(JSON.parse(String(init?.body))).toEqual(schema)
  })
})
