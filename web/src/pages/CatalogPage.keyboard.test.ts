// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import type { EventSchema } from '../api/types'
import { authState } from '../auth/store'
import CatalogPage from './CatalogPage.svelte'

const { api } = vi.hoisted(() => ({
  api: {
    listTenants: vi.fn(),
    listSources: vi.fn(),
    listSchemas: vi.fn(),
    listAdminActions: vi.fn(),
    getRetention: vi.fn(),
    createSchema: vi.fn(),
  },
}))

vi.mock('../api/useAuditApi', () => ({ useAuditApi: () => api }))

const schemas: EventSchema[] = [
  {
    tenant_id: 'tenant-a',
    schema_id: 'schema.audit.login',
    version: 7,
    event_type: 'audit.login',
    required_fields: ['trace_alpha'],
    allowed_fields: ['actor_alpha'],
    encrypted_fields: ['email_alpha'],
    searchable_fields: ['subject_alpha'],
    classification: 'personal',
    active: true,
    created_at: '2025-01-01T00:00:00Z',
  },
  {
    tenant_id: 'tenant-a',
    schema_id: 'schema.audit.export',
    version: 2,
    event_type: 'audit.export',
    required_fields: ['trace_beta'],
    allowed_fields: ['object_beta'],
    encrypted_fields: ['payload_beta'],
    searchable_fields: ['request_beta'],
    classification: 'confidential',
    active: true,
    created_at: '2025-02-01T00:00:00Z',
  },
]

describe('CatalogPage schema keyboard selection', () => {
  let target: HTMLDivElement
  let component: ReturnType<typeof mount> | undefined

  beforeEach(() => {
    api.listTenants.mockReset()
    api.listSources.mockReset().mockResolvedValue({ items: [], count: 0 })
    api.listSchemas.mockReset().mockResolvedValue({ items: schemas, count: schemas.length })
    api.listAdminActions.mockReset().mockResolvedValue({ items: [], count: 0 })
    api.getRetention.mockReset().mockResolvedValue({
      tenant_id: 'tenant-a',
      hot_days: 30,
      warm_days: 90,
      archive_days: 365,
      retention_class: 'standard',
    })
    api.createSchema.mockReset().mockResolvedValue(schemas[0])
    authState.set({
      status: 'authenticated',
      session: {
        accessToken: 'test-access-token',
        idToken: 'test-id-token',
        expiresAt: Date.now() + 60_000,
        subject: 'reviewer',
        displayName: '审核员',
        tenantId: 'tenant-a',
        roles: [],
        permissions: new Set(),
        platform: false,
      },
      error: null,
    })
    target = document.createElement('div')
    document.body.append(target)
    component = mount(CatalogPage, { target, props: { tenantId: 'tenant-a' } })
  })

  afterEach(async () => {
    if (component) await unmount(component)
    component = undefined
    target.remove()
    authState.set({ status: 'anonymous', session: null, error: null })
  })

  it('focuses schema rows and selects by Enter and Space while updating details', async () => {
    await new Promise((resolve) => setTimeout(resolve, 0))
    await tick()

    const rows = [...target.querySelectorAll<HTMLTableRowElement>('.data-table tbody tr[aria-selected]')]
    expect(rows).toHaveLength(2)
    expect(rows.every((row) => row.tabIndex === 0)).toBe(true)
    expect(rows.map((row) => row.getAttribute('aria-selected'))).toEqual(['false', 'false'])

    rows[0].focus()
    expect(document.activeElement).toBe(rows[0])
    rows[0].dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true, cancelable: true }))
    await tick()

    expect(rows.map((row) => row.getAttribute('aria-selected'))).toEqual(['true', 'false'])
    expectDetails('schema.audit.login', 'v7', [
      'trace_alpha',
      'actor_alpha',
      'email_alpha',
      'subject_alpha',
    ])

    rows[1].focus()
    expect(document.activeElement).toBe(rows[1])
    const space = new KeyboardEvent('keydown', { key: ' ', bubbles: true, cancelable: true })
    rows[1].dispatchEvent(space)
    await tick()

    expect(space.defaultPrevented).toBe(true)
    expect(rows.map((row) => row.getAttribute('aria-selected'))).toEqual(['false', 'true'])
    expectDetails('schema.audit.export', 'v2', [
      'trace_beta',
      'object_beta',
      'payload_beta',
      'request_beta',
    ])

    rows[0].click()
    await tick()
    expect(rows.map((row) => row.getAttribute('aria-selected'))).toEqual(['true', 'false'])
    expectDetails('schema.audit.login', 'v7', ['trace_alpha', 'actor_alpha'])
    expect(api.createSchema).not.toHaveBeenCalled()
  })
})

function expectDetails(schemaId: string, version: string, policyValues: string[]): void {
  const details = document.querySelector('.schema-policy-view')
  expect(details?.textContent).toContain(`策略详情：${schemaId} ${version}`)
  for (const value of policyValues) expect(details?.textContent).toContain(value)
}
