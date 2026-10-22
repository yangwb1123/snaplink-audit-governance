// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { ApiError } from '../api/client'
import type { AuditEvent, EventReceipt } from '../api/types'
import { authState } from '../auth/store'
import Detail from './EventDetail.svelte'
import Page from './EventsPage.svelte'
import Harness from './EventDetail.testHarness.svelte'

const { api } = vi.hoisted(() => ({ api: { queryEvents: vi.fn(), getEvent: vi.fn(), getReceipt: vi.fn(), listSchemas: vi.fn(), getRetention: vi.fn() } }))
vi.mock('../api/useAuditApi', () => ({ useAuditApi: () => api }))
const event = (id = 'event-a', tenant = 'tenant-a'): AuditEvent => ({ event_id: id, tenant_id: tenant, source_system: 'aero-vault', event_type: 'file.read', schema_id: 'historical.schema', schema_version: 3, retention_class: 'retention-at-ingest', data_classification: 'personal-at-ingest', actor: { id: 'actor' }, action: 'read', outcome: 'success', occurred_at: '', received_at: '', payload: { fact: 'authenticated payload' } })
const receipt = (id = 'event-a', tenant = 'tenant-a'): EventReceipt => ({ event_id: id, tenant_id: tenant, status: 'archived', accepted_at: '' })
function deferred<T>() { let resolve!: (value: T) => void; let reject!: (error: unknown) => void; const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no }); return { promise, resolve, reject } }
let target: HTMLDivElement
let page: ReturnType<typeof mount> | undefined
async function settle() { await tick(); await Promise.resolve(); await tick() }
function text() { return document.body.textContent ?? '' }
function button(label: string, root: ParentNode = document) { return [...root.querySelectorAll('button')].find(b => b.textContent?.trim() === label)! }
function login(permissions = ['audit:event:read'], token = 'token', platform = false) {
  authState.set({ status: 'authenticated', session: { accessToken: token, idToken: 'id', subject: 'actor', displayName: 'actor', expiresAt: Date.now() + 100000, tenantId: 'tenant-a', roles: [], permissions: new Set(permissions), platform }, error: null })
}

beforeEach(() => {
  vi.resetAllMocks(); login()
  window.history.replaceState(null, '', '#events')
  api.queryEvents.mockResolvedValue({ items: [], count: 0 })
  api.getEvent.mockResolvedValue(event()); api.getReceipt.mockResolvedValue(receipt())
  target = document.createElement('div'); document.body.append(target)
})
afterEach(async () => { if (page) await unmount(page); page = undefined; target.remove(); window.history.replaceState(null, '', '#'); authState.set({ status: 'anonymous', session: null, error: null }) })

describe('authenticated event detail and safe direct links', () => {
  it('loads a direct event independently of a failing list and receipt; shows exact historical governance metadata without policy reads', async () => {
    window.history.replaceState(null, '', '#events/event/event-a')
    api.queryEvents.mockRejectedValue(new ApiError(503, 'store_timeout', 'list unavailable'))
    api.getReceipt.mockRejectedValueOnce(new Error('receipt unavailable'))
    page = mount(Page, { target, props: { tenantId: 'tenant-a' } }); await settle()
    expect(api.getEvent).toHaveBeenCalledWith('event-a', 'tenant-a', expect.any(AbortSignal))
    for (const value of ['schema_id', 'schema_version', 'retention_class', 'data_classification', 'historical.schema', 'retention-at-ingest', 'personal-at-ingest', 'authenticated payload']) expect(text()).toContain(value)
    const versionLabel = [...document.querySelectorAll('dt, th, td, span')].find(node => node.textContent === 'Schema 版本 (schema_version)')
    expect(versionLabel?.parentElement?.textContent).toContain('3')
    expect(text()).toContain('receipt unavailable')
    expect(api.listSchemas).not.toHaveBeenCalled(); expect(api.getRetention).not.toHaveBeenCalled()
    button('重试', document.querySelector('.drawer-body')!).click(); await settle()
    expect(text()).toContain('archived'); expect(api.getEvent).toHaveBeenCalledTimes(1)
    expect(text()).not.toContain('查看操作时间线') // event permission alone remains enough
  })

  it('never substitutes a list snapshot or latest Schema for pending or failed authenticated detail', async () => {
    api.queryEvents.mockResolvedValue({ items: [{ ...event(), schema_id: 'list-schema', schema_version: 99, payload: { stale: 'list payload' } }], count: 1 })
    const full = deferred<AuditEvent>(); api.getEvent.mockReturnValueOnce(full.promise)
    page = mount(Page, { target, props: { tenantId: 'tenant-a' } }); await settle()
    button('event-a').click(); await new Promise(resolve => setTimeout(resolve, 0)); await settle()
    expect(window.location.hash).toBe('#events/event/event-a')
    expect(text()).toContain('正在读取认证事件')
    expect(text()).not.toContain('list-schema'); expect(text()).not.toContain('list payload')
    full.reject(new Error('authenticated read failed')); await settle()
    expect(text()).toContain('authenticated read failed'); expect(text()).not.toContain('list payload')
    button('重试').click(); await settle()
    expect(text()).toContain('historical.schema'); expect(text()).toContain('authenticated payload')
    expect(api.getReceipt).toHaveBeenCalledTimes(1); expect(api.listSchemas).not.toHaveBeenCalled()
  })

  it('shows an explicit not-found empty state and supports read-only retry', async () => {
    api.getEvent.mockRejectedValueOnce(new ApiError(404, 'not_found', 'missing'))
    page = mount(Detail, { target, props: { tenantId: 'tenant-a', eventId: 'event-a', onClose: vi.fn() } }); await settle()
    expect(text()).toContain('未找到当前租户的事件')
    button('刷新').click(); await settle()
    expect(text()).toContain('historical.schema'); expect(api.getEvent).toHaveBeenCalledTimes(2)
  })

  it('fences ignored-abort event A → B → A and tenant A → B → A responses', async () => {
    login([], 'platform-token', true)
    const oldEvent = deferred<AuditEvent>(), oldReceipt = deferred<EventReceipt>()
    api.getEvent.mockReturnValueOnce(oldEvent.promise); api.getReceipt.mockReturnValueOnce(oldReceipt.promise)
    api.getEvent.mockImplementation((id, tenant) => Promise.resolve(event(id, tenant)))
    api.getReceipt.mockImplementation((id, tenant) => Promise.resolve(receipt(id, tenant)))
    page = mount(Harness, { target }); await settle()
    const signal = api.getEvent.mock.calls[0][2] as AbortSignal
    button('事件 B').click(); await settle(); button('事件 A').click(); await settle()
    button('租户 B').click(); await settle(); button('租户 A').click(); await settle()
    expect(signal.aborted).toBe(true)
    oldEvent.resolve({ ...event(), schema_id: 'OLD SCHEMA', payload: { secret: 'STALE PAYLOAD' } })
    oldReceipt.resolve({ ...receipt(), status: 'STALE RECEIPT' }); await settle()
    expect(text()).not.toContain('STALE'); expect(text()).not.toContain('OLD SCHEMA')
    expect(text()).toContain('historical.schema')
  })

  it('clears old detail when token/permissions change and emits no unauthorized requests', async () => {
    const old = deferred<AuditEvent>(); api.getEvent.mockReturnValueOnce(old.promise)
    page = mount(Detail, { target, props: { tenantId: 'tenant-a', eventId: 'event-a', onClose: vi.fn() } }); await settle()
    login([], 'new-token'); await settle(); old.resolve({ ...event(), schema_id: 'OLD PRIVATE SCHEMA' }); await settle()
    expect(text()).not.toContain('OLD PRIVATE SCHEMA'); expect(text()).toContain('没有事件读取权限')
    expect(api.getEvent).toHaveBeenCalledTimes(1); expect(api.getReceipt).toHaveBeenCalledTimes(1)
    login(['audit:event:read'], 'third-token'); await settle()
    expect(api.getEvent).toHaveBeenCalledTimes(2); expect(text()).toContain('historical.schema')
  })

  it('rejects mismatched event/tenant identities instead of rendering the response', async () => {
    api.getEvent.mockResolvedValue(event('foreign-event', 'tenant-b'))
    api.getReceipt.mockResolvedValue(receipt('foreign-event', 'tenant-b'))
    page = mount(Detail, { target, props: { tenantId: 'tenant-a', eventId: 'event-a', onClose: vi.fn() } }); await settle()
    expect(text()).toContain('与当前租户或 ID 不匹配'); expect(text()).not.toContain('authenticated payload')
    expect(text()).not.toContain('historical.schema'); expect(text()).not.toContain('archived')
  })

  it.each(['#events/event/bad%2Fid', '#events/event/id?tenant_id=tenant-b', '#events/event/%', '#events/event/id/extra'])('does not request detail for an invalid hash: %s', async (hash) => {
    window.history.replaceState(null, '', hash)
    page = mount(Page, { target, props: { tenantId: 'tenant-a' } }); await settle()
    expect(api.getEvent).not.toHaveBeenCalled(); expect(api.getReceipt).not.toHaveBeenCalled()
  })
})
