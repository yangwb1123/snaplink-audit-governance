// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { ApiError } from '../api/client'
import { authState } from '../auth/store'
import type { EventSchema, SourceSystem } from '../api/types'
import Page from './CatalogPage.svelte'
import Harness from './CatalogPage.testHarness.svelte'

const { api } = vi.hoisted(() => ({ api: {
  listTenants: vi.fn(), listSources: vi.fn(), listSchemas: vi.fn(), listAdminActions: vi.fn(), getRetention: vi.fn(),
} }))
vi.mock('../api/useAuditApi', () => ({ useAuditApi: () => api }))
function deferred<T>() { let resolve!: (value: T) => void; let reject!: (error: unknown) => void; const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no }); return { promise, resolve, reject } }
const list = <T,>(...items: T[]) => ({ items, count: items.length })
const source = (tenant = 'tenant-a', name = 'current source'): SourceSystem => ({ id: 'aero-im.source', tenant_id: tenant, name, active: true, allowed_client_ids: ['aero-im-audit'], created_at: '' })
const schema = (tenant = 'tenant-a', id = 'aero.im.security'): EventSchema => ({ tenant_id: tenant, schema_id: id, version: 1, event_type: id, classification: 'confidential', active: true, created_at: '' })
const policy = { tenant_id: 'tenant-a', hot_days: 1, warm_days: 2, archive_days: 3, retention_class: 'historical' }
let target: HTMLDivElement
let page: ReturnType<typeof mount> | undefined
async function settle() { await tick(); await Promise.resolve(); await tick() }
function login(platform = true, permissions = ['audit:policy:read'], token = 'token') {
  authState.set({ status: 'authenticated', session: { accessToken: token, idToken: 'id', subject: 'actor', displayName: 'actor', tenantId: 'tenant-a', platform, roles: [], permissions: new Set(permissions), expiresAt: Date.now() + 100000 }, error: null })
}
function card(title: string) { return [...target.querySelectorAll('h3')].find(h => h.textContent === title)!.parentElement! }
function button(label: string, root: ParentNode = target) { return [...root.querySelectorAll('button')].find(b => b.textContent?.trim() === label)! }
function ready() { return /\bvalid\b/.test(card('平台审计接入状态').textContent ?? '') }

beforeEach(() => {
  vi.resetAllMocks(); login()
  api.listTenants.mockResolvedValue(list({ id: 'tenant-a', name: 'Tenant A' }))
  api.listSources.mockResolvedValue(list(source()))
  api.listSchemas.mockResolvedValue(list(schema()))
  api.listAdminActions.mockResolvedValue(list({ id: 'action-1', tenant_id: 'tenant-a', action: 'source.updated', actor: 'operator', created_at: '', detail: 'current action' }))
  api.getRetention.mockResolvedValue(policy)
  target = document.createElement('div'); document.body.append(target)
})
afterEach(async () => { if (page) await unmount(page); page = undefined; target.remove(); authState.set({ status: 'anonymous', session: null, error: null }) })

describe('independent catalog observations', () => {
  it('distinguishes loading, success, empty and error for each resource, with isolated retries', async () => {
    const tenants = deferred<ReturnType<typeof list>>()
    const sources = deferred<ReturnType<typeof list<SourceSystem>>>()
    const schemas = deferred<ReturnType<typeof list<EventSchema>>>()
    const actions = deferred<ReturnType<typeof list>>()
    const retention = deferred<typeof policy>()
    api.listTenants.mockReturnValueOnce(tenants.promise); api.listSources.mockReturnValueOnce(sources.promise)
    api.listSchemas.mockReturnValueOnce(schemas.promise); api.listAdminActions.mockReturnValueOnce(actions.promise); api.getRetention.mockReturnValueOnce(retention.promise)
    page = mount(Page, { target, props: { tenantId: 'tenant-a' } }); await settle()
    for (const text of ['正在读取租户目录', '正在读取来源', '正在读取 Schema', '正在读取管理自审计', '正在读取当前租户的保留策略']) expect(target.textContent).toContain(text)
    expect(target.textContent).not.toContain('暂无来源')
    expect(ready()).toBe(false)
    schemas.resolve(list(schema())); sources.resolve(list()); tenants.reject(new Error('tenant read failed'))
    actions.reject(new Error('trail read failed')); retention.reject(new ApiError(404, 'not_found', 'missing'))
    await settle()
    expect(card('租户与配额').textContent).toContain('tenant read failed')
    expect(card('来源系统与 Client 绑定').textContent).toContain('暂无来源')
    expect(card('事件 Schema').textContent).toContain('aero.im.security')
    expect(card('管理操作自审计').textContent).toContain('trail read failed')
    expect(card('保留策略').textContent).toContain('尚未配置保留策略')
    // Successful empty dependencies are known (and invalid), not unknown reads.
    expect(card('平台审计接入状态').textContent).toContain('来源未登记')
    button('重试', card('租户与配额')).click(); await settle()
    expect(card('租户与配额').textContent).toContain('Tenant A')
    expect(api.listSources).toHaveBeenCalledTimes(1); expect(api.listSchemas).toHaveBeenCalledTimes(1)
    button('重试', card('管理操作自审计')).click(); await settle()
    expect(card('管理操作自审计').textContent).toContain('current action')
    expect(api.getRetention).toHaveBeenCalledTimes(1)
    api.getRetention.mockRejectedValueOnce(new ApiError(503, 'store_timeout', 'failed'))
    button('刷新保留策略').click(); await settle()
    expect(card('保留策略').textContent).toContain('服务暂时不可用')
    expect(card('保留策略').textContent).not.toContain('尚未配置保留策略')
    button('重试', card('保留策略')).click(); await settle()
    expect(card('保留策略').textContent).toContain('保存策略')
    expect(api.listAdminActions).toHaveBeenCalledTimes(2)
  })

  it('keeps Schema and other sections usable when sources fail; a pair retry revokes old verdicts immediately', async () => {
    api.listSources.mockRejectedValueOnce(new Error('source read failed'))
    page = mount(Page, { target, props: { tenantId: 'tenant-a' } }); await settle()
    expect(card('平台审计接入状态').textContent).toContain('契约状态未知')
    expect(card('事件 Schema').textContent).toContain('aero.im.security')
    expect(card('管理操作自审计').textContent).toContain('current action')
    button('重试', card('来源系统与 Client 绑定')).click(); await settle()
    expect(ready()).toBe(true)
    expect(api.listAdminActions).toHaveBeenCalledTimes(1); expect(api.getRetention).toHaveBeenCalledTimes(1)
    const oldSource = deferred<ReturnType<typeof list<SourceSystem>>>()
    const oldSchema = deferred<ReturnType<typeof list<EventSchema>>>()
    api.listSources.mockReturnValueOnce(oldSource.promise); api.listSchemas.mockReturnValueOnce(oldSchema.promise)
    button('刷新来源').click(); await settle()
    expect(ready()).toBe(false)
    expect(card('平台审计接入状态').textContent).toContain('契约状态未知')
    const oldSignal = api.listSources.mock.lastCall?.[1] as AbortSignal
    const currentSource = deferred<ReturnType<typeof list<SourceSystem>>>()
    const currentSchema = deferred<ReturnType<typeof list<EventSchema>>>()
    api.listSources.mockReturnValueOnce(currentSource.promise); api.listSchemas.mockReturnValueOnce(currentSchema.promise)
    button('刷新 Schema').click(); await settle()
    expect(oldSignal.aborted).toBe(true)
    currentSource.resolve(list(source())); oldSchema.resolve(list(schema())); await settle()
    expect(ready()).toBe(false) // different epochs must never be combined
    expect(card('事件 Schema').textContent).toContain('正在读取 Schema')
    currentSchema.resolve(list(schema())); await settle()
    expect(ready()).toBe(true)
    oldSource.reject(new Error('stale error')); await settle()
    expect(ready()).toBe(true); expect(target.textContent).not.toContain('stale error')
  })

  it('drops ignored-abort A → B → A results, including old errors and retention data', async () => {
    const oldSource = deferred<ReturnType<typeof list<SourceSystem>>>()
    const oldSchema = deferred<ReturnType<typeof list<EventSchema>>>()
    const oldRetention = deferred<typeof policy>()
    api.listSources.mockReturnValueOnce(oldSource.promise); api.listSchemas.mockReturnValueOnce(oldSchema.promise); api.getRetention.mockReturnValueOnce(oldRetention.promise)
    page = mount(Harness, { target }); await settle()
    const signal = api.listSources.mock.calls[0][1] as AbortSignal
    api.listSources.mockResolvedValueOnce(list(source('tenant-b'))); api.listSchemas.mockResolvedValueOnce(list(schema('tenant-b')))
    api.getRetention.mockResolvedValueOnce({ ...policy, tenant_id: 'tenant-b' })
    button('选择 B').click(); await settle(); button('选择 A').click(); await settle()
    expect(signal.aborted).toBe(true); expect(ready()).toBe(true)
    oldSource.resolve(list(source('tenant-a', 'STALE SOURCE'))); oldSchema.reject(new Error('STALE SCHEMA'))
    oldRetention.resolve({ ...policy, retention_class: 'STALE RETENTION' }); await settle()
    expect(target.textContent).not.toContain('STALE')
    expect([...target.querySelectorAll('input')].some(input => input.value === 'STALE RETENTION')).toBe(false)
    expect(ready()).toBe(true)
  })

  it('clears and fences requests on token/permission changes, and never reads without permission', async () => {
    login(false)
    const old = deferred<ReturnType<typeof list<SourceSystem>>>()
    api.listSources.mockReturnValueOnce(old.promise)
    page = mount(Page, { target, props: { tenantId: 'tenant-a' } }); await settle()
    const signal = api.listSources.mock.calls[0][1] as AbortSignal
    login(false, [], 'new-token'); await settle()
    old.resolve(list(source('tenant-a', 'OLD PRIVATE SOURCE'))); await settle()
    expect(signal.aborted).toBe(true); expect(target.textContent).not.toContain('OLD PRIVATE SOURCE')
    expect(target.textContent).toContain('没有治理目录读取权限')
    expect(api.listSources).toHaveBeenCalledTimes(1); expect(api.listTenants).not.toHaveBeenCalled()
    login(false, ['audit:policy:read'], 'another-token'); await settle()
    expect(api.listSources).toHaveBeenCalledTimes(2); expect(ready()).toBe(true)
  })

  it('treats foreign-tenant dependencies as errors, never as a valid contract', async () => {
    api.listSources.mockResolvedValue(list(source('tenant-b')))
    page = mount(Page, { target, props: { tenantId: 'tenant-a' } }); await settle()
    expect(card('平台审计接入状态').textContent).toContain('契约状态未知')
    expect(card('来源系统与 Client 绑定').textContent).toContain('目录记录与当前租户不匹配')
    expect(card('事件 Schema').textContent).toContain('aero.im.security')
  })
})
