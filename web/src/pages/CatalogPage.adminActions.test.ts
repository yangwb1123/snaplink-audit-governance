// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import type { AdminAction } from '../api/types'
import type { UserSession } from '../auth/types'
import { authState } from '../auth/store'
import Page from './CatalogPage.svelte'
import Harness from './CatalogPage.testHarness.svelte'

const { api } = vi.hoisted(() => ({ api: {
  listTenants: vi.fn(), listSources: vi.fn(), listSchemas: vi.fn(), listAdminActions: vi.fn(), getRetention: vi.fn(),
  createTenant: vi.fn(), createSource: vi.fn(), createSchema: vi.fn(), setRetention: vi.fn(),
} }))
vi.mock('../api/useAuditApi', () => ({ useAuditApi: () => api }))
function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}
const action = (name: string, tenant = 'tenant-a'): AdminAction => ({ id: name, action: name, actor: 'operator', target_id: 'target', detail: `detail ${name}`, tenant_id: tenant, created_at: '' })
const list = (...items: AdminAction[]) => ({ items, count: 987654321 })
function login(overrides: Partial<UserSession> = {}) {
  authState.set({ status: 'authenticated', session: { accessToken: 'access', idToken: 'id', subject: 'operator', displayName: 'Operator', expiresAt: Date.now() + 100000, tenantId: 'tenant-a', platform: false, roles: [], permissions: new Set(['audit:policy:read']), ...overrides }, error: null })
}
let target: HTMLDivElement
let page: ReturnType<typeof mount> | undefined
async function settle() { await tick(); await Promise.resolve(); await tick() }
function card() { return [...target.querySelectorAll('h3')].find(node => node.textContent === '管理操作自审计')!.parentElement! }
function button(label: string, root: ParentNode = target) {
  const found = [...root.querySelectorAll('button')].find(node => node.textContent?.trim() === label)
  if (!found) throw new Error(`Missing button: ${label}`)
  return found
}
function readOnly() { for (const name of ['createTenant', 'createSource', 'createSchema', 'setRetention'] as const) expect(api[name]).not.toHaveBeenCalled() }
beforeEach(() => {
  vi.resetAllMocks(); login()
  api.listTenants.mockResolvedValue({ items: [], count: 0 }); api.listSources.mockResolvedValue({ items: [], count: 0 }); api.listSchemas.mockResolvedValue({ items: [], count: 0 })
  api.listAdminActions.mockImplementation((tenant: string) => Promise.resolve(list(action(`CURRENT ${tenant}`, tenant))))
  api.getRetention.mockImplementation((tenant?: string) => Promise.resolve({ tenant_id: tenant ?? 'tenant-a', hot_days: 1, warm_days: 2, archive_days: 3, retention_class: 'standard' }))
  target = document.createElement('div'); document.body.append(target)
})
afterEach(async () => { if (page) await unmount(page); page = undefined; target.remove(); authState.set({ status: 'anonymous', session: null, error: null }) })

describe('real catalog self-audit preview boundary', () => {
  it('preserves the policy-only preview, server order, explicit tenant, fixed limit and static local-filter link', async () => {
    api.listAdminActions.mockResolvedValue(list(action('z-first'), action('a-second')))
    page = mount(Page, { target }); await settle()
    expect(api.listAdminActions).toHaveBeenCalledWith('tenant-a', 100, expect.any(AbortSignal))
    expect(card().textContent).toContain('已载入 2 条')
    expect(card().textContent).toContain('非全量历史')
    expect(card().textContent).toContain('不足 100 条也不代表完整')
    expect(card().textContent!.indexOf('z-first')).toBeLessThan(card().textContent!.indexOf('a-second'))
    expect(card().querySelector('a')?.getAttribute('href')).toBe('#admin-actions')
    expect(card().textContent).not.toContain('987654321')
    expect(api.listTenants).not.toHaveBeenCalled(); readOnly()
  })

  it('shows loading, failure and read-only retry independently, then explicit empty results', async () => {
    const pending = deferred<ReturnType<typeof list>>()
    api.listAdminActions.mockReturnValueOnce(pending.promise)
    page = mount(Page, { target, props: { tenantId: 'tenant-a' } }); await settle()
    expect(card().textContent).toContain('正在读取管理自审计')
    expect(card().textContent).not.toContain('暂无管理自审计记录')
    pending.reject(new Error('preview read failed')); await settle()
    expect(card().textContent).toContain('preview read failed')
    api.listAdminActions.mockResolvedValueOnce(list())
    button('重试', card()).click(); await settle()
    expect(card().textContent).toContain('暂无管理自审计记录')
    expect(card().textContent).toContain('已载入 0 条')
    button('刷新', card()).click(); await settle()
    expect(card().textContent).toContain('CURRENT tenant-a')
    for (const call of api.listAdminActions.mock.calls) expect(call.slice(0, 2)).toEqual(['tenant-a', 100])
    expect(api.listSources).toHaveBeenCalledTimes(1); readOnly()
  })

  it.each(['anonymous', 'no permission', 'unselected platform', 'foreign scope', 'missing signed tenant'])(
    'never requests the self-audit preview for %s, including manual refresh', async (kind) => {
      let tenantId: string | undefined
      if (kind === 'anonymous') authState.set({ status: 'anonymous', session: null, error: null })
      if (kind === 'no permission') login({ permissions: new Set() })
      if (kind === 'unselected platform') login({ platform: true, permissions: new Set(), tenantId: 'signed-but-unconfirmed' })
      if (kind === 'foreign scope') tenantId = 'tenant-b'
      if (kind === 'missing signed tenant') login({ tenantId: '' })
      page = mount(Page, { target, props: { tenantId } }); await settle()
      button('刷新').click(); await settle()
      if (kind === 'unselected platform') {
        expect(target.textContent).toContain('创建租户')
        expect(button('刷新自审计').disabled).toBe(true)
        button('刷新自审计').click(); await settle()
        expect(api.listTenants).toHaveBeenCalled()
      }
      expect(api.listAdminActions).not.toHaveBeenCalled(); readOnly()
    },
  )

  it('rejects mixed-tenant rows and renders markup and URL-like fields only as text after retry', async () => {
    api.listAdminActions.mockResolvedValueOnce(list(action('OWN MUST NOT RENDER'), action('FOREIGN MUST NOT RENDER', 'tenant-b')))
    page = mount(Page, { target }); await settle()
    expect(card().textContent).toContain('管理自审计记录与当前租户不匹配')
    expect(card().textContent).not.toContain('MUST NOT RENDER')
    const fields = { action: '<img src=x onerror="evil()">', actor: '<script>evil()</script>', target_id: 'javascript:alert(1)', detail: '<a href="https://evil.test">bad</a>' }
    api.listAdminActions.mockResolvedValueOnce(list({ ...action('unsafe'), ...fields }))
    button('重试', card()).click(); await settle()
    for (const value of Object.values(fields)) expect(card().textContent).toContain(value)
    expect(card().querySelector('tbody img, tbody script, tbody a, tbody [onerror]')).toBeNull()
    expect(card().querySelectorAll('a')).toHaveLength(1) // only the static page link
    readOnly()
  })

  it.each(['success', 'failure'] as const)('fences ignored-abort %s after preview tenant A → B → A', async (outcome) => {
    login({ platform: true, permissions: new Set() })
    const old = deferred<ReturnType<typeof list>>()
    api.listAdminActions.mockReturnValueOnce(old.promise)
    page = mount(Harness, { target }); await settle()
    const signal = api.listAdminActions.mock.calls[0][2] as AbortSignal
    button('选择 B').click(); await settle(); expect(card().textContent).toContain('CURRENT tenant-b')
    button('选择 A').click(); await settle(); expect(card().textContent).toContain('CURRENT tenant-a')
    expect(signal.aborted).toBe(true)
    if (outcome === 'success') old.resolve(list(action('STALE PRIVATE')))
    else old.reject(new Error('STALE PRIVATE ERROR'))
    await settle(); expect(card().textContent).not.toContain('STALE')
    expect(api.listAdminActions.mock.calls.map(call => call.slice(0, 2))).toEqual([['tenant-a', 100], ['tenant-b', 100], ['tenant-a', 100]])
  })

  const changes: Array<[string, Partial<UserSession>]> = [
    ['subject', { subject: 'new-operator' }], ['access token', { accessToken: 'new-access' }],
    ['ID token', { idToken: 'new-id' }], ['permissions', { permissions: new Set(['audit:policy:read', 'audit:event:read']) }],
    ['revoked permission', { permissions: new Set() }],
  ]
  for (const outcome of ['success', 'failure'] as const) {
    it.each(changes)(`clears the preview and fences late ${outcome} on %s change`, async (_name, change) => {
      page = mount(Page, { target }); await settle()
      expect(card().textContent).toContain('CURRENT tenant-a')
      const old = deferred<ReturnType<typeof list>>(), fresh = deferred<ReturnType<typeof list>>()
      api.listAdminActions.mockReturnValueOnce(old.promise).mockReturnValueOnce(fresh.promise)
      button('刷新自审计').click(); await settle()
      const signal = api.listAdminActions.mock.calls[1][2] as AbortSignal
      login(change); await settle()
      expect(signal.aborted).toBe(true); expect(target.textContent).not.toContain('CURRENT tenant-a')
      if (outcome === 'success') old.resolve(list(action('STALE PRIVATE')))
      else old.reject(new Error('STALE PRIVATE ERROR'))
      await settle(); expect(target.textContent).not.toContain('STALE')
      if (change.permissions?.size === 0) expect(api.listAdminActions).toHaveBeenCalledTimes(2)
      else { fresh.resolve(list(action('FRESH'))); await settle(); expect(card().textContent).toContain('FRESH') }
    })
  }

  it.each(['success', 'failure'] as const)('disposes a late preview %s on unmount', async (outcome) => {
    const old = deferred<ReturnType<typeof list>>()
    api.listAdminActions.mockReturnValueOnce(old.promise)
    page = mount(Page, { target }); await settle()
    const signal = api.listAdminActions.mock.calls[0][2] as AbortSignal
    await unmount(page); page = undefined; expect(signal.aborted).toBe(true)
    page = mount(Page, { target }); await settle()
    if (outcome === 'success') old.resolve(list(action('STALE PRIVATE')))
    else old.reject(new Error('STALE PRIVATE ERROR'))
    await settle(); expect(card().textContent).not.toContain('STALE'); expect(card().textContent).toContain('CURRENT tenant-a')
  })
})
