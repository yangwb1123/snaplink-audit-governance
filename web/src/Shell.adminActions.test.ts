// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import type { UserSession } from './auth/types'
import type { AdminAction } from './api/types'
import { authState } from './auth/store'
import Harness from './Shell.testHarness.svelte'

const { api } = vi.hoisted(() => ({ api: {
  listTenants: vi.fn(), listAdminActions: vi.fn(), listSources: vi.fn(), listSchemas: vi.fn(), getRetention: vi.fn(), createTenant: vi.fn(),
  queryEvents: vi.fn(), getEventFacets: vi.fn(), getGovernanceStatus: vi.fn(),
} }))
vi.mock('./api/useAuditApi', () => ({ useAuditApi: () => api }))
function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}
const tenants = { items: [{ id: 'tenant-a', name: 'A' }, { id: 'tenant-b', name: 'B' }], count: 2 }
const record = (name: string, tenant = 'tenant-a'): AdminAction => ({ id: name, action: name, actor: 'operator', target_id: 'target', tenant_id: tenant, created_at: '' })
const list = (...items: AdminAction[]) => ({ items, count: items.length })
function login(overrides: Partial<UserSession> = {}) {
  authState.set({ status: 'authenticated', session: { accessToken: 'access', idToken: 'id', subject: 'operator', displayName: 'Operator', expiresAt: Date.now() + 100000, tenantId: 'tenant-a', platform: false, roles: [], permissions: new Set(['audit:policy:read']), ...overrides }, error: null })
}
let target: HTMLDivElement
let component: ReturnType<typeof mount> | undefined
async function settle() { await tick(); await Promise.resolve(); await tick(); await tick() }
function content() { return target.querySelector<HTMLElement>('[data-iris-admin-content]')! }
function button(label: string, root: ParentNode = target) {
  const found = [...root.querySelectorAll('button')].find(node => node.textContent?.trim() === label)
  if (!found) throw new Error(`Missing button: ${label}`)
  return found
}
async function navigate(hash: string) { window.location.hash = hash; window.dispatchEvent(new HashChangeEvent('hashchange')); await settle() }
async function chooseTenant(tenant: string) {
  const select = target.querySelector<HTMLSelectElement>('#tenant-scope')!
  // happy-dom 20 matches :checked only for INPUT, not OPTION. Supply the
  // browser's selected-option lookup for Svelte's native select binding;
  // the DOM event, Shell, routed page and all request epochs remain real.
  const query = select.querySelector.bind(select)
  const lookup = vi.spyOn(select, 'querySelector').mockImplementation(selector =>
    selector === ':checked' ? [...select.options].find(option => option.selected) ?? null : query(selector),
  )
  select.value = tenant
  expect(select.value).toBe(tenant)
  select.dispatchEvent(new Event('change', { bubbles: true }))
  lookup.mockRestore()
  await settle()
}
function filters() { return [...content().querySelectorAll<HTMLInputElement>('input[aria-label^="筛选"]')] }
async function draft() { for (const input of filters()) { input.value = 'private-filter'; input.dispatchEvent(new Event('input', { bubbles: true })) }; await settle() }

beforeEach(() => {
  vi.resetAllMocks(); login()
  window.history.replaceState(null, '', '#admin-actions')
  api.listTenants.mockResolvedValue(tenants)
  api.listAdminActions.mockImplementation((tenant: string) => Promise.resolve(list(record(`CURRENT ${tenant}`, tenant))))
  api.listSources.mockResolvedValue({ items: [], count: 0 }); api.listSchemas.mockResolvedValue({ items: [], count: 0 })
  api.getRetention.mockImplementation((tenant: string) => Promise.resolve({ tenant_id: tenant, hot_days: 1, warm_days: 2, archive_days: 3, retention_class: 'standard' }))
  target = document.createElement('div'); document.body.append(target)
})
afterEach(async () => {
  if (component) await unmount(component)
  component = undefined; target.remove()
  authState.set({ status: 'anonymous', session: null, error: null })
  window.history.replaceState(null, '', '#')
})

describe('Shell real admin-actions route and catalog preview', () => {
  it('serves a policy-read-only subject by direct route and static menu, retaining the catalog preview and link', async () => {
    component = mount(Harness, { target }); await settle()
    expect(content().textContent).toContain('CURRENT tenant-a')
    expect(api.listAdminActions).toHaveBeenCalledWith('tenant-a', 100, expect.any(AbortSignal))
    expect(api.queryEvents).not.toHaveBeenCalled(); expect(api.listTenants).not.toHaveBeenCalled()
    button('治理目录').click(); window.dispatchEvent(new HashChangeEvent('hashchange')); await settle()
    expect(window.location.hash).toBe('#catalog')
    expect(content().textContent).toContain('管理操作自审计')
    expect(content().textContent).toContain('CURRENT tenant-a')
    expect(content().querySelector('a[href="#admin-actions"]')?.textContent).toContain('本地筛选')
    button('管理自审计').click(); window.dispatchEvent(new HashChangeEvent('hashchange')); await settle()
    expect(window.location.hash).toBe('#admin-actions')
    expect(content().querySelector('input[aria-label="筛选动作"]')).not.toBeNull()
    expect(api.listAdminActions).toHaveBeenCalledTimes(3)
    for (const call of api.listAdminActions.mock.calls) expect(call.slice(0, 2)).toEqual(['tenant-a', 100])
    expect(api.createTenant).not.toHaveBeenCalled()
  })

  it.each(['#admin-actions', '#catalog'])('never reads %s without policy permission or a session', async (hash) => {
    login({ permissions: new Set() }); window.history.replaceState(null, '', hash)
    component = mount(Harness, { target }); await settle()
    expect(api.listAdminActions).not.toHaveBeenCalled(); expect(api.listSources).not.toHaveBeenCalled()
    expect([...target.querySelectorAll('button')].some(node => node.textContent?.trim() === '管理自审计')).toBe(false)
    await unmount(component); component = undefined
    authState.set({ status: 'anonymous', session: null, error: null })
    window.history.replaceState(null, '', hash)
    component = mount(Harness, { target }); await settle()
    expect(api.listAdminActions).not.toHaveBeenCalled(); expect(api.listTenants).not.toHaveBeenCalled()
  })

  it.each(['#admin-actions', '#catalog'])('emits zero self-audit requests for unconfirmed platform scope on %s', async (hash) => {
    login({ platform: true, permissions: new Set(), tenantId: '' })
    window.history.replaceState(null, '', hash)
    const pending = deferred<typeof tenants>()
    // Catalog bootstrap may read the tenant directory independently; neither
    // read is an admin-actions read or confirmation of the Shell scope.
    api.listTenants.mockImplementation(() => pending.promise)
    component = mount(Harness, { target }); await settle()
    expect(api.listAdminActions).not.toHaveBeenCalled()
    expect(api.listSources).not.toHaveBeenCalled(); expect(api.getRetention).not.toHaveBeenCalled()
    pending.reject(new Error('tenant directory unavailable')); await settle()
    expect(api.listAdminActions).not.toHaveBeenCalled()
    if (hash === '#catalog') {
      expect(content().textContent).toContain('创建租户')
      expect(button('刷新自审计').disabled).toBe(true)
      button('刷新自审计').click(); button('刷新', content()).click(); await settle()
      expect(api.listAdminActions).not.toHaveBeenCalled()
    } else expect(content().textContent).toContain('无法读取可用租户')
  })

  it('keeps platform tenant creation available without an audit scope, then reads only after Shell confirms the new tenant', async () => {
    login({ platform: true, permissions: new Set(), tenantId: '' })
    window.history.replaceState(null, '', '#catalog')
    api.listTenants.mockResolvedValue({ items: [], count: 0 })
    component = mount(Harness, { target }); await settle()
    expect(api.listAdminActions).not.toHaveBeenCalled()
    expect(content().textContent).toContain('创建租户')
    const card = [...content().querySelectorAll('h3')].find(h => h.textContent === '租户与配额')!.parentElement!
    const inputs = card.querySelectorAll('input')
    for (const [index, value] of ['tenant-a', 'A'].entries()) { inputs[index].value = value; inputs[index].dispatchEvent(new Event('input', { bubbles: true })) }
    await settle(); expect(button('创建租户', card).disabled).toBe(false)
    api.createTenant.mockResolvedValue(tenants.items[0]); api.listTenants.mockResolvedValue(tenants)
    button('创建租户', card).click(); await settle(); await settle()
    expect(api.createTenant).toHaveBeenCalledTimes(1)
    expect(target.querySelector<HTMLSelectElement>('#tenant-scope')!.value).toBe('tenant-a')
    expect(content().textContent).toContain('CURRENT tenant-a')
    expect(api.listAdminActions).toHaveBeenCalledTimes(1)
    expect(api.listAdminActions.mock.lastCall?.slice(0, 2)).toEqual(['tenant-a', 100])
  })

  it.each(['success', 'failure'] as const)('fences ignored-abort %s after actual Shell tenant A → B → A selection', async (outcome) => {
    login({ platform: true, permissions: new Set() })
    const old = deferred<ReturnType<typeof list>>()
    api.listAdminActions.mockReturnValueOnce(old.promise)
    component = mount(Harness, { target }); await settle(); await draft()
    const signal = api.listAdminActions.mock.calls[0][2] as AbortSignal
    await chooseTenant('tenant-b')
    expect(filters()).toHaveLength(3); for (const input of filters()) expect(input.value).toBe('')
    expect(content().textContent).toContain('CURRENT tenant-b')
    await draft(); await chooseTenant('tenant-a')
    for (const input of filters()) expect(input.value).toBe('')
    expect(content().textContent).toContain('CURRENT tenant-a'); expect(signal.aborted).toBe(true)
    if (outcome === 'success') old.resolve(list(record('STALE PRIVATE')))
    else old.reject(new Error('STALE PRIVATE ERROR'))
    await settle(); expect(content().textContent).not.toContain('STALE')
    expect(api.listAdminActions.mock.calls.map(call => call.slice(0, 2))).toEqual([['tenant-a', 100], ['tenant-b', 100], ['tenant-a', 100]])
  })

  const changes: Array<[string, Partial<UserSession>]> = [
    ['subject', { subject: 'new-subject' }], ['access token', { accessToken: 'new-access' }],
    ['ID token', { idToken: 'new-id' }], ['permissions', { permissions: new Set(['audit:policy:read', 'audit:event:read']) }],
    ['revoked permission', { permissions: new Set() }],
  ]
  for (const outcome of ['success', 'failure'] as const) {
    it.each(changes)(`fences old ${outcome} and clears drafts on Shell %s change`, async (_name, change) => {
      const old = deferred<ReturnType<typeof list>>()
      api.listAdminActions.mockReturnValueOnce(old.promise)
      component = mount(Harness, { target }); await settle(); await draft()
      const signal = api.listAdminActions.mock.calls[0][2] as AbortSignal
      login(change); await settle()
      expect(signal.aborted).toBe(true)
      if (change.permissions?.size === 0) expect(filters()).toHaveLength(0)
      else { expect(filters()).toHaveLength(3); for (const input of filters()) expect(input.value).toBe('') }
      if (outcome === 'success') old.resolve(list(record('STALE PRIVATE')))
      else old.reject(new Error('STALE PRIVATE ERROR'))
      await settle(); expect(content().textContent).not.toContain('STALE')
      expect(api.listAdminActions).toHaveBeenCalledTimes(change.permissions?.size === 0 ? 1 : 2)
    })
  }

  it('requires a fresh platform tenant confirmation after an ID-token change', async () => {
    login({ platform: true, permissions: new Set() })
    component = mount(Harness, { target }); await settle()
    await draft()
    const pending = deferred<typeof tenants>()
    api.listTenants.mockReturnValueOnce(pending.promise)
    login({ platform: true, permissions: new Set(), idToken: 'new-id' }); await settle()
    expect(content().textContent).toContain('正在确认租户范围')
    expect(api.listAdminActions).toHaveBeenCalledTimes(1); expect(filters()).toHaveLength(0)
    pending.resolve(tenants); await settle()
    expect(api.listAdminActions).toHaveBeenCalledTimes(2)
    for (const input of filters()) expect(input.value).toBe('')
  })

  it.each(['#admin-actions/extra', '#admin-actions?tenant_id=other', '#admin%2Dactions', '#events/event/bad%2Fid'])(
    'does not turn unsafe static-route or identifier input into a self-audit request: %s', async (hash) => {
      window.history.replaceState(null, '', hash)
      component = mount(Harness, { target }); await settle()
      expect(api.listAdminActions).not.toHaveBeenCalled()
      expect(content().querySelector('input[aria-label="筛选动作"]')).toBeNull()
    },
  )

  it.each(['success', 'failure'] as const)('ignores late %s after navigation unmounts the page', async (outcome) => {
    const old = deferred<ReturnType<typeof list>>()
    api.listAdminActions.mockReturnValueOnce(old.promise)
    component = mount(Harness, { target }); await settle(); await draft()
    const signal = api.listAdminActions.mock.calls[0][2] as AbortSignal
    await navigate('#catalog'); expect(signal.aborted).toBe(true)
    if (outcome === 'success') old.resolve(list(record('STALE PRIVATE')))
    else old.reject(new Error('STALE PRIVATE ERROR'))
    await settle(); expect(content().textContent).not.toContain('STALE')
    await navigate('#admin-actions')
    for (const input of filters()) expect(input.value).toBe('')
    expect(content().textContent).toContain('CURRENT tenant-a')
  })
})
