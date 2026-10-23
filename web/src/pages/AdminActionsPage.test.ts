// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { get } from 'svelte/store'
import type { AdminAction } from '../api/types'
import type { UserSession } from '../auth/types'
import { authState } from '../auth/store'
import Page from './AdminActionsPage.svelte'
import Harness from './AdminActionsPage.testHarness.svelte'

const { api } = vi.hoisted(() => ({ api: {
  listAdminActions: vi.fn(), createTenant: vi.fn(), createSource: vi.fn(), createSchema: vi.fn(), setRetention: vi.fn(),
} }))
vi.mock('../api/useAuditApi', () => ({ useAuditApi: () => api }))
function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}
const action = (id: string, tenant = 'tenant-a'): AdminAction => ({ id, tenant_id: tenant, action: id, actor: 'Alice', target_type: 'source', target_id: 'Vault-A', detail: `detail ${id}`, created_at: '2026-01-01T00:00:00Z' })
const list = (...items: AdminAction[]) => ({ items, count: 987654321 })
function login(overrides: Partial<UserSession> = {}) {
  authState.set({ status: 'authenticated', session: { accessToken: 'access', idToken: 'id', subject: 'operator', displayName: 'Operator', expiresAt: Date.now() + 100000, tenantId: 'tenant-a', platform: false, roles: [], permissions: new Set(['audit:policy:read']), ...overrides }, error: null })
}
let target: HTMLDivElement
let page: ReturnType<typeof mount> | undefined
async function settle() { await tick(); await Promise.resolve(); await tick() }
function button(label: string) {
  const found = [...target.querySelectorAll('button')].find(node => node.textContent?.trim() === label)
  if (!found) throw new Error(`Missing button: ${label}`)
  return found
}
function input(label: string) { return target.querySelector<HTMLInputElement>(`input[aria-label="${label}"]`)! }
async function filter(label: string, value: string) { const node = input(label); node.value = value; node.dispatchEvent(new Event('input', { bubbles: true })); await settle() }
function rows() { return [...target.querySelectorAll('tbody tr')].map(row => row.querySelectorAll('td')[1].textContent) }
function filtersEmpty() { for (const label of ['筛选动作', '筛选操作者', '筛选目标']) expect(input(label).value).toBe('') }
function readOnly() { for (const [name, method] of Object.entries(api)) if (name !== 'listAdminActions') expect(method).not.toHaveBeenCalled() }

beforeEach(() => {
  vi.resetAllMocks(); login()
  window.history.replaceState(null, '', '#admin-actions')
  api.listAdminActions.mockResolvedValue(list(action('CURRENT')))
  target = document.createElement('div'); document.body.append(target)
})
afterEach(async () => {
  if (page) await unmount(page)
  page = undefined; target.remove()
  authState.set({ status: 'anonymous', session: null, error: null })
  window.history.replaceState(null, '', '#')
  vi.restoreAllMocks()
})

describe('real recent admin-actions page', () => {
  it('loads an explicit tenant with limit=100, retains server order and performs memory-only local AND filtering', async () => {
    const pending = deferred<ReturnType<typeof list>>()
    api.listAdminActions.mockReturnValueOnce(pending.promise)
    page = mount(Page, { target }); await settle()
    expect(target.textContent).toContain('正在读取最近管理自审计')
    expect(button('刷新最近记录').disabled).toBe(true)
    expect(api.listAdminActions).toHaveBeenCalledWith('tenant-a', 100, expect.any(AbortSignal))
    pending.resolve(list(
      { ...action('z-source.updated'), created_at: '2025-01-01' },
      { ...action('a-source.updated'), actor: 'Bob', target_id: 'Vault-B', created_at: '2026-10-01' },
      action('m-export.created'),
    )); await settle()
    expect(rows()).toEqual(['z-source.updated', 'a-source.updated', 'm-export.created'])
    expect(target.textContent).toContain('已载入 3 条 · 筛选命中 3 条')
    expect(target.textContent).toContain('最近最多 100 条')
    expect(target.textContent).toContain('即使不足 100 条也不表示完整')
    expect(target.textContent).not.toContain('987654321')
    expect(target.textContent).not.toContain('下一页')
    const url = window.location.href
    // Reject even accessing persistence; do not depend on Node's optional
    // --localstorage-file facility when running the happy-dom environment.
    const writes = [vi.spyOn(window, 'localStorage', 'get'), vi.spyOn(window, 'sessionStorage', 'get')]
    const navigation = [vi.spyOn(window.history, 'pushState'), vi.spyOn(window.history, 'replaceState')]
    const logs = [vi.spyOn(console, 'log'), vi.spyOn(console, 'info'), vi.spyOn(console, 'warn'), vi.spyOn(console, 'error'), vi.spyOn(console, 'debug')]
    await filter('筛选动作', ' UPDATED '); await filter('筛选操作者', ' ALI '); await filter('筛选目标', ' vault-a ')
    expect(rows()).toEqual(['z-source.updated'])
    expect(target.textContent).toContain('已载入 3 条 · 筛选命中 1 条')
    await filter('筛选目标', 'private-no-match?actor=secret')
    expect(rows()).toEqual([])
    expect(target.textContent).toContain('没有筛选命中；未搜索更早记录')
    expect(target.textContent).not.toContain('列表为空')
    button('清空筛选').click(); await settle(); filtersEmpty()
    expect(rows()).toEqual(['z-source.updated', 'a-source.updated', 'm-export.created'])
    expect(api.listAdminActions).toHaveBeenCalledTimes(1)
    expect(window.location.href).toBe(url)
    for (const spy of [...writes, ...navigation, ...logs]) expect(spy).not.toHaveBeenCalled()
    readOnly()
  })

  it('distinguishes read failure, read-only retry, empty results and empty refresh', async () => {
    const pending = deferred<ReturnType<typeof list>>()
    api.listAdminActions.mockReturnValueOnce(pending.promise)
    page = mount(Page, { target, props: { tenantId: 'tenant-a' } }); await settle()
    pending.reject(new Error('read unavailable')); await settle()
    expect(target.textContent).toContain('read unavailable'); expect(rows()).toEqual([])
    expect(target.textContent).not.toContain('列表为空')
    api.listAdminActions.mockResolvedValueOnce(list())
    button('重试').click(); await settle()
    expect(target.textContent).toContain('当前租户最近记录列表为空；不代表全历史为空')
    expect(target.textContent).toContain('已载入 0 条 · 筛选命中 0 条')
    expect(target.textContent).not.toContain('没有筛选命中')
    button('刷新').click(); await settle()
    expect(rows()).toEqual(['CURRENT'])
    expect(api.listAdminActions).toHaveBeenCalledTimes(3)
    for (const call of api.listAdminActions.mock.calls) expect(call).toEqual(['tenant-a', 100, expect.any(AbortSignal)])
    readOnly()
  })

  it('renders malicious markup and URL-like fields as text, never HTML or links', async () => {
    const fields = {
      action: '<img src=x onerror="alert(1)">', actor: '<script>evil()</script>',
      target_type: '<svg onload="evil()">', target_id: 'javascript:alert(1)',
      detail: '<a href="https://evil.test/?token=secret">unsafe</a>',
    }
    api.listAdminActions.mockResolvedValue(list({ ...action('unsafe'), ...fields }))
    page = mount(Page, { target }); await settle()
    const body = target.querySelector('tbody')!
    for (const value of Object.values(fields)) expect(body.textContent).toContain(value)
    expect(body.querySelector('img, script, svg, a, iframe, [onerror], [onload]')).toBeNull()
    await filter('筛选目标', 'javascript:'); expect(rows()).toEqual([fields.action])
    expect(api.listAdminActions).toHaveBeenCalledTimes(1)
  })

  it.each(['foreign', 'missing'] as const)('rejects the entire %s-tenant response, not just foreign rows', async (kind) => {
    api.listAdminActions.mockResolvedValue(list(action('OWN MUST NOT RENDER'), { ...action('FOREIGN MUST NOT RENDER', 'tenant-b'), tenant_id: kind === 'missing' ? undefined : 'tenant-b' }))
    page = mount(Page, { target }); await settle()
    expect(target.textContent).toContain('管理自审计记录与当前租户不匹配')
    expect(target.textContent).toContain('已载入 0 条 · 筛选命中 0 条')
    expect(target.textContent).not.toContain('MUST NOT RENDER'); expect(rows()).toEqual([])
  })

  it('bounds an oversized response to 100 without sorting or asserting historical completeness', async () => {
    api.listAdminActions.mockResolvedValue(list(...Array.from({ length: 101 }, (_, i) => action(`server-${100 - i}`))))
    page = mount(Page, { target }); await settle()
    expect(rows()).toHaveLength(100)
    expect(rows()[0]).toBe('server-100'); expect(rows()[99]).toBe('server-1')
    expect(target.textContent).toContain('已载入 100 条 · 筛选命中 100 条')
    expect(target.textContent).not.toContain('987654321')
  })

  it.each(['anonymous', 'no permission', 'unselected platform', 'foreign scope', 'missing signed tenant'])(
    'makes zero self-audit requests for %s', async (kind) => {
      let tenantId: string | undefined
      if (kind === 'anonymous') authState.set({ status: 'anonymous', session: null, error: null })
      if (kind === 'no permission') login({ permissions: new Set() })
      if (kind === 'unselected platform') login({ platform: true, permissions: new Set(), tenantId: 'signed-not-confirmed' })
      if (kind === 'foreign scope') tenantId = 'tenant-b'
      if (kind === 'missing signed tenant') login({ tenantId: '' })
      page = mount(Page, { target, props: { tenantId } }); await settle()
      expect(button('刷新最近记录').disabled).toBe(true)
      button('刷新最近记录').click(); await settle()
      expect(api.listAdminActions).not.toHaveBeenCalled(); expect(rows()).toEqual([])
    },
  )
})

describe('ignored-AbortSignal isolation', () => {
  it.each(['success', 'failure'] as const)('fences old %s for tenant A → B → A and clears records and all filters', async (outcome) => {
    login({ platform: true, permissions: new Set() }) // preserve can's platform semantics
    page = mount(Harness, { target }); await settle()
    expect(rows()).toEqual(['CURRENT'])
    await filter('筛选动作', 'CURRENT'); await filter('筛选操作者', 'Alice'); await filter('筛选目标', 'Vault')
    const firstA = deferred<ReturnType<typeof list>>(), oldB = deferred<ReturnType<typeof list>>(), currentA = deferred<ReturnType<typeof list>>()
    api.listAdminActions.mockReturnValueOnce(firstA.promise).mockReturnValueOnce(oldB.promise).mockReturnValueOnce(currentA.promise)
    button('刷新最近记录').click(); await settle()
    const oldASignal = api.listAdminActions.mock.calls[1][2] as AbortSignal
    button('租户 B').click(); await settle(); filtersEmpty()
    expect(rows()).toEqual([]); expect(target.textContent).not.toContain('detail CURRENT')
    await filter('筛选动作', 'B-filter')
    const oldBSignal = api.listAdminActions.mock.calls[2][2] as AbortSignal
    button('租户 A').click(); await settle(); filtersEmpty()
    expect(rows()).toEqual([]); expect(target.textContent).toContain('正在读取最近管理自审计')
    expect(oldASignal.aborted).toBe(true); expect(oldBSignal.aborted).toBe(true)
    currentA.resolve(list(action('FRESH A'))); await settle()
    for (const pending of [firstA, oldB]) {
      if (outcome === 'success') pending.resolve(list(action('STALE PRIVATE')))
      else pending.reject(new Error('STALE PRIVATE ERROR'))
    }
    await settle(); expect(rows()).toEqual(['FRESH A']); expect(target.textContent).not.toContain('STALE')
    expect(api.listAdminActions.mock.calls.map(call => call.slice(0, 2))).toEqual([['tenant-a', 100], ['tenant-a', 100], ['tenant-b', 100], ['tenant-a', 100]])
  })

  const changes: Array<[string, Partial<UserSession>]> = [
    ['subject', { subject: 'another-operator' }], ['access token', { accessToken: 'renewed-access' }],
    ['ID token', { idToken: 'renewed-id' }], ['permissions retained', { permissions: new Set(['audit:policy:read', 'audit:event:read']) }],
    ['permissions revoked', { permissions: new Set() }],
  ]
  for (const outcome of ['success', 'failure'] as const) {
    it.each(changes)(`discards late ${outcome} after %s changes and erases old records/filters`, async (_name, change) => {
      page = mount(Page, { target }); await settle()
      await filter('筛选动作', 'CURRENT'); await filter('筛选操作者', 'Alice'); await filter('筛选目标', 'Vault')
      const old = deferred<ReturnType<typeof list>>(), fresh = deferred<ReturnType<typeof list>>()
      api.listAdminActions.mockReturnValueOnce(old.promise).mockReturnValueOnce(fresh.promise)
      button('刷新最近记录').click(); await settle()
      const signal = api.listAdminActions.mock.calls[1][2] as AbortSignal
      login(change); await settle()
      expect(signal.aborted).toBe(true); expect(rows()).toEqual([])
      const revoked = change.permissions?.size === 0
      if (!revoked) { filtersEmpty(); expect(target.textContent).toContain('正在读取最近管理自审计') }
      else { expect(target.querySelector('input')).toBeNull(); expect(api.listAdminActions).toHaveBeenCalledTimes(2) }
      if (outcome === 'success') old.resolve(list(action('STALE PRIVATE')))
      else old.reject(new Error('STALE PRIVATE ERROR'))
      await settle(); expect(rows()).toEqual([]); expect(target.textContent).not.toContain('STALE')
      if (!revoked) { fresh.resolve(list(action('FRESH'))); await settle(); expect(rows()).toEqual(['FRESH']) }
      else { login(); await settle(); filtersEmpty(); fresh.resolve(list(action('RESTORED'))); await settle(); expect(rows()).toEqual(['RESTORED']) }
    })
  }

  it('observes permission changes even when the existing Set and session object are reused', async () => {
    const pending = deferred<ReturnType<typeof list>>()
    api.listAdminActions.mockReturnValueOnce(pending.promise)
    page = mount(Page, { target }); await settle()
    const state = get(authState)
    state.session!.permissions.clear(); authState.set(state); await settle()
    pending.resolve(list(action('STALE PRIVATE'))); await settle()
    expect(rows()).toEqual([]); expect(target.textContent).toContain('没有管理自审计读取权限')
    expect(api.listAdminActions).toHaveBeenCalledTimes(1)
  })

  it.each(['success', 'failure'] as const)('disposes late %s on unmount without contaminating a new instance', async (outcome) => {
    const old = deferred<ReturnType<typeof list>>()
    api.listAdminActions.mockReturnValueOnce(old.promise)
    page = mount(Page, { target }); await settle()
    const signal = api.listAdminActions.mock.calls[0][2] as AbortSignal
    await filter('筛选动作', 'private-filter')
    await unmount(page); page = undefined
    expect(signal.aborted).toBe(true); expect(target.textContent).toBe('')
    page = mount(Page, { target }); await settle(); filtersEmpty()
    if (outcome === 'success') old.resolve(list(action('STALE PRIVATE')))
    else old.reject(new Error('STALE PRIVATE ERROR'))
    await settle(); expect(rows()).toEqual(['CURRENT']); expect(target.textContent).not.toContain('STALE')
  })
})
