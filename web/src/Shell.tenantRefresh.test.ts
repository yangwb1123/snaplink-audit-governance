// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { authState } from './auth/store'
import { TENANTS_CHANGED_EVENT } from './tenantScope'
import Harness from './Shell.testHarness.svelte'

const { api } = vi.hoisted(() => ({
  api: {
    listTenants: vi.fn(),
    queryEvents: vi.fn(),
    getEvent: vi.fn(),
    getReceipt: vi.fn(),
    getEventFacets: vi.fn(),
  },
}))

vi.mock('./api/useAuditApi', () => ({ useAuditApi: () => api }))

interface Deferred<T> {
  promise: Promise<T>
  resolve: (value: T) => void
  reject: (reason: unknown) => void
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void
  let reject!: (reason: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

interface TenantList {
  items: Array<{ id: string; name: string; active?: boolean }>
  count: number
}

const baseList: TenantList = {
  items: [
    { id: 'tenant-a', name: '甲租户' },
    { id: 'tenant-b', name: '乙租户' },
  ],
  count: 2,
}

let target: HTMLDivElement
let component: ReturnType<typeof mount> | undefined

beforeEach(() => {
  api.listTenants.mockReset().mockResolvedValue(baseList)
  api.queryEvents.mockReset().mockResolvedValue({ items: [], count: 0, next_cursor: '' })
  api.getEvent.mockReset()
  api.getReceipt.mockReset()
  api.getEventFacets.mockReset()
  window.location.hash = '#events'
  authState.set({
    status: 'authenticated',
    session: {
      accessToken: 'at',
      idToken: 'it',
      expiresAt: Date.now() + 3_600_000,
      subject: 'platform-admin',
      displayName: '平台管理员',
      tenantId: '',
      roles: ['platform-admin'],
      permissions: new Set(['audit:platform:cross_tenant', 'audit:event:read']),
      platform: true,
    },
    error: null,
  })
  target = document.createElement('div')
  document.body.append(target)
})

afterEach(async () => {
  if (component) await unmount(component)
  component = undefined
  target.remove()
  authState.set({ status: 'anonymous', session: null, error: null })
})

function content(): HTMLElement {
  const region = target.querySelector('[data-iris-admin-content]')
  if (!region) throw new Error('content region missing')
  return region as HTMLElement
}

function pageInput(): HTMLInputElement {
  const input = content().querySelector('input[type="datetime-local"]')
  if (!input) throw new Error('events page is not mounted')
  return input as HTMLInputElement
}

function tenantSelect(): HTMLSelectElement {
  return target.querySelector('#tenant-scope') as HTMLSelectElement
}

function buttonLabelled(label: string, root: ParentNode = target): HTMLButtonElement {
  const buttons = [...root.querySelectorAll('button')].filter(
    (button) => button.textContent?.trim() === label,
  )
  if (buttons.length !== 1) throw new Error(`button ${label} not found (${buttons.length})`)
  return buttons[0] as HTMLButtonElement
}

function refreshButton(): HTMLButtonElement {
  return buttonLabelled('刷新租户')
}

async function settle(): Promise<void> {
  await tick()
  await tick()
}

async function mountWithResolvedTenant(): Promise<HTMLInputElement> {
  component = mount(Harness, { target })
  await vi.waitFor(() => expect(api.queryEvents).toHaveBeenCalled())
  await settle()
  const input = pageInput()
  input.dataset.probe = 'keep-me'
  return input
}

describe('Shell platform tenant refresh', () => {
  it('keeps the events page mounted while the refreshed tenant list is pending (AC1, AC3, R1, R3)', async () => {
    const node = await mountWithResolvedTenant()
    const queryCalls = api.queryEvents.mock.calls.length
    const pending = deferred<TenantList>()
    api.listTenants.mockReturnValueOnce(pending.promise)

    window.dispatchEvent(new Event(TENANTS_CHANGED_EVENT))
    await settle()

    expect(content().textContent).not.toContain('正在确认租户范围')
    expect(content().querySelector('[data-iris-alert]')).toBeNull()
    expect(pageInput()).toBe(node)
    expect(node.dataset.probe).toBe('keep-me')
    expect(tenantSelect().value).toBe('tenant-a')
    expect(refreshButton().disabled).toBe(true)

    pending.resolve(baseList)
    await settle()
    await settle()

    expect(pageInput()).toBe(node)
    expect(tenantSelect().value).toBe('tenant-a')
    expect(content().querySelector('[data-iris-alert]')).toBeNull()
    expect(api.queryEvents.mock.calls.length).toBe(queryCalls)
    for (const call of api.queryEvents.mock.calls) expect(call[1]).toBeTruthy()
  })

  it('keeps the routed page mounted when the toolbar refresh button is used (AC2, R8)', async () => {
    const node = await mountWithResolvedTenant()
    const queryCalls = api.queryEvents.mock.calls.length
    const pending = deferred<TenantList>()
    api.listTenants.mockReturnValueOnce(pending.promise)

    refreshButton().click()
    await settle()

    expect(content().querySelector('[data-iris-alert]')).toBeNull()
    expect(pageInput()).toBe(node)
    expect(tenantSelect().disabled).toBe(true)
    expect(target.textContent).toContain('读取租户…')

    pending.resolve(baseList)
    await settle()
    await settle()

    expect(pageInput()).toBe(node)
    expect(tenantSelect().value).toBe('tenant-a')
    expect(tenantSelect().disabled).toBe(false)
    expect(api.queryEvents.mock.calls.length).toBe(queryCalls)
  })

  it('preserves EventsPage drafts across a tenant refresh (AC5)', async () => {
    const node = await mountWithResolvedTenant()
    const from = pageInput()
    from.value = '2025-03-04T05:06'
    from.dispatchEvent(new Event('input', { bubbles: true }))
    await settle()

    buttonLabelled('高级筛选').click()
    await settle()
    const actorInput = content().querySelector('input[placeholder="用户或服务主体"]') as HTMLInputElement
    actorInput.value = 'actor-probe'
    actorInput.dispatchEvent(new Event('input', { bubbles: true }))
    await settle()

    const pending = deferred<TenantList>()
    api.listTenants.mockReturnValueOnce(pending.promise)
    window.dispatchEvent(new Event(TENANTS_CHANGED_EVENT))
    await settle()
    pending.resolve(baseList)
    await settle()
    await settle()

    const afterFrom = pageInput()
    expect(afterFrom).toBe(node)
    expect(afterFrom.value).toBe('2025-03-04T05:06')
    expect(target.textContent).toContain('收起高级条件')
    const afterActor = content().querySelector('input[placeholder="用户或服务主体"]') as HTMLInputElement
    expect(afterActor).toBe(actorInput)
    expect(afterActor.value).toBe('actor-probe')
  })

  it('keeps the last-known-good tenant mounted when a refresh fails (AC4, R5)', async () => {
    const node = await mountWithResolvedTenant()
    const queryCalls = api.queryEvents.mock.calls.length

    api.listTenants.mockRejectedValueOnce(new Error('list failed'))
    window.dispatchEvent(new Event(TENANTS_CHANGED_EVENT))
    await settle()
    await settle()

    expect(pageInput()).toBe(node)
    expect(node.dataset.probe).toBe('keep-me')
    expect(tenantSelect().value).toBe('tenant-a')
    expect(content().textContent).toContain('租户列表刷新失败')
    expect(content().textContent).not.toContain('已暂停租户级请求')
    const alert = content().querySelector('[data-iris-alert]')
    expect(alert?.getAttribute('role')).toBe('alert')
    expect(api.queryEvents.mock.calls.length).toBe(queryCalls)

    const retry = buttonLabelled('重试', content())
    retry.click()
    await vi.waitFor(() => expect(content().querySelector('[data-iris-alert]')).toBeNull())
    await settle()
    expect(pageInput()).toBe(node)
    expect(tenantSelect().value).toBe('tenant-a')
    expect(api.listTenants).toHaveBeenCalledTimes(3)
  })

  it('still gates the initial platform scope and emits no tenant request (AC7, R4)', async () => {
    const pending = deferred<TenantList>()
    api.listTenants.mockReturnValueOnce(pending.promise)
    component = mount(Harness, { target })
    await settle()

    expect(content().textContent).toContain('正在确认租户范围')
    expect(content().querySelector('input[type="datetime-local"]')).toBeNull()
    expect(api.queryEvents).not.toHaveBeenCalled()

    pending.reject(new Error('initial failure'))
    await vi.waitFor(() => expect(content().textContent).toContain('无法读取可用租户'))
    expect(content().textContent).toContain('已暂停租户级请求')
    expect(api.queryEvents).not.toHaveBeenCalled()
  })

  // R5 scopes the refresh-failure banner to an already-established scope. On the
  // catalog bootstrap route the else-branch renders even without a tenant, so an
  // ungated banner would claim a retained tenant scope that never existed.
  it('never claims a retained tenant scope on the catalog bootstrap path (R5)', async () => {
    window.location.hash = '#catalog'
    api.listTenants.mockResolvedValue({ items: [], count: 0 })
    component = mount(Harness, { target })
    await settle()
    expect(content().textContent).toContain('治理目录')

    api.listTenants.mockRejectedValueOnce(new Error('list failed'))
    window.dispatchEvent(new Event(TENANTS_CHANGED_EVENT))
    await settle()
    await settle()

    expect(content().textContent).not.toContain('租户列表刷新失败')
    expect(content().textContent).not.toContain('仍在使用上次成功读取的租户范围')
    expect(tenantSelect().value).toBe('')
    expect(tenantSelect().disabled).toBe(true)
  })

  it('remounts the page when the refreshed list no longer contains the selected tenant (AC7, R2, R10)', async () => {
    const node = await mountWithResolvedTenant()
    expect(tenantSelect().value).toBe('tenant-a')
    const queryCalls = api.queryEvents.mock.calls.length

    api.listTenants.mockResolvedValueOnce({
      items: [{ id: 'tenant-b', name: '乙租户' }],
      count: 1,
    })
    window.dispatchEvent(new Event(TENANTS_CHANGED_EVENT))
    await vi.waitFor(() => expect(api.queryEvents.mock.calls.length).toBe(queryCalls + 1))
    await settle()

    expect(pageInput()).not.toBe(node)
    expect(tenantSelect().value).toBe('tenant-b')
    for (const call of api.queryEvents.mock.calls) expect(call[1]).toBeTruthy()
  })

  // A second refresh is dispatched through the event rather than the toolbar:
  // happy-dom ignores clicks on a disabled button, and the toolbar button is
  // disabled for the whole in-flight window (R8).
  it('ignores a superseded tenant list response (R7)', async () => {
    const node = await mountWithResolvedTenant()
    const stale = deferred<TenantList>()
    api.listTenants.mockReturnValueOnce(stale.promise)
    window.dispatchEvent(new Event(TENANTS_CHANGED_EVENT))
    await settle()
    api.listTenants.mockReturnValueOnce(Promise.resolve(baseList))
    window.dispatchEvent(new Event(TENANTS_CHANGED_EVENT))
    await settle()
    await settle()

    stale.resolve({ items: [{ id: 'tenant-z', name: '丙租户' }], count: 1 })
    await settle()
    await settle()

    expect(tenantSelect().value).toBe('tenant-a')
    expect(pageInput()).toBe(node)
  })
})
