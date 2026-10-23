// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import type { AuditEvent, EventReceipt, QueryResult } from './api/types'
import { authState } from './auth/store'
import Harness from './Shell.testHarness.svelte'
import { formatDate } from './components/format'
import { eventHash } from './pages/navigation'

const { api } = vi.hoisted(() => ({ api: {
  queryEvents: vi.fn(), getEvent: vi.fn(), getReceipt: vi.fn(), listTenants: vi.fn(),
  getOperation: vi.fn(), getOperationTimeline: vi.fn(), replayOperation: vi.fn(), getAggregateTimeline: vi.fn(),
  getGovernanceStatus: vi.fn(),
} }))
vi.mock('./api/useAuditApi', () => ({ useAuditApi: () => api }))
const permissions = ['audit:operation:read', 'audit:event:read']
const histories = [
  { mode: 'operation', hash: '#operations/op/op-7', otherHash: '#operations/op/op-8', method: 'getOperationTimeline' },
  { mode: 'aggregate', hash: '#operations/aggregate/file/file-7', otherHash: '#operations/aggregate/file/file-8', method: 'getAggregateTimeline' },
] as const
type History = typeof histories[number]
function event(id: string, tenant = 'tenant-a', version = 1): AuditEvent {
  return {
    event_id: id, tenant_id: tenant, source_system: 'aero-vault', event_type: 'file.downloaded',
    schema_id: 'historical.event', schema_version: 2, actor: { id: 'actor' }, action: `step-${id}`,
    outcome: 'success', occurred_at: '2026-10-01T00:00:00Z', received_at: '2026-10-01T00:00:01Z',
    aggregate_type: 'file', aggregate_id: 'file-7', aggregate_version: version,
    data_classification: 'internal', retention_class: 'standard',
  }
}
function login(granted = permissions, options: { token?: string; tenant?: string; platform?: boolean } = {}) {
  authState.set({ status: 'authenticated', session: {
    accessToken: options.token ?? 'fixture-access', idToken: 'fixture-identity', subject: 'actor', displayName: 'actor',
    expiresAt: Date.now() + 100000, tenantId: options.tenant ?? 'tenant-a', platform: options.platform ?? false,
    roles: [], permissions: new Set(granted),
  }, error: null })
}
function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (cause: unknown) => void
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}
let target: HTMLDivElement
let component: ReturnType<typeof mount> | undefined
async function settle() { await tick(); await Promise.resolve(); await tick(); await Promise.resolve(); await tick() }
function links() { return [...target.querySelectorAll<HTMLAnchorElement>('a[href^="#events/event/"]')] }
function button(label: string) { return [...target.querySelectorAll('button')].find(node => node.textContent?.trim() === label)! }
function setHash(hash: string) {
  window.history.replaceState(null, '', hash)
  window.dispatchEvent(new HashChangeEvent('hashchange'))
}
async function start(history: History) {
  window.history.replaceState(null, '', history.hash)
  component = mount(Harness, { target }); await settle()
}
async function open(link: HTMLAnchorElement, id: string, tenant = 'tenant-a') {
  link.focus(); expect(document.activeElement).toBe(link)
  expect(link.tabIndex).toBe(0)
  expect(link.getAttribute('href')).toBe(eventHash(id))
  expect(link.textContent).toContain('查看原事件')
  expect(link.textContent).toContain(id)
  link.click()
  await new Promise(resolve => setTimeout(resolve, 0)); await settle()
  expect(window.location.hash).toBe(eventHash(id))
  expect(api.getEvent).toHaveBeenLastCalledWith(id, tenant, expect.any(AbortSignal))
  expect(api.getReceipt).toHaveBeenLastCalledWith(id, tenant, expect.any(AbortSignal))
}
function chooseTenant(tenant: string) {
  const select = target.querySelector<HTMLSelectElement>('select[aria-label="租户范围"]')!
  // happy-dom omits OPTION from :checked; adapt only the native binding lookup.
  const query = select.querySelector.bind(select)
  const lookup = vi.spyOn(select, 'querySelector').mockImplementation(selector =>
    selector === ':checked' ? [...select.options].find(option => option.selected) ?? null : query(selector),
  )
  try {
    select.value = tenant
    expect(select.value).toBe(tenant)
    select.dispatchEvent(new Event('change', { bubbles: true }))
  } finally { lookup.mockRestore() }
}
beforeEach(() => {
  vi.resetAllMocks(); login()
  api.listTenants.mockResolvedValue({ items: [{ id: 'tenant-a', name: 'A' }, { id: 'tenant-b', name: 'B' }] })
  api.queryEvents.mockResolvedValue({ items: [], count: 0 })
  api.getOperation.mockResolvedValue({ operation_id: 'op-7', tenant_id: 'tenant-a', event_count: 1, outcomes: [] })
  api.replayOperation.mockResolvedValue({ event_count: 1, state: {}, last_sequence: 1 })
  for (const { method } of histories) api[method].mockResolvedValue({ items: [event('original-7')], count: 1 })
  api.getEvent.mockImplementation((id, tenant) => Promise.resolve(event(id, tenant)))
  api.getReceipt.mockImplementation((id, tenant) => Promise.resolve({ event_id: id, tenant_id: tenant, status: 'archived', accepted_at: '' }))
  target = document.createElement('div'); document.body.append(target)
})
afterEach(async () => {
  if (component) await unmount(component)
  component = undefined; target.remove()
  authState.set({ status: 'anonymous', session: null, error: null })
  window.history.replaceState(null, '', '#')
})

describe('Shell history → authenticated original event', () => {
  it.each(histories.flatMap(history => ['empty', 'hanging'].map(list => ({ ...history, list }))))(
    'opens the original event from $mode with an $list event list', async (history) => {
      if (history.list === 'hanging') api.queryEvents.mockReturnValue(new Promise(() => {}))
      // Independent operation sections must not delay navigation from its timeline.
      api.getOperation.mockReturnValue(new Promise(() => {}))
      api.replayOperation.mockReturnValue(new Promise(() => {}))
      await start(history)
      expect(links()).toHaveLength(1)
      expect(api.getEvent).not.toHaveBeenCalled(); expect(api.getReceipt).not.toHaveBeenCalled()
      expect(api.queryEvents).not.toHaveBeenCalled()
      await open(links()[0], 'original-7')
      expect(api.queryEvents).toHaveBeenCalledTimes(1)
      expect(api.getEvent).toHaveBeenCalledTimes(1); expect(api.getReceipt).toHaveBeenCalledTimes(1)
      expect(document.body.textContent).toContain('审计事件详情')
      expect(document.body.textContent).toContain('historical.event')
      expect(document.body.textContent).toContain('archived')
    },
  )

  it.each(histories)('preserves $mode order, links and live counts through a failed page and same-cursor retry', async (history) => {
    const first = { items: [event('z', 'tenant-a', 4), event('a', 'tenant-a', 1)], count: 3, next_cursor: 'next-1' }
    const second = { items: [event('y', 'tenant-a', 8), event('b', 'tenant-a', 2)], count: 4 }
    api[history.method].mockReset().mockResolvedValueOnce(first)
      .mockRejectedValueOnce(new Error('page unavailable')).mockResolvedValueOnce(second)
    await start(history)
    const expected = (items: AuditEvent[]) => items.map(item => eventHash(item.event_id))
    expect(links().map(link => link.getAttribute('href'))).toEqual(expected(first.items))
    expect(target.textContent).toContain('已显示 2 / 共 3')
    button('加载下一页').click(); await settle()
    expect(target.textContent).toContain('page unavailable')
    expect(links().map(link => link.getAttribute('href'))).toEqual(expected(first.items))
    button('重试').click(); await settle()
    expect(links().map(link => link.getAttribute('href'))).toEqual(expected([...first.items, ...second.items]))
    expect(target.textContent).toContain('已显示 4 / 共 4 条；实时观察，翻页期间总数可能随提交变化。')
    expect(target.textContent).not.toContain('page unavailable')
    const rows = [...target.querySelectorAll('[data-iris-timeline-item]')]
    ;[...first.items, ...second.items].forEach((item, index) => {
      expect(rows[index].textContent).toContain(item.action)
      expect(rows[index].textContent).toContain(item.outcome)
      expect(rows[index].textContent).toContain(formatDate(item.occurred_at))
      expect(rows[index].textContent).toContain('aero-vault · file.downloaded')
      if (history.mode === 'aggregate') expect(rows[index].textContent).toContain(`v${item.aggregate_version} ·`)
    })
    const options = { pageSize: 200, cursor: 'next-1', tenantId: 'tenant-a' }
    const args = history.mode === 'operation' ? ['op-7', options] : ['file', 'file-7', options]
    expect(api[history.method].mock.calls.slice(1)).toEqual([args, args])
    expect(api.getOperation).toHaveBeenCalledTimes(history.mode === 'operation' ? 1 : 0)
    expect(api.replayOperation).toHaveBeenCalledTimes(history.mode === 'operation' ? 1 : 0)
    expect(api.getEvent).not.toHaveBeenCalled(); expect(api.getReceipt).not.toHaveBeenCalled()
    await open(links()[3], 'b')
  })

  it.each(histories)('keeps $mode history readable with operation permission alone, without event links or reads', async (history) => {
    login(['audit:operation:read']); await start(history)
    expect(target.textContent).toContain('aero-vault · file.downloaded')
    expect(target.textContent).toContain('已显示 1 / 共 1')
    expect(links()).toHaveLength(0)
    expect(api.getEvent).not.toHaveBeenCalled(); expect(api.getReceipt).not.toHaveBeenCalled()
  })

  it.each(histories)('uses the Shell-selected platform tenant for $mode links and detail reads', async (history) => {
    login([], { tenant: '', platform: true })
    api[history.method].mockImplementation((...args: unknown[]) => {
      const options = args.at(-1) as { tenantId: string }
      return Promise.resolve({ items: [event('original-7', options.tenantId)], count: 1 })
    })
    await start(history)
    chooseTenant('tenant-b'); await settle()
    expect(links()).toHaveLength(1)
    await open(links()[0], 'original-7', 'tenant-b')
  })

  it.each(histories)('does not mount $mode or create event links before a platform tenant is available', async (history) => {
    login([], { tenant: '', platform: true })
    const tenants = deferred<{ items: [] }>(); api.listTenants.mockReturnValueOnce(tenants.promise)
    await start(history)
    expect(links()).toHaveLength(0)
    expect(api[history.method]).not.toHaveBeenCalled()
    tenants.resolve({ items: [] }); await settle()
    expect(links()).toHaveLength(0)
    expect(api[history.method]).not.toHaveBeenCalled()
    expect(api.getEvent).not.toHaveBeenCalled(); expect(api.getReceipt).not.toHaveBeenCalled()
  })

  it.each(histories)('never links foreign-tenant or rejected IDs in the mounted $mode history', async (history) => {
    api[history.method].mockResolvedValue({ items: [event('original-7'), event('foreign', 'tenant-b'),
      event('missing-tenant', ''), event('bad?tenant_id=tenant-b'), event('javascript:alert'), event('bad/id')], count: 6 })
    await start(history)
    expect(links().map(link => link.getAttribute('href'))).toEqual([eventHash('original-7')])
    expect(target.querySelectorAll('[data-iris-timeline-item]')).toHaveLength(6)
    expect(api.getEvent).not.toHaveBeenCalled(); expect(api.getReceipt).not.toHaveBeenCalled()
  })

  it.each(histories.flatMap(history => ['success', 'failure'].map(outcome => ({ ...history, outcome }))))(
    'removes $mode links on event permission revocation and fences a late page $outcome', async (history) => {
      const old = deferred<QueryResult>()
      api[history.method].mockReset()
        .mockResolvedValueOnce({ items: [event('before-revocation')], count: 2, next_cursor: 'next-1' })
        .mockReturnValueOnce(old.promise)
        .mockResolvedValue({ items: [event('readable-after-revocation')], count: 1 })
      await start(history)
      expect(links()).toHaveLength(1)
      const retainedHref = links()[0].getAttribute('href')!
      button('加载下一页').click(); await settle()
      expect(links()[0].getAttribute('href')).toBe(retainedHref)
      login(['audit:operation:read'], { token: 'renewed-without-event-permission' }); await settle()
      expect(target.textContent).toContain('step-readable-after-revocation')
      expect(links()).toHaveLength(0)
      if (history.outcome === 'success') old.resolve({ items: [event('obsolete-event')], count: 987654 })
      else old.reject(new Error('obsolete page failure'))
      await settle()
      expect(target.textContent).not.toContain('obsolete')
      expect(target.textContent).toContain('已显示 1 / 共 1')
      expect(links()).toHaveLength(0)
      // A previously copied URL still cannot authorize the event page.
      setHash(retainedHref); await settle()
      await new Promise(resolve => setTimeout(resolve, 0)); await settle()
      expect(api.getEvent).not.toHaveBeenCalled(); expect(api.getReceipt).not.toHaveBeenCalled()
      expect(api.queryEvents).not.toHaveBeenCalled()
    },
  )

  it.each(histories.flatMap(history => (['target', 'token', 'tenant', 'platform selection'] as const).flatMap(change =>
    (['first', 'append'] as const).flatMap(pending => (['success', 'failure'] as const).map(outcome =>
      ({ ...history, change, pending, outcome }))),
  )))('fences late $mode $pending $outcome across $change A → B → A without reviving old links', async (history) => {
    const old = deferred<QueryResult>()
    const read = api[history.method].mockReset()
    if (history.pending === 'append') read.mockResolvedValueOnce({ items: [event('before-change')], count: 2, next_cursor: 'next-1' })
    read.mockReturnValueOnce(old.promise).mockImplementation((...args: unknown[]) => {
      const options = args.at(-1) as { tenantId: string }
      return Promise.resolve({ items: [event('current-observation', options.tenantId)], count: 1 })
    })
    if (history.change === 'platform selection') login([], { platform: true })
    await start(history)
    if (history.pending === 'append') {
      expect(links().map(link => link.getAttribute('href'))).toEqual([eventHash('before-change')])
      button('加载下一页').click(); await settle()
    } else expect(links()).toHaveLength(0)
    for (const value of ['b', 'a']) {
      if (history.change === 'target') setHash(value === 'b' ? history.otherHash : history.hash)
      else if (history.change === 'token') login(permissions, { token: value === 'b' ? 'renewed-access' : 'fixture-access' })
      else if (history.change === 'tenant') login(permissions, { tenant: `tenant-${value}` })
      else chooseTenant(`tenant-${value}`)
      await settle()
    }
    expect(links().map(link => link.getAttribute('href'))).toEqual([eventHash('current-observation')])
    if (history.outcome === 'success') old.resolve({ items: [event('obsolete-event')], count: 987654, next_cursor: 'obsolete-cursor' })
    else old.reject(new Error('obsolete timeline failure'))
    await settle()
    expect(target.textContent).not.toContain('obsolete')
    expect(target.textContent).not.toContain('987654')
    expect(target.textContent).toContain('已显示 1 / 共 1')
    expect(links().map(link => link.getAttribute('href'))).toEqual([eventHash('current-observation')])
    expect(read).toHaveBeenCalledTimes(history.pending === 'append' ? 4 : 3)
    expect(api.getEvent).not.toHaveBeenCalled(); expect(api.getReceipt).not.toHaveBeenCalled()
    await open(links()[0], 'current-observation')
  })

  it.each(histories.flatMap(history => (['event target', 'token', 'tenant', 'platform selection', 'revocation', 'logout'] as const).flatMap(change =>
    (['success', 'failure'] as const).map(outcome => ({ ...history, change, outcome })),
  )))('discards ignored-abort detail $outcome reached from $mode after $change', async (history) => {
    const oldEvent = deferred<AuditEvent>(), oldReceipt = deferred<EventReceipt>()
    api.getEvent.mockReturnValueOnce(oldEvent.promise)
    api.getReceipt.mockReturnValueOnce(oldReceipt.promise)
    if (history.change === 'platform selection') login([], { platform: true })
    await start(history)
    await open(links()[0], 'original-7')
    const eventSignal = api.getEvent.mock.calls[0][2] as AbortSignal
    const receiptSignal = api.getReceipt.mock.calls[0][2] as AbortSignal
    expect(eventSignal.aborted).toBe(false); expect(receiptSignal.aborted).toBe(false)
    const unauthorized = history.change === 'revocation' || history.change === 'logout'
    if (history.change === 'revocation') login(['audit:operation:read'])
    else if (history.change === 'logout') authState.set({ status: 'anonymous', session: null, error: null })
    else for (const value of ['b', 'a']) {
      if (history.change === 'event target') setHash(eventHash(value === 'b' ? 'original-8' : 'original-7'))
      else if (history.change === 'token') login(permissions, { token: value === 'b' ? 'renewed-access' : 'fixture-access' })
      else if (history.change === 'tenant') login(permissions, { tenant: `tenant-${value}` })
      else chooseTenant(`tenant-${value}`)
      await settle()
    }
    await settle()
    expect(eventSignal.aborted).toBe(true); expect(receiptSignal.aborted).toBe(true)
    if (history.outcome === 'success') {
      oldEvent.resolve({ ...event('original-7'), schema_id: 'obsolete-schema' })
      oldReceipt.resolve({ event_id: 'original-7', tenant_id: 'tenant-a', status: 'obsolete-receipt', accepted_at: '' })
    } else {
      oldEvent.reject(new Error('obsolete event failure'))
      oldReceipt.reject(new Error('obsolete receipt failure'))
    }
    await settle()
    expect(document.body.textContent).not.toContain('obsolete')
    expect(links()).toHaveLength(0)
    const expectedReads = unauthorized ? 1 : 3
    expect(api.getEvent).toHaveBeenCalledTimes(expectedReads)
    expect(api.getReceipt).toHaveBeenCalledTimes(expectedReads)
    if (!unauthorized) {
      expect(api.getEvent).toHaveBeenLastCalledWith('original-7', 'tenant-a', expect.any(AbortSignal))
      expect(api.getReceipt).toHaveBeenLastCalledWith('original-7', 'tenant-a', expect.any(AbortSignal))
      expect(document.body.textContent).toContain('historical.event')
      expect(document.body.textContent).toContain('archived')
    } else expect(document.body.textContent).not.toContain('审计事件详情')
  })
})
