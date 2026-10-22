// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { authState } from './auth/store'
import { ApiError } from './api/client'
import Harness from './Shell.testHarness.svelte'

const { api } = vi.hoisted(() => ({ api: {
  queryEvents: vi.fn(), getEvent: vi.fn(), getReceipt: vi.fn(),
  getOperation: vi.fn(), getOperationTimeline: vi.fn(), replayOperation: vi.fn(),
  listExports: vi.fn(), getExport: vi.fn(), getGovernanceStatus: vi.fn(),
} }))
vi.mock('./api/useAuditApi', () => ({ useAuditApi: () => api }))
let target: HTMLDivElement
let component: ReturnType<typeof mount> | undefined
function login(permissions: string[], token = 'token') { authState.set({ status: 'authenticated', session: { accessToken: token, idToken: 'id', expiresAt: Date.now() + 100000, subject: 'actor', displayName: 'actor', tenantId: 'tenant-a', platform: false, roles: [], permissions: new Set(permissions) }, error: null }) }
async function settle() { await tick(); await Promise.resolve(); await tick() }
beforeEach(() => {
  vi.resetAllMocks()
  api.queryEvents.mockResolvedValue({ items: [], count: 0 })
  api.getEvent.mockResolvedValue({ event_id: 'event-7', tenant_id: 'tenant-a', schema_id: 'event-pinned-schema', schema_version: 2, actor: { id: 'actor' } })
  api.getReceipt.mockResolvedValue({ event_id: 'event-7', tenant_id: 'tenant-a', status: 'archived' })
  api.getOperation.mockResolvedValue({ operation_id: 'op-7', tenant_id: 'tenant-a', event_count: 0, outcomes: [] })
  api.getOperationTimeline.mockResolvedValue({ items: [], count: 0 })
  api.replayOperation.mockResolvedValue({ event_count: 0, state: {}, last_sequence: 0 })
  // An unavailable list must never gate the direct detail read.
  api.listExports.mockReturnValue(new Promise(() => {}))
  api.getExport.mockResolvedValue({ id: 'export-0123456789abcdef', tenant_id: 'tenant-a', requested_by: 'actor', status: 'completed', event_count: 0, query: { from: '', to: '' } })
  target = document.createElement('div'); document.body.append(target)
})
afterEach(async () => { if (component) await unmount(component); component = undefined; target.remove(); authState.set({ status: 'anonymous', session: null, error: null }); window.history.replaceState(null, '', '#') })

describe('Shell direct links and permission boundaries', () => {
  it.each([
    ['#events/event/event-7', 'audit:event:read', 'getEvent', 'event-7'],
    ['#operations/op/op-7', 'audit:operation:read', 'getOperation', 'op-7'],
    ['#evidence/exports/export-0123456789abcdef', 'audit:export:create', 'getExport', 'export-0123456789abcdef'],
  ])('opens %s without a current list hit', async (hash, permission, method, id) => {
    login([permission]); window.history.replaceState(null, '', hash)
    component = mount(Harness, { target }); await settle()
    const read = api[method as 'getEvent' | 'getOperation' | 'getExport']
    expect(read.mock.lastCall?.slice(0, 2)).toEqual([id, 'tenant-a'])
    for (const other of ['getEvent', 'getOperation', 'getExport'] as const) if (other !== method) expect(api[other]).not.toHaveBeenCalled()
    expect(window.location.hash).toBe(hash)
  })

  it('shows direct export loading/not-found/retry states independently of an unavailable list', async () => {
    let reject!: (error: unknown) => void
    api.getExport.mockReturnValueOnce(new Promise((_, fail) => (reject = fail)))
    login(['audit:export:create']); window.history.replaceState(null, '', '#evidence/exports/export-0123456789abcdef')
    component = mount(Harness, { target }); await settle()
    expect(target.textContent).toContain('正在读取导出详情')
    reject(new ApiError(404, 'not_found', 'missing')); await settle()
    expect(target.textContent).toContain('未找到当前租户的导出任务')
    const details = [...target.querySelectorAll('h2')].find(h => h.textContent === '导出详情')!.parentElement!
    const refresh = [...details.querySelectorAll('button')].find(b => b.textContent?.trim() === '刷新')!
    refresh.click(); await settle()
    expect(target.textContent).toContain('下载 JSONL')
    expect(api.getExport).toHaveBeenCalledTimes(2); expect(api.listExports).toHaveBeenCalledTimes(1)
  })

  it('discards an ignored-abort export response after a token renewal on the same deep link', async () => {
    let resolve!: (value: unknown) => void
    api.getExport.mockReturnValueOnce(new Promise(done => (resolve = done)))
    login(['audit:export:create']); window.history.replaceState(null, '', '#evidence/exports/export-0123456789abcdef')
    component = mount(Harness, { target }); await settle()
    const signal = api.getExport.mock.calls[0][2] as AbortSignal
    login(['audit:export:create'], 'renewed-token'); await settle()
    expect(signal.aborted).toBe(true)
    resolve({ id: 'export-0123456789abcdef', tenant_id: 'tenant-a', requested_by: 'actor', status: 'completed', event_count: 987654321, digest: 'OLD PRIVATE DIGEST', query: {} }); await settle()
    expect(target.textContent).not.toContain('OLD PRIVATE DIGEST')
    expect(target.textContent).not.toContain('987654321 条事件')
    expect(api.getExport).toHaveBeenCalledTimes(2)
  })

  it('discards an old operation observation when another direct link replaces it', async () => {
    let resolve!: (value: unknown) => void
    api.getOperation.mockReturnValueOnce(new Promise(done => (resolve = done)))
    login(['audit:operation:read']); window.history.replaceState(null, '', '#operations/op/op-7')
    component = mount(Harness, { target }); await settle()
    api.getOperation.mockResolvedValue({ operation_id: 'op-8', tenant_id: 'tenant-a', event_count: 0, outcomes: [] })
    window.location.hash = '#operations/op/op-8'; window.dispatchEvent(new HashChangeEvent('hashchange')); await settle()
    resolve({ operation_id: 'OLD PRIVATE OPERATION', event_count: 9, outcomes: [] }); await settle()
    expect(api.getOperation).toHaveBeenLastCalledWith('op-8', 'tenant-a')
    expect(target.textContent).not.toContain('OLD PRIVATE OPERATION')
    expect(target.textContent).toContain('op-8')
  })

  it.each(['#events/event/event-7', '#operations/op/op-7', '#evidence/exports/export-0123456789abcdef'])('never requests an unauthorized direct link: %s', async (hash) => {
    login([]); window.history.replaceState(null, '', hash)
    component = mount(Harness, { target }); await settle()
    for (const read of Object.values(api)) expect(read).not.toHaveBeenCalled()
  })

  it('does not interpret injected routes or tenant selectors as a request target', async () => {
    login([]); window.history.replaceState(null, '', '#events/event/event-7?tenant_id=tenant-b')
    component = mount(Harness, { target }); await settle()
    expect(api.getEvent).not.toHaveBeenCalled(); expect(api.queryEvents).not.toHaveBeenCalled()
  })
})
