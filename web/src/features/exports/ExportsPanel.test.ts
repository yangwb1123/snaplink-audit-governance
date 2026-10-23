// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { authState } from '../../auth/store'
import { ApiError } from '../../api/client'
import type { ExportJob, ExportSummary } from '../../api/types'
import Harness from './ExportsPanel.testHarness.svelte'
import { EXPORT_POLL_INTERVAL_MS } from '../../pages/governanceState'

const { api } = vi.hoisted(() => ({ api: {
  getExport: vi.fn(), listExports: vi.fn(), downloadExport: vi.fn(), createExport: vi.fn(),
  cancelExport: vi.fn(), retryExport: vi.fn(), listLegalHolds: vi.fn(),
} }))
vi.mock('../../api/useAuditApi', () => ({ useAuditApi: () => api }))
let component: ReturnType<typeof mount> | undefined
let target: HTMLDivElement

function login(token = 'current-token', permissions = ['audit:export:create']): void {
  authState.set({ status: 'authenticated', session: { accessToken: token, idToken: 'id', expiresAt: Date.now() + 100000,
    subject: 'alice', displayName: 'alice', tenantId: 'tenant-a', roles: [], permissions: new Set(permissions), platform: true }, error: null })
}
function completed(id = 'export-a', tenant = 'tenant-a'): ExportJob {
  return { id, tenant_id: tenant, requested_by: 'alice', status: 'completed', created_at: '2026-10-14T00:00:00Z',
    query: { from: '2026-10-13T00:00:00Z', to: '2026-10-14T00:00:00Z', operation_id: 'private-scope' },
    event_count: 17, digest: 'private-digest' }
}
function summary(id = 'export-a', status = 'completed'): ExportSummary {
  return { id, tenant_id: 'tenant-a', requested_by: 'alice', status, created_at: '2026-10-14T00:00:00Z' }
}
function hold(): ApiError { return new ApiError(403, 'forbidden', 'withheld', undefined, 'legal_hold') }
function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (cause: unknown) => void
  const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail })
  return { promise, resolve, reject }
}
function fresh(id = 'export-a', tenant = 'tenant-a'): ExportJob {
  const original = completed(id, tenant)
  return { ...original, digest: 'fresh-digest', event_count: 23, query: { ...original.query, operation_id: 'fresh-scope' } }
}
async function denyDownload(): Promise<void> {
  api.downloadExport.mockRejectedValueOnce(hold())
  await render(); button('下载 JSONL').click(); await settle()
  hiddenEvidence()
}
async function settle(): Promise<void> { await tick(); await tick() }
function button(label: string): HTMLButtonElement {
  const result = [...target.querySelectorAll('button')].find((node) => node.textContent?.trim() === label)
  if (!result) throw new Error(`missing button ${label}`)
  return result
}
function detail(): Element { return [...target.querySelectorAll('.section-card')].at(-1)! }
function queryInput(): HTMLInputElement { return target.querySelector('input[placeholder="export job id"]')! }
async function editQuery(id: string): Promise<void> {
  queryInput().value = id; queryInput().dispatchEvent(new Event('input', { bubbles: true })); await settle()
}
function hiddenEvidence(): void {
  expect(detail().textContent).not.toContain('范围：')
  expect(detail().textContent).not.toContain('private-scope')
  expect(detail().textContent).not.toContain('private-digest')
  expect(detail().textContent).not.toContain('17 条事件')
  expect([...target.querySelectorAll('button')].some((node) => node.textContent?.includes('下载 JSONL'))).toBe(false)
}
async function render(): Promise<void> {
  component = mount(Harness, { target }); await settle()
  await vi.waitFor(() => expect(detail().textContent).toContain('private-digest'))
  expect(detail().textContent).toContain('private-scope')
  expect(detail().textContent).toContain('17 条事件')
}

beforeEach(() => {
  vi.resetAllMocks(); login()
  api.getExport.mockImplementation(async (id: string, tenant: string) => completed(id, tenant))
  api.listExports.mockResolvedValue({ items: [summary(), summary('pending-task', 'pending'), summary('failed-task', 'failed')], count: 3 })
  vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:test-export')
  vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => {})
  vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {})
  target = document.createElement('div'); document.body.append(target)
})
afterEach(async () => {
  if (component) await unmount(component)
  component = undefined; target.remove()
  authState.set({ status: 'anonymous', session: null, error: null })
  vi.restoreAllMocks(); vi.useRealTimers()
})

describe('completed export download denied by a later Hold', () => {
  it('withdraws evidence, keeps the safe list, and explicitly refreshes only the denied task before another download', async () => {
    api.downloadExport.mockRejectedValueOnce(hold()).mockResolvedValueOnce(new Blob(['fixture']))
    await render()
    const listBefore = target.querySelector('.native-table')!.textContent
    button('下载 JSONL').click(); await settle()
    expect(detail().textContent).toContain('导出被法律保留阻断')
    hiddenEvidence()
    expect(target.querySelector('.native-table')!.textContent).toBe(listBefore)
    expect(button('取消任务').disabled).toBe(false)
    expect(button('关联新任务重试').disabled).toBe(false)
    expect(api.listExports).toHaveBeenCalledTimes(1)
    expect(URL.createObjectURL).not.toHaveBeenCalled()
    expect(HTMLAnchorElement.prototype.click).not.toHaveBeenCalled()

    await editQuery('edited-not-the-denied-task')
    button('刷新导出详情').click(); await settle()
    expect(api.getExport.mock.lastCall?.slice(0, 2)).toEqual(['export-a', 'tenant-a'])
    expect(api.getExport.mock.lastCall?.[2]).toBeInstanceOf(AbortSignal)
    expect(detail().textContent).toContain('export-a · 17 条事件')
    expect(api.downloadExport).toHaveBeenCalledTimes(1)
    expect(URL.createObjectURL).not.toHaveBeenCalled()
    button('下载 JSONL').click(); await settle()
    expect(api.downloadExport).toHaveBeenCalledTimes(2)
    expect(URL.createObjectURL).toHaveBeenCalledTimes(1)
    expect(HTMLAnchorElement.prototype.click).toHaveBeenCalledTimes(1)
    expect(api.createExport).not.toHaveBeenCalled()
    expect(api.cancelExport).not.toHaveBeenCalled()
    expect(api.retryExport).not.toHaveBeenCalled()
    expect(api.listLegalHolds).not.toHaveBeenCalled()
  })

  it.each(['held', 'unavailable', 'missing', 'wrong-id', 'wrong-tenant'])(
    'keeps evidence withdrawn after a %s refresh and binds every retry to the original task', async (failure) => {
      await denyDownload()
      await editQuery('edited-other-task')
      if (failure === 'held') api.getExport.mockRejectedValueOnce(hold())
      if (failure === 'unavailable') api.getExport.mockRejectedValueOnce(new ApiError(503, 'store_timeout', 'withheld'))
      if (failure === 'missing') api.getExport.mockRejectedValueOnce(new ApiError(404, 'not_found', 'withheld'))
      if (failure === 'wrong-id') api.getExport.mockResolvedValueOnce(completed('export-other'))
      if (failure === 'wrong-tenant') api.getExport.mockResolvedValueOnce(completed('export-a', 'tenant-b'))
      button('刷新导出详情').click(); await settle()
      hiddenEvidence()
      expect(detail().textContent).toContain('任务 export-a · 租户 tenant-a')
      expect(button('刷新导出详情').disabled).toBe(false)
      expect(api.getExport.mock.lastCall?.slice(0, 2)).toEqual(['export-a', 'tenant-a'])
      // ErrorNotice's generic retry must use the same safe binding too.
      const retry = [...target.querySelectorAll('button')].find((node) => node.textContent?.trim() === '重试')
      api.getExport.mockResolvedValueOnce(fresh())
      const refresh = retry ?? button('刷新导出详情')
      refresh.click(); await settle()
      expect(api.getExport.mock.lastCall?.slice(0, 2)).toEqual(['export-a', 'tenant-a'])
      expect(detail().textContent).toContain('fresh-digest')
      expect(detail().textContent).not.toContain('private-digest')
      expect(api.downloadExport).toHaveBeenCalledTimes(1)
      expect(api.createExport).not.toHaveBeenCalled()
      expect(api.listLegalHolds).not.toHaveBeenCalled()
      expect(URL.createObjectURL).not.toHaveBeenCalled()
    })

  it('performs no background reads after denial, including visibility changes and polling intervals', async () => {
    vi.useFakeTimers()
    api.getExport.mockResolvedValueOnce({ ...completed(), status: 'running' }).mockResolvedValueOnce(completed())
    component = mount(Harness, { target }); await settle()
    await vi.advanceTimersByTimeAsync(EXPORT_POLL_INTERVAL_MS); await settle()
    expect(detail().textContent).toContain('private-digest')
    api.downloadExport.mockRejectedValueOnce(hold())
    button('下载 JSONL').click(); await settle()
    hiddenEvidence()
    document.dispatchEvent(new Event('visibilitychange')); await settle()
    await vi.advanceTimersByTimeAsync(EXPORT_POLL_INTERVAL_MS * 3); await settle()
    expect(api.getExport).toHaveBeenCalledTimes(2)
    expect(api.downloadExport).toHaveBeenCalledTimes(1)
    expect(api.createExport).not.toHaveBeenCalled()
  })

  for (const operation of ['read', 'download'] as const) {
    for (const transition of ['task', 'tenant', 'session', 'permission', 'unmount'] as const) {
      it.each(['success', 'failure'] as const)(`ignores late ${operation} %s across ${transition} changes even without transport cancellation`, async (outcome) => {
        const lateRead = deferred<ExportJob>()
        const lateDownload = deferred<Blob>()
        if (operation === 'read') {
          await denyDownload()
          api.getExport.mockReturnValueOnce(lateRead.promise)
          button('刷新导出详情').click()
        } else {
          await render()
          api.downloadExport.mockReturnValueOnce(lateDownload.promise)
          button('下载 JSONL').click()
        }
        await settle()
        const signal = (operation === 'read' ? api.getExport : api.downloadExport).mock.lastCall?.[2] as AbortSignal
        expect(signal.aborted).toBe(false)
        api.getExport.mockImplementation(async (id: string, tenant: string) => fresh(id, tenant))
        if (transition === 'task') {
          button('任务 B').click(); await settle(); button('任务 A').click(); await settle()
        } else if (transition === 'tenant') {
          button('租户 B').click(); await settle(); button('租户 A').click(); await settle()
        } else if (transition === 'session') {
          login('new-token'); await settle(); login(); await settle()
        } else if (transition === 'permission') {
          login('restricted-token', ['audit:event:read']); await settle()
        } else {
          await unmount(component!); component = undefined
        }
        expect(signal.aborted).toBe(true)
        // These promises deliberately never observe AbortSignal.
        if (operation === 'read') {
          if (outcome === 'success') lateRead.resolve(completed())
          else lateRead.reject(hold())
        } else {
          if (outcome === 'success') lateDownload.resolve(new Blob(['stale']))
          else lateDownload.reject(hold())
        }
        await settle()
        expect(URL.createObjectURL).not.toHaveBeenCalled()
        expect(HTMLAnchorElement.prototype.click).not.toHaveBeenCalled()
        expect(target.textContent).not.toContain('private-digest')
        expect(target.textContent).not.toContain('导出被法律保留阻断')
        expect(target.textContent).not.toContain('刷新导出详情')
        if (transition !== 'permission' && transition !== 'unmount') {
          expect(detail().textContent).toContain('fresh-digest')
          expect(button('下载 JSONL').disabled).toBe(false)
        }
        expect(api.createExport).not.toHaveBeenCalled()
        expect(api.listLegalHolds).not.toHaveBeenCalled()
      })
    }
  }

  it('discards a poll result after a scope change even when the poll ignores AbortSignal', async () => {
    vi.useFakeTimers()
    const late = deferred<ExportJob>()
    api.getExport.mockResolvedValueOnce({ ...completed(), status: 'running' }).mockReturnValueOnce(late.promise)
    component = mount(Harness, { target }); await settle()
    await vi.advanceTimersByTimeAsync(EXPORT_POLL_INTERVAL_MS)
    const signal = api.getExport.mock.lastCall?.[2] as AbortSignal
    api.getExport.mockImplementation(async (id: string, tenant: string) => fresh(id, tenant))
    button('任务 B').click(); await settle(); button('任务 A').click(); await settle()
    late.resolve(completed()); await settle()
    expect(signal.aborted).toBe(true)
    expect(detail().textContent).toContain('fresh-digest')
    expect(detail().textContent).not.toContain('private-digest')
    expect(URL.createObjectURL).not.toHaveBeenCalled()
  })

  it.each([
    [403, undefined, '当前操作不被允许'],
    [409, 'legal_hold', '状态或内容发生冲突'],
    [503, 'legal_hold', '服务暂时不可用'],
    [401, undefined, '登录已失效'],
    [0, undefined, '下载失败，请检查连接后重试'],
  ] as const)('keeps non-Hold download failure semantics for status %s', async (status, reason, title) => {
    api.downloadExport.mockRejectedValueOnce(status ? new ApiError(status, 'fixture_failure', 'private diagnostic', undefined, reason) : new Error('private diagnostic'))
    await render(); button('下载 JSONL').click(); await settle()
    expect(detail().textContent).toContain(title)
    expect(detail().textContent).not.toContain('导出被法律保留阻断')
    expect(detail().textContent).not.toContain('刷新导出详情')
    expect(detail().textContent).not.toContain('private diagnostic')
    expect(URL.createObjectURL).not.toHaveBeenCalled()
    expect(HTMLAnchorElement.prototype.click).not.toHaveBeenCalled()
    expect(api.downloadExport).toHaveBeenCalledTimes(1)
  })
})
