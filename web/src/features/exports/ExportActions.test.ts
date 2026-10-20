// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { authState } from '../../auth/store'
import { ApiError } from '../../api/client'
import type { ExportSummary } from '../../api/types'
import Harness from './ExportActions.testHarness.svelte'
import { exportFailureLabel } from './exportWorkflow'

const { api } = vi.hoisted(() => ({ api: { cancelExport: vi.fn(), retryExport: vi.fn() } }))
vi.mock('../../api/useAuditApi', () => ({ useAuditApi: () => api }))
let component: ReturnType<typeof mount> | undefined
let target: HTMLDivElement
let onCommitted: ReturnType<typeof vi.fn>
let onRefresh: ReturnType<typeof vi.fn>

function login(subject = 'alice', permissions = ['audit:export:create'], token = 'current-token', platform = true): void {
  authState.set({ status: 'authenticated', session: { accessToken: token, idToken: 'id', expiresAt: Date.now() + 100000,
    subject, displayName: subject, tenantId: 'tenant-a', roles: [], permissions: new Set(permissions), platform }, error: null })
}
async function settle(): Promise<void> { await tick(); await tick() }
function button(label: string): HTMLButtonElement {
  const result = [...document.querySelectorAll('button')].find((node) => node.textContent?.trim() === label)
  if (!result) throw new Error(`missing button ${label}`)
  return result
}
function response(status = 'cancelled'): ExportSummary {
  return { id: 'export-source', tenant_id: 'tenant-a', requested_by: 'alice', status, created_at: '2026-10-14T00:00:00Z' }
}
function render(status = 'pending', requester = 'alice'): void {
  component = mount(Harness, { target, props: { status, requester, onCommitted, onRefresh } })
}

beforeEach(() => {
  vi.resetAllMocks(); login()
  onCommitted = vi.fn(); onRefresh = vi.fn().mockResolvedValue(undefined)
  target = document.createElement('div'); document.body.append(target)
})
afterEach(async () => {
  if (component) await unmount(component)
  component = undefined; target.remove()
  authState.set({ status: 'anonymous', session: null, error: null })
})

describe('owned export commands', () => {
  it('requires confirmation, returns only summary and sends authenticated scope plus AbortSignal', async () => {
    api.cancelExport.mockResolvedValue(response())
    render(); await settle()
    button('取消任务').click(); await settle()
    expect(api.cancelExport).not.toHaveBeenCalled()
    expect(document.body.textContent).toContain('不证明远端写入停止或回滚')
    button('确认取消任务').click()
    await vi.waitFor(() => expect(onCommitted).toHaveBeenCalledWith(response()))
    expect(api.cancelExport.mock.lastCall?.slice(0, 2)).toEqual(['export-source', 'tenant-a'])
    expect(api.cancelExport.mock.lastCall?.[2]).toBeInstanceOf(AbortSignal)
  })

  it('creates a linked successor without mutating/requeueing the displayed terminal source', async () => {
    const child = { ...response('pending'), id: 'export-child', retry_of: 'export-source' }
    api.retryExport.mockResolvedValue(child)
    render('failed'); await settle()
    button('关联新任务重试').click(); await settle()
    expect(api.retryExport).not.toHaveBeenCalled()
    expect(document.body.textContent).toContain('不延续旧查询快照')
    button('确认重试').click()
    await vi.waitFor(() => expect(onCommitted).toHaveBeenCalledWith(child))
    expect(api.retryExport.mock.lastCall?.slice(0, 2)).toEqual(['export-source', 'tenant-a'])
    expect(api.cancelExport).not.toHaveBeenCalled()
  })

  it.each([['other', ['audit:export:create']], ['', ['audit:export:create']], ['alice', ['audit:event:read']]])('does not grant owner control to %s with %j', async (requester, permissions) => {
    login('alice', permissions, 'tenant-token', false)
    render('pending', requester); await settle()
    expect(target.textContent).not.toContain('取消任务')
    expect(api.cancelExport).not.toHaveBeenCalled()
  })

  it('blocks blind write retries after an unknown outcome; refresh is a read callback only', async () => {
    api.cancelExport.mockRejectedValue(new ApiError(503, 'store_timeout', 'timeout', 'req-private-safe'))
    render(); await settle()
    button('取消任务').click(); await settle(); button('确认取消任务').click()
    await vi.waitFor(() => expect(target.textContent).toContain('操作结果可能已提交'))
    expect(button('取消任务').disabled).toBe(true)
    expect(onCommitted).not.toHaveBeenCalled()
    button('刷新任务核对').click()
    await vi.waitFor(() => expect(onRefresh).toHaveBeenCalledWith('export-source'))
    expect(api.cancelExport).toHaveBeenCalledTimes(1)
  })

  it('rejects wrong-tenant/corrupt linkage response instead of navigating to it', async () => {
    api.retryExport.mockResolvedValue({ ...response('pending'), id: 'wrong-child', tenant_id: 'tenant-b', retry_of: 'wrong-parent' })
    render('failed'); await settle()
    button('关联新任务重试').click(); await settle(); button('确认重试').click()
    await vi.waitFor(() => expect(target.textContent).toContain('操作结果可能已提交'))
    expect(onCommitted).not.toHaveBeenCalled()
    expect(button('关联新任务重试').disabled).toBe(true)
  })

  it('ignores an A -> B -> A late result even when mutation transport ignores abort', async () => {
    let resolve!: (value: ExportSummary) => void
    api.cancelExport.mockReturnValue(new Promise((done) => (resolve = done)))
    render(); await settle()
    button('取消任务').click(); await settle(); button('确认取消任务').click(); await settle()
    const signal = api.cancelExport.mock.lastCall?.[2] as AbortSignal
    button('选择 B').click(); await settle(); button('选择 A').click(); await settle()
    resolve(response()); await settle()
    expect(signal.aborted).toBe(true)
    expect(onCommitted).not.toHaveBeenCalled()
  })

  it('invalidates both confirmation and late writes when the token/permissions change', async () => {
    let resolve!: (value: ExportSummary) => void
    api.cancelExport.mockReturnValue(new Promise((done) => (resolve = done)))
    render(); await settle()
    button('取消任务').click(); await settle(); button('确认取消任务').click(); await settle()
    login('alice', ['audit:export:create'], 'new-token'); await settle()
    resolve(response()); await settle()
    expect(onCommitted).not.toHaveBeenCalled()
    expect((api.cancelExport.mock.lastCall?.[2] as AbortSignal).aborted).toBe(true)
    button('取消任务').click(); await settle()
    login('alice', ['audit:event:read'], 'newer-token', false); await settle()
    expect(document.body.textContent).not.toContain('确认取消导出任务')
  })

  it('closes an unsubmitted confirmation when a different task is selected', async () => {
    render(); await settle()
    button('取消任务').click(); await settle()
    button('选择其他任务').click(); await settle()
    expect(document.body.textContent).not.toContain('确认取消导出任务')
    expect(api.cancelExport).not.toHaveBeenCalled()
  })

  it('uses only fixed labels for safe failure codes, never an arbitrary diagnostic', () => {
    expect(exportFailureLabel('lease_expired')).toContain('不推断外部写入')
    expect(exportFailureLabel('cancelled')).toContain('不删除 WORM')
    expect(exportFailureLabel('/private/path/token')).not.toContain('/private/path/token')
  })
})
