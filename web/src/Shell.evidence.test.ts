// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { authState } from './auth/store'
import Harness from './Shell.testHarness.svelte'
import IntegrityHarness from './features/integrity/IntegrityPanel.testHarness.svelte'
import { ApiError } from './api/client'

const { api } = vi.hoisted(() => ({ api: { listTenants: vi.fn(), listExports: vi.fn(), listRestores: vi.fn(), listLegalHolds: vi.fn(), verifyIntegrity: vi.fn(), getRestore: vi.fn(), getExport: vi.fn(), cancelExport: vi.fn(), retryExport: vi.fn() } }))
vi.mock('./api/useAuditApi', () => ({ useAuditApi: () => api }))
let target: HTMLDivElement
let component: ReturnType<typeof mount> | undefined

function login(permissions: string[], token = 'token'): void {
  authState.set({ status: 'authenticated', session: { accessToken: token, idToken: 'id', expiresAt: Date.now() + 100000, subject: 'actor', displayName: 'actor', tenantId: 'tenant-a', roles: [], permissions: new Set(permissions), platform: permissions.includes('audit:platform:cross_tenant') }, error: null })
}

async function settle(): Promise<void> { await tick(); await tick() }

function button(label: string): HTMLButtonElement {
  const result = [...target.querySelectorAll('button')].find((node) => node.textContent?.trim() === label)
  if (!result) throw new Error(`button missing: ${label}`)
  return result
}

beforeEach(() => {
  vi.resetAllMocks()
  api.listTenants.mockResolvedValue({ items: [{ id: 'tenant-a', name: 'A' }, { id: 'tenant-b', name: 'B' }], count: 2 })
  api.listExports.mockResolvedValue({ items: [], count: 0 })
  api.listRestores.mockResolvedValue({ items: [], count: 0 })
  api.listLegalHolds.mockResolvedValue({ items: [], count: 0 })
  window.location.hash = '#evidence'
  target = document.createElement('div')
  document.body.append(target)
})

afterEach(async () => {
  if (component) await unmount(component)
  component = undefined
  target.remove()
  authState.set({ status: 'anonymous', session: null, error: null })
})

describe('independently authorized evidence pages', () => {
  it('allows export-only sessions without granting integrity, restore or Hold management', async () => {
    login(['audit:export:create'])
    component = mount(Harness, { target })
    await vi.waitFor(() => expect(api.listExports).toHaveBeenCalled())
    expect(target.textContent).toContain('创建合规导出')
    expect(target.textContent).not.toContain('账本完整性验证')
    expect(api.listLegalHolds).not.toHaveBeenCalled()
    expect(api.listRestores).not.toHaveBeenCalled()
  })

  it('keeps owner controls metadata-only under a Hold denial and navigates to the linked child', async () => {
    const parent = { id: 'export-0123456789abcdef', tenant_id: 'tenant-a', requested_by: 'actor', status: 'failed', failure_code: 'legal_hold', created_at: new Date().toISOString() }
    const child = { ...parent, id: 'export-fedcba9876543210', status: 'pending', failure_code: undefined, retry_of: parent.id }
    api.listExports.mockResolvedValue({ items: [parent], count: 1 })
    api.getExport.mockRejectedValue(new ApiError(403, 'forbidden', 'blocked', 'req-1', 'legal_hold', 'hold-1'))
    // An existing successor predates the Hold: repeating retry returns only
    // its control summary; both parent and child details still deny evidence.
    api.retryExport.mockResolvedValue(child)
    window.location.hash = '#evidence/exports/export-0123456789abcdef'
    login(['audit:export:create'])
    component = mount(Harness, { target })
    await vi.waitFor(() => expect(api.getExport).toHaveBeenCalled())
    await vi.waitFor(() => expect(target.textContent).toContain('关联新任务重试'))
    expect(target.textContent).toContain('执行时被 Legal Hold 阻断')
    expect(target.textContent).not.toContain('下载 JSONL')
    button('关联新任务重试').click(); await settle()
    const confirm = [...document.querySelectorAll('button')].find((node) => node.textContent?.trim() === '确认重试')
    expect(confirm).toBeDefined(); confirm?.click()
    await vi.waitFor(() => expect(api.retryExport).toHaveBeenCalled())
    await vi.waitFor(() => expect(window.location.hash).toBe('#evidence/exports/export-fedcba9876543210'))
    window.dispatchEvent(new HashChangeEvent('hashchange')); await settle()
    await vi.waitFor(() => expect(api.getExport.mock.lastCall?.[0]).toBe(child.id))
    expect(target.textContent).not.toContain('下载 JSONL')
    expect(api.retryExport.mock.lastCall?.slice(0, 2)).toEqual([parent.id, 'tenant-a'])
  })

  it('requires changed text filters to be applied before paging, then resets the cursor', async () => {
    api.listExports.mockResolvedValue({ items: [], count: 2, next_cursor: 'old-cursor' })
    login(['audit:export:create'])
    component = mount(Harness, { target })
    await vi.waitFor(() => expect(api.listExports).toHaveBeenCalled())
    await settle()
    expect(button('下一页').disabled).toBe(false)
    const input = target.querySelector('input[aria-label="任务申请人"]') as HTMLInputElement
    input.value = 'new-actor'
    input.dispatchEvent(new Event('input', { bubbles: true }))
    await settle()
    expect(button('下一页').disabled).toBe(true)
    button('刷新任务').click()
    await settle()
    expect(api.listExports.mock.lastCall?.[0]).toMatchObject({ requested_by: 'new-actor', cursor: '' })
  })

  it('does not fetch unauthorized subpage deep links', async () => {
    window.location.hash = '#evidence/holds'
    login(['audit:export:create'])
    component = mount(Harness, { target })
    await settle()
    expect(target.textContent).toContain('当前会话没有该功能权限')
    expect(api.listLegalHolds).not.toHaveBeenCalled()
    expect(api.listExports).not.toHaveBeenCalled()
  })

  it('discards a late integrity result after A -> B -> A even when the transport ignores AbortSignal', async () => {
    login(['audit:integrity:verify', 'audit:platform:cross_tenant'])
    // Controlled props test the feature epoch independently; actual native
    // tenant selection/remounting is also covered by the browser regression.
    component = mount(IntegrityHarness, { target })
    await settle()
    let resolve!: (value: unknown) => void
    api.verifyIntegrity.mockReturnValue(new Promise((done) => (resolve = done)))
    button('开始验证').click()
    await settle()
    button('选择 B').click()
    await settle()
    button('选择 A').click()
    await settle()
    resolve({ tenant_id: 'tenant-a', valid: true, event_count: 987654, segment_count: 1 })
    await settle()
    expect(target.textContent).not.toContain('987654')
    expect((api.verifyIntegrity.mock.calls[0]?.[2] as AbortSignal).aborted).toBe(true)
  })

  it('clears old task responses when permissions or the token change, not just when the subject changes', async () => {
    let resolve!: (value: unknown) => void
    api.listExports.mockReturnValueOnce(new Promise((done) => (resolve = done)))
    login(['audit:export:create'])
    component = mount(Harness, { target })
    await vi.waitFor(() => expect(api.listExports).toHaveBeenCalled())
    login(['audit:integrity:verify'], 'new-token')
    await settle()
    resolve({ items: [{ id: 'old-secret-task', tenant_id: 'tenant-a', status: 'completed', requested_by: 'actor', created_at: new Date().toISOString() }], count: 1 })
    await settle()
    expect(target.textContent).toContain('账本完整性验证')
    expect(target.textContent).not.toContain('old-secret-task')
  })
})
