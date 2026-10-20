// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { writable } from 'svelte/store'
import { authState } from '../../auth/store'
import { ApiError } from '../../api/client'
import type { GovernanceStatus } from '../../api/types'
import Harness from './GovernanceSummary.testHarness.svelte'
import Overview from '../../pages/OverviewPage.svelte'

const { api } = vi.hoisted(() => ({ api: { getGovernanceStatus: vi.fn(), queryEvents: vi.fn(), getEventFacets: vi.fn(), listAdminActions: vi.fn() } }))
vi.mock('../../api/useAuditApi', () => ({ useAuditApi: () => api }))
let target: HTMLDivElement
let component: ReturnType<typeof mount> | undefined
const scope = writable<{ tenantId?: string; revision: number }>({ tenantId: 'tenant-a', revision: 0 })
const allPermissions = ['audit:export:create', 'audit:operation:read', 'audit:legal_hold:manage', 'audit:policy:read']
const first: GovernanceStatus = {
  tenant_id: 'tenant-a', observed_at: '2026-10-08T00:00:00Z', source: 'control', completeness: 'not_asserted',
  exports: { total: 6, pending: 2, running: 1, completed: 2, failed: 1, cancelled: 0, unknown: 0, tenant_queue_limit: 32, tenant_running_limit: 1 },
  restores: { total: 7, pending_approval: 4, approved: 2, rejected: 1, unknown: 0, mine_pending_approval: 1, actionable: 2, execution: 'not_tracked' },
  legal_holds: { total: 3, active: 2, released: 1 },
  retention: { configured: true, policy: { tenant_id: 'tenant-a', retention_class: 'standard', hot_days: 7, warm_days: 30, archive_days: 365 } },
}

function login(permissions = allPermissions, token = 'memory-token-a', platform = false): void {
  authState.set({ status: 'authenticated', session: {
    accessToken: token, idToken: 'memory-id', subject: 'alice', displayName: 'Alice', expiresAt: Date.now() + 3_600_000,
    tenantId: platform ? '' : 'tenant-a', platform, roles: [], permissions: new Set(permissions),
  }, error: null })
}
async function settle(): Promise<void> { await tick(); await Promise.resolve(); await tick() }
function deferred() {
  let resolve!: (value: GovernanceStatus) => void
  const promise = new Promise<GovernanceStatus>((done) => { resolve = done })
  return { promise, resolve }
}
function signal(index: number): AbortSignal { return api.getGovernanceStatus.mock.calls[index]?.[1] as AbortSignal }

beforeEach(() => {
  vi.resetAllMocks()
  api.getGovernanceStatus.mockResolvedValue(first)
  scope.set({ tenantId: 'tenant-a', revision: 0 })
  login()
  target = document.createElement('div'); document.body.append(target)
})
afterEach(async () => {
  if (component) await unmount(component)
  component = undefined; target.remove()
  authState.set({ status: 'anonymous', session: null, error: null })
})

describe('authorized tenant governance observation', () => {
  it('renders control counts with approval/download/retention limitations, not execution success', async () => {
    component = mount(Harness, { target, props: { scope } }); await settle()
    expect(target.textContent).toContain('排队 2 · 运行 1 · 已完成 2 · 失败 1')
    expect(target.textContent).toContain('已取消 0')
    expect(target.textContent).toContain('待我决策 2')
    expect(target.textContent).toContain('批准不是业务执行成功')
    expect(target.textContent).toContain('不是当前可用配额')
    expect(target.textContent).toContain('不是生命周期执行')
    expect(target.textContent).toContain('完整性未声明')
    expect(target.textContent).not.toContain('memory-token-a')
    expect(target.querySelector('a[href="#evidence/exports"]')).not.toBeNull()
  })

  it('does not display unauthorized sections or decision counts even if the server returns them', async () => {
    login(['audit:operation:read'])
    component = mount(Harness, { target, props: { scope } }); await settle()
    expect(target.textContent).toContain('已批准 2')
    expect(target.textContent).not.toContain('待我决策')
    expect(target.textContent).not.toContain('导出作业')
    expect(target.textContent).not.toContain('留存策略配置')
    expect(target.querySelector('a[href="#evidence/holds"]')).toBeNull()
  })

  it('loads governance independently of event read permission on the actual overview', async () => {
    login(['audit:policy:read'])
    component = mount(Overview, { target, props: { tenantId: 'tenant-a' } }); await settle()
    expect(api.getGovernanceStatus).toHaveBeenCalledWith('tenant-a', expect.any(AbortSignal))
    expect(api.queryEvents).not.toHaveBeenCalled()
    expect(api.getEventFacets).not.toHaveBeenCalled()
    expect(target.textContent).toContain('留存策略配置')
    expect(target.textContent).toContain('Archive 365 天')
  })

  it('rejects old A and B responses after A -> B -> A, even when requests ignore cancellation', async () => {
    login(allPermissions, 'memory-token-a', true)
    const oldA = deferred(); const oldB = deferred()
    api.getGovernanceStatus.mockReturnValueOnce(oldA.promise).mockReturnValueOnce(oldB.promise)
    component = mount(Harness, { target, props: { scope } }); await settle()
    scope.set({ tenantId: 'tenant-b', revision: 0 }); await settle()
    scope.set({ tenantId: 'tenant-a', revision: 0 }); await settle()
    expect(signal(0).aborted).toBe(true); expect(signal(1).aborted).toBe(true)
    oldA.resolve({ ...first, restores: { ...first.restores!, approved: 999 } })
    oldB.resolve({ ...first, tenant_id: 'tenant-b', restores: { ...first.restores!, approved: 888 } }); await settle()
    expect(target.textContent).toContain('已批准 2')
    expect(target.textContent).not.toContain('999'); expect(target.textContent).not.toContain('888')
    expect(api.getGovernanceStatus.mock.calls.map(call => call[0])).toEqual(['tenant-a', 'tenant-b', 'tenant-a'])
  })

  it('isolates token and permission changes even with the same subject and tenant', async () => {
    const old = deferred(); const newer = deferred()
    api.getGovernanceStatus.mockReturnValueOnce(old.promise).mockReturnValueOnce(newer.promise)
    component = mount(Harness, { target, props: { scope } }); await settle()
    login(allPermissions, 'memory-token-b'); await settle()
    expect(signal(0).aborted).toBe(true)
    login(['audit:operation:read'], 'memory-token-b'); await settle()
    expect(signal(1).aborted).toBe(true)
    old.resolve({ ...first, restores: { ...first.restores!, approved: 999 } })
    newer.resolve({ ...first, restores: { ...first.restores!, approved: 888 } }); await settle()
    expect(target.textContent).toContain('已批准 2')
    expect(target.textContent).not.toContain('导出作业')
    expect(target.textContent).not.toContain('999'); expect(target.textContent).not.toContain('888')
  })

  it('does not fetch for writers or empty platform scope and cancels a request on unmount', async () => {
    login(['audit:event:write'])
    component = mount(Harness, { target, props: { scope } }); await settle()
    expect(api.getGovernanceStatus).not.toHaveBeenCalled()
    login(allPermissions, 'memory-token-a', true)
    scope.set({ tenantId: '', revision: 0 }); await settle()
    expect(api.getGovernanceStatus).not.toHaveBeenCalled()
    expect(target.textContent).toContain('请先选择一个已授权租户')
    const pending = deferred(); api.getGovernanceStatus.mockReturnValueOnce(pending.promise)
    scope.set({ tenantId: 'tenant-a', revision: 0 }); await settle()
    await unmount(component); component = undefined
    expect(signal(0).aborted).toBe(true)
    pending.resolve(first); await settle()
    expect(target.textContent).toBe('')
  })

  it('clears previous counts on refresh/error, never fabricates a zero-count success', async () => {
    component = mount(Harness, { target, props: { scope } }); await settle()
    api.getGovernanceStatus.mockRejectedValueOnce(new ApiError(503, 'store_timeout', 'internal server error', 'req-7'))
    scope.update(value => ({ ...value, revision: value.revision + 1 })); await settle()
    expect(target.textContent).toContain('req-7')
    expect(target.textContent).not.toContain('已批准 2')
    expect(target.textContent).not.toContain('已批准 0')
    const retry = [...target.querySelectorAll('button')].find(button => button.textContent?.trim() === '重试')!
    retry.click(); await settle()
    expect(target.textContent).toContain('已批准 2')
  })

  it.each([
    { ...first, tenant_id: 'tenant-b' },
    { ...first, retention: { configured: true, policy: { ...first.retention!.policy!, tenant_id: 'tenant-b' } } },
    { ...first, source: 'clickhouse' },
    { ...first, exports: { ...first.exports!, cancelled: undefined } },
    { ...first, exports: { ...first.exports!, cancelled: -1 } },
    { ...first, restores: { ...first.restores!, execution: 'completed' } },
  ])('rejects misbound/false-semantics responses instead of showing their counts', async (response) => {
    api.getGovernanceStatus.mockResolvedValue(response)
    component = mount(Harness, { target, props: { scope } }); await settle()
    expect(target.textContent).not.toContain('已批准 2')
    expect(target.textContent).toContain('不匹配')
  })
})
