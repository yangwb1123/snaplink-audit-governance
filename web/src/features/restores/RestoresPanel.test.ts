// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { authState } from '../../auth/store'
import { ApiError } from '../../api/client'
import type { RestoreRun } from '../../api/types'
import Harness from './RestoresPanel.testHarness.svelte'

const { api } = vi.hoisted(() => ({ api: {
  listRestores: vi.fn(), getRestore: vi.fn(), decideRestore: vi.fn(), previewRestore: vi.fn(), createRestore: vi.fn(),
} }))
vi.mock('../../api/useAuditApi', () => ({ useAuditApi: () => api }))
const id = 'restore-aaaaaaaaaaaaaaaa'
const otherId = 'restore-bbbbbbbbbbbbbbbb'
const permissions = ['audit:operation:read', 'audit:legal_hold:manage']
let component: ReturnType<typeof mount> | undefined
let target: HTMLDivElement

function login(subject = 'approver', scopes = permissions, token = 'session-a', platform = false): void {
  authState.set({ status: 'authenticated', error: null, session: { subject, displayName: subject,
    accessToken: token, idToken: 'id-token-a', expiresAt: Date.now() + 100000, tenantId: 'tenant-a',
    roles: [], permissions: new Set(scopes), platform } })
}
function response(overrides: Partial<RestoreRun> = {}): RestoreRun {
  return { id, tenant_id: 'tenant-a', operation_id: 'operation-fixture', status: 'pending_approval', reason: '',
    created_by: 'requester', created_at: '2026-10-22T00:00:00Z', ...overrides }
}
function terminal(action: 'approve' | 'reject' = 'approve'): RestoreRun {
  return response(action === 'approve'
    ? { status: 'approved', approved_by: 'approver', approved_at: '2026-10-22T01:00:00Z' }
    : { status: 'rejected', rejected_by: 'approver', rejected_at: '2026-10-22T01:00:00Z' })
}
function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (cause: unknown) => void
  const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail })
  return { promise, resolve, reject }
}
async function settle(): Promise<void> { await tick(); await tick() }
function button(label: string): HTMLButtonElement {
  const found = [...document.querySelectorAll('button')].find((node) => node.textContent?.trim() === label)
  if (!found) throw new Error(`missing button ${label}`)
  return found
}
function hasDecision(): boolean {
  return [...target.querySelectorAll('button')].some((node) => ['批准', '拒绝'].includes(node.textContent?.trim() ?? ''))
}
function locked(): void {
  expect(button('批准').disabled).toBe(true)
  expect(button('拒绝').disabled).toBe(true)
  expect(document.body.textContent).not.toContain('确认批准恢复申请')
  expect(document.body.textContent).not.toContain('确认拒绝恢复申请')
}
async function render(): Promise<void> {
  component = mount(Harness, { target })
  await vi.waitFor(() => expect(api.getRestore).toHaveBeenCalledTimes(1))
  await settle()
}
async function submit(action: 'approve' | 'reject' = 'approve'): Promise<void> {
  const label = action === 'approve' ? '批准' : '拒绝'
  const before = api.decideRestore.mock.calls.length
  button(label).click(); await settle()
  expect(api.decideRestore).toHaveBeenCalledTimes(before)
  button(`确认${label}`).click(); await settle()
}
beforeEach(() => {
  vi.resetAllMocks(); login()
  api.listRestores.mockResolvedValue({ items: [], count: 0 })
  api.getRestore.mockImplementation((runId: string, tenant: string) => Promise.resolve(response({ id: runId, tenant_id: tenant })))
  target = document.createElement('div'); document.body.append(target)
})
afterEach(async () => {
  if (component) await unmount(component)
  component = undefined; target.remove()
  authState.set({ status: 'anonymous', session: null, error: null })
  expect(api.previewRestore).not.toHaveBeenCalled()
  expect(api.createRestore).not.toHaveBeenCalled()
})

describe('restore decision reconciliation', () => {
  for (const action of ['approve', 'reject'] as const) {
    it.each([
      ['network', new TypeError('response lost')],
      ['499', new ApiError(499, 'client_cancelled', 'cancelled')],
      ['503', new ApiError(503, 'store_timeout', 'timeout')],
      ['409', new ApiError(409, 'conflict', 'conflict')],
    ])(`${action}: unknown %s closes confirmation and forbids reusing old pending`, async (_label, error) => {
      api.decideRestore.mockRejectedValue(error)
      await render(); await submit(action)
      locked()
      expect(target.textContent).toContain('决策可能已提交')
      button('批准').click(); button('拒绝').click(); await settle()
      expect(api.decideRestore).toHaveBeenCalledTimes(1)
      expect(api.decideRestore.mock.lastCall?.slice(0, 3)).toEqual([id, action, 'tenant-a'])
      expect(api.decideRestore.mock.lastCall?.[3]).toBeInstanceOf(AbortSignal)
      expect(api.getRestore).toHaveBeenCalledTimes(1)
    })

    it(`${action}: valid pending observation needs a fresh confirmation`, async () => {
      api.decideRestore.mockRejectedValueOnce(new TypeError('response lost'))
      await render(); await submit(action); locked()
      const read = deferred<RestoreRun>()
      api.getRestore.mockReturnValueOnce(read.promise)
      button('刷新核对').click(); await settle(); locked()
      expect(button('刷新核对').disabled).toBe(true)
      read.resolve(response()); await settle()
      expect(button('批准').disabled).toBe(false)
      expect(button('拒绝').disabled).toBe(false)
      expect(api.decideRestore).toHaveBeenCalledTimes(1)
      api.decideRestore.mockResolvedValueOnce(terminal(action))
      await submit(action)
      expect(api.decideRestore).toHaveBeenCalledTimes(2)
      expect(hasDecision()).toBe(false)
    })

    it(`${action}: GET observes durable governance actor/time, never execution`, async () => {
      api.decideRestore.mockRejectedValue(new TypeError('response lost'))
      await render(); await submit(action)
      api.getRestore.mockResolvedValueOnce(terminal(action))
      button('刷新核对').click(); await settle()
      expect(hasDecision()).toBe(false)
      expect(target.textContent).toContain('决策人：approver')
      expect(target.querySelector('time')?.dateTime).toBe('2026-10-22T01:00:00Z')
      expect(target.textContent).toContain('当前只记录审批事实，不代表业务数据已经恢复')
      expect(api.decideRestore).toHaveBeenCalledTimes(1)
    })

    it(`${action}: terminal observation cannot regress, including selection A -> B -> A`, async () => {
      api.getRestore.mockResolvedValueOnce(terminal(action))
      await render()
      button('查询运行').click(); await settle()
      expect(hasDecision()).toBe(false)
      expect(target.querySelector('time')?.dateTime).toBe('2026-10-22T01:00:00Z')
      button('申请 B').click(); await settle()
      button('申请 A').click(); await settle()
      expect(hasDecision()).toBe(false)
      expect(target.querySelector('time')?.dateTime).toBe('2026-10-22T01:00:00Z')
      expect(api.decideRestore).not.toHaveBeenCalled()
    })
  }

  it.each([
    ['network', new TypeError('read failed')], ['499', new ApiError(499, 'client_cancelled', 'cancelled')],
    ['503', new ApiError(503, 'store_timeout', 'timeout')], ['404', new ApiError(404, 'not_found', 'missing')],
    ['409', new ApiError(409, 'conflict', 'conflict')],
    ['wrong tenant', response({ tenant_id: 'tenant-b' })], ['wrong id', response({ id: otherId })],
  ])('only GET original request; %s cannot unlock', async (_label, result) => {
    api.decideRestore.mockRejectedValue(new TypeError('response lost'))
    await render(); await submit()
    const lists = api.listRestores.mock.calls.length
    if (result instanceof Error) api.getRestore.mockRejectedValueOnce(result)
    else api.getRestore.mockResolvedValueOnce(result)
    const input = target.querySelector<HTMLInputElement>('input[placeholder="restore run id"]')!
    input.value = otherId; input.dispatchEvent(new Event('input', { bubbles: true })); await settle()
    button('刷新核对').click(); await settle(); locked()
    expect(api.getRestore).toHaveBeenCalledTimes(2)
    expect(api.getRestore.mock.lastCall?.slice(0, 2)).toEqual([id, 'tenant-a'])
    expect(api.listRestores).toHaveBeenCalledTimes(lists)
    expect(api.decideRestore).toHaveBeenCalledTimes(1)
  })

  it.each([
    ['tenant', { tenant_id: 'tenant-b' }], ['id', { id: otherId }],
    ['status', { status: 'pending_approval' }], ['actor', { approved_by: 'someone-else' }],
  ])('mismatched decision success %s remains uncertain', async (_label, fields) => {
    api.decideRestore.mockResolvedValue({ ...terminal(), ...fields })
    await render(); await submit(); locked()
    expect(target.querySelector('time')).toBeNull()
  })

  it.each([['requester', permissions], ['approver', ['audit:operation:read']]])('self/missing permission blocks %s', async (subject, scopes) => {
    login(subject, scopes); await render(); locked()
    expect(api.decideRestore).not.toHaveBeenCalled()
  })

  it('task change closes an unsubmitted confirmation', async () => {
    await render(); button('批准').click(); await settle()
    button('申请 B').click(); await settle()
    expect(document.body.textContent).not.toContain('确认批准恢复申请')
    expect(api.getRestore.mock.lastCall?.[0]).toBe(otherId)
    expect(api.decideRestore).not.toHaveBeenCalled()
  })

  it('a failed return to uncertain A exposes neither B nor write permission', async () => {
    api.decideRestore.mockRejectedValue(new TypeError('lost'))
    await render(); await submit()
    button('申请 B').click(); await settle()
    api.getRestore.mockRejectedValueOnce(new ApiError(503, 'store_timeout', 'failed'))
    button('申请 A').click(); await settle()
    expect(target.textContent).toContain('决策可能已提交')
    expect(target.textContent).not.toContain(`${otherId} · operation-fixture`)
    expect(hasDecision()).toBe(false)
    api.getRestore.mockResolvedValueOnce(response())
    button('刷新核对').click(); await settle()
    expect(button('批准').disabled).toBe(false)
    expect(api.decideRestore).toHaveBeenCalledTimes(1)
  })

  for (const scope of ['task', 'tenant', 'session'] as const) {
    for (const outcome of ['success', 'failure'] as const) {
      for (const request of ['decision', 'read'] as const) {
        it(`${scope} A -> B -> A: ignores late ${request} ${outcome} without AbortSignal cooperation`, async () => {
          login('approver', permissions, 'session-a', true)
          const old = deferred<RestoreRun>()
          if (request === 'decision') api.decideRestore.mockReturnValueOnce(old.promise)
          else api.decideRestore.mockRejectedValueOnce(new ApiError(503, 'store_timeout', 'unknown'))
          await render(); await submit()
          if (request === 'read') {
            api.getRestore.mockReturnValueOnce(old.promise)
            button('刷新核对').click(); await settle()
          }
          const signal = (request === 'decision' ? api.decideRestore.mock.lastCall?.[3] : api.getRestore.mock.lastCall?.[2]) as AbortSignal
          if (scope === 'session') login('approver', permissions, 'session-b', true)
          else button(scope === 'task' ? '申请 B' : '租户 B').click()
          await settle()
          if (scope === 'session') login('approver', permissions, 'session-a', true)
          else button(scope === 'task' ? '申请 A' : '租户 A').click()
          await settle()
          api.decideRestore.mockRejectedValueOnce(new ApiError(503, 'current_unknown', 'unknown'))
          await submit('reject'); locked()
          expect(signal.aborted).toBe(true)
          if (outcome === 'success') old.resolve(request === 'decision' ? terminal() : response())
          else old.reject(new ApiError(403, 'old_failure', 'old failure'))
          await settle(); locked()
          expect(target.querySelector('time')).toBeNull()
          expect(target.textContent).toContain('current_unknown')
          expect(target.textContent).not.toContain('old_failure')
          expect(api.decideRestore).toHaveBeenCalledTimes(2)
        })
      }
    }
  }

  it.each(['subject', 'token', 'id token', 'permission', 'expired', 'cleared task'] as const)('invalidates submitted confirmation after %s change', async (change) => {
    const old = deferred<RestoreRun>()
    api.decideRestore.mockReturnValueOnce(old.promise)
    await render(); await submit()
    const signal = api.decideRestore.mock.lastCall?.[3] as AbortSignal
    if (change === 'expired') authState.set({ status: 'anonymous', session: null, error: null })
    else if (change === 'subject') login('another-approver')
    else if (change === 'token') login('approver', permissions, 'session-b')
    else if (change === 'id token') authState.update((state) => ({ ...state, session: { ...state.session!, idToken: 'id-b' } }))
    else if (change === 'permission') login('approver', ['audit:operation:read'])
    else button('清除申请').click()
    await settle(); expect(signal.aborted).toBe(true)
    old.resolve(terminal()); await settle()
    expect(target.querySelector('time')).toBeNull()
    expect(document.body.textContent).not.toContain('确认批准恢复申请')
    expect(api.decideRestore).toHaveBeenCalledTimes(1)
    if (change === 'permission') locked()
  })

  it.each(['subject', 'token', 'id token'] as const)('a same-frame %s change cannot submit an old confirmation', async (change) => {
    await render(); button('批准').click(); await settle()
    const confirm = button('确认批准')
    if (change === 'subject') login('another-approver')
    else if (change === 'token') login('approver', permissions, 'session-b')
    else authState.update((state) => ({ ...state, session: { ...state.session!, idToken: 'id-b' } }))
    // Deliberately do not flush effects before invoking the stale dialog.
    confirm.click(); await settle()
    expect(api.decideRestore).not.toHaveBeenCalled()
    expect(document.body.textContent).not.toContain('确认批准恢复申请')
  })

  it.each([{ tenant_id: 'tenant-b' }, { id: otherId }])('an initial foreign detail cannot expose either decision: %j', async (fields) => {
    api.getRestore.mockResolvedValueOnce(response(fields))
    await render()
    expect(hasDecision()).toBe(false)
    expect(api.decideRestore).not.toHaveBeenCalled()
  })

  it('permission revocation closes confirmation before any dispatch', async () => {
    await render(); button('拒绝').click(); await settle()
    login('approver', ['audit:operation:read']); await settle(); locked()
    expect(api.decideRestore).not.toHaveBeenCalled()
  })
})
