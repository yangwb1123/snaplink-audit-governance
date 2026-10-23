// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { authState } from './auth/store'
import { ApiError } from './api/client'
import type { LegalHold } from './api/types'
import Harness from './Shell.testHarness.svelte'

const { api } = vi.hoisted(() => ({ api: {
  listTenants: vi.fn(), listLegalHolds: vi.fn(), createLegalHold: vi.fn(), releaseLegalHold: vi.fn(),
  listExports: vi.fn(), listRestores: vi.fn(), getGovernanceStatus: vi.fn(),
} }))
vi.mock('./api/useAuditApi', () => ({ useAuditApi: () => api }))
let target: HTMLDivElement
let component: ReturnType<typeof mount> | undefined
const permissions = ['audit:legal_hold:manage', 'audit:export:create', 'audit:platform:cross_tenant']
function login(overrides: Partial<NonNullable<Parameters<typeof authState.set>[0]['session']>> = {}): void {
  authState.set({ status: 'authenticated', error: null, session: {
    subject: 'actor', displayName: 'Actor', accessToken: 'token-a', idToken: 'id-a',
    tenantId: 'tenant-a', platform: true, permissions: new Set(permissions), roles: [],
    expiresAt: Date.now() + 100000, ...overrides,
  } })
}
async function settle(): Promise<void> { await tick(); await tick() }
function button(label: string): HTMLButtonElement {
  const node = [...document.querySelectorAll('button')].find((entry) => entry.textContent?.trim() === label)
  if (!node) throw new Error(`missing button ${label}`)
  return node
}
function deferred<T = unknown>() {
  let resolve!: (value: T) => void
  let reject!: (reason: unknown) => void
  const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail })
  return { promise, resolve, reject }
}
function forceClick(node: HTMLButtonElement): void {
  const disabled = node.disabled
  const detached = !node.isConnected
  node.disabled = false
  if (detached) target.append(node)
  try { node.dispatchEvent(new MouseEvent('click', { bubbles: true })) }
  finally {
    node.disabled = disabled
    if (detached) node.remove()
  }
}
function fields(): HTMLInputElement[] { return [...target.querySelectorAll<HTMLInputElement>('.three-field-form input')] }
async function draft(name = 'private-name', reason = 'private-reason'): Promise<void> {
  fields().forEach((field, index) => {
    field.value = index === 0 ? name : reason
    field.dispatchEvent(new Event('input', { bubbles: true }))
  })
  await settle()
}
async function render(): Promise<void> {
  component = mount(Harness, { target })
  await vi.waitFor(() => expect(api.listLegalHolds).toHaveBeenCalledTimes(1))
  await settle()
}
async function tenant(id: string): Promise<void> {
  const picker = target.querySelector<HTMLSelectElement>('select[aria-label="租户范围"]')!
  expect(picker.disabled).toBe(false)
  const oldField = fields()[0]
  // happy-dom 20 omits OPTION from :checked. Only supply the browser lookup;
  // selection, the native change event, Shell and keyed pages remain real.
  const query = picker.querySelector.bind(picker)
  const lookup = vi.spyOn(picker, 'querySelector').mockImplementation((selector) =>
    selector === ':checked' ? [...picker.options].find((option) => option.selected) ?? null : query(selector))
  picker.value = id
  expect(picker.value).toBe(id)
  picker.dispatchEvent(new Event('change', { bubbles: true }))
  lookup.mockRestore()
  await settle()
  expect(oldField?.isConnected).toBe(false)
}
async function route(hash: string): Promise<void> {
  window.location.hash = hash
  window.dispatchEvent(new HashChangeEvent('hashchange'))
  await settle()
}
type Departure = 'tenant' | 'subpage' | 'top-level'
async function leave(kind: Departure): Promise<void> {
  if (kind === 'tenant') await tenant('tenant-b')
  else if (kind === 'subpage') { button('导出任务').click(); await route(window.location.hash) }
  else {
    target.querySelector<HTMLButtonElement>('[data-iris-nav-item][data-key="overview"]')!.click()
    expect(window.location.hash).toBe('#overview')
    await route(window.location.hash)
  }
}
async function returnToHold(kind: Departure): Promise<void> {
  if (kind === 'tenant') await tenant('tenant-a')
  else {
    if (kind === 'top-level') {
      target.querySelector<HTMLButtonElement>('[data-iris-nav-item][data-key="evidence"]')!.click()
      expect(window.location.hash).toBe('#evidence')
      await route(window.location.hash)
    }
    button('法律保留').click(); await route(window.location.hash)
  }
}
function hold(overrides: Partial<LegalHold> = {}): LegalHold {
  return { id: 'hold-a', tenant_id: 'tenant-a', name: 'observed', reason: 'observed reason',
    created_by: 'actor', created_at: '2026-10-22T00:00:00Z',
    filter: { from: '2026-10-01T00:00:00Z', to: '2026-10-22T00:00:00Z', page_size: 1000 }, ...overrides }
}
beforeEach(() => {
  vi.resetAllMocks(); login()
  api.listTenants.mockResolvedValue({ items: [{ id: 'tenant-a', name: 'A' }, { id: 'tenant-b', name: 'B' }], count: 2 })
  api.listLegalHolds.mockResolvedValue({ items: [], count: 0 })
  api.listExports.mockResolvedValue({ items: [], count: 0 })
  api.listRestores.mockResolvedValue({ items: [], count: 0 })
  api.getGovernanceStatus.mockResolvedValue({ tenant_id: 'tenant-a' })
  window.location.hash = '#evidence/holds'
  target = document.createElement('div'); document.body.append(target)
})
afterEach(async () => {
  if (component) await unmount(component)
  component = undefined; target.remove()
  authState.set({ status: 'anonymous', session: null, error: null })
})

describe('real Shell Hold reconciliation across keyed remounts', () => {
  // Kept red-before-fix: the native tenant picker actually destroys the old
  // EvidencePage/HoldsPanel; changing a prop on the same panel is insufficient.
  for (const [label, cause] of [
    ['network', new TypeError('response lost')],
    ['503', new ApiError(503, 'store_timeout', 'timeout')],
    ['499', new ApiError(499, 'client_cancelled', 'cancelled')],
    ['500', new ApiError(500, 'internal_error', 'failed')],
    ['bad JSON', new SyntaxError('invalid JSON')],
  ] as const) {
    for (const read of ['pending', 'failure', 'null', 'missing count', 'mixed tenants'] as const) {
      it(`red regression: ${label}, native A -> B -> A with ${read} GET never permits a second POST`, async () => {
        api.createLegalHold.mockRejectedValue(cause)
        await render(); await draft()
        const oldFields = fields(); const oldCreate = button('创建范围保留')
        oldCreate.click(); await settle()
        expect(api.createLegalHold).toHaveBeenCalledTimes(1)
        await tenant('tenant-b')
        expect(oldFields[0]?.isConnected).toBe(false)
        const returned = deferred()
        api.listLegalHolds.mockReturnValueOnce(returned.promise)
        await tenant('tenant-a')
        expect(api.listLegalHolds.mock.lastCall?.[0]).toBe('tenant-a')
        expect(fields()[0]).not.toBe(oldFields[0])
        expect(fields().every((field) => field.value === '')).toBe(true)
        if (read === 'failure') returned.reject(new TypeError('read failed'))
        else if (read === 'null') returned.resolve({ items: null, count: 0 })
        else if (read === 'missing count') returned.resolve({ items: [] })
        else if (read === 'mixed tenants') returned.resolve({ items: [hold(), hold({ id: 'hold-b', tenant_id: 'tenant-b' })], count: 2 })
        await settle()
        if (read !== 'pending') {
          expect(target.textContent).toContain('保留读取失败，状态未知')
          expect(target.textContent).not.toContain('尚无法律保留')
          expect(target.textContent).not.toContain('0 条保留')
        }
        await draft('edited-name', 'edited-reason')
        const currentCreate = button('创建范围保留')
        currentCreate.click(); currentCreate.click(); forceClick(currentCreate); forceClick(oldCreate)
        await settle()
        expect(api.createLegalHold).toHaveBeenCalledTimes(1)
        expect(currentCreate.disabled).toBe(true)
        expect(target.textContent).toContain('创建可能已提交')
        expect(button('已核对，开始新草稿').disabled).toBe(true)
        expect(api.releaseLegalHold).not.toHaveBeenCalled()
      })
    }
  }

  for (const kind of ['tenant', 'subpage', 'top-level'] as const) {
    for (const read of ['pending', 'failure', 'null'] as const) {
      it(`${kind} departure preserves the constraint but never the draft or old GET acknowledgement (${read})`, async () => {
        api.createLegalHold.mockRejectedValueOnce(new ApiError(503, 'store_timeout', 'timeout'))
        await render(); await draft(); button('创建范围保留').click(); await settle()
        const filter = target.querySelector<HTMLInputElement>('input[placeholder="业务操作标识"]')!
        filter.value = 'private-operation'; filter.dispatchEvent(new Event('input', { bubbles: true }))
        button('刷新核对').click(); await settle()
        const oldAcknowledgement = button('已核对，开始新草稿')
        const oldCreate = button('创建范围保留'); const oldField = fields()[0]!
        expect(oldAcknowledgement.disabled).toBe(false)
        await leave(kind)
        expect(oldField.isConnected).toBe(false)
        const current = deferred(); api.listLegalHolds.mockReturnValueOnce(current.promise)
        await returnToHold(kind)
        expect(fields()[0]).not.toBe(oldField)
        expect(fields().every((field) => field.value === '')).toBe(true)
        expect(target.querySelector<HTMLInputElement>('input[placeholder="业务操作标识"]')!.value).toBe('')
        if (read === 'failure') current.reject(new TypeError('current read failed'))
        else if (read === 'null') current.resolve(null)
        await settle()
        forceClick(oldAcknowledgement); forceClick(oldCreate)
        await draft(); forceClick(button('创建范围保留')); forceClick(button('已核对，开始新草稿')); await settle()
        expect(api.createLegalHold).toHaveBeenCalledTimes(1)
        expect(target.textContent).toContain('创建可能已提交')
        expect(button('已核对，开始新草稿').disabled).toBe(true)
        expect(fields()[0]?.value).toBe('private-name')
      })
    }

    for (const outcome of ['success', 'failure'] as const) {
      it(`${kind} in-flight unmount ignores late ${outcome}, and pre-exit GET cannot authorize the new instance or request`, async () => {
        const oldWrite = deferred(); const preExit = deferred(); const returned = deferred()
        api.createLegalHold.mockReturnValueOnce(oldWrite.promise)
        await render(); await draft()
        const oldCreate = button('创建范围保留'); oldCreate.click(); await settle()
        const requested = api.createLegalHold.mock.calls[0]?.[0]
        const signal = api.createLegalHold.mock.calls[0]?.[2] as AbortSignal
        api.listLegalHolds.mockReturnValueOnce(preExit.promise)
        button('刷新保留').click(); await settle()
        const oldReadSignal = api.listLegalHolds.mock.lastCall?.[1] as AbortSignal
        const oldField = fields()[0]!
        await leave(kind)
        expect(oldField.isConnected).toBe(false)
        expect(signal.aborted).toBe(true); expect(oldReadSignal.aborted).toBe(true)
        api.listLegalHolds.mockReturnValueOnce(returned.promise)
        await returnToHold(kind)
        expect(target.textContent).toContain('创建可能已提交')
        expect(button('已核对，开始新草稿').disabled).toBe(true)
        preExit.resolve({ items: [hold({ name: 'stale-private-row' })], count: 1 }); await settle()
        expect(target.textContent).not.toContain('stale-private-row')
        expect(button('已核对，开始新草稿').disabled).toBe(true)
        await draft(); forceClick(oldCreate); forceClick(button('创建范围保留')); await settle()
        expect(api.createLegalHold).toHaveBeenCalledTimes(1)
        returned.resolve({ items: [], count: 0 }); await settle()
        expect(button('创建范围保留').disabled).toBe(true)
        const acknowledgement = button('已核对，开始新草稿')
        expect(acknowledgement.disabled).toBe(false)
        acknowledgement.click(); await settle()
        expect(api.createLegalHold).toHaveBeenCalledTimes(1)
        expect(fields().every((field) => field.value === '')).toBe(true)
        api.createLegalHold.mockRejectedValueOnce(new ApiError(503, 'current_unknown', 'current unknown'))
        await draft('new-name', 'new-reason'); button('创建范围保留').click(); await settle()
        expect(api.createLegalHold).toHaveBeenCalledTimes(2)
        const reads = api.listLegalHolds.mock.calls.length
        if (outcome === 'success') oldWrite.resolve(hold({ ...requested }))
        else oldWrite.reject(new ApiError(400, 'old_failure', 'old failure'))
        await settle(); forceClick(acknowledgement); forceClick(oldCreate); forceClick(button('创建范围保留')); await settle()
        expect(api.createLegalHold).toHaveBeenCalledTimes(2)
        expect(api.listLegalHolds).toHaveBeenCalledTimes(reads)
        expect(target.textContent).toContain('current_unknown')
        expect(target.textContent).not.toContain('old_failure')
        expect(target.textContent).toContain('创建可能已提交')
        expect(button('已核对，开始新草稿').disabled).toBe(true)
        expect(fields()[0]?.value).toBe('new-name')
      })
    }
  }

  for (const kind of ['tenant', 'subpage', 'top-level'] as const) {
    for (const outcome of ['success', 'failure'] as const) {
      for (const read of ['pending', 'failure'] as const) {
        it(`${kind} late POST ${outcome} cannot clear the original attempt while the new GET is ${read}`, async () => {
          const oldWrite = deferred(); const returned = deferred()
          api.createLegalHold.mockReturnValueOnce(oldWrite.promise)
          await render(); await draft()
          const oldField = fields()[0]!
          const oldCreate = button('创建范围保留'); oldCreate.click(); await settle()
          const requested = api.createLegalHold.mock.calls[0]?.[0]
          await leave(kind)
          expect(oldField.isConnected).toBe(false)
          expect((api.createLegalHold.mock.calls[0]?.[2] as AbortSignal).aborted).toBe(true)
          api.listLegalHolds.mockReturnValueOnce(returned.promise)
          await returnToHold(kind)
          if (read === 'failure') returned.reject(new TypeError('current read failed'))
          await settle()
          const readCount = api.listLegalHolds.mock.calls.length
          if (outcome === 'success') oldWrite.resolve(hold({ ...requested }))
          else oldWrite.reject(new ApiError(400, 'old_rejection', 'old rejection'))
          await settle()
          expect(api.listLegalHolds).toHaveBeenCalledTimes(readCount)
          expect(target.textContent).toContain('创建可能已提交')
          expect(target.textContent).not.toContain('old_rejection')
          await draft('new-input', 'new-reason')
          forceClick(oldCreate); forceClick(button('创建范围保留')); forceClick(button('已核对，开始新草稿')); await settle()
          expect(api.createLegalHold).toHaveBeenCalledTimes(1)
          expect(button('已核对，开始新草稿').disabled).toBe(true)
          expect(fields()[0]?.value).toBe('new-input')
          if (read === 'pending') returned.resolve({ items: [], count: 0 })
          else button('刷新核对').click()
          await settle()
          expect(button('已核对，开始新草稿').disabled).toBe(false)
          expect(button('创建范围保留').disabled).toBe(true)
          button('已核对，开始新草稿').click(); await settle()
          expect(api.createLegalHold).toHaveBeenCalledTimes(1)
          expect(fields().every((field) => field.value === '')).toBe(true)
        })
      }
    }
  }

  it.each(['empty', 'same name'] as const)('a new instance GET (%s) still requires explicit zero-POST acknowledgement and a clean draft', async (result) => {
    api.createLegalHold.mockRejectedValueOnce(new TypeError('response lost'))
    await render(); await draft(); button('创建范围保留').click(); await settle()
    await leave('subpage')
    api.listLegalHolds.mockResolvedValueOnce({ items: result === 'empty' ? [] : [hold({ name: 'private-name', reason: 'private-reason' })], count: 0 })
    await returnToHold('subpage')
    expect(target.textContent).toContain('空列表或同名记录都不能确定原请求是否已提交')
    expect(button('已核对，开始新草稿').disabled).toBe(false)
    await draft('edited-name', 'edited-reason')
    forceClick(button('创建范围保留')); await settle()
    expect(api.createLegalHold).toHaveBeenCalledTimes(1)
    button('已核对，开始新草稿').click(); await settle()
    expect(api.createLegalHold).toHaveBeenCalledTimes(1)
    expect(fields().every((field) => field.value === '')).toBe(true)
    expect(button('创建范围保留').disabled).toBe(true)
  })

  it.each(['null', 'tenant', 'name', 'reason', 'filter', 'actor'] as const)(
    'mismatched %s success remains constrained across native A -> B -> A and fresh GET', async (mismatch) => {
      api.createLegalHold.mockImplementationOnce((request) => Promise.resolve(mismatch === 'null' ? null : hold({
        ...request,
        ...(mismatch === 'tenant' ? { tenant_id: 'tenant-b' } : mismatch === 'name' ? { name: 'different' } :
          mismatch === 'reason' ? { reason: 'different' } : mismatch === 'actor' ? { created_by: 'other' } :
          { filter: { ...request.filter, operation_id: 'different' } }),
      })))
      await render(); await draft()
      const oldCreate = button('创建范围保留'); oldCreate.click(); await settle()
      expect(target.textContent).toContain('创建可能已提交')
      await tenant('tenant-b'); await tenant('tenant-a')
      expect(button('已核对，开始新草稿').disabled).toBe(false)
      await draft('replacement-name', 'replacement-reason')
      forceClick(oldCreate); forceClick(button('创建范围保留')); await settle()
      expect(api.createLegalHold).toHaveBeenCalledTimes(1)
      button('已核对，开始新草稿').click(); await settle()
      expect(api.createLegalHold).toHaveBeenCalledTimes(1)
      expect(fields().every((field) => field.value === '')).toBe(true)
      expect(button('创建范围保留').disabled).toBe(true)
      expect(api.releaseLegalHold).not.toHaveBeenCalled()
    },
  )

  for (const change of ['token', 'subject', 'permission', 'id token'] as const) {
    for (const restore of [false, true]) {
      it(`real Shell fences an unsubmitted draft and release on same-frame ${change} change (restore=${restore})`, async () => {
        api.listLegalHolds.mockResolvedValue({ items: [hold()], count: 1 })
        await render(); await draft(); button('释放').click(); await settle()
        const oldField = fields()[0]!
        const oldCreate = button('创建范围保留'); const oldConfirm = button('确认释放')
        let original!: Parameters<typeof authState.set>[0]
        const stop = authState.subscribe((state) => { original = state }); stop()
        authState.set({ ...original, session: { ...original.session!,
          ...(change === 'token' ? { accessToken: 'token-b' } : change === 'subject' ? { subject: 'other' } :
            change === 'id token' ? { idToken: 'id-b' } : { permissions: new Set(['audit:export:create', 'audit:platform:cross_tenant']) }),
        } })
        forceClick(oldCreate); forceClick(oldConfirm)
        if (restore) {
          authState.set(original)
          forceClick(oldCreate); forceClick(oldConfirm)
        }
        await settle()
        expect(api.createLegalHold).not.toHaveBeenCalled()
        expect(api.releaseLegalHold).not.toHaveBeenCalled()
        // Restoring the auth binding in one frame keeps Shell's existing key.
        // The owner still retires synchronously and clears this panel's draft.
        expect(oldField.isConnected).toBe(restore)
        if (restore) expect(fields()[0]).toBe(oldField)
        expect(document.body.textContent).not.toContain('确认释放法律保留')
        expect(fields().every((field) => field.value === '')).toBe(true)
      })
    }
  }

  it('explicit 400 rejection permits a corrected intentional create without an unknown-outcome acknowledgement', async () => {
    api.createLegalHold.mockRejectedValueOnce(new ApiError(400, 'invalid_request', 'invalid'))
    await render(); await draft(); button('创建范围保留').click(); await settle()
    expect(target.textContent).not.toContain('创建可能已提交')
    expect(button('创建范围保留').disabled).toBe(false)
    api.createLegalHold.mockImplementationOnce((request) => Promise.resolve(hold({ ...request })))
    await draft('corrected-name', 'corrected-reason'); button('创建范围保留').click(); await settle()
    expect(api.createLegalHold).toHaveBeenCalledTimes(2)
    expect(fields().every((field) => field.value === '')).toBe(true)
    expect(api.releaseLegalHold).not.toHaveBeenCalled()
  })

  it('navigation keeps raw drafts and uncertain constraints out of URLs, logs and browser storage', async () => {
    await render()
    const storage = [vi.spyOn(window, 'localStorage', 'get'), vi.spyOn(window, 'sessionStorage', 'get')]
    const logs = [vi.spyOn(console, 'log'), vi.spyOn(console, 'info'), vi.spyOn(console, 'warn'), vi.spyOn(console, 'error'), vi.spyOn(console, 'debug')]
    try {
      api.createLegalHold.mockRejectedValueOnce(new TypeError('response lost'))
      await draft()
      const filter = target.querySelector<HTMLInputElement>('input[placeholder="业务操作标识"]')!
      filter.value = 'private-operation'; filter.dispatchEvent(new Event('input', { bubbles: true })); await settle()
      button('创建范围保留').click(); await settle()
      for (const kind of ['subpage', 'top-level', 'tenant'] as const) {
        await leave(kind)
        expect(window.location.href).not.toMatch(/private-name|private-reason|private-operation/)
        await returnToHold(kind)
        expect(window.location.hash).toBe('#evidence/holds')
        expect(fields().every((field) => field.value === '')).toBe(true)
        expect(target.querySelector<HTMLInputElement>('input[placeholder="业务操作标识"]')!.value).toBe('')
        expect(target.textContent).toContain('创建可能已提交')
      }
      expect(api.createLegalHold).toHaveBeenCalledTimes(1)
      for (const spy of [...storage, ...logs]) expect(spy).not.toHaveBeenCalled()
      expect(api.listLegalHolds.mock.calls.every((call) => call.length === 2 && typeof call[0] === 'string' && call[1] instanceof AbortSignal)).toBe(true)
    } finally { for (const spy of [...storage, ...logs]) spy.mockRestore() }
  })

  it('keeps independent constraints for other tenants through tenant selection and top-level routing', async () => {
    api.createLegalHold.mockRejectedValue(new ApiError(503, 'unknown', 'unknown'))
    await render(); await draft(); button('创建范围保留').click(); await settle()
    await tenant('tenant-b')
    await draft('tenant-b-name', 'tenant-b-reason'); button('创建范围保留').click(); await settle()
    expect(api.createLegalHold.mock.calls.map((call) => call[1])).toEqual(['tenant-a', 'tenant-b'])
    await tenant('tenant-a')
    button('已核对，开始新草稿').click(); await settle()
    expect(api.createLegalHold).toHaveBeenCalledTimes(2)
    await route('#overview'); await route('#evidence/holds')
    const pending = deferred(); api.listLegalHolds.mockReturnValueOnce(pending.promise)
    await tenant('tenant-b')
    await draft(); forceClick(button('创建范围保留')); await settle()
    expect(api.createLegalHold).toHaveBeenCalledTimes(2)
    expect(target.textContent).toContain('创建可能已提交')
    expect(button('已核对，开始新草稿').disabled).toBe(true)
  })

  for (const change of ['token', 'subject', 'permission', 'id token'] as const) {
    it(`synchronously retires the owner on same-frame ${change} A -> B -> A, fencing old DOM and late success`, async () => {
      const oldWrite = deferred(); api.createLegalHold.mockReturnValueOnce(oldWrite.promise)
      await render(); await draft()
      const oldCreate = button('创建范围保留'); oldCreate.click(); await settle()
      const requested = api.createLegalHold.mock.calls[0]?.[0]
      let original!: Parameters<typeof authState.set>[0]
      const stop = authState.subscribe((state) => { original = state }); stop()
      authState.set({ ...original, session: { ...original.session!,
        ...(change === 'token' ? { accessToken: 'token-b' } : change === 'subject' ? { subject: 'other' } :
          change === 'id token' ? { idToken: 'id-b' } : { permissions: new Set(['audit:export:create', 'audit:platform:cross_tenant']) }),
      } })
      forceClick(oldCreate)
      authState.set(original)
      forceClick(oldCreate); await settle()
      await vi.waitFor(() => expect(fields()[0]?.value).toBe(''))
      expect(api.createLegalHold).toHaveBeenCalledTimes(1)
      expect((api.createLegalHold.mock.calls[0]?.[2] as AbortSignal).aborted).toBe(true)
      api.createLegalHold.mockRejectedValueOnce(new ApiError(503, 'new_identity_unknown', 'new unknown'))
      await draft('new-identity-name', 'new-identity-reason'); button('创建范围保留').click(); await settle()
      expect(api.createLegalHold).toHaveBeenCalledTimes(2)
      const reads = api.listLegalHolds.mock.calls.length
      oldWrite.resolve(hold({ ...requested })); await settle()
      expect(api.listLegalHolds).toHaveBeenCalledTimes(reads)
      expect(target.textContent).toContain('new_identity_unknown')
      expect(button('已核对，开始新草稿').disabled).toBe(true)
      forceClick(oldCreate); forceClick(button('创建范围保留')); await settle()
      expect(api.createLegalHold).toHaveBeenCalledTimes(2)
    })
  }
})
