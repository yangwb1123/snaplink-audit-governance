// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { authState } from '../../auth/store'
import { ApiError } from '../../api/client'
import type { LegalHold } from '../../api/types'
import Harness from './HoldsPanel.testHarness.svelte'

const { api } = vi.hoisted(() => ({ api: {
  listLegalHolds: vi.fn(), createLegalHold: vi.fn(), releaseLegalHold: vi.fn(),
} }))
vi.mock('../../api/useAuditApi', () => ({ useAuditApi: () => api }))
let component: ReturnType<typeof mount> | undefined
let target: HTMLDivElement

function login(subject = 'actor', token = 'session-a', permissions = ['audit:legal_hold:manage'], platform = false): void {
  authState.set({ status: 'authenticated', error: null, session: { subject, displayName: subject,
    accessToken: token, idToken: 'id-token-a', expiresAt: Date.now() + 100000, tenantId: 'tenant-a',
    roles: [], permissions: new Set(permissions), platform } })
}
async function settle(): Promise<void> { await tick(); await tick() }
function button(label: string): HTMLButtonElement {
  const found = [...document.querySelectorAll('button')].find((node) => node.textContent?.trim() === label)
  if (!found) throw new Error(`missing button ${label}`)
  return found
}
function input(index: number, value: string): void {
  const field = target.querySelectorAll<HTMLInputElement>('.three-field-form input')[index]!
  field.value = value; field.dispatchEvent(new Event('input', { bubbles: true }))
}
async function draft(): Promise<void> { input(0, 'private-hold-name'); input(1, 'private-hold-reason'); await settle() }
async function render(initialTenant = 'tenant-a'): Promise<void> {
  component = mount(Harness, { target, props: { initialTenant } })
  await settle()
}
function response(overrides: Partial<LegalHold> = {}): LegalHold {
  return { id: 'hold-aaaaaaaaaaaaaaaa', tenant_id: 'tenant-a', name: 'observed-hold', reason: 'observed-reason',
    created_by: 'actor', created_at: '2026-10-22T00:00:00Z',
    filter: { from: '2026-10-01T00:00:00Z', to: '2026-10-22T00:00:00Z', page_size: 1000 }, ...overrides }
}
function deferred<T = unknown>() {
  let resolve!: (value: T) => void
  let reject!: (cause: unknown) => void
  const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail })
  return { promise, resolve, reject }
}
function forceClick(node: HTMLButtonElement): void {
  // Exercise the handler, not just the browser's disabled-button suppression.
  const disabled = node.disabled
  const detached = !node.isConnected
  node.disabled = false
  if (detached) target.append(node)
  try { node.dispatchEvent(new MouseEvent('click', { bubbles: true })) }
  finally {
    node.disabled = disabled
    // Restore connectivity too: stale probes must not shadow the new dialog.
    if (detached) node.remove()
  }
}
function hasButton(label: string): boolean {
  return [...document.querySelectorAll('button')].some((node) => node.textContent?.trim() === label)
}
function fields(): HTMLInputElement[] { return [...target.querySelectorAll<HTMLInputElement>('.three-field-form input, .evidence-scope-form input:not([readonly])')] }
async function uncertain(): Promise<void> {
  api.createLegalHold.mockRejectedValueOnce(new ApiError(503, 'store_timeout', 'timeout'))
  await render(); await draft(); button('创建范围保留').click(); await settle()
  expect(target.textContent).toContain('创建可能已提交')
}
beforeEach(() => {
  vi.resetAllMocks(); login()
  api.listLegalHolds.mockResolvedValue({ items: [], count: 0 })
  target = document.createElement('div'); document.body.append(target)
})
afterEach(async () => {
  if (component) await unmount(component)
  component = undefined; target.remove()
  authState.set({ status: 'anonymous', session: null, error: null })
})

describe('Hold create reconciliation', () => {
  // Keep these regression cases: before the fix both dispatch a second POST
  // without any intervening GET after an uncertain creation result.
  it.each([
    ['network', new TypeError('response lost')],
    ['503', new ApiError(503, 'store_timeout', 'timeout')],
  ])('red regression: %s cannot resubmit the original or edited draft without reconciliation', async (_label, cause) => {
    api.createLegalHold.mockRejectedValue(cause)
    await render(); await draft()
    const create = button('创建范围保留')
    create.click(); await settle()
    expect(api.createLegalHold).toHaveBeenCalledTimes(1)
    expect(api.listLegalHolds).toHaveBeenCalledTimes(1)
    create.click(); await settle()
    input(0, 'edited-name'); input(1, 'edited-reason'); await settle()
    create.click(); await settle()
    expect(api.createLegalHold).toHaveBeenCalledTimes(1)
    expect(button('创建范围保留').disabled).toBe(true)
    expect(target.textContent).toContain('创建可能已提交')
    expect(api.listLegalHolds).toHaveBeenCalledTimes(1)
    expect(api.releaseLegalHold).not.toHaveBeenCalled()
  })

  it.each([
    ['network', new TypeError('response lost')],
    ['503', new ApiError(503, 'store_timeout', 'timeout')],
    ['500', new ApiError(500, 'internal_error', 'failed')],
    ['504', new ApiError(504, 'http_504', 'failed')],
    ['499', new ApiError(499, 'client_cancelled', 'cancelled')],
    ['bad JSON', new SyntaxError('invalid JSON')],
    ['null success', null], ['missing fields', {}],
    ['wrong tenant', response({ tenant_id: 'tenant-b' })],
    ['wrong name', response()],
    ['wrong actor', response({ name: 'private-hold-name', reason: 'private-hold-reason', created_by: 'other' })],
    ['wrong reason', response({ name: 'private-hold-name' })],
    ['wrong filter', response({ name: 'private-hold-name', reason: 'private-hold-reason' })],
    ['released success', response({ name: 'private-hold-name', reason: 'private-hold-reason', released_at: '2026-10-22T01:00:00Z' })],
  ])('%s success/failure, stale DOM events and editing never replay creation', async (_label, result) => {
    const write = deferred()
    api.createLegalHold.mockReturnValueOnce(write.promise)
    await render(); await draft()
    const create = button('创建范围保留')
    create.click(); forceClick(create); await settle()
    expect(api.createLegalHold).toHaveBeenCalledTimes(1)
    if (result instanceof Error) write.reject(result)
    else write.resolve(result)
    await settle()
    expect(target.textContent).toContain('创建可能已提交')
    expect(button('创建范围保留').disabled).toBe(true)
    forceClick(create); input(0, 'edited-name'); input(1, 'edited-reason')
    const filter = target.querySelector<HTMLInputElement>('input[placeholder="业务操作标识"]')!
    filter.value = 'private-operation'; filter.dispatchEvent(new Event('input', { bubbles: true }))
    await settle(); forceClick(create); await settle()
    expect(api.createLegalHold).toHaveBeenCalledTimes(1)
    expect(api.listLegalHolds).toHaveBeenCalledTimes(1)
    expect(button('已核对，开始新草稿').disabled).toBe(true)
  })

  it('a create response with release metadata is not a matching successful creation', async () => {
    api.createLegalHold.mockImplementationOnce((request) => Promise.resolve(response({ ...request, released_by: 'actor' })))
    await render(); await draft(); button('创建范围保留').click(); await settle()
    expect(target.textContent).toContain('创建可能已提交')
    forceClick(button('创建范围保留')); await settle()
    expect(api.createLegalHold).toHaveBeenCalledTimes(1)
    expect(button('已核对，开始新草稿').disabled).toBe(true)
  })

  it.each([
    ['empty', []],
    ['same name', [response({ name: 'private-hold-name', reason: 'private-hold-reason' })]],
  ] as const)('valid GET %s requires explicit new empty draft, never proves the original outcome', async (_label, items) => {
    await uncertain()
    const filter = target.querySelector<HTMLInputElement>('input[placeholder="业务操作标识"]')!
    filter.value = 'private-operation'; filter.dispatchEvent(new Event('input', { bubbles: true })); await settle()
    const read = deferred()
    api.listLegalHolds.mockReturnValueOnce(read.promise)
    button('刷新核对').click(); await settle()
    expect(button('已核对，开始新草稿').disabled).toBe(true)
    read.resolve({ items, count: 999 }); await settle()
    expect(button('已核对，开始新草稿').disabled).toBe(false)
    expect(button('创建范围保留').disabled).toBe(true)
    expect(target.textContent).toContain('空列表或同名记录都不能确定原请求是否已提交')
    forceClick(button('创建范围保留')); await settle()
    expect(api.createLegalHold).toHaveBeenCalledTimes(1)
    button('已核对，开始新草稿').click(); await settle()
    expect(fields().every((field) => field.value === '')).toBe(true)
    expect(api.createLegalHold).toHaveBeenCalledTimes(1)
    expect(api.releaseLegalHold).not.toHaveBeenCalled()
    expect(button('创建范围保留').disabled).toBe(true)
    api.createLegalHold.mockImplementationOnce((request) => Promise.resolve(response({ ...request })))
    await draft(); button('创建范围保留').click(); await settle()
    expect(api.createLegalHold).toHaveBeenCalledTimes(2)
    expect(api.createLegalHold.mock.calls[1]?.[0].filter.operation_id).toBe('')
    expect(target.textContent).not.toContain('创建可能已提交')
  })

  it.each([
    ['network', new TypeError('read failed')], ['503', new ApiError(503, 'store_timeout', 'failed')],
    ['499', new ApiError(499, 'client_cancelled', 'cancelled')],
    ['bad JSON', new SyntaxError('bad JSON')], ['null', null], ['null items', { items: null }],
    ['missing items', { count: 0 }], ['non-array', { items: {} }], ['null record', { items: [null] }],
    ['missing count', { items: [] }], ['invalid count', { items: [], count: '0' }], ['negative count', { items: [], count: -1 }],
    ['mixed tenants', { items: [response(), response({ id: 'hold-b', tenant_id: 'tenant-b' })] }],
    ['bad record', { items: [response({ filter: undefined })] }],
    ['duplicate id', { items: [response(), response()] }],
  ])('GET %s cannot unlock, count as empty, or enable release', async (_label, result) => {
    await uncertain()
    if (result instanceof Error) api.listLegalHolds.mockRejectedValueOnce(result)
    else api.listLegalHolds.mockResolvedValueOnce(result)
    button('刷新核对').click(); await settle()
    expect(button('已核对，开始新草稿').disabled).toBe(true)
    forceClick(button('已核对，开始新草稿')); await settle()
    expect(target.textContent).toContain('保留读取失败，状态未知')
    expect(target.textContent).not.toContain('尚无法律保留')
    expect(target.textContent).not.toContain('0 条保留')
    expect(hasButton('释放')).toBe(false)
    expect(api.createLegalHold).toHaveBeenCalledTimes(1)
    expect(api.releaseLegalHold).not.toHaveBeenCalled()
    expect(api.listLegalHolds.mock.lastCall?.[0]).toBe('tenant-a')
  })

  it('GET started before failure cannot qualify, even if it succeeds after failure', async () => {
    const write = deferred(); const read = deferred()
    api.createLegalHold.mockReturnValueOnce(write.promise)
    await render(); await draft(); button('创建范围保留').click(); await settle()
    api.listLegalHolds.mockReturnValueOnce(read.promise)
    button('刷新保留').click(); await settle()
    write.reject(new TypeError('response lost')); await settle()
    read.resolve({ items: [], count: 0 }); await settle()
    expect(button('已核对，开始新草稿').disabled).toBe(true)
    expect(api.createLegalHold).toHaveBeenCalledTimes(1)
    button('刷新核对').click(); await settle()
    expect(button('已核对，开始新草稿').disabled).toBe(false)
  })

  it('a pre-create GET ignoring abort cannot restore rows or authorize a new draft', async () => {
    const old = deferred()
    api.listLegalHolds.mockReturnValueOnce(old.promise)
    await render(); await draft()
    api.createLegalHold.mockRejectedValueOnce(new TypeError('response lost'))
    button('创建范围保留').click(); await settle()
    expect((api.listLegalHolds.mock.calls[0]?.[1] as AbortSignal).aborted).toBe(true)
    old.resolve({ items: [response({ name: 'old-secret-row' })], count: 1 }); await settle()
    expect(target.textContent).not.toContain('old-secret-row')
    expect(button('已核对，开始新草稿').disabled).toBe(true)
    expect(api.createLegalHold).toHaveBeenCalledTimes(1)
  })

  it('refresh synchronously revokes a previously qualifying observation and stale acknowledgement', async () => {
    await uncertain(); button('刷新核对').click(); await settle()
    const acknowledgement = button('已核对，开始新草稿')
    expect(acknowledgement.disabled).toBe(false)
    const read = deferred(); api.listLegalHolds.mockReturnValueOnce(read.promise)
    button('刷新保留').click(); forceClick(acknowledgement); await settle()
    expect(target.textContent).toContain('创建可能已提交')
    expect(fields()[0]?.value).toBe('private-hold-name')
    expect(button('已核对，开始新草稿').disabled).toBe(true)
    read.reject(new TypeError('read failed')); await settle()
    forceClick(acknowledgement); forceClick(button('创建范围保留')); await settle()
    expect(api.createLegalHold).toHaveBeenCalledTimes(1)
  })

  it('keeps names, reasons, filters and uncertain state out of URLs, logs and browser storage', async () => {
    const url = window.location.href
    const storage = [vi.spyOn(window, 'localStorage', 'get'), vi.spyOn(window, 'sessionStorage', 'get')]
    const logs = [vi.spyOn(console, 'log'), vi.spyOn(console, 'info'), vi.spyOn(console, 'warn'), vi.spyOn(console, 'error'), vi.spyOn(console, 'debug')]
    try {
      api.createLegalHold.mockRejectedValueOnce(new ApiError(503, 'store_timeout', 'timeout'))
      await render(); await draft()
      const filter = target.querySelector<HTMLInputElement>('input[placeholder="业务操作标识"]')!
      filter.value = 'private-operation?owner=secret'; filter.dispatchEvent(new Event('input', { bubbles: true })); await settle()
      button('创建范围保留').click(); await settle()
      expect(target.textContent).toContain('创建可能已提交')
      button('刷新核对').click(); await settle()
      expect(window.location.href).toBe(url)
      for (const spy of storage) expect(spy).not.toHaveBeenCalled()
      for (const spy of logs) expect(spy).not.toHaveBeenCalled()
      expect(api.listLegalHolds.mock.calls.every((call) => call.length === 2 && call[0] === 'tenant-a' && call[1] instanceof AbortSignal)).toBe(true)
      expect(api.createLegalHold.mock.calls[0]?.[0]).toMatchObject({ name: 'private-hold-name', reason: 'private-hold-reason', filter: { operation_id: 'private-operation?owner=secret' } })
    } finally {
      for (const spy of [...storage, ...logs]) spy.mockRestore()
    }
  })

  it('explicit 400 validation rejection permits editing and an intentional second create', async () => {
    api.createLegalHold.mockRejectedValueOnce(new ApiError(400, 'invalid_request', 'invalid'))
    await render(); await draft(); button('创建范围保留').click(); await settle()
    expect(target.textContent).not.toContain('创建可能已提交')
    expect(button('创建范围保留').disabled).toBe(false)
    input(0, 'corrected-name'); await settle()
    api.createLegalHold.mockImplementationOnce((request) => Promise.resolve(response({ ...request })))
    button('创建范围保留').click(); await settle()
    expect(api.createLegalHold).toHaveBeenCalledTimes(2)
    expect(fields()[0]?.value).toBe('')
  })
})

describe('Hold scope, identity and lifecycle fencing', () => {
  for (const request of ['read', 'create'] as const) {
    for (const outcome of ['success', 'failure'] as const) {
      it(`A -> B -> A ignores late ${request} ${outcome}, including a newer uncertain create`, async () => {
        login('actor', 'session-a', ['audit:legal_hold:manage'], true)
        const old = deferred()
        api.listLegalHolds.mockImplementation((tenant: string) => Promise.resolve({ items: [response({ tenant_id: tenant, name: `current-${tenant}` })], count: 1 }))
        if (request === 'read') api.listLegalHolds.mockReturnValueOnce(old.promise)
        else api.createLegalHold.mockReturnValueOnce(old.promise)
        await render()
        if (request === 'create') { await draft(); button('创建范围保留').click(); await settle() }
        const signal = (request === 'read' ? api.listLegalHolds.mock.calls[0]?.[1] : api.createLegalHold.mock.calls[0]?.[2]) as AbortSignal
        const original = api.createLegalHold.mock.calls[0]?.[0]
        button('租户 B').click(); await settle()
        button('租户 A').click(); await settle()
        expect(signal.aborted).toBe(true)
        if (request === 'create') {
          expect(target.textContent).toContain('创建可能已提交')
          expect(button('创建范围保留').disabled).toBe(true)
          expect(api.createLegalHold).toHaveBeenCalledTimes(1)
          // A current GET may enable this explicit discard, never replay.
          button('已核对，开始新草稿').click(); await settle()
          expect(api.createLegalHold).toHaveBeenCalledTimes(1)
        }
        api.createLegalHold.mockRejectedValueOnce(new ApiError(503, 'current_unknown', 'current unknown'))
        await draft(); button('创建范围保留').click(); await settle()
        const reads = api.listLegalHolds.mock.calls.length
        const writes = api.createLegalHold.mock.calls.length
        if (outcome === 'success') old.resolve(request === 'read'
          ? { items: [response({ name: 'late-private-row' })], count: 1 }
          : response({ ...original }))
        else old.reject(new ApiError(400, 'old_failure', 'old failure'))
        await settle()
        expect(target.textContent).toContain('创建可能已提交')
        expect(target.textContent).toContain('current_unknown')
        expect(target.textContent).not.toContain('old_failure')
        expect(target.textContent).not.toContain('late-private-row')
        expect(button('已核对，开始新草稿').disabled).toBe(true)
        forceClick(button('创建范围保留')); await settle()
        expect(api.createLegalHold).toHaveBeenCalledTimes(writes)
        expect(api.listLegalHolds).toHaveBeenCalledTimes(reads)
      })
    }
  }

  for (const change of ['token', 'subject', 'id token', 'permission'] as const) {
    for (const outcome of ['success', 'failure'] as const) {
      it(`${change} change discards late create ${outcome} and prevents same-frame stale dispatch`, async () => {
        const old = deferred(); api.createLegalHold.mockReturnValueOnce(old.promise)
        await render(); await draft()
        const create = button('创建范围保留'); create.click(); await settle()
        const signal = api.createLegalHold.mock.calls[0]?.[2] as AbortSignal
        const original = api.createLegalHold.mock.calls[0]?.[0]
        if (change === 'token') login('actor', 'session-b')
        else if (change === 'subject') login('other-actor')
        else if (change === 'id token') authState.update((state) => ({ ...state, session: { ...state.session!, idToken: 'id-token-b' } }))
        else login('actor', 'session-a', [])
        forceClick(create); await settle()
        expect(signal.aborted).toBe(true)
        expect(fields()[0]?.value).toBe('')
        if (change !== 'permission') {
          api.createLegalHold.mockRejectedValueOnce(new ApiError(503, 'current_unknown', 'current unknown'))
          await draft(); button('创建范围保留').click(); await settle()
        }
        const reads = api.listLegalHolds.mock.calls.length
        if (outcome === 'success') old.resolve(response({ ...original }))
        else old.reject(new ApiError(400, 'old_failure', 'old failure'))
        await settle()
        expect(target.textContent).not.toContain('old_failure')
        expect(button('创建范围保留').disabled).toBe(true)
        if (change !== 'permission') {
          expect(target.textContent).toContain('创建可能已提交')
          expect(button('已核对，开始新草稿').disabled).toBe(true)
        }
        forceClick(button('创建范围保留')); await settle()
        expect(api.createLegalHold).toHaveBeenCalledTimes(change === 'permission' ? 1 : 2)
        expect(api.listLegalHolds).toHaveBeenCalledTimes(reads)
      })
    }
  }

  it.each(['token', 'subject', 'id token', 'permission'] as const)('same-frame %s change cannot submit an unsubmitted old draft or release confirmation', async (change) => {
    api.listLegalHolds.mockResolvedValueOnce({ items: [response()], count: 1 })
    await render(); await draft(); button('释放').click(); await settle()
    const create = button('创建范围保留'); const confirm = button('确认释放')
    if (change === 'token') login('actor', 'session-b')
    else if (change === 'subject') login('other-actor')
    else if (change === 'id token') authState.update((state) => ({ ...state, session: { ...state.session!, idToken: 'id-token-b' } }))
    else login('actor', 'session-a', [])
    forceClick(create); forceClick(confirm); await settle()
    expect(api.createLegalHold).not.toHaveBeenCalled()
    expect(api.releaseLegalHold).not.toHaveBeenCalled()
    expect(document.body.textContent).not.toContain('确认释放法律保留')
    expect(fields()[0]?.value).toBe('')
  })

  it.each(['success', 'failure'] as const)('auth A -> B -> A before effects still fences late read %s', async (outcome) => {
    const old = deferred(); api.listLegalHolds.mockReturnValueOnce(old.promise)
    await render()
    let original!: Parameters<typeof authState.set>[0]
    const stop = authState.subscribe((state) => { original = state }); stop()
    login('other-actor', 'session-b'); authState.set(original)
    await settle()
    if (outcome === 'success') old.resolve({ items: [response({ name: 'late-private-row' })], count: 1 })
    else old.reject(new Error('late-private-error'))
    await settle()
    expect((api.listLegalHolds.mock.calls[0]?.[1] as AbortSignal).aborted).toBe(true)
    expect(api.listLegalHolds).toHaveBeenCalledTimes(2)
    expect(target.textContent).not.toContain('late-private-row')
    expect(target.textContent).not.toContain('late-private-error')
  })

  for (const request of ['read', 'create', 'release'] as const) {
    for (const outcome of ['success', 'failure'] as const) {
      it(`unmount ignores late ${request} ${outcome} when AbortSignal is ignored`, async () => {
        const old = deferred()
        api.listLegalHolds.mockResolvedValue({ items: [response()], count: 1 })
        if (request === 'read') api.listLegalHolds.mockReturnValueOnce(old.promise)
        else if (request === 'create') api.createLegalHold.mockReturnValueOnce(old.promise)
        else api.releaseLegalHold.mockReturnValueOnce(old.promise)
        await render()
        if (request === 'create') { await draft(); button('创建范围保留').click(); await settle() }
        else if (request === 'release') { button('释放').click(); await settle(); button('确认释放').click(); await settle() }
        const signal = (request === 'read' ? api.listLegalHolds.mock.calls[0]?.[1]
          : request === 'create' ? api.createLegalHold.mock.calls[0]?.[2] : api.releaseLegalHold.mock.calls[0]?.[2]) as AbortSignal
        await unmount(component!); component = undefined
        const reads = api.listLegalHolds.mock.calls.length
        expect(signal.aborted).toBe(true)
        if (outcome === 'failure') old.reject(new Error('late-private-error'))
        else old.resolve(request === 'read' ? { items: [response()], count: 1 }
          : response({ ...api.createLegalHold.mock.calls[0]?.[0], released_at: request === 'release' ? '2026-10-22T01:00:00Z' : undefined }))
        await settle()
        expect(target.textContent).toBe('')
        expect(api.listLegalHolds).toHaveBeenCalledTimes(reads)
      })
    }
  }

  for (const change of ['tenant', 'token', 'permission'] as const) {
    for (const outcome of ['success', 'failure'] as const) {
      it(`${change} scope change ignores late release ${outcome} without restoring a stale observation`, async () => {
        login('actor', 'session-a', ['audit:legal_hold:manage'], change === 'tenant')
        api.listLegalHolds.mockImplementation((tenant: string) => Promise.resolve({ items: [response({ tenant_id: tenant })], count: 1 }))
        const old = deferred(); api.releaseLegalHold.mockReturnValueOnce(old.promise)
        await render(); button('释放').click(); await settle()
        const confirm = button('确认释放'); confirm.click(); await settle()
        const signal = api.releaseLegalHold.mock.calls[0]?.[2] as AbortSignal
        if (change === 'tenant') {
          button('租户 B').click(); await settle(); button('租户 A').click(); await settle()
        } else if (change === 'token') { login('actor', 'session-b'); await settle() }
        else { login('actor', 'session-a', []); await settle() }
        expect(signal.aborted).toBe(true)
        forceClick(confirm); await settle()
        expect(api.releaseLegalHold).toHaveBeenCalledTimes(1)
        if (change !== 'permission') {
          api.createLegalHold.mockRejectedValueOnce(new ApiError(503, 'current_unknown', 'current unknown'))
          await draft(); button('创建范围保留').click(); await settle()
        }
        const reads = api.listLegalHolds.mock.calls.length
        if (outcome === 'success') old.resolve(response({ released_at: '2026-10-22T01:00:00Z', released_by: 'actor' }))
        else old.reject(new Error('late-private-release-error'))
        await settle()
        expect(api.listLegalHolds).toHaveBeenCalledTimes(reads)
        expect(target.textContent).not.toContain('late-private-release-error')
        expect(document.body.textContent).not.toContain('确认释放法律保留')
        if (change !== 'permission') {
          expect(target.textContent).toContain('创建可能已提交')
          expect(button('已核对，开始新草稿').disabled).toBe(true)
        }
        expect(button('创建范围保留').disabled).toBe(true)
      })
    }
  }

  it.each(['no permission', 'unconfirmed platform', 'foreign tenant', 'anonymous'] as const)('%s emits no request even via stale/forced handlers', async (scope) => {
    if (scope === 'no permission') login('actor', 'session-a', [])
    else if (scope === 'unconfirmed platform') login('actor', 'session-a', ['audit:legal_hold:manage'], true)
    else if (scope === 'anonymous') authState.set({ status: 'anonymous', session: null, error: null })
    await render(scope === 'unconfirmed platform' ? '' : scope === 'foreign tenant' ? 'tenant-b' : 'tenant-a')
    await draft(); forceClick(button('创建范围保留')); forceClick(button('刷新保留')); await settle()
    expect(api.listLegalHolds).not.toHaveBeenCalled()
    expect(api.createLegalHold).not.toHaveBeenCalled()
    expect(api.releaseLegalHold).not.toHaveBeenCalled()
  })
})

describe('Hold list observations and release confirmation', () => {
  it('distinguishes loading, successful empty, successful rows and read failure; ignores response count', async () => {
    const read = deferred(); api.listLegalHolds.mockReturnValueOnce(read.promise)
    await render()
    expect(target.textContent).toContain('读取中…')
    expect(target.textContent).not.toContain('尚无法律保留')
    expect(target.textContent).not.toContain('0 条保留')
    read.resolve({ items: [], count: 456 }); await settle()
    expect(target.textContent).toContain('0 条保留')
    expect(target.textContent).toContain('当前租户尚无法律保留')
    api.listLegalHolds.mockResolvedValueOnce({ items: [response()], count: 999 })
    button('刷新保留').click(); await settle()
    expect(target.textContent).toContain('1 条保留')
    expect(target.textContent).toContain('observed-hold')
    expect(target.textContent).not.toContain('尚无法律保留')
    api.listLegalHolds.mockRejectedValueOnce(new TypeError('read failed'))
    button('刷新保留').click(); await settle()
    expect(target.textContent).toContain('保留读取失败，状态未知')
    expect(target.textContent).not.toContain('observed-hold')
    expect(target.textContent).not.toContain('尚无法律保留')
    button('重试').click(); await settle()
    expect(api.listLegalHolds).toHaveBeenCalledTimes(4)
    expect(api.createLegalHold).not.toHaveBeenCalled()
    expect(api.releaseLegalHold).not.toHaveBeenCalled()
  })

  it('refresh closes old release confirmation synchronously; only new successful GET plus confirmation releases', async () => {
    api.listLegalHolds.mockResolvedValueOnce({ items: [response()], count: 1 })
    await render()
    const release = button('释放'); release.click(); await settle()
    const confirm = button('确认释放')
    expect(api.releaseLegalHold).not.toHaveBeenCalled()
    const read = deferred(); api.listLegalHolds.mockReturnValueOnce(read.promise)
    button('刷新保留').click(); forceClick(confirm); forceClick(release); await settle()
    expect(api.releaseLegalHold).not.toHaveBeenCalled()
    expect(document.body.textContent).not.toContain('确认释放法律保留')
    read.resolve({ items: null }); await settle()
    forceClick(confirm); forceClick(release); await settle()
    expect(api.releaseLegalHold).not.toHaveBeenCalled()
    api.listLegalHolds.mockResolvedValueOnce({ items: [response()], count: 1 })
    button('刷新保留').click(); await settle()
    expect(api.releaseLegalHold).not.toHaveBeenCalled()
    api.releaseLegalHold.mockResolvedValueOnce(response({ released_at: '2026-10-22T01:00:00Z', released_by: 'actor' }))
    button('释放').click(); await settle()
    const nextConfirm = button('确认释放')
    nextConfirm.click(); forceClick(nextConfirm); await settle()
    expect(api.releaseLegalHold).toHaveBeenCalledTimes(1)
    expect(api.releaseLegalHold.mock.lastCall?.slice(0, 2)).toEqual(['hold-aaaaaaaaaaaaaaaa', 'tenant-a'])
    expect(api.createLegalHold).not.toHaveBeenCalled()
  })

  it('a retired release dialog cannot confirm a new observation even after another dialog opens', async () => {
    api.listLegalHolds.mockResolvedValue({ items: [response()], count: 1 })
    await render(); button('释放').click(); await settle()
    const oldConfirm = button('确认释放')
    button('刷新保留').click(); await settle()
    expect(document.body.textContent).not.toContain('确认释放法律保留')
    button('释放').click(); await settle()
    const currentConfirm = button('确认释放')
    expect(currentConfirm).not.toBe(oldConfirm)
    forceClick(oldConfirm); await settle()
    expect(api.releaseLegalHold).not.toHaveBeenCalled()
    api.releaseLegalHold.mockResolvedValueOnce(response({ released_at: '2026-10-22T01:00:00Z', released_by: 'actor' }))
    currentConfirm.click(); await settle()
    expect(api.releaseLegalHold).toHaveBeenCalledTimes(1)
    expect(api.createLegalHold).not.toHaveBeenCalled()
  })

  it('an uncertain create does not block releasing another Hold or imply release of remaining COMPLIANCE protection', async () => {
    await uncertain()
    const other = response({ id: 'hold-other', name: 'another hold' })
    api.listLegalHolds.mockResolvedValue({ items: [response(), other], count: 2 })
    button('刷新核对').click(); await settle()
    button('释放').click(); await settle()
    expect(document.body.textContent).toContain('其他保留和底层 COMPLIANCE 保护仍然有效')
    const released = response({ released_at: '2026-10-22T01:00:00Z', released_by: 'actor' })
    api.releaseLegalHold.mockResolvedValueOnce(released)
    api.listLegalHolds.mockResolvedValueOnce({ items: [released, other], count: 2 })
    button('确认释放').click(); await settle()
    expect(api.releaseLegalHold).toHaveBeenCalledTimes(1)
    expect(api.releaseLegalHold.mock.lastCall?.slice(0, 2)).toEqual(['hold-aaaaaaaaaaaaaaaa', 'tenant-a'])
    expect(api.createLegalHold).toHaveBeenCalledTimes(1)
    expect(target.textContent).toContain('创建可能已提交')
    expect(target.textContent).toContain('another hold')
    expect([...target.querySelectorAll('button')].filter((node) => node.textContent?.trim() === '释放')).toHaveLength(1)
    expect(button('创建范围保留').disabled).toBe(true)
  })

  it.each(['create', 'release'] as const)('%s error retry only reads, never replays a write', async (action) => {
    if (action === 'create') await uncertain()
    else {
      api.listLegalHolds.mockResolvedValueOnce({ items: [response()], count: 1 })
      api.releaseLegalHold.mockRejectedValueOnce(new ApiError(503, 'store_timeout', 'timeout'))
      await render(); button('释放').click(); await settle(); button('确认释放').click(); await settle()
    }
    button('重试').click(); await settle()
    expect(api.listLegalHolds).toHaveBeenCalledTimes(2)
    expect(api.createLegalHold).toHaveBeenCalledTimes(action === 'create' ? 1 : 0)
    expect(api.releaseLegalHold).toHaveBeenCalledTimes(action === 'release' ? 1 : 0)
  })
})
