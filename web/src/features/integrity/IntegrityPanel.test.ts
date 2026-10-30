// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { authState } from '../../auth/store'
import { ApiError } from '../../api/client'
import type { IntegrityResult } from '../../api/types'
import Harness from './IntegrityPanel.testHarness.svelte'

const { api } = vi.hoisted(() => ({ api: { verifyIntegrity: vi.fn() } }))
vi.mock('../../api/useAuditApi', () => ({ useAuditApi: () => api }))

let component: ReturnType<typeof mount> | undefined
let target: HTMLDivElement

function login(): void {
  authState.set({
    status: 'authenticated',
    session: {
      accessToken: 'current-token',
      idToken: 'id',
      expiresAt: Date.now() + 100000,
      subject: 'alice',
      displayName: 'alice',
      tenantId: 'tenant-a',
      roles: [],
      permissions: new Set(['audit:integrity:verify']),
      platform: true,
    },
    error: null,
  })
}

function result(overrides: Partial<IntegrityResult> = {}): IntegrityResult {
  return {
    tenant_id: 'tenant-a',
    valid: true,
    event_count: 12,
    segment_count: 2,
    checked_at: '2026-10-25T00:00:00Z',
    ...overrides,
  }
}

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (cause: unknown) => void
  const promise = new Promise<T>((done, fail) => {
    resolve = done
    reject = fail
  })
  return { promise, resolve, reject }
}

async function settle(): Promise<void> {
  await tick()
  await tick()
}

function button(label: string): HTMLButtonElement {
  const found = [...target.querySelectorAll('button')].find((node) => node.textContent?.trim() === label)
  if (!found) throw new Error(`missing button ${label}`)
  return found
}

function form(): HTMLFormElement {
  const found = target.querySelector('form')
  if (!found) throw new Error('missing integrity form')
  return found
}

function status(): HTMLElement {
  const found = target.querySelector('[role="status"]')
  if (!found) throw new Error('missing integrity status')
  return found as HTMLElement
}

async function render(): Promise<void> {
  component = mount(Harness, { target })
  await settle()
}

async function submit(label = '开始验证'): Promise<void> {
  button(label).click()
  await settle()
}

beforeEach(() => {
  vi.resetAllMocks()
  login()
  target = document.createElement('div')
  document.body.append(target)
})

afterEach(async () => {
  if (component) await unmount(component)
  component = undefined
  target.remove()
  authState.set({ status: 'anonymous', session: null, error: null })
})

describe('ledger integrity verification form', () => {
  it('submits through the native form, keeps the Stream ID label wired and verifies the tenant scope', async () => {
    await render()
    const input = target.querySelector('input')!
    expect(form().getAttribute('aria-busy')).toBe('false')
    expect(button('开始验证').type).toBe('submit')
    expect(input.id).not.toBe('')
    expect([...target.querySelectorAll('label')].some((label) => label.getAttribute('for') === input.id)).toBe(true)
    expect(api.verifyIntegrity).not.toHaveBeenCalled()
    expect(status().textContent).toBe('尚未验证')

    api.verifyIntegrity.mockResolvedValueOnce(result())
    await submit()

    expect(api.verifyIntegrity).toHaveBeenCalledTimes(1)
    expect(api.verifyIntegrity.mock.calls[0]?.slice(0, 2)).toEqual(['', 'tenant-a'])
    expect(api.verifyIntegrity.mock.calls[0]?.[2]).toBeInstanceOf(AbortSignal)
    expect(form().getAttribute('aria-busy')).toBe('false')
    expect(status().textContent).toBe('验证通过')
    expect(target.textContent).toContain('事件 12')
  })

  it('trims a non-empty stream and treats valid=false as a completed integrity finding, not a request failure', async () => {
    await render()
    const input = target.querySelector('input')!
    input.value = '  stream-a  '
    input.dispatchEvent(new Event('input', { bubbles: true }))
    await settle()

    api.verifyIntegrity.mockResolvedValueOnce(result({
      valid: false,
      stream_id: 'stream-a',
      event_count: 3,
      segment_count: 1,
      errors: ['chain hash mismatch at sequence 2'],
    }))
    await submit()

    expect(api.verifyIntegrity.mock.calls[0]?.[0]).toBe('stream-a')
    expect(status().textContent).toBe('发现完整性问题')
    expect(status().textContent).not.toContain('chain hash')
    expect(status().textContent).not.toContain('请求失败')
    expect(target.textContent).toContain('chain hash mismatch at sequence 2')
  })

  it('deduplicates submits while busy and never retries automatically', async () => {
    await render()
    const pending = deferred<IntegrityResult>()
    api.verifyIntegrity.mockReturnValueOnce(pending.promise)

    button('开始验证').click()
    await settle()
    expect(api.verifyIntegrity).toHaveBeenCalledTimes(1)
    expect(form().getAttribute('aria-busy')).toBe('true')
    expect(button('验证中…').disabled).toBe(true)
    expect(status().textContent).toBe('完整性验证中…')

    form().requestSubmit()
    form().dispatchEvent(new SubmitEvent('submit', { bubbles: true, cancelable: true }))
    await settle()
    expect(api.verifyIntegrity).toHaveBeenCalledTimes(1)

    pending.resolve(result())
    await settle()
    expect(api.verifyIntegrity).toHaveBeenCalledTimes(1)
    expect(form().getAttribute('aria-busy')).toBe('false')
    expect(status().textContent).toBe('验证通过')

    await settle()
    expect(api.verifyIntegrity).toHaveBeenCalledTimes(1)
  })

  it('announces a request failure without copying the raw diagnostic into the live region', async () => {
    await render()
    api.verifyIntegrity.mockRejectedValueOnce(new ApiError(503, 'store_timeout', 'private diagnostic', 'req-1'))

    await submit()

    expect(status().textContent).toBe('请求失败')
    expect(status().textContent).not.toContain('private diagnostic')
    expect(target.textContent).toContain('服务暂时不可用')
    expect(target.textContent).not.toContain('验证通过')
    expect(api.verifyIntegrity).toHaveBeenCalledTimes(1)
  })

  it('treats a mismatched tenant result as a failed request instead of an integrity verdict', async () => {
    await render()
    api.verifyIntegrity.mockResolvedValueOnce(result({ tenant_id: 'tenant-b', event_count: 987654 }))

    await submit()

    expect(status().textContent).toBe('请求失败')
    expect(target.textContent).not.toContain('987654')
    expect(target.textContent).not.toContain('验证通过')
  })

  it.each(['success', 'failure'] as const)('ignores a late %s from an old tenant context', async (outcome) => {
    await render()
    const pending = deferred<IntegrityResult>()
    api.verifyIntegrity.mockReturnValueOnce(pending.promise)

    button('开始验证').click()
    await settle()
    const staleSignal = api.verifyIntegrity.mock.calls[0]?.[2] as AbortSignal
    button('选择 B').click()
    await settle()
    button('选择 A').click()
    await settle()
    expect(staleSignal.aborted).toBe(true)

    if (outcome === 'success') pending.resolve(result({ event_count: 987654 }))
    else pending.reject(new ApiError(503, 'store_timeout', 'private diagnostic'))
    await settle()

    expect(api.verifyIntegrity).toHaveBeenCalledTimes(1)
    expect(form().getAttribute('aria-busy')).toBe('false')
    expect(status().textContent).toBe('尚未验证')
    expect(target.textContent).not.toContain('987654')
    expect(target.textContent).not.toContain('请求失败')
  })

  it.each(['requestSubmit', 'native submit event'] as const)(
    'starts exactly one verification from an idle %s path',
    async (submission) => {
      await render()
      api.verifyIntegrity.mockResolvedValueOnce(result())

      if (submission === 'requestSubmit') form().requestSubmit()
      else form().dispatchEvent(new SubmitEvent('submit', { bubbles: true, cancelable: true }))
      await settle()

      expect(api.verifyIntegrity).toHaveBeenCalledTimes(1)
      expect(api.verifyIntegrity.mock.calls[0]?.slice(0, 2)).toEqual(['', 'tenant-a'])
      expect(status().textContent).toBe('验证通过')
    },
  )

  it('cancels a pending wait through a native button without retrying or submitting on its own', async () => {
    await render()
    const pending = deferred<IntegrityResult>()
    api.verifyIntegrity.mockReturnValueOnce(pending.promise)
    await submit()

    const signal = api.verifyIntegrity.mock.calls[0]?.[2] as AbortSignal
    const cancel = button('取消等待')
    expect(cancel.type).toBe('button')
    expect(cancel.disabled).toBe(false)
    expect(form().contains(cancel)).toBe(true)
    expect(signal.aborted).toBe(false)
    expect(target.textContent).toContain('不撤销服务端验证')
    expect(target.textContent).toContain('自审计事实')
    expect(target.textContent).not.toContain('后台任务')

    cancel.click()
    await settle()

    expect(signal.aborted).toBe(true)
    expect(api.verifyIntegrity).toHaveBeenCalledTimes(1)
    expect(form().getAttribute('aria-busy')).toBe('false')
    expect(status().textContent).toBe('尚未验证')
    expect(target.querySelector('.result-panel')).toBeNull()
    expect(target.textContent).not.toContain('请求失败')
    expect(button('开始验证').disabled).toBe(false)

    pending.resolve(result({ event_count: 987654 }))
    await settle()
    expect(api.verifyIntegrity).toHaveBeenCalledTimes(1)
    expect(status().textContent).toBe('尚未验证')
    expect(target.textContent).not.toContain('987654')
  })

  it.each([
    { stale: 'success', fresh: 'failure', arrival: 'while the new request waits' },
    { stale: 'failure', fresh: 'success', arrival: 'while the new request waits' },
    { stale: 'success', fresh: 'failure', arrival: 'after the new request completed' },
    { stale: 'failure', fresh: 'success', arrival: 'after the new request completed' },
  ] as const)(
    'ignores a stale $stale arriving $arrival after cancel and a fresh submit',
    async ({ stale, fresh, arrival }) => {
      await render()
      const old = deferred<IntegrityResult>()
      const next = deferred<IntegrityResult>()
      api.verifyIntegrity.mockReturnValueOnce(old.promise).mockReturnValueOnce(next.promise)
      await submit()
      const staleSignal = api.verifyIntegrity.mock.calls[0]?.[2] as AbortSignal
      button('取消等待').click()
      await settle()
      expect(staleSignal.aborted).toBe(true)

      await submit()
      expect(api.verifyIntegrity).toHaveBeenCalledTimes(2)
      const freshSignal = api.verifyIntegrity.mock.calls[1]?.[2] as AbortSignal
      expect(freshSignal.aborted).toBe(false)

      const settleStale = (): void => {
        if (stale === 'success') old.resolve(result({ event_count: 987654 }))
        else old.reject(new ApiError(503, 'store_timeout', 'private diagnostic'))
      }
      if (arrival === 'while the new request waits') {
        expect(form().getAttribute('aria-busy')).toBe('true')
        settleStale()
        await settle()
        expect(form().getAttribute('aria-busy')).toBe('true')
        expect(status().textContent).toBe('完整性验证中…')
        expect(target.querySelector('.result-panel')).toBeNull()
        expect(target.textContent).not.toContain('请求失败')
      }

      if (fresh === 'success') next.resolve(result())
      else next.reject(new ApiError(502, 'query_failed', 'fresh diagnostic'))
      await settle()
      expect(form().getAttribute('aria-busy')).toBe('false')
      expect(status().textContent).toBe(fresh === 'success' ? '验证通过' : '请求失败')
      expect(target.textContent).not.toContain('987654')

      if (arrival === 'after the new request completed') {
        const before = status().textContent
        settleStale()
        await settle()
        expect(form().getAttribute('aria-busy')).toBe('false')
        expect(status().textContent).toBe(before)
        expect(target.textContent).not.toContain('987654')
        if (fresh === 'success') expect(target.textContent).not.toContain('请求失败')
      }
      expect(api.verifyIntegrity).toHaveBeenCalledTimes(2)
      expect(freshSignal.aborted).toBe(false)
    },
  )

  it('aborts the previous generation when the Stream ID is edited and submits the trimmed new value', async () => {
    await render()
    const old = deferred<IntegrityResult>()
    api.verifyIntegrity.mockReturnValueOnce(old.promise)
    await submit()
    const staleSignal = api.verifyIntegrity.mock.calls[0]?.[2] as AbortSignal

    const input = target.querySelector('input')!
    input.value = '  stream-b  '
    input.dispatchEvent(new Event('input', { bubbles: true }))
    await settle()

    expect(staleSignal.aborted).toBe(true)
    expect(form().getAttribute('aria-busy')).toBe('false')
    expect(status().textContent).toBe('尚未验证')
    old.resolve(result({ event_count: 987654 }))
    await settle()
    expect(target.textContent).not.toContain('987654')

    api.verifyIntegrity.mockResolvedValueOnce(result({ stream_id: 'stream-b', event_count: 4 }))
    await submit()
    expect(api.verifyIntegrity).toHaveBeenCalledTimes(2)
    expect(api.verifyIntegrity.mock.calls[1]?.slice(0, 2)).toEqual(['stream-b', 'tenant-a'])
    expect(status().textContent).toBe('验证通过')
  })

  it('aborts an in-flight request on unmount and ignores its late completion', async () => {
    await render()
    const pending = deferred<IntegrityResult>()
    api.verifyIntegrity.mockReturnValueOnce(pending.promise)
    await submit()
    const signal = api.verifyIntegrity.mock.calls[0]?.[2] as AbortSignal
    expect(signal.aborted).toBe(false)

    const mounted = component
    if (!mounted) throw new Error('missing mounted component')
    component = undefined
    await unmount(mounted)
    expect(signal.aborted).toBe(true)

    pending.resolve(result({ event_count: 987654 }))
    await settle()
    expect(api.verifyIntegrity).toHaveBeenCalledTimes(1)
    expect(target.textContent).not.toContain('987654')
  })

  it('shows the validated scope, check time and every duplicate diagnostic without echoing them into the live region', async () => {
    await render()
    const diagnostic = 'chain <script>not-executed</script> mismatch'
    const payload = '<img src=x onerror="globalThis.integrityXss = true">'
    api.verifyIntegrity.mockResolvedValueOnce(result({
      valid: false,
      stream_id: 'stream-a',
      event_count: 0,
      segment_count: 0,
      checked_at: '2026-10-25T12:34:56.123456789+08:00',
      errors: [diagnostic, diagnostic, payload],
    }))
    const input = target.querySelector('input')!
    input.value = '  stream-a  '
    input.dispatchEvent(new Event('input', { bubbles: true }))
    await settle()

    await submit()

    expect(status().textContent).toBe('发现完整性问题')
    expect(status().textContent).not.toContain('chain')
    expect(target.textContent).toContain('范围：Stream ID stream-a')
    const time = target.querySelector('time')
    expect(time?.getAttribute('datetime')).toBe('2026-10-25T12:34:56.123456789+08:00')
    expect(time?.textContent).not.toBe('')
    expect(target.textContent).toContain('事件 0 · 分段 0')
    expect(target.textContent).toContain('计数是本次核对范围，不单独证明完整性')
    const items = [...target.querySelectorAll('ul li')]
    expect(items.map((item) => item.textContent)).toEqual([diagnostic, diagnostic, payload])
    expect(target.querySelector('ul img')).toBeNull()
    expect(target.querySelector('ul script')).toBeNull()
    expect((globalThis as { integrityXss?: boolean }).integrityXss).not.toBe(true)
  })

  it('accepts an empty returned stream for a whole-tenant request and shows the whole-tenant scope', async () => {
    await render()
    api.verifyIntegrity.mockResolvedValueOnce(result({
      valid: false,
      stream_id: '',
      event_count: 0,
      segment_count: 0,
      errors: [],
    }))

    await submit()

    expect(status().textContent).toBe('发现完整性问题')
    expect(target.textContent).toContain('范围：整个租户')
    expect(target.textContent).toContain('事件 0 · 分段 0')
    expect(target.querySelectorAll('ul li')).toHaveLength(0)
  })

  it.each([
    ['a tenant outside the request scope', result({ tenant_id: 'tenant-b', event_count: 987654, errors: ['private diagnostic'] })],
    ['a stream outside the whole-tenant scope', result({ stream_id: 'stream-b', event_count: 987654, errors: ['private diagnostic'] })],
    ['a missing checked_at', { tenant_id: 'tenant-a', valid: true, event_count: 987654, segment_count: 1, errors: ['private diagnostic'] }],
    ['a string valid flag', result({ valid: 'false' as unknown as boolean, event_count: 987654, errors: ['private diagnostic'] })],
    ['a fractional count', result({ segment_count: 1.5, event_count: 987654, errors: ['private diagnostic'] })],
    ['an impossible checked_at', result({ checked_at: '2026-02-30T00:00:00Z', event_count: 987654, errors: ['private diagnostic'] })],
    ['non-string errors', result({ errors: ['private diagnostic', 7 as unknown as string], event_count: 987654 })],
  ])('rejects %s as a fixed failure without a verdict, counts or raw diagnostics', async (_label, payload) => {
    await render()
    api.verifyIntegrity.mockResolvedValueOnce(payload)

    await submit()

    expect(status().textContent).toBe('请求失败')
    expect(target.textContent).toContain('完整性结果无法核对')
    expect(target.querySelector('.result-panel')).toBeNull()
    expect(target.textContent).not.toContain('987654')
    expect(target.textContent).not.toContain('private diagnostic')
    expect(target.textContent).not.toContain('验证通过')
    expect(target.textContent).not.toContain('范围：')
  })

  it('rejects a result for a different stream before showing it as a completed finding', async () => {
    await render()
    const input = target.querySelector('input')!
    input.value = 'stream-a'
    input.dispatchEvent(new Event('input', { bubbles: true }))
    await settle()
    api.verifyIntegrity.mockResolvedValueOnce(result({
      valid: false,
      stream_id: 'stream-b',
      event_count: 987654,
      errors: ['private diagnostic'],
    }))

    await submit()

    expect(status().textContent).toBe('请求失败')
    expect(target.textContent).toContain('完整性结果无法核对')
    expect(target.querySelector('.result-panel')).toBeNull()
    expect(target.textContent).not.toContain('987654')
    expect(target.textContent).not.toContain('private diagnostic')
    expect(target.textContent).not.toContain('发现完整性问题')
  })
})
