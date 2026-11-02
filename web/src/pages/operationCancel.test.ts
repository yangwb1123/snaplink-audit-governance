// @vitest-environment happy-dom
import { get } from 'svelte/store'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { authState, type AuthState } from '../auth/store'
import type { UserSession } from '../auth/types'
import type { AuditEvent, OperationSummary, QueryResult, ReplayResult } from '../api/types'
import OperationsPage from './OperationsPage.svelte'
import Harness from './operationCancel.testHarness.svelte'

const { api, impl, calls, useAuditApi, resetCapture } = vi.hoisted(() => {
  type Captured = { args: unknown[]; signal?: AbortSignal }
  const calls = {
    summary: [] as Captured[],
    timeline: [] as Captured[],
    replay: [] as Captured[],
  }
  const pendingContexts: Array<{ signal?: AbortSignal } | undefined> = []
  const impl = { summary: vi.fn(), timeline: vi.fn(), replay: vi.fn() }
  const take = (name: keyof typeof calls, args: unknown[]): void => {
    const context = pendingContexts.shift()
    calls[name].push({ args, signal: context?.signal })
  }
  const api = {
    getOperation: vi.fn((...args: unknown[]) => { take('summary', args); return impl.summary(...args) }),
    getOperationTimeline: vi.fn((...args: unknown[]) => { take('timeline', args); return impl.timeline(...args) }),
    replayOperation: vi.fn((...args: unknown[]) => { take('replay', args); return impl.replay(...args) }),
    getAggregateTimeline: vi.fn(),
  }
  const useAuditApi = (context?: { signal?: AbortSignal }) => {
    pendingContexts.push(context)
    return api
  }
  const resetCapture = (): void => {
    for (const key of ['summary', 'timeline', 'replay'] as const) calls[key].length = 0
    pendingContexts.length = 0
  }
  return { api, impl, calls, useAuditApi, resetCapture }
})
vi.mock('../api/useAuditApi', () => ({ useAuditApi }))

type Section = 'summary' | 'timeline' | 'replay'
const sections: Section[] = ['summary', 'timeline', 'replay']
const methods = {
  summary: 'getOperation',
  timeline: 'getOperationTimeline',
  replay: 'replayOperation',
} as const
const labels = {
  summary: '正在读取操作摘要…',
  timeline: '正在读取事件时间线…',
  replay: '正在读取状态回放…',
} as const
const headings = {
  summary: '操作摘要',
  timeline: '事件时间线',
  replay: '状态回放',
} as const

let target: HTMLDivElement
let component: ReturnType<typeof mount> | undefined

function session(token = 'token-a', tenantId = 'tenant-a'): UserSession {
  return {
    accessToken: token,
    idToken: 'id-token',
    expiresAt: Date.now() + 100_000,
    subject: 'actor',
    displayName: 'actor',
    tenantId,
    roles: [],
    permissions: new Set(['audit:event:read', 'audit:operation:read']),
    platform: false,
  }
}

function authenticated(current = session()): AuthState {
  return { status: 'authenticated', session: current, error: null }
}

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (cause: unknown) => void
  const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail })
  return { promise, resolve, reject }
}

function auditEvent(id: string): AuditEvent {
  return {
    event_id: id,
    tenant_id: 'tenant-a',
    source_system: 'aero-vault',
    event_type: 'file.downloaded',
    schema_id: 'audit.event',
    schema_version: 1,
    occurred_at: '2026-10-01T00:00:00Z',
    received_at: '2026-10-01T00:00:01Z',
    actor: { id: 'alice' },
    action: `step-${id}`,
    outcome: 'success',
    data_classification: 'internal',
    retention_class: 'standard',
  }
}

function values(marker = 'current'): {
  summary: OperationSummary
  timeline: QueryResult
  replay: ReplayResult
} {
  return {
    summary: {
      operation_id: 'op-a',
      tenant_id: 'tenant-a',
      event_count: marker === 'current' ? 1 : 987654,
      first_at: '2026-10-01T00:00:00Z',
      last_at: '2026-10-01T00:00:00Z',
      outcomes: [marker],
    },
    timeline: {
      items: [],
      count: marker === 'current' ? 1 : 987654,
      next_cursor: marker === 'current' ? undefined : 'obsolete-cursor',
    },
    replay: {
      tenant_id: 'tenant-a',
      operation_id: 'op-a',
      event_count: marker === 'current' ? 1 : 987654,
      state: { status: marker },
      last_sequence: marker === 'current' ? 1 : 987654,
    },
  }
}

function resetReads(): void {
  for (const section of sections) {
    impl[section].mockReset()
    api[methods[section]].mockClear()
  }
  api.getAggregateTimeline.mockClear()
  resetCapture()
  const current = values()
  for (const section of sections) impl[section].mockResolvedValue(current[section])
}

async function settle(): Promise<void> {
  await tick()
  await Promise.resolve()
  await tick()
  await Promise.resolve()
  await tick()
}

function mountHarness(): void {
  component = mount(Harness, { target, props: { tenantId: 'tenant-a', initialId: 'op-a' } })
}

function mountOperations(): void {
  window.history.replaceState(null, '', '#operations/op/op-a')
  component = mount(OperationsPage, { target, props: { tenantId: 'tenant-a' } })
}

function card(section: Section): HTMLElement {
  const heading = [...target.querySelectorAll('h3')].find(node => node.textContent === headings[section])
  if (!heading?.parentElement) throw new Error(`missing ${section} card`)
  return heading.parentElement
}

function button(label: string, scope: ParentNode = target): HTMLButtonElement {
  const found = [...scope.querySelectorAll('button')].find(node => node.textContent?.trim() === label)
  if (!found) throw new Error(`missing button ${label}`)
  return found as HTMLButtonElement
}

function loadingLabels(): string[] {
  return [...target.querySelectorAll('.loading-block[role="status"]')]
    .map(node => node.lastElementChild?.textContent?.trim() ?? '')
}

function visible(section: Section, marker = 'current'): boolean {
  if (section === 'timeline') {
    return card(section).textContent?.includes(`共 ${marker === 'current' ? 1 : 987654} 条`) ?? false
  }
  return card(section).textContent?.includes(marker) ?? false
}

function input(value: string): void {
  const node = target.querySelector('input')!
  node.value = value
  node.dispatchEvent(new Event('input', { bubbles: true }))
}

function enter(): void {
  target.querySelector('input')!.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }))
}

function signalFor(section: Section, index = 0): AbortSignal {
  const signal = calls[section][index]?.signal
  if (!(signal instanceof AbortSignal)) throw new Error(`missing signal for ${section}[${index}]`)
  return signal
}

function holdReads() {
  const reads = {
    summary: deferred<OperationSummary>(),
    timeline: deferred<QueryResult>(),
    replay: deferred<ReplayResult>(),
  }
  for (const section of sections) impl[section].mockReturnValueOnce(reads[section].promise)
  return reads
}

function displayedOrder(): string[] {
  const text = target.textContent ?? ''
  return ['one', 'two', 'three', 'four']
    .filter(id => text.includes(`step-${id}`))
    .sort((a, b) => text.indexOf(`step-${a}`) - text.indexOf(`step-${b}`))
}

beforeEach(() => {
  resetReads()
  authState.set({ status: 'anonymous', session: null, error: null })
  target = document.createElement('div')
  document.body.append(target)
})

afterEach(async () => {
  if (component) await unmount(component)
  component = undefined
  target.remove()
  window.history.replaceState(null, '', '#')
  authState.set({ status: 'anonymous', session: null, error: null })
  vi.restoreAllMocks()
})

describe('successful operation timeline append completion', () => {
  it.each(['cursor-2', undefined])('releases page and query busy state after appending a page (next=%s)', async (nextCursor) => {
    impl.timeline.mockReset()
      .mockResolvedValueOnce({ items: [auditEvent('one')], count: 3, next_cursor: 'cursor-1' })
      .mockResolvedValueOnce({ items: [auditEvent('two')], count: 3, next_cursor: nextCursor })
    mountHarness()
    await settle()
    button('加载下一页').click()
    await settle()
    expect(displayedOrder()).toEqual(['one', 'two'])
    expect(target.textContent).not.toContain('取消加载下一页')
    expect(button('查询操作').disabled).toBe(false)
    if (nextCursor) {
      expect(button('加载下一页').disabled).toBe(false)
      const third = deferred<QueryResult>()
      impl.timeline.mockReturnValueOnce(third.promise)
      button('加载下一页').click()
      await settle()
      expect(api.getOperationTimeline).toHaveBeenLastCalledWith('op-a', {
        pageSize: 200, cursor: 'cursor-2', tenantId: 'tenant-a',
      })
      button('取消加载下一页').click()
      await settle()
      expect(calls.timeline[2].signal?.aborted).toBe(true)
      expect(button('加载下一页').disabled).toBe(false)
      third.resolve({ items: [auditEvent('obsolete')], count: 987654 })
      await settle()
      expect(displayedOrder()).toEqual(['one', 'two'])
      expect(target.textContent).not.toContain('987654')
    }
  })
})

describe('operation section cancellation', () => {
  it.each(sections.flatMap(section => (['success', 'failure'] as const).map(stale => ({ section, stale }))))(
    'cancels only a pending $section and ignores its late $stale after retry',
    async ({ section, stale }) => {
      const held = holdReads()
      mountHarness()
      await settle()
      expect(loadingLabels()).toHaveLength(3)
      const pendingSignal = signalFor(section)
      expect(pendingSignal.aborted).toBe(false)

      button('取消等待', card(section)).click()
      await settle()

      expect(pendingSignal.aborted).toBe(true)
      expect(card(section).textContent).toContain('已取消等待')
      expect(loadingLabels()).not.toContain(labels[section])
      expect(card(section).textContent).not.toContain('obsolete failure')
      const remaining = sections.filter(part => part !== section)
      expect(loadingLabels().sort()).toEqual(remaining.map(part => labels[part]).sort())

      for (const part of remaining) held[part].resolve(values()[part] as never)
      await settle()
      for (const part of remaining) expect(visible(part)).toBe(true)

      if (stale === 'success') held[section].resolve(values('obsolete')[section] as never)
      else held[section].reject(new Error('obsolete failure'))
      await settle()
      expect(card(section).textContent).not.toContain('obsolete')
      expect(card(section).textContent).not.toContain('obsolete failure')
      expect(loadingLabels()).not.toContain(labels[section])

      const retry = deferred<OperationSummary | QueryResult | ReplayResult>()
      impl[section].mockReturnValueOnce(retry.promise)
      button('重试', card(section)).click()
      await settle()
      expect(api[methods[section]]).toHaveBeenCalledTimes(2)
      const freshSignal = signalFor(section, 1)
      expect(freshSignal).not.toBe(pendingSignal)
      expect(freshSignal.aborted).toBe(false)
      expect(loadingLabels()).toEqual([labels[section]])

      retry.resolve(values()[section])
      await settle()
      expect(visible(section)).toBe(true)
      for (const part of remaining) expect(visible(part)).toBe(true)
      expect(loadingLabels()).toEqual([])
      expect(api[methods[section]]).toHaveBeenCalledTimes(2)
    },
  )

  it.each(
    sections.flatMap(section => (['success', 'failure'] as const).flatMap(stale => (
      (['while retry waits', 'after retry completes'] as const).map(arrival => ({ section, stale, arrival }))
    ))),
  )(
    'does not let a stale $stale arriving $arrival replace a fresh $section result, error or busy state',
    async ({ section, stale, arrival }) => {
      const old = deferred<OperationSummary | QueryResult | ReplayResult>()
      impl[section].mockReturnValueOnce(old.promise)
      mountHarness()
      await settle()
      const oldSignal = signalFor(section)
      button('取消等待', card(section)).click()
      await settle()
      expect(oldSignal.aborted).toBe(true)

      const fresh = deferred<OperationSummary | QueryResult | ReplayResult>()
      impl[section].mockReturnValueOnce(fresh.promise)
      button('重试', card(section)).click()
      await settle()
      expect(loadingLabels()).toEqual([labels[section]])
      const freshSignal = signalFor(section, 1)
      expect(freshSignal.aborted).toBe(false)

      const settleOld = (): void => {
        if (stale === 'success') old.resolve(values('obsolete')[section])
        else old.reject(new Error('obsolete failure'))
      }
      const freshOutcome = stale === 'success' ? 'failure' : 'success'
      const settleFresh = (): void => {
        if (freshOutcome === 'success') fresh.resolve(values()[section])
        else fresh.reject(new Error('fresh failure'))
      }

      if (arrival === 'while retry waits') {
        settleOld()
        await settle()
        expect(loadingLabels()).toEqual([labels[section]])
        expect(card(section).textContent).not.toContain('obsolete')
        expect(card(section).textContent).not.toContain('obsolete failure')
      }

      settleFresh()
      await settle()
      expect(loadingLabels()).toEqual([])
      if (freshOutcome === 'success') {
        expect(visible(section)).toBe(true)
        expect(card(section).textContent).not.toContain('fresh failure')
      } else {
        expect(visible(section)).toBe(false)
        expect(card(section).textContent).toContain('fresh failure')
      }

      if (arrival === 'after retry completes') {
        const before = card(section).textContent
        settleOld()
        await settle()
        expect(card(section).textContent).toBe(before)
        expect(loadingLabels()).toEqual([])
      }
      expect(card(section).textContent).not.toContain('obsolete')
      expect(api[methods[section]]).toHaveBeenCalledTimes(2)
    },
  )
})

describe('operation timeline next-page cancellation', () => {
  it.each(['success', 'failure'] as const)(
    'preserves loaded order and the same cursor when a next-page %s arrives after cancel',
    async stale => {
      const firstPage: QueryResult = {
        items: [auditEvent('two'), auditEvent('one')],
        count: 3,
        next_cursor: 'cursor-1',
      }
      const oldNext = deferred<QueryResult>()
      const freshNext = deferred<QueryResult>()
      impl.timeline.mockReset()
        .mockResolvedValueOnce(firstPage)
        .mockReturnValueOnce(oldNext.promise)
        .mockReturnValueOnce(freshNext.promise)
      mountHarness()
      await settle()
      expect(displayedOrder()).toEqual(['two', 'one'])
      expect(button('加载下一页').disabled).toBe(false)

      button('加载下一页').click()
      await settle()
      const oldSignal = signalFor('timeline', 1)
      expect(oldSignal.aborted).toBe(false)
      button('取消加载下一页').click()
      await settle()

      expect(oldSignal.aborted).toBe(true)
      expect(displayedOrder()).toEqual(['two', 'one'])
      expect(target.textContent).toContain('已显示 2 / 共 3')
      expect(target.textContent).toContain('已取消下一次分页读取')
      expect(button('加载下一页').disabled).toBe(false)
      expect(api.getOperation).toHaveBeenCalledTimes(1)
      expect(api.replayOperation).toHaveBeenCalledTimes(1)

      if (stale === 'success') {
        oldNext.resolve({ items: [auditEvent('obsolete')], count: 987654, next_cursor: 'obsolete-cursor' })
      } else {
        oldNext.reject(new Error('obsolete page failure'))
      }
      await settle()
      expect(displayedOrder()).toEqual(['two', 'one'])
      expect(target.textContent).not.toContain('obsolete')
      expect(target.textContent).not.toContain('obsolete page failure')
      expect(target.textContent).toContain('已显示 2 / 共 3')

      button('加载下一页').click()
      await settle()
      expect(api.getOperationTimeline).toHaveBeenCalledTimes(3)
      expect(api.getOperationTimeline.mock.calls.slice(1)).toEqual([
        ['op-a', { pageSize: 200, cursor: 'cursor-1', tenantId: 'tenant-a' }],
        ['op-a', { pageSize: 200, cursor: 'cursor-1', tenantId: 'tenant-a' }],
      ])
      const freshSignal = signalFor('timeline', 2)
      expect(freshSignal).not.toBe(oldSignal)
      expect(freshSignal.aborted).toBe(false)

      freshNext.resolve({ items: [auditEvent('four'), auditEvent('three')], count: 4 })
      await settle()
      expect(displayedOrder()).toEqual(['two', 'one', 'four', 'three'])
      expect(target.textContent).toContain('已显示 4 / 共 4')
      expect(api.getOperation).toHaveBeenCalledTimes(1)
      expect(api.replayOperation).toHaveBeenCalledTimes(1)
    },
  )
})

describe('operation context changes', () => {
  it('aborts every pending read when the edited ID changes, then ignores its late result', async () => {
    const held = holdReads()
    mountHarness()
    await settle()
    const oldSignals = sections.map(section => signalFor(section))
    expect(oldSignals.every(signal => !signal.aborted)).toBe(true)

    input('op-b')
    await settle()
    expect(oldSignals.every(signal => signal.aborted)).toBe(true)
    expect(loadingLabels()).toEqual([])
    expect(target.querySelector('h3')).toBeNull()

    enter()
    await settle()
    expect(api.getOperation).toHaveBeenCalledTimes(2)
    expect(api.getOperation).toHaveBeenLastCalledWith('op-b', 'tenant-a')
    expect(api.getOperationTimeline).toHaveBeenLastCalledWith('op-b', { pageSize: 200, tenantId: 'tenant-a' })
    expect(api.replayOperation).toHaveBeenLastCalledWith('op-b', 'tenant-a')
    const freshSignals = sections.map(section => signalFor(section, 1))
    expect(freshSignals.every(signal => !signal.aborted)).toBe(true)

    for (const section of sections) held[section].resolve(values('obsolete')[section] as never)
    await settle()
    for (const section of sections) expect(visible(section)).toBe(true)
    expect(target.textContent).not.toContain('obsolete')
    expect(target.textContent).not.toContain('987654')
    expect(loadingLabels()).toEqual([])
  })

  it('aborts and reloads when tenant and initialId props change A → B → A', async () => {
    const held = holdReads()
    mountHarness()
    await settle()
    const initialSignals = sections.map(section => signalFor(section))

    const tenantB = {
      summary: deferred<OperationSummary>(),
      timeline: deferred<QueryResult>(),
      replay: deferred<ReplayResult>(),
    }
    for (const section of sections) impl[section].mockReturnValueOnce(tenantB[section].promise)
    button('切换到租户 B').click()
    await settle()
    expect(initialSignals.every(signal => signal.aborted)).toBe(true)
    expect(api.getOperation).toHaveBeenLastCalledWith('op-a', 'tenant-b')
    const tenantBSignals = sections.map(section => signalFor(section, 1))
    expect(tenantBSignals.every(signal => !signal.aborted)).toBe(true)

    const tenantA = {
      summary: deferred<OperationSummary>(),
      timeline: deferred<QueryResult>(),
      replay: deferred<ReplayResult>(),
    }
    for (const section of sections) impl[section].mockReturnValueOnce(tenantA[section].promise)
    button('切换到租户 A').click()
    await settle()
    expect(tenantBSignals.every(signal => signal.aborted)).toBe(true)
    expect(api.getOperation).toHaveBeenLastCalledWith('op-a', 'tenant-a')
    const tenantASignals = sections.map(section => signalFor(section, 2))
    expect(tenantASignals.every(signal => !signal.aborted)).toBe(true)

    const initialB = {
      summary: deferred<OperationSummary>(),
      timeline: deferred<QueryResult>(),
      replay: deferred<ReplayResult>(),
    }
    for (const section of sections) impl[section].mockReturnValueOnce(initialB[section].promise)
    button('初始 ID 改为 B').click()
    await settle()
    expect(tenantASignals.every(signal => signal.aborted)).toBe(true)
    expect(api.getOperation).toHaveBeenLastCalledWith('op-b', 'tenant-a')
    const initialBSignals = sections.map(section => signalFor(section, 3))
    expect(initialBSignals.every(signal => !signal.aborted)).toBe(true)

    for (const section of sections) held[section].resolve(values('obsolete')[section] as never)
    await settle()
    for (const section of sections) expect(visible(section)).toBe(false)
    for (const section of sections) initialB[section].resolve(values()[section] as never)
    await settle()
    for (const section of sections) expect(visible(section)).toBe(true)
    expect(target.textContent).not.toContain('obsolete')
    expect(target.textContent).not.toContain('987654')
    expect(loadingLabels()).toEqual([])
  })

  it('aborts stale route reads and commits only the current deep-linked operation', async () => {
    const firstA = holdReads()
    mountOperations()
    await settle()
    const routeA = sections.map(section => signalFor(section))

    const secondB = {
      summary: deferred<OperationSummary>(),
      timeline: deferred<QueryResult>(),
      replay: deferred<ReplayResult>(),
    }
    for (const section of sections) impl[section].mockReturnValueOnce(secondB[section].promise)
    window.history.replaceState(null, '', '#operations/op/op-b')
    window.dispatchEvent(new HashChangeEvent('hashchange'))
    await settle()
    expect(routeA.every(signal => signal.aborted)).toBe(true)
    const routeB = sections.map(section => signalFor(section, 1))
    expect(routeB.every(signal => !signal.aborted)).toBe(true)

    window.history.replaceState(null, '', '#operations/op/op-a')
    window.dispatchEvent(new HashChangeEvent('hashchange'))
    await settle()
    expect(routeB.every(signal => signal.aborted)).toBe(true)
    expect(api.getOperation).toHaveBeenLastCalledWith('op-a', 'tenant-a')
    const currentA = sections.map(section => signalFor(section, 2))
    expect(currentA.every(signal => !signal.aborted)).toBe(true)

    for (const reads of [firstA, secondB]) {
      for (const section of sections) reads[section].resolve(values('obsolete')[section] as never)
    }
    await settle()
    for (const section of sections) expect(visible(section)).toBe(true)
    expect(api.getOperation).toHaveBeenCalledTimes(3)
    expect(target.textContent).not.toContain('obsolete')
    expect(target.textContent).not.toContain('987654')
    expect(loadingLabels()).toEqual([])
  })

  it('fences a synchronous auth A → B → original A batch before late section successes', async () => {
    const original = authenticated(session('token-a'))
    authState.set(original)
    const firstA = holdReads()
    mountHarness()
    await settle()
    const initialSignals = sections.map(section => signalFor(section))

    const secondB = {
      summary: deferred<OperationSummary>(),
      timeline: deferred<QueryResult>(),
      replay: deferred<ReplayResult>(),
    }
    const finalA = {
      summary: deferred<OperationSummary>(),
      timeline: deferred<QueryResult>(),
      replay: deferred<ReplayResult>(),
    }
    for (const section of sections) {
      impl[section].mockReturnValueOnce(secondB[section].promise)
      impl[section].mockReturnValueOnce(finalA[section].promise)
    }

    authState.set(authenticated(session('token-b', 'tenant-b')))
    // A notification only retires old reads; a user explicitly starts B/A.
    // There is still no tick/await between either auth notification.
    expect(api.getOperation).toHaveBeenCalledTimes(1)
    enter()
    authState.set(original)
    expect(api.getOperation).toHaveBeenCalledTimes(2)
    enter()
    await settle()

    expect(api.getOperation).toHaveBeenCalledTimes(3)
    const bSignals = sections.map(section => signalFor(section, 1))
    const aSignals = sections.map(section => signalFor(section, 2))
    expect(initialSignals.every(signal => signal.aborted)).toBe(true)
    expect(bSignals.every(signal => signal.aborted)).toBe(true)
    expect(aSignals.every(signal => !signal.aborted)).toBe(true)
    expect(get(authState)).toBe(original)

    for (const reads of [firstA, secondB]) {
      for (const section of sections) reads[section].resolve(values('obsolete')[section] as never)
    }
    await settle()
    for (const section of sections) expect(visible(section)).toBe(false)
    for (const section of sections) finalA[section].resolve(values()[section] as never)
    await settle()
    for (const section of sections) expect(visible(section)).toBe(true)
    expect(target.textContent).not.toContain('obsolete')
    expect(target.textContent).not.toContain('987654')
    expect(loadingLabels()).toEqual([])
  })

  it('aborts pending reads, ignores late results and unsubscribes auth notifications on unmount', async () => {
    authState.set(authenticated())
    const originalSubscribe = authState.subscribe
    let active = 0
    const subscription = vi.spyOn(authState, 'subscribe').mockImplementation((run, invalidate) => {
      active += 1
      const stop = originalSubscribe(run, invalidate)
      return () => {
        active -= 1
        stop()
      }
    })
    try {
      const held = holdReads()
      mountHarness()
      await settle()
      expect(active).toBe(1)
      const signals = sections.map(section => signalFor(section))

      const mounted = component
      if (!mounted) throw new Error('missing mounted component')
      component = undefined
      await unmount(mounted)

      expect(signals.every(signal => signal.aborted)).toBe(true)
      expect(active).toBe(0)
      for (const section of sections) held[section].resolve(values('obsolete')[section] as never)
      await settle()
      expect(target.childElementCount).toBe(0)
      expect(api.getOperation).toHaveBeenCalledTimes(1)
    } finally {
      subscription.mockRestore()
    }
  })
})

describe('preserved operation lookup controls', () => {
  it('keeps Enter and repeated clicks to one lookup per pending generation', async () => {
    holdReads()
    mountHarness()
    await settle()
    expect(api.getOperation).toHaveBeenCalledTimes(1)

    enter()
    enter()
    button('读取中…').click()
    await settle()

    expect(api.getOperation).toHaveBeenCalledTimes(1)
    expect(api.getOperationTimeline).toHaveBeenCalledTimes(1)
    expect(api.replayOperation).toHaveBeenCalledTimes(1)
    expect(api.getOperation).toHaveBeenLastCalledWith('op-a', 'tenant-a')
    expect(api.getOperationTimeline).toHaveBeenLastCalledWith('op-a', { pageSize: 200, tenantId: 'tenant-a' })
    expect(api.replayOperation).toHaveBeenLastCalledWith('op-a', 'tenant-a')
  })
})
