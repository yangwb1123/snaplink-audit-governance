// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import type { OperationSummary, QueryResult, ReplayResult } from '../api/types'
import OperationsPage from './OperationsPage.svelte'

const { api } = vi.hoisted(() => ({ api: {
  getOperation: vi.fn(), getOperationTimeline: vi.fn(), replayOperation: vi.fn(),
  getAggregateTimeline: vi.fn(),
} }))
vi.mock('../api/useAuditApi', () => ({ useAuditApi: () => api }))

type Section = 'summary' | 'timeline' | 'replay'
const sections: Section[] = ['summary', 'timeline', 'replay']
const methods = { summary: 'getOperation', timeline: 'getOperationTimeline', replay: 'replayOperation' } as const
const labels = { summary: '正在读取操作摘要…', timeline: '正在读取事件时间线…', replay: '正在读取状态回放…' }
const headings = { summary: '操作摘要', timeline: '事件时间线', replay: '状态回放' }

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (cause: unknown) => void
  const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail })
  return { promise, resolve, reject }
}

function values(marker = 'current') {
  return {
    summary: {
      operation_id: 'op-a', tenant_id: 'tenant-a', event_count: 1,
      first_at: '2026-10-01T00:00:00Z', last_at: '2026-10-01T00:00:00Z', outcomes: [marker],
    } satisfies OperationSummary,
    timeline: { items: [], count: marker === 'current' ? 1 : 987654 } satisfies QueryResult,
    replay: { tenant_id: 'tenant-a', operation_id: 'op-a', event_count: 1, state: { status: marker }, last_sequence: 1 } satisfies ReplayResult,
  }
}

function holdReads() {
  const reads = {
    summary: deferred<OperationSummary>(), timeline: deferred<QueryResult>(), replay: deferred<ReplayResult>(),
  }
  for (const section of sections) api[methods[section]].mockReturnValueOnce(reads[section].promise)
  return reads
}

let target: HTMLDivElement
let component: ReturnType<typeof mount> | undefined

// Drain only controlled promise continuations and Svelte updates; no timers or polling.
async function settle(): Promise<void> {
  await tick()
  await Promise.resolve()
  await tick()
  await Promise.resolve()
  await tick()
}

function mountOperation(): void {
  window.history.replaceState(null, '', '#operations/op/op-a')
  component = mount(OperationsPage, { target, props: { tenantId: 'tenant-a' } })
}

function card(section: Section): HTMLElement {
  return [...target.querySelectorAll('h3')].find(node => node.textContent === headings[section])!.parentElement!
}

function visible(section: Section, marker = 'current'): boolean {
  if (section === 'timeline') return card(section).textContent?.includes(`共 ${marker === 'current' ? 1 : 987654} 条`) ?? false
  return card(section).textContent?.includes(marker) ?? false
}

function loadingLabels(): string[] {
  return [...target.querySelectorAll('.loading-block[role="status"]')].map(node => node.lastElementChild?.textContent?.trim() ?? '')
}

function input(value: string): void {
  const node = target.querySelector('input')!
  node.value = value
  node.dispatchEvent(new Event('input', { bubbles: true }))
}

function enter(): void {
  target.querySelector('input')!.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }))
}

beforeEach(() => {
  vi.resetAllMocks()
  const current = values()
  for (const section of sections) api[methods[section]].mockResolvedValue(current[section])
  target = document.createElement('div')
  document.body.append(target)
})
afterEach(async () => {
  if (component) await unmount(component)
  component = undefined
  target.remove()
  window.history.replaceState(null, '', '#')
})

describe('independent operation reads with a pending sibling', () => {
  it.each(sections)('commits other successes immediately while %s never settles', async pending => {
    const held = holdReads()
    mountOperation()
    await settle()
    expect(loadingLabels()).toHaveLength(3)
    const current = values()
    for (const section of sections) if (section !== pending) held[section].resolve(current[section] as never)
    await settle()
    for (const section of sections) if (section !== pending) expect(visible(section)).toBe(true)
    expect(loadingLabels()).toEqual([labels[pending]])
  })

  it.each(sections)('commits an error and a success immediately while %s never settles', async pending => {
    const held = holdReads()
    const [failed, succeeded] = sections.filter(section => section !== pending)
    mountOperation()
    await settle()
    held[failed].reject(new Error('section unavailable'))
    held[succeeded].resolve(values()[succeeded] as never)
    await settle()
    expect(card(failed).textContent?.includes('section unavailable')).toBe(true)
    expect(visible(succeeded)).toBe(true)
    expect(target.textContent?.includes('部分区块读取失败')).toBe(true)
    expect(loadingLabels()).toEqual([labels[pending]])
  })

  it.each(sections)('retries only failed %s without waiting for a pending sibling', async failed => {
    const pending = sections.find(section => section !== failed)!
    const held = holdReads()
    const retry = deferred<OperationSummary | QueryResult | ReplayResult>()
    api[methods[failed]].mockReturnValueOnce(retry.promise)
    mountOperation()
    await settle()
    held[failed].reject(new Error('section unavailable'))
    const succeeded = sections.find(section => section !== failed && section !== pending)!
    held[succeeded].resolve(values()[succeeded] as never)
    await settle()
    const button = [...card(failed).querySelectorAll('button')].find(node => node.textContent?.trim() === '重试')
    expect(Boolean(button)).toBe(true)
    button!.click()
    await settle()
    for (const section of sections) expect(api[methods[section]]).toHaveBeenCalledTimes(section === failed ? 2 : 1)
    expect(loadingLabels().includes(labels[failed])).toBe(true)
    expect(card(failed).textContent?.includes('section unavailable')).toBe(false)
    retry.resolve(values()[failed])
    await settle()
    expect(visible(failed)).toBe(true)
    expect(visible(succeeded)).toBe(true)
    expect(loadingLabels()).toEqual([labels[pending]])
  })
})

describe('operation observation lifetime', () => {
  it.each(sections.flatMap(section => (['success', 'failure'] as const).map(outcome => ({ section, outcome }))))(
    'ignores late $outcome from a $section retry after query A → B → A', async ({ section, outcome }) => {
      const retry = deferred<OperationSummary | QueryResult | ReplayResult>()
      api[methods[section]].mockRejectedValueOnce(new Error('section unavailable')).mockReturnValueOnce(retry.promise)
      mountOperation()
      await settle()
      const button = [...card(section).querySelectorAll('button')].find(node => node.textContent?.trim() === '重试')!
      button.click()
      await settle()
      for (const id of ['op-b', 'op-a']) {
        input(id)
        await settle()
        enter()
        await settle()
      }
      if (outcome === 'success') retry.resolve(values('obsolete')[section])
      else retry.reject(new Error('obsolete failure'))
      await settle()
      for (const part of sections) {
        expect(visible(part)).toBe(true)
        expect(api[methods[part]]).toHaveBeenCalledTimes(part === section ? 4 : 3)
      }
      expect(target.textContent?.includes('obsolete')).toBe(false)
      expect(target.textContent?.includes('987654')).toBe(false)
      expect(loadingLabels()).toEqual([])
    },
  )

  it.each(['success', 'failure'] as const)('ignores old %s after query A → B → A and preserves Enter lookup', async outcome => {
    const oldA = holdReads()
    const oldB = holdReads()
    mountOperation()
    await settle()
    input('op-b')
    await settle()
    expect(target.querySelector('h3')).toBeNull()
    expect(api.getOperation).toHaveBeenCalledTimes(1)
    enter()
    await settle()
    input('op-a')
    await settle()
    enter()
    await settle()
    expect(api.getOperation).toHaveBeenCalledTimes(3)
    expect(api.getOperation).toHaveBeenLastCalledWith('op-a', 'tenant-a')
    for (const reads of [oldA, oldB]) for (const section of sections) {
      if (outcome === 'success') reads[section].resolve(values('obsolete')[section] as never)
      else reads[section].reject(new Error('obsolete failure'))
    }
    await settle()
    for (const section of sections) expect(visible(section)).toBe(true)
    expect(target.textContent?.includes('obsolete')).toBe(false)
    expect(target.textContent?.includes('987654')).toBe(false)
    expect(loadingLabels()).toEqual([])
    // Manual query drafts never replace the bounded navigation target.
    expect(window.location.hash).toBe('#operations/op/op-a')
  })

  it.each(['success', 'failure'] as const)('ignores old %s after deep-link A → B → A', async outcome => {
    const oldA = holdReads()
    const oldB = holdReads()
    mountOperation()
    await settle()
    for (const id of ['op-b', 'op-a']) {
      window.history.replaceState(null, '', `#operations/op/${id}`)
      window.dispatchEvent(new HashChangeEvent('hashchange'))
      await settle()
    }
    expect(api.getOperation).toHaveBeenCalledTimes(3)
    for (const reads of [oldA, oldB]) for (const section of sections) {
      if (outcome === 'success') reads[section].resolve(values('obsolete')[section] as never)
      else reads[section].reject(new Error('obsolete failure'))
    }
    await settle()
    for (const section of sections) expect(visible(section)).toBe(true)
    expect(target.textContent?.includes('obsolete')).toBe(false)
    expect(target.textContent?.includes('987654')).toBe(false)
    expect(loadingLabels()).toEqual([])
  })

  it.each(['success', 'failure'] as const)('discards late %s after unmount, including a new instance at the same target', async outcome => {
    const old = holdReads()
    mountOperation()
    await settle()
    await unmount(component!)
    component = undefined
    expect(target.childElementCount).toBe(0)
    mountOperation()
    await settle()
    for (const section of sections) {
      if (outcome === 'success') old[section].resolve(values('obsolete')[section] as never)
      else old[section].reject(new Error('obsolete failure'))
    }
    await settle()
    for (const section of sections) expect(visible(section)).toBe(true)
    expect(target.textContent?.includes('obsolete')).toBe(false)
    expect(target.textContent?.includes('987654')).toBe(false)
    expect(loadingLabels()).toEqual([])
  })
})
