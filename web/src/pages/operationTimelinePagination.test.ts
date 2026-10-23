// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import type { AuditEvent, QueryResult } from '../api/types'
import OperationsPage from './OperationsPage.svelte'

const { api } = vi.hoisted(() => ({
  api: {
    getOperation: vi.fn(),
    getOperationTimeline: vi.fn(),
    replayOperation: vi.fn(),
    getAggregateTimeline: vi.fn(),
  },
}))
vi.mock('../api/useAuditApi', () => ({ useAuditApi: () => api }))

function auditEvent(id: string): AuditEvent {
  return {
    event_id: id, tenant_id: 'tenant-a', source_system: 'aero-vault', event_type: 'file.downloaded',
    schema_id: 'audit.event', schema_version: 1, occurred_at: '2026-10-01T00:00:00Z',
    received_at: '2026-10-01T00:00:01Z', actor: { id: 'alice' }, action: 'download', outcome: 'success',
    data_classification: 'internal', retention_class: 'standard',
  }
}

const firstPage = { items: [auditEvent('evt-1'), auditEvent('evt-2')], count: 3, next_cursor: 'cursor-1' }
const secondPage = { items: [auditEvent('evt-3')], count: 3 }

let target: HTMLDivElement
let component: ReturnType<typeof mount> | undefined

async function settle(): Promise<void> {
  await tick()
  await Promise.resolve()
  await tick()
  await Promise.resolve()
  await tick()
}

function button(label: string): HTMLButtonElement | undefined {
  return [...target.querySelectorAll('button')].find((node) => node.textContent?.trim() === label) as
    | HTMLButtonElement
    | undefined
}

function mountOperation(): void {
  window.location.hash = '#operations/op/op-1'
  component = mount(OperationsPage, { target, props: { tenantId: 'tenant-a' } })
}

beforeEach(() => {
  vi.resetAllMocks()
  api.getOperation.mockResolvedValue({
    operation_id: 'op-1', tenant_id: 'tenant-a', event_count: 3,
    first_at: '2026-10-01T00:00:00Z', last_at: '2026-10-01T00:00:02Z', outcomes: ['success'],
  })
  api.getOperationTimeline.mockResolvedValueOnce(firstPage).mockResolvedValueOnce(secondPage)
  api.replayOperation.mockResolvedValue({
    tenant_id: 'tenant-a', operation_id: 'op-1', event_count: 3, state: {}, last_sequence: 3,
  })
  target = document.createElement('div')
  document.body.append(target)
})

afterEach(async () => {
  if (component) await unmount(component)
  component = undefined
  target.remove()
  window.history.replaceState(null, '', '#')
})

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (cause: unknown) => void
  const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail })
  return { promise, resolve, reject }
}

function orderedEvent(id: string): AuditEvent {
  return { ...auditEvent(id), action: `step-${id}` }
}

function displayedOrder(): string[] {
  const text = target.textContent ?? ''
  return ['one', 'two', 'three', 'four']
    .filter(id => text.includes(`step-${id}`))
    .sort((a, b) => text.indexOf(`step-${a}`) - text.indexOf(`step-${b}`))
}

describe('operation timeline pagination', () => {
  it('shows displayed/total and appends the next page without re-reading other sections', async () => {
    mountOperation()
    await settle()
    expect(target.textContent).toContain('已显示 2 / 共 3')
    const more = button('加载下一页')
    expect(more).toBeDefined()
    more?.click()
    await settle()
    expect(api.getOperationTimeline).toHaveBeenLastCalledWith('op-1', {
      pageSize: 200, cursor: 'cursor-1', tenantId: 'tenant-a',
    })
    expect(api.getOperation).toHaveBeenCalledTimes(1)
    expect(api.replayOperation).toHaveBeenCalledTimes(1)
    expect(target.textContent).toContain('已显示 3 / 共 3')
    expect(button('加载下一页')).toBeUndefined()
  })

  it('keeps loaded events when the next page fails and retries the same cursor', async () => {
    api.getOperationTimeline
      .mockReset()
      .mockResolvedValueOnce(firstPage)
      .mockRejectedValueOnce(new Error('next page unavailable'))
      .mockResolvedValueOnce(secondPage)
    mountOperation()
    await settle()
    button('加载下一页')?.click()
    await settle()
    expect(target.textContent).toContain('next page unavailable')
    expect(target.textContent).toContain('已显示 2 / 共 3')
    const retry = button('重试')
    expect(retry).toBeDefined()
    retry?.click()
    await settle()
    expect(target.textContent).toContain('已显示 3 / 共 3')
    expect(target.textContent).not.toContain('next page unavailable')
    expect(api.getOperationTimeline.mock.calls.slice(1)).toEqual([
      ['op-1', { pageSize: 200, cursor: 'cursor-1', tenantId: 'tenant-a' }],
      ['op-1', { pageSize: 200, cursor: 'cursor-1', tenantId: 'tenant-a' }],
    ])
    expect(api.getOperation).toHaveBeenCalledTimes(1)
    expect(api.replayOperation).toHaveBeenCalledTimes(1)
  })

  it.each((['summary', 'replay'] as const).flatMap(pending => (['success', 'failure'] as const).map(outcome => ({ pending, outcome }))))(
    'paginates and retries the same cursor while $pending hangs, then accepts its late $outcome without replacing appended events',
    async ({ pending, outcome }) => {
      const sibling = deferred<unknown>()
      const next = deferred<QueryResult>()
      const retry = deferred<QueryResult>()
      const method = pending === 'summary' ? 'getOperation' : 'replayOperation'
      api[method].mockReturnValueOnce(sibling.promise)
      api.getOperationTimeline.mockReset()
        .mockResolvedValueOnce({ items: [orderedEvent('two'), orderedEvent('one')], count: 3, next_cursor: 'cursor-1' })
        .mockReturnValueOnce(next.promise).mockReturnValueOnce(retry.promise)
      mountOperation()
      await settle()
      expect(displayedOrder()).toEqual(['two', 'one'])
      expect(button('加载下一页')?.disabled).toBe(false)
      button('加载下一页')!.click()
      button('加载下一页')?.click()
      await settle()
      expect(api.getOperationTimeline).toHaveBeenCalledTimes(2)
      next.reject(new Error('next page unavailable'))
      await settle()
      expect(displayedOrder()).toEqual(['two', 'one'])
      expect(target.textContent?.includes('已显示 2 / 共 3')).toBe(true)
      expect(target.textContent?.includes('next page unavailable')).toBe(true)
      const retryButton = button('重试')
      expect(Boolean(retryButton)).toBe(true)
      retryButton!.click()
      await settle()
      expect(api.getOperationTimeline).toHaveBeenCalledTimes(3)
      expect(api.getOperationTimeline.mock.calls.slice(1)).toEqual([
        ['op-1', { pageSize: 200, cursor: 'cursor-1', tenantId: 'tenant-a' }],
        ['op-1', { pageSize: 200, cursor: 'cursor-1', tenantId: 'tenant-a' }],
      ])
      retry.resolve({ items: [orderedEvent('four'), orderedEvent('three')], count: 4 })
      await settle()
      expect(displayedOrder()).toEqual(['two', 'one', 'four', 'three'])
      expect(target.textContent?.includes('已显示 4 / 共 4')).toBe(true)
      expect(target.textContent?.includes('实时观察，翻页期间总数可能随提交变化')).toBe(true)
      if (outcome === 'success') sibling.resolve(pending === 'summary'
        ? { operation_id: 'op-1', tenant_id: 'tenant-a', event_count: 3, outcomes: ['observed'] }
        : { tenant_id: 'tenant-a', event_count: 3, state: {}, last_sequence: 3 })
      else sibling.reject(new Error('sibling unavailable'))
      await settle()
      expect(displayedOrder()).toEqual(['two', 'one', 'four', 'three'])
      expect(target.textContent?.includes('已显示 4 / 共 4')).toBe(true)
      expect(target.querySelectorAll('.loading-block[role="status"]')).toHaveLength(0)
      expect(target.textContent?.includes(outcome === 'failure' ? 'sibling unavailable' : pending === 'summary' ? 'observed' : '共回放 3 个事件')).toBe(true)
      expect(api.getOperation).toHaveBeenCalledTimes(1)
      expect(api.replayOperation).toHaveBeenCalledTimes(1)
      expect(api.getOperationTimeline).toHaveBeenCalledTimes(3)
    },
  )

  it.each((['query', 'deep link', 'unmount'] as const).flatMap(change => (['success', 'failure'] as const).map(outcome => ({ change, outcome }))))(
    'ignores obsolete page $outcome after $change, without clearing a newer page busy state', async ({ change, outcome }) => {
      const old = deferred<QueryResult>()
      const current = deferred<QueryResult>()
      api.getOperationTimeline.mockReset().mockResolvedValueOnce(firstPage).mockReturnValueOnce(old.promise)
      if (change !== 'unmount') api.getOperationTimeline.mockResolvedValueOnce(firstPage)
      api.getOperationTimeline.mockResolvedValueOnce(firstPage).mockReturnValueOnce(current.promise)
      mountOperation()
      await settle()
      button('加载下一页')!.click()
      await settle()
      if (change === 'unmount') {
        await unmount(component!)
        component = undefined
        mountOperation()
        await settle()
      } else for (const id of ['op-2', 'op-1']) {
        if (change === 'query') {
          const input = target.querySelector('input')!
          input.value = id
          input.dispatchEvent(new Event('input', { bubbles: true }))
          await settle()
          input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }))
        } else {
          window.history.replaceState(null, '', `#operations/op/${id}`)
          window.dispatchEvent(new HashChangeEvent('hashchange'))
        }
        await settle()
      }
      button('加载下一页')!.click()
      await settle()
      const calls = api.getOperationTimeline.mock.calls.length
      if (outcome === 'success') old.resolve({ items: [orderedEvent('obsolete')], count: 987654 })
      else old.reject(new Error('obsolete page failure'))
      await settle()
      expect(target.textContent?.includes('obsolete')).toBe(false)
      expect(target.textContent?.includes('987654')).toBe(false)
      expect(target.textContent?.includes('已显示 2 / 共 3')).toBe(true)
      expect(button('加载中…')?.disabled).toBe(true)
      button('加载中…')!.click()
      expect(api.getOperationTimeline).toHaveBeenCalledTimes(calls)
      current.resolve(secondPage)
      await settle()
      expect(target.textContent?.includes('已显示 3 / 共 3')).toBe(true)
      expect(button('加载下一页')).toBeUndefined()
    },
  )
})
