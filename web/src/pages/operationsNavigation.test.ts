// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
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

let target: HTMLDivElement
let component: ReturnType<typeof mount> | undefined

async function settle(): Promise<void> {
  await tick()
  await Promise.resolve()
  await new Promise((resolve) => setTimeout(resolve, 0))
  await tick()
}

function mountAt(hash: string): void {
  window.location.hash = hash
  component = mount(OperationsPage, { target, props: { tenantId: 'tenant-a' } })
}

function inputValues(): string[] {
  return [...target.querySelectorAll('input')].map((input) => (input as HTMLInputElement).value)
}

beforeEach(() => {
  vi.resetAllMocks()
  api.getOperation.mockResolvedValue({
    operation_id: 'op-1', tenant_id: 'tenant-a', event_count: 2,
    first_at: '2026-10-01T00:00:00Z', last_at: '2026-10-01T00:01:00Z', outcomes: ['success'],
  })
  api.getOperationTimeline.mockResolvedValue({ items: [], count: 2 })
  api.replayOperation.mockResolvedValue({
    tenant_id: 'tenant-a', operation_id: 'op-1', event_count: 2, state: { status: 'done' }, last_sequence: 2,
  })
  api.getAggregateTimeline.mockResolvedValue({ items: [], count: 3 })
  target = document.createElement('div')
  document.body.append(target)
})

afterEach(async () => {
  if (component) await unmount(component)
  component = undefined
  target.remove()
  window.history.replaceState(null, '', '#')
})

describe('investigation deep links', () => {
  it('opens an operation timeline directly from an event link', async () => {
    mountAt('#operations/op/op-1')
    await settle()
    expect(api.getOperation).toHaveBeenCalledWith('op-1', 'tenant-a')
    expect(api.getOperationTimeline).toHaveBeenCalledWith('op-1', { pageSize: 200, tenantId: 'tenant-a' })
    expect(api.replayOperation).toHaveBeenCalledWith('op-1', 'tenant-a')
    expect(inputValues()).toContain('op-1')
    expect(target.textContent).toContain('操作摘要')
    expect(api.getAggregateTimeline).not.toHaveBeenCalled()
  })

  it('opens aggregate history in the aggregate tab with the same bounded identifiers', async () => {
    mountAt('#operations/aggregate/file/file-9')
    await settle()
    expect(api.getAggregateTimeline).toHaveBeenCalledWith('file', 'file-9', {
      pageSize: 200, cursor: '', tenantId: 'tenant-a',
    })
    expect(api.getOperation).not.toHaveBeenCalled()
    expect(inputValues()).toContain('file-9')
  })

  it('does not issue a lookup for a malformed or untrusted link', async () => {
    mountAt('#operations/op/bad%2Fid')
    await settle()
    expect(api.getOperation).not.toHaveBeenCalled()
    expect(api.getAggregateTimeline).not.toHaveBeenCalled()
    expect(inputValues().every((value) => value === '')).toBe(true)
  })

  it('re-runs the lookup when a new investigation link arrives on the same page', async () => {
    mountAt('#operations/op/op-1')
    await settle()
    window.location.hash = '#operations/op/op-2'
    await settle()
    expect(api.getOperation).toHaveBeenLastCalledWith('op-2', 'tenant-a')
    expect(inputValues()).toContain('op-2')
  })
})
