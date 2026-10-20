// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import type { AuditEvent, OperationSummary } from '../api/types'
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

const event: AuditEvent = {
  event_id: 'evt-1', tenant_id: 'tenant-a', source_system: 'aero-vault', event_type: 'file.downloaded',
  schema_id: 'audit.event', schema_version: 1, occurred_at: '2026-10-01T00:00:00Z',
  received_at: '2026-10-01T00:00:01Z', actor: { id: 'alice' }, action: 'download', outcome: 'success',
  data_classification: 'internal', retention_class: 'standard',
}
const summary: OperationSummary = {
  operation_id: 'op-1', tenant_id: 'tenant-a', event_count: 1,
  first_at: '2026-10-01T00:00:00Z', last_at: '2026-10-01T00:00:00Z', outcomes: ['success'],
}
const replay = { tenant_id: 'tenant-a', operation_id: 'op-1', event_count: 1, state: { status: 'done' }, last_sequence: 1 }

let target: HTMLDivElement
let component: ReturnType<typeof mount> | undefined

async function settle(): Promise<void> {
  await tick()
  await Promise.resolve()
  await new Promise((resolve) => setTimeout(resolve, 0))
  await tick()
}

function mountOperation(): void {
  window.location.hash = '#operations/op/op-1'
  component = mount(OperationsPage, { target, props: { tenantId: 'tenant-a' } })
}

function replayState(): unknown {
  const pre = target.querySelector('pre.json-view')
  return pre ? JSON.parse(pre.textContent ?? '{}') : null
}

function retryButtons(): HTMLButtonElement[] {
  return [...target.querySelectorAll('button')].filter((node) => node.textContent?.trim() === '重试') as HTMLButtonElement[]
}

beforeEach(() => {
  vi.resetAllMocks()
  api.getOperation.mockResolvedValue(summary)
  api.getOperationTimeline.mockResolvedValue({ items: [event], count: 1 })
  api.replayOperation.mockResolvedValue(replay)
  target = document.createElement('div')
  document.body.append(target)
})

afterEach(async () => {
  if (component) await unmount(component)
  component = undefined
  target.remove()
  window.history.replaceState(null, '', '#')
})

describe('resilient operation investigation', () => {
  it('keeps summary and timeline when the replay read fails, and retries only replay', async () => {
    api.replayOperation.mockRejectedValueOnce(new Error('replay unavailable')).mockResolvedValueOnce(replay)
    mountOperation()
    await settle()
    expect(target.textContent).toContain('操作摘要')
    expect(target.textContent).toContain('aero-vault · file.downloaded')
    expect(target.textContent).toContain('部分区块读取失败')
    expect(replayState()).toBeNull()
    const buttons = retryButtons()
    expect(buttons).toHaveLength(1)
    buttons[0].click()
    await settle()
    expect(api.replayOperation).toHaveBeenCalledTimes(2)
    expect(api.getOperation).toHaveBeenCalledTimes(1)
    expect(api.getOperationTimeline).toHaveBeenCalledTimes(1)
    expect(replayState()).toEqual({ status: 'done' })
    expect(target.textContent).not.toContain('部分区块读取失败')
  })

  it('keeps replay and timeline when the summary read fails', async () => {
    api.getOperation.mockRejectedValueOnce(new Error('summary unavailable'))
    mountOperation()
    await settle()
    expect(target.textContent).toContain('summary unavailable')
    expect(replayState()).toEqual({ status: 'done' })
    expect(target.textContent).toContain('aero-vault · file.downloaded')
  })

  it('shows a timeline failure without hiding the other sections', async () => {
    api.getOperationTimeline.mockRejectedValueOnce(new Error('timeline unavailable'))
    mountOperation()
    await settle()
    expect(target.textContent).toContain('timeline unavailable')
    expect(target.textContent).toContain('操作摘要')
    expect(replayState()).toEqual({ status: 'done' })
  })
})
