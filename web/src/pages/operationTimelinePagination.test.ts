// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import type { AuditEvent } from '../api/types'
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
  await new Promise((resolve) => setTimeout(resolve, 0))
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
  })
})
