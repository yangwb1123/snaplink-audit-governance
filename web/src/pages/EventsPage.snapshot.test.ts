// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { ApiError } from '../api/client'
import Page from './EventsPage.svelte'

const { api } = vi.hoisted(() => ({ api: { queryEvents: vi.fn(), getEvent: vi.fn(), getReceipt: vi.fn() } }))
vi.mock('../api/useAuditApi', () => ({ useAuditApi: () => api }))
let target: HTMLDivElement
let page: ReturnType<typeof mount> | undefined
async function settle() { await tick(); await Promise.resolve(); await tick() }
function button(label: string) { return [...target.querySelectorAll('button')].find(item => item.textContent?.trim() === label)! }
const first = { items: [], count: 4, next_cursor: 'opaque-snapshot-cursor', snapshot: { observed_at: '2026-10-08T00:00:00Z', expires_at: '2026-10-08T00:15:00Z', source: 'ledger', completeness: 'not_asserted' } }

beforeEach(() => {
  api.queryEvents.mockReset().mockResolvedValue(first)
  target = document.createElement('div'); document.body.append(target)
})
afterEach(async () => { if (page) await unmount(page); page = undefined; target.remove() })

describe('fixed event result membership UI', () => {
  it('clears expired pagination and explicitly restarts the query', async () => {
    page = mount(Page, { target, props: { tenantId: 'tenant-a' } }); await settle()
    expect(target.textContent).toContain('共 4 条事件')
    expect(target.textContent).toContain('不表示完整性已确认')
    api.queryEvents.mockRejectedValueOnce(new ApiError(400, 'invalid_request', 'query cursor requires a new query'))
    button('加载下一页').click(); await settle()
    expect(api.queryEvents.mock.calls[1]?.[0].cursor).toBe('opaque-snapshot-cursor')
    expect(button('加载下一页')).toBeUndefined()
    button('重新查询最新结果').click(); await settle()
    expect(api.queryEvents.mock.calls[2]?.[0].cursor).toBe('')
  })

  it('aborts unmounted scope and rejects results that ignore AbortSignal', async () => {
    let release!: (value: typeof first) => void
    api.queryEvents.mockImplementationOnce(() => new Promise(resolve => { release = resolve }))
    page = mount(Page, { target, props: { tenantId: 'tenant-a' } }); await settle()
    const signal = api.queryEvents.mock.calls[0]?.[2] as AbortSignal
    await unmount(page); page = undefined
    expect(signal.aborted).toBe(true)
    api.queryEvents.mockResolvedValue({ ...first, count: 7 })
    page = mount(Page, { target, props: { tenantId: 'tenant-b' } }); await settle()
    release({ ...first, count: 999 }); await settle()
    expect(target.textContent).toContain('共 7 条事件')
    expect(target.textContent).not.toContain('999')
    expect(api.queryEvents.mock.calls[1]?.[1]).toBe('tenant-b')
  })
})
