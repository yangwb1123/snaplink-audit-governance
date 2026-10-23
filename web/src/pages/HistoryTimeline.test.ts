// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import type { AuditEvent } from '../api/types'
import { authState } from '../auth/store'
import { formatDate } from '../components/format'
import HistoryTimeline from './HistoryTimeline.svelte'
import { eventHash } from './navigation'

const { api } = vi.hoisted(() => ({ api: { getEvent: vi.fn(), getReceipt: vi.fn() } }))
vi.mock('../api/useAuditApi', () => ({ useAuditApi: () => api }))
function event(id: string, tenant = 'tenant-a', version = 1): AuditEvent {
  return {
    event_id: id, tenant_id: tenant, aggregate_version: version, source_system: 'aero-vault',
    event_type: 'file.downloaded', schema_id: 'audit.event', schema_version: 1,
    occurred_at: '2026-10-01T00:00:00Z', received_at: '2026-10-01T00:00:01Z',
    actor: { id: 'actor' }, action: 'download', outcome: 'success',
    data_classification: 'internal', retention_class: 'standard',
  }
}
function login(permissions = ['audit:event:read'], tenantId = 'tenant-a', platform = false) {
  authState.set({ status: 'authenticated', session: {
    accessToken: 'fixture-access', idToken: 'fixture-identity', subject: 'actor', displayName: 'actor',
    expiresAt: Date.now() + 100000, tenantId, platform, roles: [], permissions: new Set(permissions),
  }, error: null })
}
let target: HTMLDivElement
let component: ReturnType<typeof mount> | undefined
async function settle() { await tick(); await Promise.resolve(); await tick() }
function render(items: AuditEvent[], tenantId?: string) {
  component = mount(HistoryTimeline, { target, props: {
    result: { items, count: items.length + 1 }, title: '聚合版本历史', showVersion: true, tenantId,
  } })
}
function links() { return [...target.querySelectorAll<HTMLAnchorElement>('a[href]')] }
beforeEach(() => {
  vi.resetAllMocks(); login()
  target = document.createElement('div'); document.body.append(target)
})
afterEach(async () => {
  if (component) await unmount(component)
  component = undefined; target.remove()
  authState.set({ status: 'anonymous', session: null, error: null })
})

describe('HistoryTimeline original event links', () => {
  it('renders focusable native links for each original event without fetching details', async () => {
    const items = [event('事件-z:@+1', 'tenant-a', 9), { ...event('event-a', 'tenant-a', 2), outcome: 'failed' }]
    render(items, 'tenant-a'); await settle()
    expect(links()).toHaveLength(2)
    links().forEach((link, index) => {
      expect(link.tagName).toBe('A')
      expect(link.getAttribute('href')).toBe(eventHash(items[index].event_id))
      expect(link.textContent).toContain('查看原事件')
      expect(link.textContent).toContain(items[index].event_id)
      expect(link.tabIndex).toBe(0)
      link.focus(); expect(document.activeElement).toBe(link)
    })
    const rows = [...target.querySelectorAll('[data-iris-timeline-item]')]
    rows.forEach((row, index) => {
      expect(row.textContent).toContain(formatDate(items[index].occurred_at))
      expect(row.textContent).toContain('aero-vault · file.downloaded')
      expect(row.textContent).toContain(`v${items[index].aggregate_version} · actor · download · ${items[index].outcome}`)
    })
    expect(rows.map(row => row.getAttribute('data-variant'))).toEqual(['success', 'danger'])
    expect(target.textContent).toContain('已显示 2 / 共 3 条；实时观察，翻页期间总数可能随提交变化。')
    expect(api.getEvent).not.toHaveBeenCalled(); expect(api.getReceipt).not.toHaveBeenCalled()
  })

  it('reactively removes event links on permission revocation while preserving history text', async () => {
    render([event('event-a')], 'tenant-a'); await settle()
    expect(links()).toHaveLength(1)
    login(['audit:operation:read']); await settle()
    expect(links()).toHaveLength(0)
    expect(target.textContent).toContain('v1 · actor · download · success')
    expect(api.getEvent).not.toHaveBeenCalled(); expect(api.getReceipt).not.toHaveBeenCalled()
  })

  it.each([
    { name: 'operation permission only', permissions: ['audit:operation:read'], signedTenant: 'tenant-a', tenant: 'tenant-a', platform: false },
    { name: 'missing signed tenant', permissions: ['audit:event:read'], signedTenant: '', tenant: 'tenant-a', platform: false },
    { name: 'mismatched selected tenant', permissions: ['audit:event:read'], signedTenant: 'tenant-b', tenant: 'tenant-a', platform: false },
    { name: 'platform without selection', permissions: [], signedTenant: 'tenant-a', tenant: undefined, platform: true },
    { name: 'platform empty selection', permissions: [], signedTenant: 'tenant-a', tenant: '', platform: true },
  ])('does not link with $name', async ({ permissions, signedTenant, tenant, platform }) => {
    login(permissions, signedTenant, platform)
    render([event('event-a')], tenant); await settle()
    expect(links()).toHaveLength(0)
    expect(target.textContent).toContain('aero-vault · file.downloaded')
  })

  it.each([false, true])('uses the current effective tenant with platform=%s', async (platform) => {
    login(platform ? [] : ['audit:event:read'], 'tenant-a', platform)
    render([event('event-a'), event('foreign', 'tenant-b'), event('missing-tenant', '')], platform ? 'tenant-a' : undefined)
    await settle()
    expect(links().map(link => link.getAttribute('href'))).toEqual([eventHash('event-a')])
  })

  it.each([
    '', '.', '..', 'bad/id', 'bad\\id', 'bad?tenant_id=tenant-b', 'bad#events',
    'https:example', 'javascript:alert', 'data:text', '%2F', '%252F', 'bad id',
    'bad\nID', 'bad\u200bID', '\ud800', 'e'.repeat(257), '<img:onerror=alert>',
  ].map((id, index) => ({ id, index })))('does not turn rejected identifier case $index into a fallback or injected link', async ({ id }) => {
    render([event('allowed'), event(id)], 'tenant-a'); await settle()
    expect(links().map(link => link.getAttribute('href'))).toEqual([eventHash('allowed')])
    expect(target.querySelector('img, script, iframe')).toBeNull()
    expect(target.querySelectorAll('[data-iris-timeline-item]')).toHaveLength(2)
  })

  it('does not link after logout', async () => {
    render([event('event-a')], 'tenant-a'); await settle()
    expect(links()).toHaveLength(1)
    authState.set({ status: 'anonymous', session: null, error: null }); await settle()
    expect(links()).toHaveLength(0)
  })
})
