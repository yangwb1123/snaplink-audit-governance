// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { authState } from '../auth/store'
import QuickJump from './QuickJump.svelte'
let target: HTMLDivElement
let component: ReturnType<typeof mount> | undefined
function login(permissions = ['audit:event:read', 'audit:operation:read', 'audit:export:create'], token = 'token') {
  authState.set({ status: 'authenticated', session: { accessToken: token, idToken: 'id', expiresAt: Date.now() + 100000, subject: 'actor', displayName: 'actor', tenantId: 'tenant-a', platform: false, roles: [], permissions: new Set(permissions) }, error: null })
}
async function choose(kind: string, id: string) {
  const select = target.querySelector('select')!
  select.value = kind; select.dispatchEvent(new Event('change', { bubbles: true })); await tick()
  const input = target.querySelector('input')!
  input.value = id; input.dispatchEvent(new Event('input', { bubbles: true })); await tick()
}
async function submit() { target.querySelector('form')!.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true })); await tick() }
beforeEach(() => { login(); window.history.replaceState(null, '', '#catalog'); target = document.createElement('div'); document.body.append(target) })
afterEach(async () => { if (component) await unmount(component); component = undefined; target.remove(); authState.set({ status: 'anonymous', session: null, error: null }); window.history.replaceState(null, '', '#') })

describe('explicit global quick jump', () => {
  it('requires a type and uses existing operation/export routes without changing tenant or storing a token', async () => {
    component = mount(QuickJump, { target, props: { scopeReady: true, scopeKey: 'tenant-a:token' } }); await tick()
    expect(target.querySelector('input')!.disabled).toBe(true)
    await submit(); expect(window.location.hash).toBe('#catalog')
    await choose('event', 'op-1'); await submit(); expect(window.location.hash).toBe('#events/event/op-1')
    await choose('operation', 'op-1'); await submit(); expect(window.location.hash).toBe('#operations/op/op-1')
    await choose('export', 'export-0123456789abcdef'); await submit(); expect(window.location.hash).toBe('#evidence/exports/export-0123456789abcdef')
    expect(window.location.hash).not.toContain('tenant'); expect(window.location.hash).not.toContain('token')
  })

  it('rejects URL/query injection and an ID that does not match the explicitly selected type', async () => {
    component = mount(QuickJump, { target, props: { scopeReady: true, scopeKey: 'tenant-a:token' } }); await tick()
    for (const [kind, id] of [['event', 'id?tenant_id=tenant-b'], ['operation', 'https://evil.test'], ['export', 'event-1']]) {
      await choose(kind, id); await submit()
      expect(window.location.hash).toBe('#catalog'); expect(target.textContent).toContain('请输入有效的 ID')
    }
  })

  it('does not navigate without the selected permission or a confirmed scope; token changes erase drafts', async () => {
    login(['audit:event:read'])
    component = mount(QuickJump, { target, props: { scopeReady: true, scopeKey: 'tenant-a:token' } }); await tick()
    await choose('export', 'export-0123456789abcdef'); await submit(); expect(window.location.hash).toBe('#catalog')
    await choose('event', 'event-a'); login([], 'new-token'); await tick()
    expect(target.querySelector('input')!.value).toBe(''); expect(target.querySelector('select')!.value).toBe('')
    await submit(); expect(window.location.hash).toBe('#catalog')
    await unmount(component)
    login(); component = mount(QuickJump, { target, props: { scopeReady: false, scopeKey: 'unconfirmed' } }); await tick()
    await choose('event', 'event-a'); await submit(); expect(window.location.hash).toBe('#catalog')
  })
})
