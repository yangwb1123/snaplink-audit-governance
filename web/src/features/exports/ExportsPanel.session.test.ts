// @vitest-environment happy-dom
import { get } from 'svelte/store'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { authState, type AuthState } from '../../auth/store'
import type { ExportJob } from '../../api/types'
import Harness from './ExportsPanel.session.testHarness.svelte'

// No adapter/auth mocks: ExportsPanel -> useAuditApi -> AuditApi -> ApiClient -> fetch.
type Path = 'JSON detail' | 'blob download'
const paths: Path[] = ['JSON detail', 'blob download']
let target: HTMLDivElement
let component: ReturnType<typeof mount> | undefined

function authenticated(): AuthState {
  return { status: 'authenticated', error: null, session: {
    subject: 'actor', displayName: 'Actor', accessToken: 'fixture-access', idToken: 'fixture-id',
    expiresAt: Date.now() + 100000, tenantId: 'tenant-a', platform: false, roles: [],
    permissions: new Set(['audit:export:create']),
  } }
}

function completed(id: string, fresh = false): ExportJob {
  return { id, tenant_id: 'tenant-a', requested_by: 'actor', status: 'completed',
    created_at: '2026-10-22T00:00:00Z', event_count: fresh ? 23 : 17,
    query: { from: '2026-10-21T00:00:00Z', to: '2026-10-22T00:00:00Z' },
    digest: fresh ? 'current-evidence' : 'previous-evidence' }
}

function controlledFetch(path: Path) {
  let resolve!: (response: Response) => void
  let reject!: (cause: unknown) => void
  const delayed = new Promise<Response>((done, fail) => { resolve = done; reject = fail })
  let pending = true
  let signal: AbortSignal | null | undefined
  const calls: Array<{ method: string; path: string; tenant: string | null; credentialMatches: boolean }> = []
  const fetcher = vi.fn<typeof fetch>(async (input, init) => {
    const url = new URL(String(input), window.location.origin)
    const pathname = url.pathname.replace(/^\/audit-api/, '')
    calls.push({ method: init?.method ?? 'GET', path: pathname, tenant: url.searchParams.get('tenant_id'),
      credentialMatches: new Headers(init?.headers).get('Authorization') === `Bearer ${get(authState).session?.accessToken}` })
    const delayedPath = `/api/v1/exports/export-a${path === 'blob download' ? '/download' : ''}`
    if (pending && pathname === delayedPath) {
      pending = false
      signal = init?.signal
      // Return even after cancellation; never subscribe to signal or reject on abort.
      return delayed
    }
    if (pathname === '/api/v1/exports') {
      return Response.json({ items: [{ id: 'export-b', tenant_id: 'tenant-a', requested_by: 'actor',
        status: 'completed', created_at: '2026-10-22T00:00:00Z' }], count: 1 })
    }
    if (pathname === '/api/v1/exports/export-a') return Response.json(completed('export-a'))
    if (pathname === '/api/v1/exports/export-b') return Response.json(completed('export-b', true))
    throw new Error('Unexpected fixture request')
  })
  vi.stubGlobal('fetch', fetcher)
  return { calls, resolve, reject, get signal() { return signal } }
}

async function settle(): Promise<void> { await tick(); await tick() }
function button(label: string): HTMLButtonElement {
  const node = [...target.querySelectorAll('button')].find((item) => item.textContent?.trim() === label)
  if (!node) throw new Error(`missing button ${label}`)
  return node
}
function detail(): Element | undefined { return [...target.querySelectorAll('.section-card')].at(-1) }
function hasEvidence(value: string): boolean { return detail()?.textContent?.includes(value) ?? false }

async function render(path: Path): Promise<ReturnType<typeof controlledFetch>> {
  const transport = controlledFetch(path)
  component = mount(Harness, { target })
  await settle()
  if (path === 'blob download') {
    await vi.waitFor(() => expect(hasEvidence('previous-evidence')).toBe(true))
    button('下载 JSONL').click()
    await settle()
  }
  expect(transport.signal instanceof AbortSignal).toBe(true)
  expect(transport.signal?.aborted).toBe(false)
  return transport
}

function expectNoDownload(): void {
  expect(URL.createObjectURL).not.toHaveBeenCalled()
  expect(URL.revokeObjectURL).not.toHaveBeenCalled()
  expect(HTMLAnchorElement.prototype.click).not.toHaveBeenCalled()
}

beforeEach(() => {
  authState.set(authenticated())
  vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:fixture-export')
  vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => {})
  vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {})
  target = document.createElement('div')
  document.body.append(target)
})
afterEach(async () => {
  if (component) await unmount(component)
  component = undefined
  target.remove()
  authState.set({ status: 'anonymous', session: null, error: null })
  vi.restoreAllMocks()
  vi.unstubAllGlobals()
})

describe.each(paths)('real ExportsPanel %s session failures', (path) => {
  for (const timing of ['same turn', 'separate turns'] as const) {
    it.each(['equal values', 'original objects'] as const)(`an ignored-abort old 401 cannot retire new evidence after ${timing} ABA (%s)`, async (restoration) => {
      const original = get(authState)
      const transport = await render(path)
      const location = window.location.href
      const storage = ['getItem', 'setItem', 'removeItem'].map((method) => vi.spyOn(Storage.prototype, method as 'setItem'))
      const logs = ['log', 'info', 'warn', 'error', 'debug'].map((method) => vi.spyOn(console, method as 'log'))
      const changed: AuthState = { ...original, session: { ...original.session!, accessToken: 'fixture-other', subject: 'other-actor' } }
      authState.set(changed)
      if (timing === 'separate turns') await settle()
      authState.set(restoration === 'original objects' ? original : { ...original, session: { ...original.session! } })
      // Same-turn auth changes deliberately have no tick between A -> B -> A.
      button('任务 B').click()
      await settle()
      await vi.waitFor(() => expect(hasEvidence('current-evidence')).toBe(true))
      expect(transport.signal?.aborted).toBe(true)
      const current = get(authState)
      const currentDetail = detail()?.textContent
      const currentList = target.querySelector('.native-table')?.textContent
      const requestCount = transport.calls.length
      const response = Response.json({ error: { code: 'unauthorized' } }, { status: 401 })
      transport.resolve(response)
      // bodyUsed establishes that the real client's pre-toError callback has run.
      await vi.waitFor(() => expect(response.bodyUsed).toBe(true))
      await settle()
      expect(get(authState).status).toBe('authenticated')
      expect(get(authState) === current).toBe(true)
      expect(hasEvidence('current-evidence')).toBe(true)
      expect(hasEvidence('previous-evidence')).toBe(false)
      expect(detail()?.textContent === currentDetail).toBe(true)
      expect(target.querySelector('.native-table')?.textContent === currentList).toBe(true)
      expect(target.querySelector<HTMLInputElement>('input[placeholder="export job id"]')?.value).toBe('export-b')
      expect(button('下载 JSONL').disabled).toBe(false)
      expectNoDownload()
      expect(transport.calls.length).toBe(requestCount)
      expect(transport.calls.every((call) => call.method === 'GET' && call.tenant === 'tenant-a' && call.credentialMatches)).toBe(true)
      expect(transport.calls.filter((call) => call.path.endsWith('/download')).length).toBe(path === 'blob download' ? 1 : 0)
      expect(window.location.href === location).toBe(true)
      // Counts only: a failing privacy assertion must not print sensitive call arguments.
      for (const spy of [...storage, ...logs]) expect(spy.mock.calls.length).toBe(0)
    })
  }

  it('still invalidates the current generation on 401 through the actual callback', async () => {
    const transport = await render(path)
    const response = Response.json({ error: { code: 'unauthorized' } }, { status: 401 })
    transport.resolve(response)
    await vi.waitFor(() => expect(response.bodyUsed).toBe(true))
    await settle()
    expect(get(authState).status).toBe('anonymous')
    expect(get(authState).session === null).toBe(true)
    expect(target.textContent?.includes('当前会话不可用')).toBe(true)
    expect(hasEvidence('previous-evidence')).toBe(false)
    expectNoDownload()
  })

  it.each(['403', 'network'] as const)('keeps the session on a current %s and does not download or retry', async (failure) => {
    const original = get(authState)
    const transport = await render(path)
    const requestCount = transport.calls.length
    if (failure === '403') {
      const response = Response.json({ error: { code: 'forbidden' } }, { status: 403 })
      transport.resolve(response)
      await vi.waitFor(() => expect(response.bodyUsed).toBe(true))
    } else transport.reject(new TypeError('fixture network failure'))
    await settle()
    expect(get(authState) === original).toBe(true)
    expect(transport.calls.length).toBe(requestCount)
    expectNoDownload()
  })
})
