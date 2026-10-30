// @vitest-environment happy-dom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { MockInstance } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { authState } from '../../auth/store'
import type { ExportJob } from '../../api/types'
import ExportsPanelHarness from './ExportsPanel.testHarness.svelte'
import Harness from './ExportDigestPanel.testHarness.svelte'

const { api } = vi.hoisted(() => ({ api: { getExport: vi.fn(), listExports: vi.fn(), downloadExport: vi.fn() } }))
vi.mock('../../api/useAuditApi', () => ({ useAuditApi: () => api }))

let component: ReturnType<typeof mount> | undefined
let target: HTMLDivElement
let digestSpy: MockInstance

function login(token = 'current-token', tenantId = 'tenant-a'): void {
  authState.set({
    status: 'authenticated',
    session: {
      accessToken: token,
      idToken: 'id',
      expiresAt: Date.now() + 100000,
      subject: 'alice',
      displayName: 'alice',
      tenantId,
      roles: [],
      permissions: new Set(['audit:export:create']),
      platform: true,
    },
    error: null,
  })
}

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (cause: unknown) => void
  const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail })
  return { promise, resolve, reject }
}

function textBytes(value: string): ArrayBuffer {
  return new TextEncoder().encode(value).buffer as ArrayBuffer
}

function delayedFile(value: string): { file: File; pending: ReturnType<typeof deferred<ArrayBuffer>> } {
  const file = new File([value], 'private-local-name.jsonl')
  const pending = deferred<ArrayBuffer>()
  vi.spyOn(file, 'slice').mockImplementation(() =>
    ({ arrayBuffer: () => pending.promise } as unknown as Blob))
  return { file, pending }
}

async function settle(): Promise<void> {
  await tick()
  await tick()
}

async function waitForDigest(spy: MockInstance, before: number): Promise<void> {
  await vi.waitFor(() => expect(spy.mock.calls.length).toBeGreaterThan(before))
  const result = spy.mock.results.at(-1)?.value
  if (result instanceof Promise) await result
  await settle()
}

function button(label: string): HTMLButtonElement {
  const found = [...target.querySelectorAll('button')].find((node) => node.textContent?.trim() === label)
  if (!found) throw new Error(`missing button ${label}`)
  return found
}

function hasButton(label: string): boolean {
  return [...target.querySelectorAll('button')].some((node) => node.textContent?.trim() === label)
}

function fileInput(): HTMLInputElement {
  const found = target.querySelector<HTMLInputElement>('input[type="file"]')
  if (!found) throw new Error('missing file input')
  return found
}

function hasFileInput(): boolean {
  return target.querySelector('input[type="file"]') !== null
}

function statusLabel(): string | null {
  const found = target.querySelector('[role="status"] strong')
  return found?.textContent?.trim() ?? null
}

function hasStatus(): boolean {
  return target.querySelector('[role="status"]') !== null
}

async function choose(file: File): Promise<void> {
  const input = fileInput()
  Object.defineProperty(input, 'files', { configurable: true, value: [file] })
  input.dispatchEvent(new Event('change', { bubbles: true }))
  await settle()
}

async function click(label: string): Promise<void> {
  button(label).click()
  await settle()
}

async function render(): Promise<void> {
  component = mount(Harness, { target })
  await settle()
}

beforeEach(() => {
  vi.resetAllMocks()
  login()
  digestSpy = vi.spyOn(globalThis.crypto.subtle, 'digest')
  target = document.createElement('div')
  document.body.append(target)
})

afterEach(async () => {
  if (component) await unmount(component)
  component = undefined
  target.remove()
  authState.set({ status: 'anonymous', session: null, error: null })
  vi.restoreAllMocks()
})

describe('export local digest verification panel', () => {
  it('hashes the selected raw bytes locally, reports a match, and never uploads or persists local details', async () => {
    const fetcher = vi.fn()
    vi.stubGlobal('fetch', fetcher)
    const storage = ['getItem', 'setItem', 'removeItem'].map((method) =>
      vi.spyOn(Storage.prototype, method as 'setItem'))
    const pushState = vi.spyOn(history, 'pushState')
    const logs = ['log', 'info', 'warn', 'error', 'debug'].map((method) =>
      vi.spyOn(console, method as 'log'))

    await render()
    expect(statusLabel()).toBe('尚未核对')
    expect(target.textContent).toContain('32 MiB')
    expect(target.textContent).toContain('不是内容来源、签名、全链、所有业务事实完整性或当前 Hold/下载授权证明')
    expect(target.textContent).not.toContain('完整性验证通过')

    await choose(new File(['abc'], 'private-local-name.jsonl'))
    expect(statusLabel()).toBe('尚未核对')
    expect(target.textContent).not.toContain('private-local-name.jsonl')
    await click('核对本地文件')
    await vi.waitFor(() => expect(statusLabel()).toBe('摘要一致'))

    expect(digestSpy).toHaveBeenCalledTimes(1)
    expect(fetcher).not.toHaveBeenCalled()
    expect(storage.every((spy) => spy.mock.calls.length === 0)).toBe(true)
    expect(pushState).not.toHaveBeenCalled()
    expect(logs.every((spy) => spy.mock.calls.length === 0)).toBe(true)
    expect(target.textContent).not.toContain('private-local-name.jsonl')
    expect(target.textContent).not.toContain('完整性验证通过')
    vi.unstubAllGlobals()
  })

  it('reports a mismatch without rewriting newline bytes', async () => {
    await render()
    await choose(new File(['abc\n'], 'private-local-name.jsonl'))
    await click('核对本地文件')
    await vi.waitFor(() => expect(statusLabel()).toBe('摘要不一致'))
    expect(target.textContent).toContain('不一致')
    expect(target.textContent).not.toContain('摘要一致')
    expect(target.textContent).not.toContain('完整性验证通过')
  })

  it.each(['missing', 'invalid'] as const)('shows an explicit unavailable state for a %s digest without reading a file', async (kind) => {
    await render()
    await click(kind === 'missing' ? '摘要缺失' : '摘要无效')
    expect(statusLabel()).toBe('无法核对')
    expect(fileInput().disabled).toBe(true)
    expect(button('核对本地文件').disabled).toBe(true)
    if (kind === 'missing') expect(target.textContent).toContain('没有可核对的 SHA-256 摘要')
    else expect(target.textContent).toContain('不是有效的 64 位 SHA-256 十六进制值')

    const tracked = new File(['abc'], 'private-local-name.jsonl')
    const slice = vi.spyOn(tracked, 'slice')
    await choose(tracked)
    expect(slice).not.toHaveBeenCalled()
    expect(statusLabel()).toBe('无法核对')
  })

  it('renders only for a completed task in the current session, valid tenant and ready scope', async () => {
    await render()
    expect(hasFileInput()).toBe(true)

    await click('范围不可用')
    expect(hasFileInput()).toBe(false)
    expect(hasStatus()).toBe(false)

    await click('范围可用')
    expect(hasFileInput()).toBe(true)

    await click('租户 B')
    expect(hasFileInput()).toBe(true)
    await click('租户 A')

    await click('运行中')
    expect(hasFileInput()).toBe(false)
    expect(hasStatus()).toBe(false)

    login('other-token')
    await settle()
    expect(hasFileInput()).toBe(false)
    expect(hasStatus()).toBe(false)
  })

  it.each(['success', 'failure'] as const)('cancel abandons only the local wait and ignores an old read %s including its finish', async (outcome) => {
    await render()
    const old = delayedFile('abc')
    await choose(old.file)
    await click('核对本地文件')
    expect(statusLabel()).toBe('本地核对中')
    expect(hasButton('取消本地等待')).toBe(true)

    await click('取消本地等待')
    expect(statusLabel()).toBe('已取消本地等待')
    expect(hasButton('取消本地等待')).toBe(false)

    const before = digestSpy.mock.calls.length
    if (outcome === 'success') old.pending.resolve(textBytes('abc'))
    else old.pending.reject(new Error('private local read failure'))
    if (outcome === 'success') await waitForDigest(digestSpy, before)
    else await settle()

    expect(statusLabel()).toBe('已取消本地等待')
    expect(hasButton('取消本地等待')).toBe(false)
    expect(target.textContent).not.toContain('摘要一致')
    expect(target.textContent).not.toContain('摘要不一致')
    expect(target.textContent).not.toContain('private local read failure')
  })

  it('does not let an old file finish clear or overwrite a newer file check', async () => {
    await render()
    const first = delayedFile('abc\n')
    const second = delayedFile('abc')
    await choose(first.file)
    await click('核对本地文件')
    expect(statusLabel()).toBe('本地核对中')

    await choose(second.file)
    expect(statusLabel()).toBe('尚未核对')
    await click('核对本地文件')
    expect(statusLabel()).toBe('本地核对中')

    const before = digestSpy.mock.calls.length
    first.pending.resolve(textBytes('abc\n'))
    await waitForDigest(digestSpy, before)
    expect(statusLabel()).toBe('本地核对中')
    expect(hasButton('取消本地等待')).toBe(true)

    second.pending.resolve(textBytes('abc'))
    await vi.waitFor(() => expect(statusLabel()).toBe('摘要一致'))
    expect(statusLabel()).not.toBe('摘要不一致')
  })

  const transitions = ['task', 'digest', 'tenant', 'session', 'missing', 'scope', 'unmount'] as const
  for (const transition of transitions) {
    it.each(['success', 'failure'] as const)(`isolates an old read %s across ${transition} changes`, async (outcome) => {
      await render()
      const old = delayedFile('abc')
      await choose(old.file)
      await click('核对本地文件')
      expect(statusLabel()).toBe('本地核对中')

      if (transition === 'task') await click('任务 B')
      else if (transition === 'digest') await click('摘要变更')
      else if (transition === 'tenant') await click('租户 B')
      else if (transition === 'session') { login('next-token'); await settle() }
      else if (transition === 'missing') await click('摘要缺失')
      else if (transition === 'scope') await click('范围不可用')
      else { await unmount(component!); component = undefined }

      const before = digestSpy.mock.calls.length
      if (outcome === 'success') old.pending.resolve(textBytes('abc'))
      else old.pending.reject(new Error('private local read failure'))
      if (outcome === 'success') await waitForDigest(digestSpy, before)
      else await settle()

      if (transition === 'unmount') {
        expect(target.textContent).toBe('')
      } else if (transition === 'missing') {
        expect(statusLabel()).toBe('无法核对')
        expect(target.textContent).not.toContain('摘要一致')
      } else if (transition === 'scope') {
        expect(hasStatus()).toBe(false)
        expect(hasFileInput()).toBe(false)
      } else {
        expect(statusLabel()).toBe('尚未核对')
        expect(target.textContent).not.toContain('摘要一致')
        expect(target.textContent).not.toContain('摘要不一致')
      }
      expect(target.textContent).not.toContain('private local read failure')
    })
  }

  it('ignores a late old hash success or failure after cancellation', async () => {
    await render()
    const old = delayedFile('abc')
    await choose(old.file)
    await click('核对本地文件')
    await click('取消本地等待')

    digestSpy.mockResolvedValueOnce(new Uint8Array(32).buffer)
    old.pending.resolve(textBytes('abc'))
    await waitForDigest(digestSpy, 0)
    expect(statusLabel()).toBe('已取消本地等待')
    expect(target.textContent).not.toContain('摘要一致')
    expect(target.textContent).not.toContain('摘要不一致')
    expect(target.textContent).not.toContain('Error')
  })

  it('is mounted only in the validated completed export detail and leaves the download control and API behavior unchanged', async () => {
    const job: ExportJob = {
      id: 'export-a',
      tenant_id: 'tenant-a',
      requested_by: 'alice',
      status: 'completed',
      created_at: '2026-10-14T00:00:00Z',
      query: { from: '2026-10-13T00:00:00Z', to: '2026-10-14T00:00:00Z' },
      event_count: 17,
      digest: 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad',
    }
    api.getExport.mockResolvedValue(job)
    api.listExports.mockResolvedValue({ items: [], count: 0 })
    component = mount(ExportsPanelHarness, { target })
    await settle()
    await vi.waitFor(() => expect(target.textContent).toContain('内容摘要'))

    expect(target.querySelector('.digest-card')).not.toBeNull()
    expect(hasFileInput()).toBe(true)
    expect(button('下载 JSONL').disabled).toBe(false)
    expect(api.downloadExport).not.toHaveBeenCalled()

    await choose(new File(['abc'], 'private-local-name.jsonl'))
    await click('核对本地文件')
    await vi.waitFor(() => expect(statusLabel()).toBe('摘要一致'))
    expect(api.downloadExport).not.toHaveBeenCalled()
  })
})
