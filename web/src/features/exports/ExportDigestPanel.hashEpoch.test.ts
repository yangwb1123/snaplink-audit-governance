// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { authState, type AuthState } from '../../auth/store'
import Harness from './ExportDigestPanel.testHarness.svelte'

const ABC = 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad'
let component: ReturnType<typeof mount> | undefined
let target: HTMLDivElement

function session(token: string): AuthState {
  return { status: 'authenticated', error: null, session: {
    accessToken: token, idToken: 'fixture-id', expiresAt: Date.now() + 100000,
    subject: 'fixture-alice', displayName: 'fixture-alice', tenantId: 'tenant-a', roles: [],
    permissions: new Set(['audit:export:create']), platform: true,
  } }
}
function pendingHash() {
  let resolve!: (value: ArrayBuffer) => void
  let reject!: (cause: unknown) => void
  const promise = new Promise<ArrayBuffer>((done, fail) => { resolve = done; reject = fail })
  return { promise, resolve, reject }
}
function digest(): ArrayBuffer {
  return Uint8Array.from(ABC.match(/../g)!, (value) => Number.parseInt(value, 16)).buffer
}
async function settle(): Promise<void> { await tick(); await tick() }
function button(label: string): HTMLButtonElement {
  const result = [...target.querySelectorAll('button')].find((b) => b.textContent?.trim() === label)
  if (!result) throw new Error('missing fixture button')
  return result
}
async function startHash(): Promise<void> {
  const input = target.querySelector('input[type="file"]') as HTMLInputElement
  Object.defineProperty(input, 'files', { configurable: true, value: [new File(['abc'], 'private-fixture.jsonl')] })
  input.dispatchEvent(new Event('change', { bubbles: true }))
  await settle();button('核对本地文件').click();await settle()
}
function status(): string | undefined { return target.querySelector('[role="status"] strong')?.textContent?.trim() }

afterEach(async () => {
  if (component) await unmount(component)
  component = undefined
  target?.remove()
  authState.set({ status: 'anonymous', session: null, error: null })
  vi.restoreAllMocks()
})

describe('local digest auth epoch while actual hash is pending', () => {
  it.each([
    ['replace', 'resolve'], ['replace', 'reject'], ['in-place', 'resolve'], ['in-place', 'reject'],
  ] as const)('rejects an old hash after same-turn %s A-B-A with old %s', async (mode, finish) => {
    const original = session('fixture-token-a')
    authState.set(original)
    target = document.createElement('div');document.body.append(target)
    component = mount(Harness, { target });await settle()
    const old = pendingHash()
    const hash = vi.spyOn(globalThis.crypto.subtle, 'digest').mockImplementationOnce(() => old.promise)
    await startHash()
    await vi.waitFor(() => expect(hash).toHaveBeenCalledTimes(1))
    expect(status()).toBe('本地核对中')

    // No tick between transitions: a batching effect sees only the restored
    // original object, but the authentication lifecycle has changed twice.
    if (mode === 'replace') {
      authState.set(session('fixture-token-b'))
      authState.set(original)
    } else {
      original.session!.accessToken = 'fixture-token-b'
      authState.set(original)
      original.session!.accessToken = 'fixture-token-a'
      authState.set(original)
    }
    if (finish === 'resolve') old.resolve(digest())
    else old.reject(new Error('private fixture hash diagnostic'))
    await settle();await settle()

    expect(status()).toBe('尚未核对')
    expect(target.textContent).not.toContain('摘要一致')
    expect(target.textContent).not.toContain('摘要不一致')
    expect(target.textContent).not.toContain('private fixture hash diagnostic')
  })
})
