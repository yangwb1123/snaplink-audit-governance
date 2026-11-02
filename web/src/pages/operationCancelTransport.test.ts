// @vitest-environment happy-dom
import { get } from 'svelte/store'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mount, tick, unmount } from 'svelte'
import { authState, type AuthState } from '../auth/store'
import type { UserSession } from '../auth/types'
import type { AuditEvent, OperationSummary, QueryResult, ReplayResult } from '../api/types'
import { ApiError } from '../api/client'
import OperationsPage from './OperationsPage.svelte'

interface FetchJob {
  url: string
  init: RequestInit
  resolve: (response: Response) => void
  reject: (cause: unknown) => void
}

function session(token = 'token-a', tenantId = 'tenant-a'): UserSession {
  return {
    accessToken: token,
    idToken: 'id-token',
    expiresAt: Date.now() + 100_000,
    subject: 'actor',
    displayName: 'actor',
    tenantId,
    roles: [],
    permissions: new Set(['audit:event:read', 'audit:operation:read']),
    platform: true,
  }
}

function authenticated(current = session()): AuthState {
  return { status: 'authenticated', session: current, error: null }
}

// The transport deliberately ignores AbortSignal. Cancellation therefore has
// to be proven by both the recorded fetch signal and the page generation fence.
function controlledFetch() {
  const jobs: FetchJob[] = []
  const fetcher = vi.fn<typeof fetch>((input, init = {}) => new Promise<Response>((resolve, reject) => {
    const normalized = init ?? {}
    jobs.push({ url: String(input), init: normalized, resolve, reject })
  }))
  vi.stubGlobal('fetch', fetcher)
  return { fetcher, jobs }
}

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

function auditEvent(id: string): AuditEvent {
  return {
    event_id: id,
    tenant_id: 'tenant-a',
    source_system: 'aero-vault',
    event_type: 'file.downloaded',
    schema_id: 'audit.event',
    schema_version: 1,
    occurred_at: '2026-10-01T00:00:00Z',
    received_at: '2026-10-01T00:00:01Z',
    actor: { id: 'alice' },
    action: `step-${id}`,
    outcome: 'success',
    data_classification: 'internal',
    retention_class: 'standard',
  }
}

const summaryBody: OperationSummary = {
  operation_id: 'op-a',
  tenant_id: 'tenant-a',
  event_count: 1,
  first_at: '2026-10-01T00:00:00Z',
  last_at: '2026-10-01T00:00:00Z',
  outcomes: ['success'],
}

const replayBody: ReplayResult = {
  tenant_id: 'tenant-a',
  operation_id: 'op-a',
  event_count: 1,
  state: { status: 'done' },
  last_sequence: 1,
}

let target: HTMLDivElement
let component: ReturnType<typeof mount> | undefined

async function settle(): Promise<void> {
  await tick()
  await Promise.resolve()
  await tick()
  await Promise.resolve()
  await tick()
}

function mountOperations(): void {
  window.history.replaceState(null, '', '#operations/op/op-a')
  component = mount(OperationsPage, { target, props: { tenantId: 'tenant-a' } })
}

function jobsFor(jobs: FetchJob[], path: string): FetchJob[] {
  return jobs.filter(job => job.url.includes(path))
}

function jobAt(jobs: FetchJob[], path: string, index = 0): FetchJob {
  const found = jobsFor(jobs, path)[index]
  if (!found) throw new Error(`missing fetch job ${path}[${index}]`)
  return found
}

function card(section: 'summary' | 'timeline' | 'replay'): HTMLElement {
  const label = section === 'summary' ? '操作摘要' : section === 'timeline' ? '事件时间线' : '状态回放'
  const heading = [...target.querySelectorAll('h3')].find(node => node.textContent === label)
  if (!heading?.parentElement) throw new Error(`missing ${section} card`)
  return heading.parentElement
}

function button(label: string, scope: ParentNode = target): HTMLButtonElement {
  const found = [...scope.querySelectorAll('button')].find(node => node.textContent?.trim() === label)
  if (!found) throw new Error(`missing button ${label}`)
  return found as HTMLButtonElement
}

function enter(): void {
  target.querySelector('input')!.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }))
}

function displayedOrder(): string[] {
  const text = target.textContent ?? ''
  return ['one', 'two', 'three', 'four']
    .filter(id => text.includes(`step-${id}`))
    .sort((a, b) => text.indexOf(`step-${a}`) - text.indexOf(`step-${b}`))
}

beforeEach(() => {
  vi.clearAllMocks()
  authState.set(authenticated())
  target = document.createElement('div')
  document.body.append(target)
})

afterEach(async () => {
  if (component) await unmount(component)
  component = undefined
  target.remove()
  vi.unstubAllGlobals()
  authState.set({ status: 'anonymous', session: null, error: null })
  window.history.replaceState(null, '', '#')
  vi.restoreAllMocks()
})

describe('real operation transport cancellation', () => {
  it('cancels only summary, keeps siblings, and ignores its late 401 without retiring auth', async () => {
    const transport = controlledFetch()
    mountOperations()
    await settle()
    expect(transport.jobs).toHaveLength(3)
    expect(transport.jobs.every(job => job.init.method === undefined)).toBe(true)
    expect(transport.jobs.every(job => job.url.startsWith('/audit-api/api/v1/operations/op-a'))).toBe(true)

    const summaryJob = jobAt(transport.jobs, '/api/v1/operations/op-a?')
    const timelineJob = jobAt(transport.jobs, '/api/v1/operations/op-a/timeline')
    const replayJob = jobAt(transport.jobs, '/api/v1/operations/op-a/replay')
    const summarySignal = summaryJob.init.signal
    const timelineSignal = timelineJob.init.signal
    const replaySignal = replayJob.init.signal
    expect(summarySignal).toBeInstanceOf(AbortSignal)
    expect(timelineSignal).toBeInstanceOf(AbortSignal)
    expect(replaySignal).toBeInstanceOf(AbortSignal)
    expect(new Set([summarySignal, timelineSignal, replaySignal]).size).toBe(3)
    expect([summarySignal, timelineSignal, replaySignal].every(signal => signal?.aborted === false)).toBe(true)

    button('取消等待', card('summary')).click()
    await settle()
    expect(summarySignal?.aborted).toBe(true)
    expect(timelineSignal?.aborted).toBe(false)
    expect(replaySignal?.aborted).toBe(false)
    expect(card('summary').textContent).toContain('已取消等待')

    timelineJob.resolve(json({ items: [auditEvent('evt-1')], count: 1 }))
    replayJob.resolve(json(replayBody))
    await settle()
    expect(target.textContent).toContain('aero-vault · file.downloaded')
    expect(target.textContent).toContain('共回放 1 个事件')
    expect(card('summary').textContent).not.toContain('请求失败')

    summaryJob.resolve(json({ error: { code: 'unauthorized', request_id: 'late-request' } }, 401))
    await settle()
    expect(get(authState).status).toBe('authenticated')
    expect(get(authState).session?.accessToken).toBe('token-a')
    expect(card('summary').textContent).toContain('已取消等待')
    expect(target.textContent).not.toContain('late-request')

    button('重试', card('summary')).click()
    await settle()
    const retryJob = jobAt(transport.jobs, '/api/v1/operations/op-a?', 1)
    expect(retryJob.init.signal).toBeInstanceOf(AbortSignal)
    expect(retryJob.init.signal).not.toBe(summarySignal)
    expect(retryJob.init.signal?.aborted).toBe(false)
    retryJob.resolve(json(summaryBody))
    await settle()
    expect(card('summary').textContent).toContain('op-a')
    expect(card('summary').textContent).not.toContain('已取消等待')
    expect(target.textContent).toContain('aero-vault · file.downloaded')
    expect(get(authState).status).toBe('authenticated')
  })

  it('cancels a next page without losing loaded order, then retries the same cursor', async () => {
    const transport = controlledFetch()
    const firstPage: QueryResult = {
      items: [auditEvent('two'), auditEvent('one')],
      count: 3,
      next_cursor: 'cursor-1',
    }
    mountOperations()
    await settle()
    jobAt(transport.jobs, '/api/v1/operations/op-a?').resolve(json(summaryBody))
    jobAt(transport.jobs, '/api/v1/operations/op-a/timeline').resolve(json(firstPage))
    jobAt(transport.jobs, '/api/v1/operations/op-a/replay').resolve(json(replayBody))
    await settle()
    expect(displayedOrder()).toEqual(['two', 'one'])
    expect(target.textContent).toContain('已显示 2 / 共 3')

    button('加载下一页').click()
    await settle()
    const nextJob = jobAt(transport.jobs, '/api/v1/operations/op-a/timeline', 1)
    expect(nextJob.url).toContain('cursor=cursor-1')
    expect(nextJob.init.signal).toBeInstanceOf(AbortSignal)
    expect(nextJob.init.signal?.aborted).toBe(false)

    button('取消加载下一页').click()
    await settle()
    expect(nextJob.init.signal?.aborted).toBe(true)
    expect(displayedOrder()).toEqual(['two', 'one'])
    expect(target.textContent).toContain('已显示 2 / 共 3')
    expect(target.textContent).toContain('已取消下一次分页读取')
    expect(button('加载下一页').disabled).toBe(false)

    nextJob.resolve(json({ items: [auditEvent('obsolete')], count: 987654, next_cursor: 'obsolete-cursor' }))
    await settle()
    expect(displayedOrder()).toEqual(['two', 'one'])
    expect(target.textContent).not.toContain('obsolete')
    expect(target.textContent).toContain('已显示 2 / 共 3')

    button('加载下一页').click()
    await settle()
    const retryJob = jobAt(transport.jobs, '/api/v1/operations/op-a/timeline', 2)
    expect(retryJob.url).toContain('cursor=cursor-1')
    expect(retryJob.init.signal).not.toBe(nextJob.init.signal)
    expect(retryJob.init.signal?.aborted).toBe(false)
    retryJob.resolve(json({ items: [auditEvent('four'), auditEvent('three')], count: 4 }))
    await settle()
    expect(displayedOrder()).toEqual(['two', 'one', 'four', 'three'])
    expect(target.textContent).toContain('已显示 4 / 共 4')
    expect(jobsFor(transport.jobs, '/api/v1/operations/op-a?')).toHaveLength(1)
    expect(jobsFor(transport.jobs, '/api/v1/operations/op-a/replay')).toHaveLength(1)
  })

  it('fences a synchronous auth A → B → original A batch and its ignored-abort 401', async () => {
    const transport = controlledFetch()
    const original = authenticated(session('token-a'))
    authState.set(original)
    mountOperations()
    await settle()
    expect(transport.jobs).toHaveLength(3)
    const initialSummary = jobAt(transport.jobs, '/api/v1/operations/op-a?', 0)
    const initialTimeline = jobAt(transport.jobs, '/api/v1/operations/op-a/timeline', 0)
    const initialReplay = jobAt(transport.jobs, '/api/v1/operations/op-a/replay', 0)

    authState.set(authenticated(session('token-b', 'tenant-b')))
    // Only an explicit user query starts reads for each changed context.
    expect(transport.jobs).toHaveLength(3)
    enter()
    authState.set(original)
    expect(transport.jobs).toHaveLength(6)
    enter()
    await settle()

    expect(transport.jobs).toHaveLength(9)
    const bSummary = jobAt(transport.jobs, '/api/v1/operations/op-a?', 1)
    const aSummary = jobAt(transport.jobs, '/api/v1/operations/op-a?', 2)
    const aTimeline = jobAt(transport.jobs, '/api/v1/operations/op-a/timeline', 2)
    const aReplay = jobAt(transport.jobs, '/api/v1/operations/op-a/replay', 2)
    expect(initialSummary.init.signal?.aborted).toBe(true)
    expect(bSummary.init.signal?.aborted).toBe(true)
    expect(aSummary.init.signal).toBeInstanceOf(AbortSignal)
    expect(aSummary.init.signal?.aborted).toBe(false)
    expect(aTimeline.init.signal?.aborted).toBe(false)
    expect(aReplay.init.signal?.aborted).toBe(false)

    initialSummary.resolve(json({ error: { code: 'unauthorized', request_id: 'old-request' } }, 401))
    bSummary.resolve(json({ error: { code: 'unauthorized', request_id: 'b-request' } }, 401))
    initialTimeline.resolve(json({ items: [auditEvent('obsolete')], count: 987654 }))
    initialReplay.resolve(json({ ...replayBody, event_count: 987654 }))
    await settle()
    expect(get(authState)).toBe(original)
    expect(get(authState).status).toBe('authenticated')
    expect(target.textContent).not.toContain('old-request')
    expect(target.textContent).not.toContain('obsolete')
    expect(target.textContent).not.toContain('987654')

    aSummary.resolve(json(summaryBody))
    aTimeline.resolve(json({ items: [auditEvent('evt-1')], count: 1 }))
    aReplay.resolve(json(replayBody))
    await settle()
    expect(target.textContent).toContain('aero-vault · file.downloaded')
    expect(target.textContent).toContain('共回放 1 个事件')
    expect(target.textContent).not.toContain('obsolete')
    expect(target.textContent).not.toContain('987654')
    expect(get(authState)).toBe(original)
  })

  it('aborts every transport on unmount, unsubscribes auth, and ignores a late 401', async () => {
    const transport = controlledFetch()
    const originalSubscribe = authState.subscribe
    let active = 0
    const subscription = vi.spyOn(authState, 'subscribe').mockImplementation((run, invalidate) => {
      active += 1
      const stop = originalSubscribe(run, invalidate)
      return () => {
        active -= 1
        stop()
      }
    })
    try {
      mountOperations()
      await settle()
      expect(transport.jobs).toHaveLength(3)
      expect(active).toBe(1)
      const jobs = [...transport.jobs]
      expect(jobs.every(job => job.init.signal instanceof AbortSignal)).toBe(true)
      expect(jobs.every(job => job.init.signal?.aborted === false)).toBe(true)

      const mounted = component
      if (!mounted) throw new Error('missing mounted component')
      component = undefined
      await unmount(mounted)

      expect(jobs.every(job => job.init.signal?.aborted)).toBe(true)
      expect(active).toBe(0)

      jobs[0].resolve(json({ error: { code: 'unauthorized', request_id: 'after-unmount' } }, 401))
      await settle()
      expect(get(authState).status).toBe('authenticated')
      expect(target.childElementCount).toBe(0)
    } finally {
      subscription.mockRestore()
    }
  })
})

describe('real transport failure contract', () => {
  it('keeps the ApiError rejection for a cancelled read without publishing an error', async () => {
    const transport = controlledFetch()
    mountOperations()
    await settle()
    const summaryJob = jobAt(transport.jobs, '/api/v1/operations/op-a?')
    button('取消等待', card('summary')).click()
    await settle()
    summaryJob.reject(new ApiError(401, 'unauthorized', 'private diagnostic', 'late-request'))
    await settle()
    expect(target.textContent).not.toContain('private diagnostic')
    expect(target.textContent).not.toContain('late-request')
    expect(card('summary').textContent).toContain('已取消等待')
    expect(get(authState).status).toBe('authenticated')
  })
})
