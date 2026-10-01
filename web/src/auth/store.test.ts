// @vitest-environment happy-dom
import { get } from 'svelte/store'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { useAuditApi } from '../api/useAuditApi'
import { completeAuthorization } from './oauth'
import { authState, bootstrapAuth, invalidate, logout } from './store'
import type { DiscoveryDocument, UserSession } from './types'

vi.mock('./oauth', async (importOriginal) => ({
  ...(await importOriginal<typeof import('./oauth')>()),
  completeAuthorization: vi.fn(),
}))

const now = new Date('2026-08-23T00:00:00.000Z')
const maxTimeoutMs = 2_147_483_647
const expiredMessage = 'Snaplink 会话已过期，请重新登录'
const rejectedMessage = 'Audit API 拒绝了当前会话，请重新登录'
// Private by design: renaming the key invalidates every in-flight login, so a
// test that fails on the rename is the intended regression signal.
const pendingKey = 'audit-governance.snaplink.pending.v1'
const issuer = 'http://localhost:18082'

function session(expiresAt: number, overrides: Partial<UserSession> = {}): UserSession {
  return {
    accessToken: 'access-token',
    idToken: 'id-token',
    expiresAt,
    subject: 'reviewer',
    displayName: '审核员',
    tenantId: 'tenant-a',
    roles: [],
    permissions: new Set<string>(),
    platform: false,
    ...overrides,
  }
}

function enterCallback(): void {
  history.replaceState(null, '', '/auth/callback?code=abc&state=state-1')
}

async function bootstrapWith(
  expiresAt: number,
  overrides: Partial<UserSession> = {},
): Promise<UserSession> {
  const value = session(expiresAt, overrides)
  vi.mocked(completeAuthorization).mockResolvedValue(value)
  enterCallback()
  await bootstrapAuth()
  return value
}

function discovery(extra: Partial<DiscoveryDocument> = {}): DiscoveryDocument {
  return {
    issuer,
    authorization_endpoint: `${issuer}/oauth/authorize`,
    token_endpoint: `${issuer}/oauth/token`,
    jwks_uri: `${issuer}/jwks`,
    code_challenge_methods_supported: ['S256'],
    token_endpoint_auth_methods_supported: ['none'],
    ...extra,
  }
}

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

beforeEach(() => {
  vi.useFakeTimers()
  vi.setSystemTime(now)
  vi.mocked(completeAuthorization).mockReset()
  // A leaked runtime override would move redirectUri and silently flip
  // bootstrapAuth to its anonymous early-return branch.
  delete window.__AUDIT_GOVERNANCE_CONFIG__
  history.replaceState(null, '', '/')
  sessionStorage.clear()
})

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
  // authState, callbackPromise and expiryTimer are module singletons; only the
  // store itself can clear the pending timer between cases.
  invalidate()
  history.replaceState(null, '', '/')
  sessionStorage.clear()
  delete window.__AUDIT_GOVERNANCE_CONFIG__
})

describe('setSession expiry scheduling', () => {
  it('fires exactly at the session lifetime and falls back to anonymous', async () => {
    const authenticated = await bootstrapWith(now.getTime() + 5_000)

    expect(get(authState)).toEqual({ status: 'authenticated', session: authenticated, error: null })
    expect(vi.getTimerCount()).toBe(1)

    await vi.advanceTimersByTimeAsync(4_999)
    expect(get(authState)).toEqual({ status: 'authenticated', session: authenticated, error: null })

    await vi.advanceTimersByTimeAsync(1)
    expect(get(authState)).toEqual({
      status: 'anonymous',
      session: null,
      error: expiredMessage,
    })
  })

  it('clamps an expiry beyond the browser setTimeout maximum', async () => {
    const authenticated = await bootstrapWith(now.getTime() + 10 * maxTimeoutMs)

    expect(vi.getTimerCount()).toBe(1)
    await vi.advanceTimersByTimeAsync(maxTimeoutMs - 1)
    expect(get(authState)).toEqual({ status: 'authenticated', session: authenticated, error: null })

    await vi.advanceTimersByTimeAsync(1)
    expect(get(authState)).toEqual({ status: 'anonymous', session: null, error: expiredMessage })
  })

  it('never schedules a timer for a session that already expired', async () => {
    await bootstrapWith(now.getTime())

    expect(get(authState)).toEqual({ status: 'anonymous', session: null, error: expiredMessage })
    expect(vi.getTimerCount()).toBe(0)
  })

  it('clears the session and the expiry timer when the audit API rejects the token', async () => {
    await bootstrapWith(now.getTime() + 600_000)
    expect(vi.getTimerCount()).toBe(1)
    const fetcher = vi.fn<typeof fetch>(
      async () =>
        jsonResponse(401, {
          error: { code: 'unauthorized', message: '会话已过期', request_id: 'req-9' },
        }),
    )
    // ApiClient binds the global fetch at construction time.
    vi.stubGlobal('fetch', fetcher)
    const api = useAuditApi()

    await expect(api.getEvent('event-1')).rejects.toMatchObject({
      status: 401,
      code: 'unauthorized',
      requestId: 'req-9',
    })
    expect(get(authState)).toEqual({ status: 'anonymous', session: null, error: rejectedMessage })
    expect(vi.getTimerCount()).toBe(0)

    // The stale timer must have been cleared, not merely overwritten.
    await vi.advanceTimersByTimeAsync(600_000)
    expect(get(authState)).toEqual({ status: 'anonymous', session: null, error: rejectedMessage })
  })
})

describe('bootstrapAuth callback handling', () => {
  it('scrubs the callback code after a successful login', async () => {
    const authenticated = await bootstrapWith(now.getTime() + 60_000)

    expect(get(authState)).toEqual({ status: 'authenticated', session: authenticated, error: null })
    expect(window.location.pathname).toBe('/')
    expect(window.location.search).toBe('')
    expect(window.location.hash).toBe('#overview')
  })

  it('scrubs the callback code after a rejected login', async () => {
    vi.mocked(completeAuthorization).mockRejectedValue(
      new Error('Snaplink 登录状态不存在或已使用'),
    )
    enterCallback()

    await bootstrapAuth()

    expect(get(authState)).toEqual({
      status: 'error',
      session: null,
      error: 'Snaplink 登录状态不存在或已使用',
    })
    expect(window.location.pathname).toBe('/')
    expect(window.location.search).toBe('')
  })

  it('stays anonymous when the console is not entered through the callback route', async () => {
    history.replaceState(null, '', '/events')

    await bootstrapAuth()

    expect(get(authState)).toEqual({ status: 'anonymous', session: null, error: null })
    expect(completeAuthorization).not.toHaveBeenCalled()
    expect(vi.getTimerCount()).toBe(0)
  })

  it('consumes the pending authorization once and refuses a replayed callback', async () => {
    const actual = await vi.importActual<typeof import('./oauth')>('./oauth')
    vi.mocked(completeAuthorization).mockImplementation(actual.completeAuthorization)
    sessionStorage.setItem(
      pendingKey,
      JSON.stringify({
        state: 'state-1',
        nonce: 'nonce-1',
        verifier: 'verifier-1',
        redirectUri: `${window.location.origin}/auth/callback`,
        issuer,
        createdAt: Date.now(),
      }),
    )
    const fetcher = vi.fn<typeof fetch>(async (input) =>
      String(input).includes('.well-known/openid-configuration')
        ? jsonResponse(200, discovery())
        : jsonResponse(400, { error: 'invalid_grant' }),
    )
    vi.stubGlobal('fetch', fetcher)
    enterCallback()

    await bootstrapAuth()

    expect(sessionStorage.getItem(pendingKey)).toBeNull()
    expect(get(authState)).toEqual({
      status: 'error',
      session: null,
      error: 'Snaplink code 交换失败 (400)',
    })
    expect(fetcher).toHaveBeenCalledTimes(2)
    expect(String(fetcher.mock.calls[1]?.[0])).toContain('/snaplink-api/oauth/token')

    // Replaying the callback URL is a hard failure, never a second session.
    enterCallback()
    await bootstrapAuth()

    expect(get(authState)).toEqual({
      status: 'error',
      session: null,
      error: 'Snaplink 登录状态不存在或已使用',
    })
    expect(fetcher).toHaveBeenCalledTimes(2)
  })

  it('exchanges the callback code once for concurrent bootstrap calls', async () => {
    const authenticated = session(now.getTime() + 60_000)
    let release: (value: UserSession) => void = () => undefined
    vi.mocked(completeAuthorization).mockImplementation(
      () =>
        new Promise<UserSession>((resolve) => {
          release = resolve
        }),
    )
    enterCallback()

    const first = bootstrapAuth()
    const second = bootstrapAuth()
    expect(completeAuthorization).toHaveBeenCalledTimes(1)

    release(authenticated)
    await Promise.all([first, second])

    expect(get(authState)).toEqual({ status: 'authenticated', session: authenticated, error: null })
    expect(vi.getTimerCount()).toBe(1)

    vi.mocked(completeAuthorization).mockResolvedValue(authenticated)
    enterCallback()
    await bootstrapAuth()

    expect(completeAuthorization).toHaveBeenCalledTimes(2)
  })
})

describe('logout', () => {
  it('clears the local session without navigation when there is no id token', async () => {
    const assign = vi.spyOn(window.location, 'assign').mockImplementation(() => undefined)
    await bootstrapWith(now.getTime() + 60_000, { idToken: '' })
    expect(vi.getTimerCount()).toBe(1)

    await logout()

    expect(assign).not.toHaveBeenCalled()
    expect(get(authState)).toEqual({ status: 'anonymous', session: null, error: null })
    expect(vi.getTimerCount()).toBe(0)
  })

  it('redirects to the end-session endpoint with the id token hint', async () => {
    await bootstrapWith(now.getTime() + 60_000)
    const assign = vi.spyOn(window.location, 'assign').mockImplementation(() => undefined)
    const replaceState = vi.spyOn(window.history, 'replaceState')
    vi.stubGlobal(
      'fetch',
      vi.fn<typeof fetch>(async () =>
        jsonResponse(200, discovery({ end_session_endpoint: `${issuer}/logout` })),
      ),
    )

    await logout()

    expect(assign).toHaveBeenCalledTimes(1)
    const target = new URL(String(assign.mock.calls[0]?.[0]))
    expect(`${target.origin}${target.pathname}`).toBe(`${issuer}/logout`)
    expect(target.searchParams.get('id_token_hint')).toBe('id-token')
    expect(target.searchParams.get('post_logout_redirect_uri')).toBe(`${window.location.origin}/`)
    expect(replaceState).not.toHaveBeenCalled()
    expect(get(authState)).toEqual({ status: 'anonymous', session: null, error: null })
  })

  it('falls back to the app base path when the end-session discovery fails', async () => {
    await bootstrapWith(now.getTime() + 60_000)
    const assign = vi.spyOn(window.location, 'assign').mockImplementation(() => undefined)
    const replaceState = vi.spyOn(window.history, 'replaceState')
    vi.stubGlobal(
      'fetch',
      vi.fn<typeof fetch>(async () => {
        throw new TypeError('network error')
      }),
    )

    await logout()

    expect(assign).not.toHaveBeenCalled()
    expect(replaceState.mock.calls).toEqual([[null, '', '/']])
    expect(get(authState)).toEqual({ status: 'anonymous', session: null, error: null })
  })
})
