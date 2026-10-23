import { expect, type APIRequestContext, type Page } from '@playwright/test'

const idp = process.env.SNAPLINK_IDP_URL ?? 'http://localhost:18082'
export const audit = process.env.AUDIT_API_URL ?? 'http://localhost:19089'
const hosted = process.env.SNAPLINK_HOSTED_LOGIN_URL ?? 'http://localhost:4444/login/'

function fixtureError(): Error {
  // Never attach a cause: Playwright request exceptions contain header/body
  // diagnostics even when tracing is off. JSON/assertion errors can too.
  return new Error('Browser fixture failed; sensitive diagnostics withheld')
}

/** Sanitize before Playwright records its API-step error, not just afterwards.
 * Playwright 1.60's scoped API zone keeps nested calls in one reported step.
 * This fixture-only pinned hook changes no request/PKCE/binding parameters;
 * a missing hook fails closed BEFORE executing credential-bearing I/O. */
export async function fixtureCall<T>(transport: APIRequestContext, operation: () => Promise<T>): Promise<T> {
  type ScopedTransport = APIRequestContext & {
    _wrapApiCall?: <T>(operation: () => Promise<T>, options: { title: string }) => Promise<T>
  }
  try {
    const scoped = transport as ScopedTransport
    if (typeof scoped._wrapApiCall !== 'function') throw fixtureError()
    return await scoped._wrapApiCall(async () => {
      try { return await operation() }
      catch { throw fixtureError() }
    }, { title: 'Sensitive browser fixture' })
  } catch { throw fixtureError() }
}

/** Automates the password-provider authorization step, NOT the sibling
 * Flutter Hosted Login UI. Codes, PKCE, JWT signature verification and Audit
 * API authorization remain real; tokens stay in this test's memory only. */
export async function login(page: Page, username: string, hash = '#evidence', scopes?: string[]): Promise<string> {
  return loginWithHeading(page, username, hash, scopes, '证据与治理')
}

/** Same real PKCE/JWT flow, but for journeys whose landing page has another heading. */
export async function loginWithHeading(
  page: Page,
  username: string,
  hash: string,
  scopes: string[] | undefined,
  heading: string,
): Promise<string> {
  return fixtureCall(page.request, async () => {
    let routeFailed!: (error: Error) => void
    const routeFailure = new Promise<never>((_resolve, reject) => { routeFailed = reject })
    // A route callback is asynchronous and outside the caller's try/catch.
    // Settle the login race safely; never throw a request error from the route.
    void routeFailure.catch(() => {})
    await page.route(`${hosted}**`, async (route) => {
      try {
        await fixtureCall(page.request, async () => {
          const params = new URL(route.request().url()).searchParams
          const response = await page.request.post(`${idp}/auth/login`, {
            data: {
              provider: 'password', client_id: params.get('client_id'), redirect_uri: params.get('redirect_uri'),
              response_type: 'code', scope: scopes ?? (params.get('scope') ?? '').split(' ').filter(Boolean),
              resource: [params.get('resource')], state: params.get('state'), nonce: params.get('nonce'),
              code_challenge: params.get('code_challenge'), code_challenge_method: 'S256',
              credential: { username, password: process.env.AUDIT_WEB_PASSWORD ?? 'audit-demo-password' },
            },
          })
          expect(response.status()).toBe(200)
          const grant = await response.json() as { code: string; state: string; iss: string }
          const callback = new URL(params.get('redirect_uri')!)
          callback.searchParams.set('code', grant.code)
          callback.searchParams.set('state', grant.state)
          callback.searchParams.set('iss', grant.iss)
          await route.fulfill({ status: 302, headers: { location: callback.toString() } })
        })
      } catch {
        routeFailed(fixtureError())
        try { await fixtureCall(page.request, () => route.abort('failed')) } catch { /* Cleanup must not expose route diagnostics. */ }
      }
    })
    const tokenResponse = fixtureCall(page.request, () => page.waitForResponse((response) => new URL(response.url()).pathname.endsWith('/token') && response.request().method() === 'POST'))
    // Navigation/route failure may finish before this waiter does.
    void tokenResponse.catch(() => {})
    const journey = (async () => {
      await page.goto(`/${hash}`)
      await page.getByRole('button', { name: '使用 Snaplink 登录', exact: true }).click()
      const response = await tokenResponse
      expect(response.status()).toBe(200)
      const body = await response.json() as { access_token: string }
      await expect(page.getByRole('heading', { name: heading, exact: true })).toBeVisible()
      return body.access_token
    })()
    return Promise.race([journey, routeFailure])
  })
}

export function headers(token: string): Record<string, string> {
  return { Authorization: `Bearer ${token}` }
}

export async function bindSource(page: Page, token: string): Promise<void> {
  await fixtureCall(page.request, async () => {
    const catalog = await page.request.get(`${audit}/api/v1/sources?tenant_id=demo`, { headers: headers(token) })
    expect(catalog.status()).toBe(200)
    const body = await catalog.json() as { items: Array<{ id: string; name: string; active: boolean; allowed_client_ids?: string[] }> }
    const source = body.items.find((item) => item.id === 'demo')!
    expect(Boolean(source?.active)).toBe(true)
    const clients = [...new Set([...(source.allowed_client_ids ?? []), 'audit-governance-web'])]
    const response = await page.request.put(`${audit}/api/v1/sources/demo?tenant_id=demo`, {
      headers: headers(token), data: { name: source.name, allowed_client_ids: clients, active: source.active },
    })
    expect(response.status()).toBe(200)
  })
}
