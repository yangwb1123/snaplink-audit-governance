import { expect, type Page } from '@playwright/test'

const idp = process.env.SNAPLINK_IDP_URL ?? 'http://localhost:18082'
export const audit = process.env.AUDIT_API_URL ?? 'http://localhost:19089'
const hosted = process.env.SNAPLINK_HOSTED_LOGIN_URL ?? 'http://localhost:4444/login/'

/** Automates the password-provider authorization step, NOT the sibling
 * Flutter Hosted Login UI. Codes, PKCE, JWT signature verification and Audit
 * API authorization remain real; tokens stay in this test's memory only. */
export async function login(page: Page, username: string, hash = '#evidence', scopes?: string[]): Promise<string> {
  await page.route(`${hosted}**`, async (route) => {
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
  const tokenResponse = page.waitForResponse((response) => new URL(response.url()).pathname.endsWith('/token') && response.request().method() === 'POST')
  await page.goto(`/${hash}`)
  await page.getByRole('button', { name: '使用 Snaplink 登录', exact: true }).click()
  const response = await tokenResponse
  expect(response.status()).toBe(200)
  const body = await response.json() as { access_token: string }
  await expect(page.getByRole('heading', { name: '证据与治理', exact: true })).toBeVisible()
  return body.access_token
}

export function headers(token: string): Record<string, string> {
  return { Authorization: `Bearer ${token}` }
}

export async function bindSource(page: Page, token: string): Promise<void> {
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
}
