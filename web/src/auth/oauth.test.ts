import { exportJWK, generateKeyPair, SignJWT } from 'jose'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { RuntimeConfig } from '../config'
import { authorizationUrl, completeAuthorization } from './oauth'
import type { DiscoveryDocument, PendingAuthorization } from './types'

class MemoryStorage implements Storage {
  private readonly values = new Map<string, string>()

  get length(): number {
    return this.values.size
  }

  clear(): void {
    this.values.clear()
  }

  getItem(key: string): string | null {
    return this.values.get(key) ?? null
  }

  key(index: number): string | null {
    return [...this.values.keys()][index] ?? null
  }

  removeItem(key: string): void {
    this.values.delete(key)
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value)
  }
}

const config: RuntimeConfig = {
  auditApiBaseUrl: '/audit-api',
  snaplinkIssuer: 'https://id.example.test',
  snaplinkDiscoveryUrl: '/snaplink/.well-known/openid-configuration',
  snaplinkApiBaseUrl: '/snaplink',
  snaplinkHostedLoginUrl: 'https://console.example.test/login/',
  snaplinkClientId: 'audit-governance-web',
  snaplinkResource: 'audit-governance',
  snaplinkScope: 'openid profile',
  appBasePath: '/',
}

const discovery: DiscoveryDocument = {
  issuer: config.snaplinkIssuer,
  authorization_endpoint: `${config.snaplinkIssuer}/auth/login`,
  token_endpoint: `${config.snaplinkIssuer}/token`,
  jwks_uri: `${config.snaplinkIssuer}/.well-known/jwks.json`,
  end_session_endpoint: `${config.snaplinkIssuer}/logout`,
  code_challenge_methods_supported: ['S256'],
  token_endpoint_auth_methods_supported: ['none'],
}

afterEach(() => vi.unstubAllGlobals())

describe('Snaplink OAuth flow', () => {
  it('completes code + PKCE and verifies both signed tokens', async () => {
    vi.stubGlobal('window', { location: { origin: 'http://localhost:5178' } })
    const storage = new MemoryStorage()
    const { publicKey, privateKey } = await generateKeyPair('ES256')
    const publicJwk = await exportJWK(publicKey)
    publicJwk.kid = 'test-key'
    publicJwk.alg = 'ES256'

    const discoveryOnly = vi.fn<typeof fetch>(
      async () => new Response(JSON.stringify(discovery), { status: 200 }),
    )
    const loginTarget = new URL(await authorizationUrl({ config, storage, fetcher: discoveryOnly }))
    expect(loginTarget.origin).toBe('https://console.example.test')
    expect(loginTarget.searchParams.get('code_challenge_method')).toBe('S256')
    expect(loginTarget.searchParams.get('resource')).toBe('audit-governance')

    const pending = JSON.parse(storage.getItem(storage.key(0)!)!) as PendingAuthorization
    const baseClaims = { iss: config.snaplinkIssuer, sub: 'user-1' }
    const accessToken = await new SignJWT({
      ...baseClaims,
      aud: config.snaplinkResource,
      tenant_id: 'tenant-a',
      roles: ['auditor'],
    })
      .setProtectedHeader({ alg: 'ES256', kid: 'test-key' })
      .setExpirationTime('5m')
      .sign(privateKey)
    const idToken = await new SignJWT({
      ...baseClaims,
      aud: config.snaplinkClientId,
      nonce: pending.nonce,
      preferred_username: '审计员',
    })
      .setProtectedHeader({ alg: 'ES256', kid: 'test-key' })
      .setExpirationTime('5m')
      .sign(privateKey)

    const fetcher = vi.fn<typeof fetch>(async (input) => {
      const url = String(input)
      if (url === config.snaplinkDiscoveryUrl) {
        return new Response(JSON.stringify(discovery), { status: 200 })
      }
      if (url.endsWith('/snaplink/token')) {
        return new Response(
          JSON.stringify({ access_token: accessToken, id_token: idToken, token_type: 'Bearer' }),
          { status: 200 },
        )
      }
      if (url.endsWith('/snaplink/.well-known/jwks.json')) {
        return new Response(JSON.stringify({ keys: [publicJwk] }), { status: 200 })
      }
      return new Response(null, { status: 404 })
    })
    const callback = new URL('http://localhost:5178/auth/callback')
    callback.searchParams.set('code', 'one-use-code')
    callback.searchParams.set('state', pending.state)
    callback.searchParams.set('iss', config.snaplinkIssuer)
    const session = await completeAuthorization(callback, { config, storage, fetcher })

    expect(session.displayName).toBe('审计员')
    expect(session.tenantId).toBe('tenant-a')
    expect(session.permissions.has('audit:event:read')).toBe(true)
    expect(storage.length).toBe(0)
  })
})
