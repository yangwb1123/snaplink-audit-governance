import { describe, expect, it } from 'vitest'
import type { RuntimeConfig } from '../config'
import { can, decodeJwtClaims, sessionFromTokens } from './claims'

const config: RuntimeConfig = {
  auditApiBaseUrl: '/audit-api',
  snaplinkIssuer: 'https://id.example.test',
  snaplinkDiscoveryUrl: '/discovery',
  snaplinkApiBaseUrl: '/snaplink',
  snaplinkHostedLoginUrl: 'https://login.example.test/login/',
  snaplinkClientId: 'audit-governance-web',
  snaplinkResource: 'audit-governance',
  snaplinkScope: 'openid profile',
  appBasePath: '/',
}

function token(claims: Record<string, unknown>): string {
  const encode = (value: unknown) => {
    const bytes = new TextEncoder().encode(JSON.stringify(value))
    const binary = Array.from(bytes, (byte) => String.fromCharCode(byte)).join('')
    return btoa(binary).replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_')
  }
  return `${encode({ alg: 'none' })}.${encode(claims)}.`
}

describe('Snaplink claims', () => {
  it('decodes unicode JWT payloads', () => {
    expect(decodeJwtClaims(token({ sub: '用户一' })).sub).toBe('用户一')
  })

  it('derives the audit permission matrix from roles', () => {
    const exp = Math.floor(Date.now() / 1000) + 300
    const session = sessionFromTokens(
      {
        token_type: 'Bearer',
        access_token: token({
          sub: 'u1',
          iss: config.snaplinkIssuer,
          aud: config.snaplinkResource,
          exp,
          roles: ['tenant-admin'],
          tenant_id: 'tenant-a',
        }),
        id_token: token({
          sub: 'u1',
          iss: config.snaplinkIssuer,
          aud: config.snaplinkClientId,
          exp,
          nonce: 'n1',
          preferred_username: '审计员',
        }),
      },
      config,
      'n1',
    )
    expect(session.displayName).toBe('审计员')
    expect(session.tenantId).toBe('tenant-a')
    expect(can(session, 'audit:policy:write')).toBe(true)
    expect(can(session, 'audit:platform:cross_tenant')).toBe(false)
  })

  it('rejects an ID token nonce mismatch', () => {
    const exp = Math.floor(Date.now() / 1000) + 300
    expect(() =>
      sessionFromTokens(
        {
          token_type: 'Bearer',
          access_token: token({
            sub: 'u1',
            iss: config.snaplinkIssuer,
            aud: config.snaplinkResource,
            exp,
          }),
          id_token: token({
            sub: 'u1',
            iss: config.snaplinkIssuer,
            aud: config.snaplinkClientId,
            exp,
            nonce: 'wrong',
          }),
        },
        config,
        'expected',
      ),
    ).toThrow('nonce')
  })
})
