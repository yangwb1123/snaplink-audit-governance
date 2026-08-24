import type { RuntimeConfig } from '../config'
import type { TokenClaims, TokenResponse, UserSession } from './types'

const rolePermissions: Record<string, string[]> = {
  service: ['audit:event:write'],
  'event-writer': ['audit:event:write'],
  auditor: ['audit:event:read', 'audit:operation:read'],
  'tenant-auditor': ['audit:event:read', 'audit:operation:read'],
  compliance: [
    'audit:event:read',
    'audit:operation:read',
    'audit:export:create',
    'audit:integrity:verify',
    'audit:legal_hold:manage',
  ],
  'tenant-admin': [
    'audit:event:read',
    'audit:operation:read',
    'audit:export:create',
    'audit:integrity:verify',
    'audit:legal_hold:manage',
    'audit:policy:read',
    'audit:policy:write',
  ],
  'platform-admin': ['audit:platform:cross_tenant'],
}

function decodeBase64Url(value: string): string {
  const normalized = value.replace(/-/g, '+').replace(/_/g, '/')
  const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, '=')
  return decodeURIComponent(
    Array.from(
      atob(padded),
      (character) => `%${character.charCodeAt(0).toString(16).padStart(2, '0')}`,
    ).join(''),
  )
}

export function decodeJwtClaims(token: string): TokenClaims {
  const parts = token.split('.')
  if (parts.length !== 3 || !parts[1]) throw new Error('Snaplink 返回了无效的 JWT')
  const value: unknown = JSON.parse(decodeBase64Url(parts[1]))
  if (!value || typeof value !== 'object') throw new Error('Snaplink JWT claims 无效')
  return value as TokenClaims
}

function stringList(value: unknown): string[] {
  if (Array.isArray(value)) return value.filter((item): item is string => typeof item === 'string')
  if (typeof value === 'string') return value.split(/\s+/).filter(Boolean)
  return []
}

function hasAudience(claims: TokenClaims, expected: string): boolean {
  return typeof claims.aud === 'string'
    ? claims.aud === expected
    : Array.isArray(claims.aud) && claims.aud.includes(expected)
}

function validateClaims(
  claims: TokenClaims,
  expectedIssuer: string,
  expectedAudience: string,
  expectedNonce?: string,
): void {
  if (claims.iss !== expectedIssuer) throw new Error('Snaplink token issuer 不匹配')
  if (!hasAudience(claims, expectedAudience)) throw new Error('Snaplink token audience 不匹配')
  if (!Number.isFinite(claims.exp) || claims.exp * 1000 <= Date.now()) {
    throw new Error('Snaplink token 已过期')
  }
  if (typeof claims.sub !== 'string' || claims.sub.length === 0) {
    throw new Error('Snaplink token 缺少 subject')
  }
  if (expectedNonce !== undefined && claims.nonce !== expectedNonce) {
    throw new Error('Snaplink 登录 nonce 不匹配')
  }
}

export function sessionFromTokens(
  response: TokenResponse,
  config: RuntimeConfig,
  expectedNonce: string,
): UserSession {
  if (response.token_type.toLowerCase() !== 'bearer')
    throw new Error('Snaplink token_type 必须为 Bearer')
  const accessClaims = decodeJwtClaims(response.access_token)
  const identityClaims = decodeJwtClaims(response.id_token)
  validateClaims(identityClaims, config.snaplinkIssuer, config.snaplinkClientId, expectedNonce)
  validateClaims(accessClaims, config.snaplinkIssuer, config.snaplinkResource)

  const roles = stringList(accessClaims.roles)
  const permissions = new Set([
    ...stringList(accessClaims.permissions),
    ...stringList(accessClaims.scope),
  ])
  for (const role of roles) {
    for (const permission of rolePermissions[role] ?? []) permissions.add(permission)
  }
  const platform =
    roles.includes('platform-admin') || permissions.has('audit:platform:cross_tenant')
  const expiresAt = Math.min(accessClaims.exp, identityClaims.exp) * 1000
  return {
    accessToken: response.access_token,
    idToken: response.id_token,
    expiresAt,
    subject: accessClaims.sub,
    displayName:
      identityClaims.preferred_username ??
      identityClaims.name ??
      identityClaims.email ??
      accessClaims.sub,
    tenantId: accessClaims.tenant_id ?? accessClaims.tenant ?? '',
    roles,
    permissions,
    platform,
  }
}

export function can(session: UserSession, permission: string): boolean {
  return session.platform || session.permissions.has(permission)
}
