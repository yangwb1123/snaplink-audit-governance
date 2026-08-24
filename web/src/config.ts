export interface RuntimeConfig {
  auditApiBaseUrl: string
  snaplinkIssuer: string
  snaplinkDiscoveryUrl: string
  snaplinkApiBaseUrl: string
  snaplinkHostedLoginUrl: string
  snaplinkClientId: string
  snaplinkResource: string
  snaplinkScope: string
  appBasePath: string
}

const defaults: RuntimeConfig = {
  auditApiBaseUrl: '/audit-api',
  snaplinkIssuer: 'http://localhost:18082',
  snaplinkDiscoveryUrl: '/snaplink-api/.well-known/openid-configuration',
  snaplinkApiBaseUrl: '/snaplink-api',
  snaplinkHostedLoginUrl: 'http://localhost:4444/login/',
  snaplinkClientId: 'audit-governance-web',
  snaplinkResource: 'audit-governance',
  snaplinkScope:
    'openid profile audit:event:read audit:event:write audit:operation:read audit:export:create audit:integrity:verify audit:legal_hold:manage audit:policy:read audit:policy:write audit:platform:cross_tenant',
  appBasePath: '/',
}

const fromEnvironment: Partial<RuntimeConfig> = {
  auditApiBaseUrl: import.meta.env.VITE_AUDIT_API_BASE_URL,
  snaplinkIssuer: import.meta.env.VITE_SNAPLINK_ISSUER,
  snaplinkDiscoveryUrl: import.meta.env.VITE_SNAPLINK_DISCOVERY_URL,
  snaplinkApiBaseUrl: import.meta.env.VITE_SNAPLINK_API_BASE_URL,
  snaplinkHostedLoginUrl: import.meta.env.VITE_SNAPLINK_HOSTED_LOGIN_URL,
  snaplinkClientId: import.meta.env.VITE_SNAPLINK_CLIENT_ID,
  snaplinkResource: import.meta.env.VITE_SNAPLINK_RESOURCE,
  snaplinkScope: import.meta.env.VITE_SNAPLINK_SCOPE,
  appBasePath: import.meta.env.VITE_APP_BASE_PATH,
}

function definedValues(values: Partial<RuntimeConfig>): Partial<RuntimeConfig> {
  return Object.fromEntries(Object.entries(values).filter(([, value]) => value !== undefined))
}

export function loadRuntimeConfig(): RuntimeConfig {
  return {
    ...defaults,
    ...definedValues(fromEnvironment),
    ...definedValues(window.__AUDIT_GOVERNANCE_CONFIG__ ?? {}),
  }
}

export function redirectUri(config: RuntimeConfig): string {
  return new URL('auth/callback', new URL(config.appBasePath, window.location.origin)).toString()
}

export function postLogoutUri(config: RuntimeConfig): string {
  return new URL(config.appBasePath, window.location.origin).toString()
}
