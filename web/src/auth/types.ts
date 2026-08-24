import type { RuntimeConfig } from '../config'

export interface DiscoveryDocument {
  issuer: string
  authorization_endpoint: string
  token_endpoint: string
  jwks_uri: string
  end_session_endpoint?: string
  code_challenge_methods_supported?: string[]
  token_endpoint_auth_methods_supported?: string[]
}

export interface TokenResponse {
  access_token: string
  token_type: string
  expires_in?: number
  id_token: string
  scope?: string
}

export interface TokenClaims extends Record<string, unknown> {
  sub: string
  iss: string
  aud?: string | string[]
  exp: number
  nonce?: string
  name?: string
  preferred_username?: string
  email?: string
  tenant_id?: string
  tenant?: string
  roles?: string[]
  permissions?: string[]
  scope?: string
}

export interface UserSession {
  accessToken: string
  idToken: string
  expiresAt: number
  subject: string
  displayName: string
  tenantId: string
  roles: string[]
  permissions: Set<string>
  platform: boolean
}

export interface PendingAuthorization {
  state: string
  nonce: string
  verifier: string
  redirectUri: string
  issuer: string
  createdAt: number
}

export interface AuthDependencies {
  config: RuntimeConfig
  fetcher?: typeof fetch
  storage?: Storage
  crypto?: Crypto
}
