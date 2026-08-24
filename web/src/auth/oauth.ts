import { postLogoutUri, redirectUri, type RuntimeConfig } from '../config'
import { sessionFromTokens } from './claims'
import { browserEndpoint } from './endpoints'
import { pkceChallenge, randomValue } from './pkce'
import { verifyTokenSignatures } from './verify'
import type {
  AuthDependencies,
  DiscoveryDocument,
  PendingAuthorization,
  TokenResponse,
  UserSession,
} from './types'

const pendingKey = 'audit-governance.snaplink.pending.v1'
const maxPendingAgeMs = 10 * 60 * 1000

function trimSlash(value: string): string {
  return value.replace(/\/+$/, '')
}

function isLoopback(hostname: string): boolean {
  return hostname === 'localhost' || hostname === '127.0.0.1' || hostname === '[::1]'
}

function validateEndpoint(endpoint: string, issuer: URL): void {
  const value = new URL(endpoint)
  if (value.origin !== issuer.origin)
    throw new Error('Snaplink discovery endpoint 与 issuer 不同源')
  if (value.protocol !== 'https:' && !(value.protocol === 'http:' && isLoopback(value.hostname))) {
    throw new Error('Snaplink endpoint 必须使用 HTTPS')
  }
}

function validateBrowserUrl(value: string, label: string): URL {
  const url = new URL(value)
  if (url.protocol !== 'https:' && !(url.protocol === 'http:' && isLoopback(url.hostname))) {
    throw new Error(`${label} 必须使用 HTTPS`)
  }
  return url
}

function validateDiscovery(document: DiscoveryDocument, config: RuntimeConfig): void {
  if (trimSlash(document.issuer) !== trimSlash(config.snaplinkIssuer)) {
    throw new Error('Snaplink discovery issuer 不匹配')
  }
  const issuer = new URL(config.snaplinkIssuer)
  validateEndpoint(document.authorization_endpoint, issuer)
  validateEndpoint(document.token_endpoint, issuer)
  validateEndpoint(document.jwks_uri, issuer)
  if (document.end_session_endpoint) validateEndpoint(document.end_session_endpoint, issuer)
  if (!document.code_challenge_methods_supported?.includes('S256')) {
    throw new Error('Snaplink 未声明 PKCE S256 支持')
  }
  if (!document.token_endpoint_auth_methods_supported?.includes('none')) {
    throw new Error('Snaplink 未声明 public client 支持')
  }
}

export async function discover(
  config: RuntimeConfig,
  fetcher: typeof fetch = fetch,
): Promise<DiscoveryDocument> {
  const response = await fetcher(config.snaplinkDiscoveryUrl, {
    headers: { Accept: 'application/json' },
    credentials: 'omit',
  })
  if (!response.ok) throw new Error(`Snaplink discovery 请求失败 (${response.status})`)
  const document = (await response.json()) as DiscoveryDocument
  validateDiscovery(document, config)
  return document
}

function storePending(storage: Storage, pending: PendingAuthorization): void {
  storage.setItem(pendingKey, JSON.stringify(pending))
}

function consumePending(storage: Storage): PendingAuthorization {
  const raw = storage.getItem(pendingKey)
  storage.removeItem(pendingKey)
  if (!raw) throw new Error('Snaplink 登录状态不存在或已使用')
  const pending = JSON.parse(raw) as PendingAuthorization
  if (Date.now() - pending.createdAt > maxPendingAgeMs) throw new Error('Snaplink 登录状态已过期')
  return pending
}

export async function authorizationUrl(dependencies: AuthDependencies): Promise<string> {
  const {
    config,
    fetcher = fetch,
    storage = sessionStorage,
    crypto = globalThis.crypto,
  } = dependencies
  if (!config.snaplinkClientId || !config.snaplinkResource) {
    throw new Error('Snaplink client_id 和 resource 必须配置')
  }
  const document = await discover(config, fetcher)
  const state = randomValue(crypto)
  const nonce = randomValue(crypto)
  const verifier = randomValue(crypto, 64)
  const callback = redirectUri(config)
  storePending(storage, {
    state,
    nonce,
    verifier,
    redirectUri: callback,
    issuer: document.issuer,
    createdAt: Date.now(),
  })
  const url = validateBrowserUrl(config.snaplinkHostedLoginUrl, 'Snaplink Hosted Login')
  url.searchParams.set('client_id', config.snaplinkClientId)
  url.searchParams.set('response_type', 'code')
  url.searchParams.set('redirect_uri', callback)
  url.searchParams.set('scope', config.snaplinkScope)
  url.searchParams.set('resource', config.snaplinkResource)
  url.searchParams.set('state', state)
  url.searchParams.set('nonce', nonce)
  url.searchParams.set('code_challenge', await pkceChallenge(crypto, verifier))
  url.searchParams.set('code_challenge_method', 'S256')
  return url.toString()
}

function uniqueParameter(params: URLSearchParams, name: string, required = true): string {
  const values = params.getAll(name)
  if (values.length > 1) throw new Error(`Snaplink callback 包含重复的 ${name}`)
  if (required && (!values[0] || values[0].trim() === '')) {
    throw new Error(`Snaplink callback 缺少 ${name}`)
  }
  return values[0] ?? ''
}

export async function completeAuthorization(
  callbackUrl: URL,
  dependencies: AuthDependencies,
): Promise<UserSession> {
  const { config, fetcher = fetch, storage = sessionStorage } = dependencies
  const error = uniqueParameter(callbackUrl.searchParams, 'error', false)
  if (error) {
    const description = uniqueParameter(callbackUrl.searchParams, 'error_description', false)
    throw new Error(description || `Snaplink 拒绝了登录 (${error})`)
  }
  const code = uniqueParameter(callbackUrl.searchParams, 'code')
  const state = uniqueParameter(callbackUrl.searchParams, 'state')
  const responseIssuer = uniqueParameter(callbackUrl.searchParams, 'iss', false)
  const pending = consumePending(storage)
  if (state !== pending.state) throw new Error('Snaplink 登录 state 不匹配')
  if (responseIssuer && trimSlash(responseIssuer) !== trimSlash(pending.issuer)) {
    throw new Error('Snaplink callback issuer 不匹配')
  }
  const document = await discover(config, fetcher)
  if (trimSlash(document.issuer) !== trimSlash(pending.issuer)) {
    throw new Error('Snaplink issuer 在登录过程中发生变化')
  }
  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    code,
    redirect_uri: pending.redirectUri,
    client_id: config.snaplinkClientId,
    code_verifier: pending.verifier,
  })
  const response = await fetcher(browserEndpoint(document.token_endpoint, config), {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    credentials: 'omit',
    body,
  })
  if (!response.ok) throw new Error(`Snaplink code 交换失败 (${response.status})`)
  const tokens = (await response.json()) as TokenResponse
  if (!tokens.access_token || !tokens.id_token || !tokens.token_type) {
    throw new Error('Snaplink token 响应不完整')
  }
  await verifyTokenSignatures(tokens, document, config, fetcher)
  return sessionFromTokens(tokens, config, pending.nonce)
}

export async function logoutUrl(
  config: RuntimeConfig,
  idToken: string,
  fetcher: typeof fetch = fetch,
): Promise<string | null> {
  const document = await discover(config, fetcher)
  if (!document.end_session_endpoint) return null
  const url = new URL(document.end_session_endpoint)
  url.searchParams.set('id_token_hint', idToken)
  url.searchParams.set('post_logout_redirect_uri', postLogoutUri(config))
  return url.toString()
}
