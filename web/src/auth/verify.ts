import { createLocalJWKSet, jwtVerify, type JSONWebKeySet } from 'jose'
import type { RuntimeConfig } from '../config'
import { browserEndpoint } from './endpoints'
import type { DiscoveryDocument, TokenResponse } from './types'

const allowedAlgorithms = ['EdDSA', 'ES256', 'ES384', 'ES512', 'RS256', 'PS256']

export async function verifyTokenSignatures(
  tokens: TokenResponse,
  discovery: DiscoveryDocument,
  config: RuntimeConfig,
  fetcher: typeof fetch,
): Promise<void> {
  const response = await fetcher(browserEndpoint(discovery.jwks_uri, config), {
    headers: { Accept: 'application/json' },
    credentials: 'omit',
  })
  if (!response.ok) throw new Error(`Snaplink JWKS 请求失败 (${response.status})`)
  const jwks = (await response.json()) as JSONWebKeySet
  if (!Array.isArray(jwks.keys) || jwks.keys.length === 0) throw new Error('Snaplink JWKS 为空')
  const keySet = createLocalJWKSet(jwks)
  await jwtVerify(tokens.id_token, keySet, {
    issuer: config.snaplinkIssuer,
    audience: config.snaplinkClientId,
    algorithms: allowedAlgorithms,
  })
  await jwtVerify(tokens.access_token, keySet, {
    issuer: config.snaplinkIssuer,
    audience: config.snaplinkResource,
    algorithms: allowedAlgorithms,
  })
}
