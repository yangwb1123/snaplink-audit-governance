import { get } from 'svelte/store'
import { loadRuntimeConfig } from '../config'
import { invalidate, authState } from '../auth/store'
import { AuditApi } from './audit'

export function useAuditApi(): AuditApi {
  const session = get(authState).session
  if (!session) throw new Error('useAuditApi requires an authenticated session')
  const config = loadRuntimeConfig()
  return new AuditApi({
    baseUrl: config.auditApiBaseUrl,
    accessToken: session.accessToken,
    platform: session.platform,
    onUnauthorized: invalidate,
  })
}
