import * as React from 'react'
import { useAuth } from '../auth/AuthProvider'
import { loadRuntimeConfig } from '../config'
import { AuditApi } from './audit'

export function useAuditApi(): AuditApi {
  const { session, invalidate } = useAuth()
  const config = React.useMemo(loadRuntimeConfig, [])
  const api = React.useMemo(
    () =>
      new AuditApi({
        baseUrl: config.auditApiBaseUrl,
        accessToken: session?.accessToken ?? '',
        onUnauthorized: invalidate,
      }),
    [config.auditApiBaseUrl, invalidate, session?.accessToken],
  )
  if (!session) throw new Error('useAuditApi requires an authenticated session')
  return api
}
