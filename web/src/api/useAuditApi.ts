import { get } from 'svelte/store'
import { loadRuntimeConfig } from '../config'
import { invalidate, authState } from '../auth/store'
import { AuditApi } from './audit'

// One synchronous observer for all adapters, not one permanent subscription per
// request. A fresh opaque generation cannot revive when auth values (or even the
// original state/session objects) return in the same Svelte render turn.
let generation = Symbol()
let lastSession = get(authState).session
let lastBinding = ''
const unsubscribe = authState.subscribe((state) => {
  const session = state.session
  // Snapshot mutable permission/role collections as well as scalar credentials.
  // This binding stays in memory only; never expose it as a URL, log or storage key.
  const binding = JSON.stringify([state.status, session?.subject, session?.tenantId, session?.platform,
    session?.accessToken, session?.idToken, session?.expiresAt,
    session ? [...session.permissions].sort() : [], session ? [...session.roles].sort() : []])
  if (session === lastSession && binding === lastBinding) return
  lastSession = session
  lastBinding = binding
  generation = Symbol()
})
if (import.meta.hot) import.meta.hot.dispose(() => {
  unsubscribe()
  generation = Symbol()
  lastSession = null
  lastBinding = ''
})

export function useAuditApi(): AuditApi {
  const session = get(authState).session
  if (!session) throw new Error('useAuditApi requires an authenticated session')
  const capturedGeneration = generation
  const config = loadRuntimeConfig()
  return new AuditApi({
    baseUrl: config.auditApiBaseUrl,
    accessToken: session.accessToken,
    platform: session.platform,
    // ApiClient calls this before rejecting: fence the side effect here, not
    // after a page's await. Retirement never changes the original API rejection.
    onUnauthorized: () => {
      if (capturedGeneration === generation && get(authState).status === 'authenticated') invalidate()
    },
  })
}
