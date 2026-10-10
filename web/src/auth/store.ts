import { get, writable, type Readable } from 'svelte/store'
import { loadRuntimeConfig, redirectUri } from '../config'
import { authorizationUrl, completeAuthorization, logoutUrl } from './oauth'
import type { UserSession } from './types'

export type AuthStatus = 'checking' | 'anonymous' | 'authenticated' | 'error'

export interface AuthState {
  status: AuthStatus
  session: UserSession | null
  error: string | null
}

const initialState: AuthState = { status: 'checking', session: null, error: null }
export const authState = writable<AuthState>(initialState)

let callbackPromise: Promise<{ session: UserSession; returnHash: string }> | null = null
let expiryTimer: number | undefined

function clearExpiryTimer(): void {
  if (expiryTimer !== undefined) window.clearTimeout(expiryTimer)
  expiryTimer = undefined
}

function setSession(session: UserSession): void {
  clearExpiryTimer()
  authState.set({ status: 'authenticated', session, error: null })
  const remaining = session.expiresAt - Date.now()
  if (remaining <= 0) {
    invalidate('Snaplink 会话已过期，请重新登录')
    return
  }
  expiryTimer = window.setTimeout(() => {
    authState.set({ status: 'anonymous', session: null, error: 'Snaplink 会话已过期，请重新登录' })
  }, Math.min(remaining, 2_147_483_647))
}

export async function bootstrapAuth(): Promise<void> {
  const config = loadRuntimeConfig()
  if (window.location.pathname !== new URL(redirectUri(config)).pathname) {
    authState.set({ status: 'anonymous', session: null, error: null })
    return
  }
  try {
    callbackPromise ??= (async () => {
      let returnHash = '#overview'
      const session = await completeAuthorization(new URL(window.location.href), { config, onReturnHash: (hash) => (returnHash = hash) })
      return { session, returnHash }
    })()
    const result = await callbackPromise
    setSession(result.session)
    window.history.replaceState(null, '', `${config.appBasePath}${result.returnHash}`)
  } catch (cause) {
    authState.set({
      status: 'error',
      session: null,
      error: cause instanceof Error ? cause.message : 'Snaplink 登录失败',
    })
    window.history.replaceState(null, '', config.appBasePath)
  } finally {
    callbackPromise = null
  }
}

export async function login(): Promise<void> {
  const config = loadRuntimeConfig()
  authState.update((state) => ({ ...state, status: 'checking', error: null }))
  try {
    window.location.assign(await authorizationUrl({ config, returnHash: window.location.hash }))
  } catch (cause) {
    authState.set({
      status: 'error',
      session: null,
      error: cause instanceof Error ? cause.message : '无法连接 Snaplink',
    })
  }
}

export async function logout(): Promise<void> {
  const current = get(authState).session
  const config = loadRuntimeConfig()
  clearExpiryTimer()
  authState.set({ status: 'anonymous', session: null, error: null })
  if (!current?.idToken) return
  try {
    const target = await logoutUrl(config, current.idToken)
    if (target) window.location.assign(target)
  } catch {
    window.history.replaceState(null, '', config.appBasePath)
  }
}

export function invalidate(message = 'Audit API 拒绝了当前会话，请重新登录'): void {
  clearExpiryTimer()
  authState.set({ status: 'anonymous', session: null, error: message })
}

export function clearAuthError(): void {
  authState.update((state) => ({ ...state, error: null }))
}

export function useAuth(): {
  state: Readable<AuthState>
  login: typeof login
  logout: typeof logout
  invalidate: typeof invalidate
  clearError: typeof clearAuthError
} {
  return { state: authState, login, logout, invalidate, clearError: clearAuthError }
}
