import * as React from 'react'
import { loadRuntimeConfig, redirectUri } from '../config'
import { authorizationUrl, completeAuthorization, logoutUrl } from './oauth'
import type { UserSession } from './types'

type AuthStatus = 'checking' | 'anonymous' | 'authenticated' | 'error'

interface AuthContextValue {
  status: AuthStatus
  session: UserSession | null
  error: string | null
  login(): Promise<void>
  logout(): Promise<void>
  invalidate(): void
  clearError(): void
}

const AuthContext = React.createContext<AuthContextValue | null>(null)

export function AuthProvider({ children }: { children: React.ReactNode }): React.ReactElement {
  const config = React.useMemo(loadRuntimeConfig, [])
  const [status, setStatus] = React.useState<AuthStatus>('checking')
  const [session, setSession] = React.useState<UserSession | null>(null)
  const [error, setError] = React.useState<string | null>(null)
  const callbackPromiseRef = React.useRef<Promise<UserSession> | null>(null)

  React.useEffect(() => {
    let active = true
    const bootstrap = async () => {
      if (window.location.pathname !== new URL(redirectUri(config)).pathname) {
        setStatus('anonymous')
        return
      }
      try {
        callbackPromiseRef.current ??= completeAuthorization(new URL(window.location.href), {
          config,
        })
        const next = await callbackPromiseRef.current
        if (!active) return
        setSession(next)
        setStatus('authenticated')
        window.history.replaceState(null, '', `${config.appBasePath}#overview`)
      } catch (cause) {
        if (!active) return
        setError(cause instanceof Error ? cause.message : 'Snaplink 登录失败')
        setStatus('error')
        window.history.replaceState(null, '', config.appBasePath)
      }
    }
    void bootstrap()
    return () => {
      active = false
    }
  }, [config])

  React.useEffect(() => {
    if (!session) return
    const remaining = session.expiresAt - Date.now()
    if (remaining <= 0) {
      setSession(null)
      setStatus('anonymous')
      return
    }
    const timer = window.setTimeout(
      () => {
        setSession(null)
        setStatus('anonymous')
        setError('Snaplink 会话已过期，请重新登录')
      },
      Math.min(remaining, 2_147_483_647),
    )
    return () => window.clearTimeout(timer)
  }, [session])

  const login = React.useCallback(async () => {
    setError(null)
    setStatus('checking')
    try {
      window.location.assign(await authorizationUrl({ config }))
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : '无法连接 Snaplink')
      setStatus('error')
    }
  }, [config])

  const logout = React.useCallback(async () => {
    const idToken = session?.idToken
    setSession(null)
    setStatus('anonymous')
    if (!idToken) return
    try {
      const target = await logoutUrl(config, idToken)
      if (target) window.location.assign(target)
    } catch {
      window.history.replaceState(null, '', config.appBasePath)
    }
  }, [config, session])

  const invalidate = React.useCallback(() => {
    setSession(null)
    setStatus('anonymous')
    setError('Audit API 拒绝了当前会话，请重新登录')
  }, [])

  const value = React.useMemo<AuthContextValue>(
    () => ({
      status,
      session,
      error,
      login,
      logout,
      invalidate,
      clearError: () => setError(null),
    }),
    [error, invalidate, login, logout, session, status],
  )
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth(): AuthContextValue {
  const context = React.useContext(AuthContext)
  if (!context) throw new Error('useAuth must be used inside AuthProvider')
  return context
}
