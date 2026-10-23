import type { AdminAction } from '../api/types'
import type { UserSession } from '../auth/types'

export const ADMIN_ACTION_LIMIT = 100

/** Memory-only identity fence; never use in URLs, storage or diagnostics. */
export function adminActionsContext(session: UserSession | null, tenantId?: string): string {
  return JSON.stringify([
    tenantId ?? null,
    session ? [session.subject, session.accessToken, session.idToken, session.platform,
      session.tenantId, [...session.permissions].sort()] : null,
  ])
}

export interface AdminActionFilters {
  action: string
  actor: string
  target: string
}

/** Literal, case-insensitive AND filters over the already-loaded recent window.
 * No sorting, pagination, network requests or persistence belong here. */
export function filterAdminActions(items: readonly AdminAction[], filters: AdminActionFilters): AdminAction[] {
  const action = filters.action.trim().toLowerCase()
  const actor = filters.actor.trim().toLowerCase()
  const target = filters.target.trim().toLowerCase()
  return items.filter((item) =>
    item.action.toLowerCase().includes(action) &&
    item.actor.toLowerCase().includes(actor) &&
    `${item.target_type ?? ''} ${item.target_id ?? ''}`.toLowerCase().includes(target),
  )
}
