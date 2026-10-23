import { get } from 'svelte/store'
import { SvelteMap } from 'svelte/reactivity'
import { authState, type AuthState } from '../../auth/store'

export type CreateAttempt = symbol
export interface PendingCreate {
  readonly attempt: CreateAttempt
  readonly phase: 'sending' | 'uncertain'
}

/** Minimal session memory: no drafts, filters, responses or GET authorization.
 * Attempts are never reused, including after a tenant leaves and re-enters. */
export class HoldCreateOwner {
  private retired = $state(false)
  private readonly pending = new SvelteMap<string, PendingCreate>()

  get active(): boolean { return !this.retired }
  forTenant(tenant: string): PendingCreate | undefined { return this.pending.get(tenant) }

  begin(tenant: string): CreateAttempt | undefined {
    if (!this.active || !tenant || this.pending.has(tenant)) return
    const attempt = Symbol()
    this.pending.set(tenant, { attempt, phase: 'sending' })
    return attempt
  }
  matches(tenant: string, attempt: CreateAttempt): boolean {
    return this.active && this.pending.get(tenant)?.attempt === attempt
  }
  uncertain(tenant: string, attempt: CreateAttempt): void {
    if (this.matches(tenant, attempt)) this.pending.set(tenant, { attempt, phase: 'uncertain' })
  }
  clear(tenant: string, attempt: CreateAttempt): boolean {
    if (!this.matches(tenant, attempt)) return false
    this.pending.delete(tenant)
    return true
  }
  retire(): void { this.retired = true; this.pending.clear() }
}

function binding(state: AuthState): string {
  const session = state.session
  return JSON.stringify([state.status, session?.subject, session?.platform, session?.tenantId,
    session?.accessToken, session?.idToken, session?.expiresAt, session ? [...session.permissions].sort() : []])
}

/** Owned above Shell's route/tenant keys. Store subscription is deliberately
 * synchronous: even auth A -> B -> A in one frame retires the original owner. */
export function holdCreateSession() {
  let owner = $state.raw(new HoldCreateOwner())
  let lastBinding = binding(get(authState))
  const unsubscribe = authState.subscribe((state) => {
    const next = binding(state)
    if (next === lastBinding) return
    lastBinding = next
    owner.retire()
    owner = new HoldCreateOwner()
  })
  return {
    get owner(): HoldCreateOwner { return owner },
    dispose(): void { unsubscribe(); owner.retire(); lastBinding = '' },
  }
}
