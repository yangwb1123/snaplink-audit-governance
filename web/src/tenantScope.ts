export const TENANTS_CHANGED_EVENT = 'audit-governance:tenants-changed'

export interface TenantOption {
  id: string
}

export function resolveTenantSelection(
  tenants: readonly TenantOption[],
  currentSelection: string,
  preferredTenantId = '',
): string {
  const availableIds = new Set(tenants.map((tenant) => tenant.id))
  if (currentSelection && availableIds.has(currentSelection)) return currentSelection
  if (preferredTenantId && availableIds.has(preferredTenantId)) return preferredTenantId
  return tenants[0]?.id ?? ''
}
