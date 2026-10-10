<script lang="ts">
  import { onDestroy, untrack } from 'svelte'
  import { IrisButton, IrisCard, IrisFormField, IrisInput } from '@iris-ui-kit/svelte'
  import { useAuditApi } from '../../api/useAuditApi'
  import { RequestScope } from '../../api/requestScope'
  import type { LegalHold } from '../../api/types'
  import ConfirmDialog from '../../components/ConfirmDialog.svelte'
  import ErrorNotice from '../../components/ErrorNotice.svelte'
  import StatusBadge from '../../components/StatusBadge.svelte'
  import { formatDate } from '../../components/format'
  import EvidenceScopeFields, { type DateRange } from '../../pages/EvidenceScopeFields.svelte'
  import { buildEvidenceScopeQuery, defaultEvidenceRange, emptyEvidenceScope, evidenceScopeSummary, type EvidenceScopeDraft } from '../../pages/evidenceScope'

  let { tenantId }: { tenantId?: string } = $props()
  const api = useAuditApi()
  const reads = new RequestScope()
  const writes = new RequestScope()
  let lastTenant = untrack(() => tenantId)
  let range = $state<DateRange>(defaultEvidenceRange(30))
  let scope = $state<EvidenceScopeDraft>({ ...emptyEvidenceScope })
  let items = $state<LegalHold[]>([])
  let name = $state('')
  let reason = $state('')
  let error = $state<unknown>(null)
  let loading = $state(false)
  let busy = $state(false)
  let pendingRelease = $state<LegalHold | null>(null)
  let revision = $state(0)

  $effect(() => {
    const currentTenant = tenantId
    if (currentTenant === lastTenant) return () => { reads.invalidate(); writes.invalidate() }
    lastTenant = currentTenant
    reads.invalidate()
    writes.invalidate()
    items = []
    name = ''
    reason = ''
    range = defaultEvidenceRange(30)
    scope = { ...emptyEvidenceScope }
    pendingRelease = null
    error = null
    busy = false
    return () => { reads.invalidate(); writes.invalidate() }
  })
  $effect(() => { void tenantId; void revision; void load() })
  onDestroy(() => { reads.dispose(); writes.dispose() })

  async function load(): Promise<void> {
    const ticket = reads.next()
    const scopeTenant = tenantId
    loading = true
    error = null
    try {
      const result = await api.listLegalHolds(scopeTenant, ticket.signal)
      if (!ticket.current()) return
      if (scopeTenant && result.items.some((hold) => hold.tenant_id !== scopeTenant)) throw new Error('法律保留列表与当前租户不匹配')
      items = result.items
    } catch (cause) {
      if (ticket.current()) error = cause
    } finally {
      if (ticket.current()) loading = false
      ticket.finish()
    }
  }

  async function create(): Promise<void> {
    const query = buildEvidenceScopeQuery(range, scope)
    if (!query) { error = new Error('请选择有效且非空的保留时间范围'); return }
    const ticket = writes.next()
    const scopeTenant = tenantId
    busy = true
    error = null
    try {
      const next = await api.createLegalHold({ name: name.trim(), reason: reason.trim(), filter: query }, scopeTenant, ticket.signal)
      if (!ticket.current()) return
      if (scopeTenant && next.tenant_id !== scopeTenant) throw new Error('法律保留与当前租户不匹配')
      name = ''
      reason = ''
      revision += 1
    } catch (cause) {
      if (ticket.current()) error = cause
    } finally {
      if (ticket.current()) busy = false
      ticket.finish()
    }
  }

  async function release(): Promise<void> {
    const hold = pendingRelease
    if (!hold || (tenantId && hold.tenant_id !== tenantId)) return
    const ticket = writes.next()
    const scopeTenant = tenantId
    busy = true
    error = null
    try {
      const next = await api.releaseLegalHold(hold.id, scopeTenant, ticket.signal)
      if (!ticket.current()) return
      if (scopeTenant && next.tenant_id !== scopeTenant) throw new Error('释放结果与当前租户不匹配')
      pendingRelease = null
      revision += 1
    } catch (cause) {
      if (ticket.current()) error = cause
    } finally {
      if (ticket.current()) busy = false
      ticket.finish()
    }
  }
</script>

<IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
  <h2 class="card-heading">法律保留</h2>
  <p class="card-copy">有效保留会阻断命中证据的导出创建、详情和下载。释放平台保留不会缩短对象存储已有的 COMPLIANCE 保留期。</p>
  <div class="three-field-form">
    <IrisFormField label="名称"><IrisInput value={name} disabled={busy} oninput={(event) => (name = event.currentTarget.value)} /></IrisFormField>
    <IrisFormField label="原因"><IrisInput value={reason} disabled={busy} oninput={(event) => (reason = event.currentTarget.value)} /></IrisFormField>
    <IrisButton variant="solid" disabled={busy || !name.trim() || !reason.trim()} onclick={() => void create()}>{busy ? '处理中…' : '创建范围保留'}</IrisButton>
  </div>
  <EvidenceScopeFields {range} {scope} disabled={busy} onRangeChange={(next) => (range = next)} onScopeChange={(field, value) => (scope = { ...scope, [field]: value })} />
  <ErrorNotice {error} onRetry={() => void load()} />
  <div class="filter-footer"><span>{loading ? '读取中…' : `${items.length} 条保留`}</span><IrisButton size="sm" variant="outline" disabled={loading} onclick={() => void load()}>刷新保留</IrisButton></div>
  {#if !loading && !items.length}<p class="muted-copy">当前租户尚无法律保留。</p>{/if}
  <table class="native-table">
    <thead><tr><th>名称与原因</th><th>范围</th><th>创建者 / 时间</th><th>状态</th><th>操作</th></tr></thead>
    <tbody>{#each items as hold (hold.id)}<tr>
      <td>{hold.name}<p class="muted-copy">{hold.reason}</p></td><td>{evidenceScopeSummary(hold.filter)}</td>
      <td>{hold.created_by}<p>{formatDate(hold.created_at)}</p></td><td><StatusBadge value={hold.released_at ? 'released' : 'active'} /></td>
      <td>{#if !hold.released_at}<IrisButton size="sm" variant="outline" disabled={busy} onclick={() => (pendingRelease = hold)}>释放</IrisButton>{/if}</td>
    </tr>{/each}</tbody>
  </table>
  <ConfirmDialog open={pendingRelease !== null} title="确认释放法律保留" description={`释放 ${pendingRelease?.name ?? ''} 将写入治理自审计；其他保留和底层 COMPLIANCE 保护仍然有效。`} confirmLabel="确认释放" {busy} onOpenChange={(open) => !open && (pendingRelease = null)} onConfirm={release} />
</IrisCard>
