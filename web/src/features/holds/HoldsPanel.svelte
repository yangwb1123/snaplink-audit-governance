<script lang="ts">
  import { onDestroy, untrack } from 'svelte'
  import { IrisButton, IrisCard, IrisFormField, IrisInput } from '@iris-ui-kit/svelte'
  import { useAuditApi } from '../../api/useAuditApi'
  import { RequestScope } from '../../api/requestScope'
  import { ApiError } from '../../api/client'
  import type { LegalHold } from '../../api/types'
  import { authState } from '../../auth/store'
  import { can } from '../../auth/claims'
  import type { CreateAttempt, HoldCreateOwner } from './createOwner.svelte'
  import { record, sameQuery, validHold } from './validation'
  import ConfirmDialog from '../../components/ConfirmDialog.svelte'
  import ErrorNotice from '../../components/ErrorNotice.svelte'
  import StatusBadge from '../../components/StatusBadge.svelte'
  import { formatDate } from '../../components/format'
  import EvidenceScopeFields, { type DateRange } from '../../pages/EvidenceScopeFields.svelte'
  import { buildEvidenceScopeQuery, defaultEvidenceRange, emptyEvidenceScope, evidenceScopeSummary, type EvidenceScopeDraft } from '../../pages/evidenceScope'

  let { tenantId, createOwner }: { tenantId?: string; createOwner: HoldCreateOwner } = $props()
  const reads = new RequestScope()
  const writes = new RequestScope()
  const session = $derived($authState.session)
  const scopedTenant = $derived(session?.platform ? tenantId : session?.tenantId)
  const scopeReady = $derived(Boolean($authState.status === 'authenticated' && session &&
    can(session, 'audit:legal_hold:manage') && scopedTenant && (!tenantId || tenantId === scopedTenant)))
  const identity = $derived(JSON.stringify([tenantId, scopedTenant]))
  let lastIdentity = untrack(() => identity)
  let lastOwner = untrack(() => createOwner)
  let alive = true
  let sending: { owner: HoldCreateOwner; tenant: string; attempt: CreateAttempt } | null = null
  let range = $state<DateRange>(defaultEvidenceRange(30))
  let scope = $state<EvidenceScopeDraft>({ ...emptyEvidenceScope })
  let items = $state<LegalHold[]>([])
  let name = $state('')
  let reason = $state('')
  let readError = $state<unknown>(null)
  let writeError = $state<unknown>(null)
  let readState = $state<'idle' | 'loading' | 'success' | 'error'>('idle')
  let busy = $state(false)
  // GET authorization is local to this mounted instance, never the session.
  type Observation = { identity: string; reviewedAttempt: CreateAttempt | null }
  let observation = $state<Observation | null>(null)
  type ReleaseConfirmation = { hold: LegalHold; observation: Observation }
  let pendingRelease = $state<ReleaseConfirmation | null>(null)
  const pendingCreate = $derived(scopedTenant ? createOwner.forTenant(scopedTenant) : undefined)
  const loading = $derived(readState === 'loading')
  const canStartDraft = $derived(Boolean(pendingCreate?.phase === 'uncertain' && readState === 'success' &&
    observation?.identity === identity && pendingCreate.attempt === observation?.reviewedAttempt))

  function clearDraft(): void {
    name = ''; reason = ''; range = defaultEvidenceRange(30); scope = { ...emptyEvidenceScope }
  }
  function revokeObservation(): void {
    observation = null; pendingRelease = null; items = []; readState = 'idle'; readError = null
  }
  function invalidateScope(): void {
    reads.invalidate(); writes.invalidate()
    // Abort/unmount is not proof of rollback. Only the originating live panel
    // can accept its response; the owner preserves the constraint on exit.
    if (sending) sending.owner.uncertain(sending.tenant, sending.attempt)
    sending = null
    revokeObservation(); busy = false
  }
  $effect(() => {
    const key = identity
    const owner = createOwner
    const ready = scopeReady
    untrack(() => {
      if (key !== lastIdentity || owner !== lastOwner) {
        invalidateScope(); clearDraft(); writeError = null
        lastIdentity = key; lastOwner = owner
      }
      if (ready) void load()
    })
  })
  onDestroy(() => { alive = false; invalidateScope(); reads.dispose(); writes.dispose() })

  function currentScope(): boolean {
    return alive && createOwner.active && createOwner === lastOwner && scopeReady && identity === lastIdentity &&
      Boolean(session && session.expiresAt > Date.now())
  }
  async function load(): Promise<void> {
    if (!currentScope() || !scopedTenant) return
    const ticket = reads.next()
    const key = identity
    const tenant = scopedTenant
    const owner = createOwner
    // A GET started while sending cannot later become a reconciliation GET.
    const pending = owner.forTenant(tenant)
    const reviewedAttempt = pending?.phase === 'uncertain' ? pending.attempt : null
    const current = () => ticket.current() && currentScope() && key === identity && owner === createOwner
    // Revoke rows, release confirmation and prior acknowledgement synchronously.
    revokeObservation(); readState = 'loading'
    try {
      const result: unknown = await useAuditApi().listLegalHolds(tenant, ticket.signal)
      if (!current()) return
      if (!record(result) || !Number.isInteger(result.count) || Number(result.count) < 0 ||
          !Array.isArray(result.items) || !result.items.every((hold) => validHold(hold, tenant)) ||
          new Set(result.items.map((hold) => hold.id)).size !== result.items.length) {
        throw new Error('法律保留列表结构无效或与当前租户不匹配')
      }
      items = result.items
      observation = { identity: key, reviewedAttempt }; readState = 'success'
    } catch (cause) {
      if (current()) { readError = cause; readState = 'error' }
    } finally {
      ticket.finish()
    }
  }

  async function create(): Promise<void> {
    // Handler guards also fence forced/stale DOM events and same-frame clicks.
    if (!currentScope() || !scopedTenant || busy || createOwner.forTenant(scopedTenant) || !name.trim() || !reason.trim()) return
    const query = buildEvidenceScopeQuery(range, scope)
    if (!query) { writeError = new Error('请选择有效且非空的保留时间范围'); return }
    const requested = { name: name.trim(), reason: reason.trim(), filter: query }
    const tenant = scopedTenant
    const owner = createOwner
    const key = identity
    const subject = session?.subject
    // Register synchronously BEFORE the non-idempotent API invocation.
    const attempt = owner.begin(tenant)
    if (!attempt) return
    sending = { owner, tenant, attempt }
    const ticket = writes.next()
    const current = () => ticket.current() && currentScope() && key === identity && owner === createOwner && owner.matches(tenant, attempt)
    busy = true; writeError = null; reads.invalidate(); revokeObservation()
    try {
      const next: unknown = await useAuditApi().createLegalHold(requested, tenant, ticket.signal)
      if (!current()) return
      if (!validHold(next, tenant) || next.name !== requested.name || next.reason !== requested.reason ||
          next.created_by !== subject || next.released_at !== undefined || next.released_by !== undefined || !sameQuery(next.filter, query)) {
        throw new Error('创建结果与当前请求不匹配，请只读刷新核对')
      }
      owner.clear(tenant, attempt); sending = null; clearDraft(); busy = false
      void load()
    } catch (cause) {
      if (!current()) return
      writeError = cause; busy = false; sending = null
      // Explicit contract rejection only. Network/499/5xx/invalid success stay unknown.
      if (cause instanceof ApiError && [400, 401, 403, 404, 422, 429].includes(cause.status)) owner.clear(tenant, attempt)
      else owner.uncertain(tenant, attempt)
    } finally {
      ticket.finish()
    }
  }

  function startNewDraft(attempt: CreateAttempt): void {
    if (!currentScope() || !scopedTenant || busy || !canStartDraft || observation?.reviewedAttempt !== attempt) return
    if (!createOwner.clear(scopedTenant, attempt)) return
    clearDraft(); writeError = null; pendingRelease = null
  }
  function confirmRelease(hold: LegalHold): void {
    if (!currentScope() || busy || readState !== 'success' || !observation || observation.identity !== identity ||
        !items.includes(hold) || hold.released_at) return
    pendingRelease = { hold, observation }
  }
  async function release(confirmation: ReleaseConfirmation): Promise<void> {
    if (!currentScope() || !scopedTenant || busy || pendingRelease !== confirmation || readState !== 'success' ||
        !observation || confirmation.observation !== observation || observation.identity !== identity ||
        !items.includes(confirmation.hold) || confirmation.hold.tenant_id !== scopedTenant || confirmation.hold.released_at) return
    const { hold } = confirmation
    const ticket = writes.next()
    const key = identity
    const tenant = scopedTenant
    const owner = createOwner
    const current = () => ticket.current() && currentScope() && key === identity && owner === createOwner
    busy = true; writeError = null; reads.invalidate(); revokeObservation()
    try {
      const next: unknown = await useAuditApi().releaseLegalHold(hold.id, tenant, ticket.signal)
      if (!current()) return
      if (!validHold(next, tenant) || next.id !== hold.id || !next.released_at) throw new Error('释放结果与当前保留不匹配，请只读刷新核对')
      busy = false; void load()
    } catch (cause) {
      if (current()) { writeError = cause; busy = false }
    } finally {
      ticket.finish()
    }
  }
</script>

<IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
  <h2 class="card-heading">法律保留</h2>
  <p class="card-copy">有效保留会阻断命中证据的导出创建、详情和下载。释放平台保留不会缩短对象存储已有的 COMPLIANCE 保留期。</p>
  <div class="three-field-form">
    <IrisFormField label="名称"><IrisInput value={name} disabled={busy || !scopeReady} oninput={(event) => (name = event.currentTarget.value)} /></IrisFormField>
    <IrisFormField label="原因"><IrisInput value={reason} disabled={busy || !scopeReady} oninput={(event) => (reason = event.currentTarget.value)} /></IrisFormField>
    <IrisButton variant="solid" disabled={!scopeReady || busy || Boolean(pendingCreate) || !name.trim() || !reason.trim()} onclick={() => void create()}>{busy ? '处理中…' : '创建范围保留'}</IrisButton>
  </div>
  <EvidenceScopeFields {range} {scope} disabled={busy || !scopeReady} onRangeChange={(next) => (range = next)} onScopeChange={(field, value) => (scope = { ...scope, [field]: value })} />
  <ErrorNotice error={writeError} onRetry={() => void load()} />
  {#each pendingCreate?.phase === 'uncertain' ? [pendingCreate] : [] as pending (pending.attempt)}
    <p role="status" class="muted-copy">创建可能已提交；请先只读刷新核对，勿重复创建。列表没有创建幂等键，空列表或同名记录都不能确定原请求是否已提交。核对不会自动重发。</p>
    <div class="button-row">
      <IrisButton variant="outline" disabled={!scopeReady || loading || busy} onclick={() => void load()}>刷新核对</IrisButton>
      <IrisButton variant="outline" disabled={!canStartDraft || busy || !scopeReady} onclick={() => startNewDraft(pending.attempt)}>已核对，开始新草稿</IrisButton>
    </div>
  {/each}
  <div class="filter-footer"><span role="status">{loading ? '读取中…' : readState === 'success' ? `${items.length} 条保留` : readState === 'error' ? '保留读取失败，状态未知' : '尚未读取保留'}</span><IrisButton size="sm" variant="outline" disabled={!scopeReady || loading} onclick={() => void load()}>刷新保留</IrisButton></div>
  <ErrorNotice error={readError} onRetry={() => void load()} />
  {#if readState === 'success' && !items.length}<p class="muted-copy">当前租户尚无法律保留。</p>{/if}
  {#if readState === 'success' && items.length}
    <table class="native-table">
      <thead><tr><th>名称与原因</th><th>范围</th><th>创建者 / 时间</th><th>状态</th><th>操作</th></tr></thead>
      <tbody>{#each items as hold (hold.id)}<tr>
        <td>{hold.name}<p class="muted-copy">{hold.reason}</p></td><td>{evidenceScopeSummary(hold.filter)}</td>
        <td>{hold.created_by}<p>{formatDate(hold.created_at)}</p></td><td><StatusBadge value={hold.released_at ? 'released' : 'active'} /></td>
        <td>{#if !hold.released_at}<IrisButton size="sm" variant="outline" disabled={busy || !scopeReady} onclick={() => confirmRelease(hold)}>释放</IrisButton>{/if}</td>
      </tr>{/each}</tbody>
    </table>
  {/if}
  {#each pendingRelease ? [pendingRelease] : [] as confirmation (confirmation)}
    <ConfirmDialog open={true} title="确认释放法律保留" description={`释放 ${confirmation.hold.name} 将写入治理自审计；其他保留和底层 COMPLIANCE 保护仍然有效。`} confirmLabel="确认释放" {busy} onOpenChange={(open) => !open && pendingRelease === confirmation && (pendingRelease = null)} onConfirm={() => release(confirmation)} />
  {/each}
</IrisCard>
