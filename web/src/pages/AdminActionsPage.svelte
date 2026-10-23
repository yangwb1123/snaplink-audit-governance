<script lang="ts">
  import { onDestroy, untrack } from 'svelte'
  import { IrisButton, IrisCard, IrisFormField, IrisInput } from '@iris-ui-kit/svelte'
  import { RequestScope } from '../api/requestScope'
  import type { AdminAction } from '../api/types'
  import { useAuditApi } from '../api/useAuditApi'
  import { can } from '../auth/claims'
  import { authState } from '../auth/store'
  import PageHeader from '../components/PageHeader.svelte'
  import ReadState from '../components/ReadState.svelte'
  import { formatDate } from '../components/format'
  import { ADMIN_ACTION_LIMIT, adminActionsContext, filterAdminActions, type AdminActionFilters } from './adminActions'

  // Shell supplies a platform tenant only after resolving its available list.
  let { tenantId }: { tenantId?: string } = $props()
  const readable = $derived(Boolean($authState.session && can($authState.session, 'audit:policy:read')))
  const scopeTenantId = $derived.by(() => {
    const session = $authState.session
    if (!session) return ''
    if (session.platform) return tenantId || ''
    return !tenantId || tenantId === session.tenantId ? session.tenantId : ''
  })
  const allowed = $derived(readable && Boolean(scopeTenantId))
  const requests = new RequestScope()
  let actions = $state<AdminAction[]>([])
  let filters = $state<AdminActionFilters>({ action: '', actor: '', target: '' })
  let status = $state<'idle' | 'loading' | 'success' | 'error'>('idle')
  let error = $state<unknown>(null)
  let loadedContext = $state('')

  // Capture primitive values, not just session identity: a renewed token or a
  // changed permission set must fence a transport that ignores AbortSignal.
  // This key and all filters remain in component memory, never in navigation.
  function scopeKey(): string {
    return adminActionsContext($authState.session, tenantId)
  }
  const context = $derived(scopeKey())
  const currentContext = $derived(loadedContext === context)
  const loaded = $derived(currentContext ? actions : [])
  const matching = $derived(filterAdminActions(loaded, filters))
  onDestroy(() => requests.dispose())

  async function refresh(): Promise<void> {
    if (!allowed || !currentContext) return
    const tenant = scopeTenantId
    const expectedContext = context
    const request = requests.next()
    const current = () => request.current() && scopeKey() === expectedContext && allowed
    actions = []; error = null; status = 'loading'
    try {
      const result = await useAuditApi().listAdminActions(tenant, ADMIN_ACTION_LIMIT, request.signal)
      if (!current()) return
      if (result.items.some((item) => item.tenant_id !== tenant)) throw new Error('管理自审计记录与当前租户不匹配')
      // Ignore count: this API exposes neither a historical total nor a cursor.
      // Keep the server's order and never display beyond the requested window.
      actions = result.items.slice(0, ADMIN_ACTION_LIMIT)
      status = 'success'
    } catch (cause) {
      if (current()) { error = cause; status = 'error' }
    } finally { request.finish() }
  }

  $effect(() => {
    const key = context
    untrack(() => {
      requests.invalidate()
      loadedContext = key
      actions = []; filters = { action: '', actor: '', target: '' }
      error = null; status = 'idle'
      void refresh()
    })
    return () => requests.invalidate()
  })
</script>

<section>
  {#snippet headerActions()}
    <IrisButton variant="outline" disabled={!allowed || status === 'loading'} onclick={() => void refresh()}>刷新最近记录</IrisButton>
  {/snippet}
  <PageHeader title="管理操作自审计" description="只读查看当前租户最近的管理操作；筛选仅作用于本页已载入记录。" actions={headerActions} />
  <p class="muted-copy">最近最多 {ADMIN_ACTION_LIMIT} 条，保持服务端顺序。这不是全量历史；即使不足 {ADMIN_ACTION_LIMIT} 条也不表示完整，不提供全局总数或完整性证明。</p>
  <p class="table-hint">刷新与重试仅重新读取最近记录，不执行管理动作。筛选仅保存在页面内存，不写入 URL、浏览器持久存储或日志。</p>
  {#if !readable}
    <p role="alert">当前会话没有管理自审计读取权限，不会发出请求。</p>
  {:else if !allowed}
    <p role="status">请先选择有效租户；未确认租户范围前不会读取管理自审计。</p>
  {:else}
    <IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
      <div class="three-field-form">
        <IrisFormField label="动作（本页筛选）"><IrisInput aria-label="筛选动作" value={currentContext ? filters.action : ''} oninput={(event) => (filters.action = event.currentTarget.value)} /></IrisFormField>
        <IrisFormField label="操作者（本页筛选）"><IrisInput aria-label="筛选操作者" value={currentContext ? filters.actor : ''} oninput={(event) => (filters.actor = event.currentTarget.value)} /></IrisFormField>
        <IrisFormField label="目标类型或 ID（本页筛选）"><IrisInput aria-label="筛选目标" value={currentContext ? filters.target : ''} oninput={(event) => (filters.target = event.currentTarget.value)} /></IrisFormField>
      </div>
      <p class="table-hint">不区分大小写的文本包含匹配；多个条件须同时满足，不追加请求。</p>
      <IrisButton size="sm" variant="outline" onclick={() => (filters = { action: '', actor: '', target: '' })}>清空筛选</IrisButton>
      <p role="status" aria-live="polite">已载入 {loaded.length} 条 · 筛选命中 {matching.length} 条（仅本页最近记录）</p>
      <ReadState loading={!currentContext || status === 'loading'} error={currentContext ? error : null} empty={status === 'success' && loaded.length === 0} loadingLabel="正在读取最近管理自审计…" emptyLabel="当前租户最近记录列表为空；不代表全历史为空。" onRetry={() => void refresh()}>
        {#if status === 'success' && matching.length === 0}
          <p role="status">已载入的最近记录中没有筛选命中；未搜索更早记录。</p>
        {:else if status === 'success'}
          <div class="data-table-wrap">
            <table class="data-table">
              <caption>当前租户最近管理操作（最多 {ADMIN_ACTION_LIMIT} 条，本地筛选）</caption>
              <thead><tr><th>时间</th><th>动作</th><th>操作者</th><th>目标类型</th><th>目标 ID</th><th>详情</th></tr></thead>
              <tbody>
                {#each matching as action (action.id)}
                  <tr><td>{formatDate(action.created_at)}</td><td>{action.action}</td><td>{action.actor}</td><td>{action.target_type || '—'}</td><td>{action.target_id || '—'}</td><td>{action.detail || '—'}</td></tr>
                {/each}
              </tbody>
            </table>
          </div>
        {/if}
      </ReadState>
    </IrisCard>
  {/if}
</section>
