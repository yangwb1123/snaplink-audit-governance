<script lang="ts">
  import { onDestroy, untrack } from 'svelte'
  import { IrisButton } from '@iris-ui-kit/svelte'
  import { authState } from '../../auth/store'
  import { can } from '../../auth/claims'
  import { useAuditApi } from '../../api/useAuditApi'
  import { ApiError } from '../../api/client'
  import { RequestScope } from '../../api/requestScope'
  import type { ExportSummary } from '../../api/types'
  import ConfirmDialog from '../../components/ConfirmDialog.svelte'
  import ErrorNotice from '../../components/ErrorNotice.svelte'

  let { job, tenantId, onCommitted, onRefresh }: {
    job: Pick<ExportSummary, 'id' | 'tenant_id' | 'requested_by' | 'status'>
    tenantId?: string
    onCommitted: (summary: ExportSummary) => void
    onRefresh: (id: string) => Promise<void>
  } = $props()
  const requests = new RequestScope()
  const session = $derived($authState.session)
  const scopedTenant = $derived(session?.platform ? tenantId : session?.tenantId)
  const owned = $derived(Boolean(session && can(session, 'audit:export:create') &&
    scopedTenant && job.tenant_id === scopedTenant && (!tenantId || tenantId === scopedTenant) &&
    job.requested_by && job.requested_by === session.subject))
  const cancellable = $derived(owned && (job.status === 'pending' || job.status === 'running'))
  const retryable = $derived(owned && (job.status === 'failed' || job.status === 'cancelled'))
  // Identity/command binding is in memory only, never rendered or persisted.
  const binding = $derived(JSON.stringify([job.id, job.tenant_id, job.requested_by, scopedTenant,
    session?.subject, session?.platform, session?.tenantId, session?.accessToken,
    session ? [...session.permissions].sort() : []]))
  let action = $state<'cancel' | 'retry' | null>(null)
  let busy = $state(false)
  let error = $state<unknown>(null)
  let uncertain = $state(false)

  $effect(() => {
    void binding
    untrack(() => { requests.invalidate(); action = null; busy = false; error = null; uncertain = false })
    return () => requests.invalidate()
  })
  onDestroy(() => requests.dispose())

  async function refresh(): Promise<void> {
    const identity = binding
    await onRefresh(job.id)
    if (identity === binding) { uncertain = false; error = null }
  }

  async function submit(): Promise<void> {
    const selected = action
    if (!selected || busy || uncertain || (selected === 'cancel' ? !cancellable : !retryable)) {
      action = null
      return
    }
    const original = { ...job }
    const identity = binding
    const scopeTenant = scopedTenant
    const ticket = requests.next()
    busy = true; error = null
    try {
      // Capture the current token only when issuing the command; identity
      // changes invalidate late responses even if the transport ignores abort.
      const api = useAuditApi()
      const result = await (selected === 'cancel'
        ? api.cancelExport(original.id, scopeTenant, ticket.signal)
        : api.retryExport(original.id, scopeTenant, ticket.signal))
      if (!ticket.current() || identity !== binding) return
      if (result.tenant_id !== original.tenant_id || result.requested_by !== original.requested_by ||
          (selected === 'cancel' ? result.id !== original.id || result.status !== 'cancelled'
            : !result.id || result.id === original.id || result.retry_of !== original.id)) {
        throw new Error('导出操作结果与当前任务不匹配，请刷新核对')
      }
      action = null
      onCommitted(result)
    } catch (cause) {
      if (!ticket.current() || identity !== binding) return
      error = cause; action = null
      uncertain = !(cause instanceof ApiError) || cause.status >= 500 || cause.status === 499
    } finally {
      if (ticket.current() && identity === binding) busy = false
      ticket.finish()
    }
  }
</script>

{#if owned}
  <div class="button-row">
    {#if cancellable}<IrisButton size="sm" variant="outline" disabled={busy || uncertain} onclick={() => (action = 'cancel')}>取消任务</IrisButton>{/if}
    {#if retryable}<IrisButton size="sm" variant="outline" disabled={busy || uncertain} onclick={() => (action = 'retry')}>关联新任务重试</IrisButton>{/if}
  </div>
  <ErrorNotice {error} onRetry={() => void refresh()} />
  {#if uncertain}<p role="status" class="muted-copy">操作结果可能已提交；请先刷新任务核对，不要盲目重复写入。</p><IrisButton size="sm" variant="outline" onclick={() => void refresh()}>刷新任务核对</IrisButton>{/if}
  <ConfirmDialog open={action !== null} title={action === 'cancel' ? '确认取消导出任务' : '确认关联新任务重试'}
    description={action === 'cancel'
      ? `${job.id} 将撤销控制面运行权；不证明远端写入停止或回滚，不删除 WORM。`
      : `${job.id} 的原终态保持不变；新任务重新选取当前证据并检查 Legal Hold，不延续旧查询快照。重复请求返回同一后继任务。`}
    confirmLabel={action === 'cancel' ? '确认取消任务' : '确认重试'} {busy}
    onOpenChange={(open) => !open && (action = null)} onConfirm={submit} />
{/if}
