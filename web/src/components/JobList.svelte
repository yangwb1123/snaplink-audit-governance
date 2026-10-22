<script lang="ts">
  import { evidenceHash } from '../pages/evidenceNavigation'
  import { onDestroy, untrack, type Snippet } from 'svelte'
  import { IrisButton, IrisCard } from '@iris-ui-kit/svelte'
  import { useAuditApi } from '../api/useAuditApi'
  import { RequestScope } from '../api/requestScope'
  import type { ExportSummary, JobQuery, RestoreRun } from '../api/types'
  import { exportFailureLabel } from '../features/exports/exportWorkflow'
  import { authState } from '../auth/store'
  import { can } from '../auth/claims'
  import ErrorNotice from './ErrorNotice.svelte'
  import StatusBadge from './StatusBadge.svelte'
  import { formatDate } from './format'

  let { kind, tenantId, revision = 0, actions }: { kind: 'exports' | 'restores'; tenantId?: string; revision?: number; actions?: Snippet<[ExportSummary | RestoreRun]> } = $props()
  const api = useAuditApi()
  const requests = new RequestScope()
  let view = $state<JobQuery['view']>('all')
  let status = $state('')
  let owner = $state('')
  let operation = $state('')
  let appliedOwner = $state('')
  let appliedOperation = $state('')
  const filtersDirty = $derived(owner.trim() !== appliedOwner || operation.trim() !== appliedOperation)
  let items = $state<Array<ExportSummary | RestoreRun>>([])
  let total = $state(0)
  let cursor = $state('')
  let history = $state<string[]>([''])
  let pageIndex = $state(0)
  let nextCursor = $state('')
  let loading = $state(false)
  let error = $state<unknown>(null)
  let refresh = $state(0)
  const canDecide = $derived(Boolean($authState.session && can($authState.session, 'audit:legal_hold:manage')))
  const statuses = $derived(kind === 'exports' ? ['pending', 'running', 'completed', 'failed', 'cancelled'] : ['pending_approval', 'approved', 'rejected'])

  $effect(() => {
    void kind; void tenantId; void revision; void view; void status; void refresh
    history = ['']
    pageIndex = 0
    untrack(() => { void load('') })
    return () => requests.invalidate()
  })
  onDestroy(() => requests.dispose())

  async function load(position: string): Promise<void> {
    const ticket = requests.next()
    const scopeTenant = tenantId
    const query: JobQuery = { status, view, requested_by: appliedOwner, operation_id: appliedOperation, cursor: position, page_size: 50 }
    loading = true
    items = []
    nextCursor = ''
    error = null
    try {
      const page = await (kind === 'exports' ? api.listExports(query, scopeTenant, ticket.signal) : api.listRestores(query, scopeTenant, ticket.signal))
      if (!ticket.current()) return
      if (scopeTenant && page.items.some((item) => item.tenant_id !== scopeTenant)) throw new Error('任务列表与当前租户不匹配')
      items = page.items
      total = page.count
      cursor = position
      nextCursor = page.next_cursor ?? ''
    } catch (cause) {
      if (ticket.current()) error = cause
    } finally {
      if (ticket.current()) loading = false
      ticket.finish()
    }
  }

  function next(): void {
    if (!nextCursor || loading || filtersDirty) return
    history = [...history.slice(0, pageIndex + 1), nextCursor]
    pageIndex += 1
    void load(nextCursor)
  }

  function previous(): void {
    if (pageIndex === 0 || loading || filtersDirty) return
    pageIndex -= 1
    void load(history[pageIndex])
  }

  function requester(item: ExportSummary | RestoreRun): string {
    return 'requested_by' in item && typeof item.requested_by === 'string' ? item.requested_by : (item as RestoreRun).created_by ?? '—'
  }
</script>

<IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
  <h2 class="card-heading">{kind === 'exports' ? '导出任务' : '恢复申请'}</h2>
  <div class="four-field-form">
    <label>任务视图<select class="native-control" aria-label="任务视图" bind:value={view}><option value="all">全部</option><option value="mine">我的申请</option>{#if kind === 'restores' && canDecide}<option value="actionable">待我决策</option>{/if}</select></label>
    <label>任务状态<select class="native-control" aria-label="任务状态" bind:value={status}><option value="">全部状态</option>{#each statuses as value (value)}<option {value}>{value}</option>{/each}</select></label>
    <label>申请人<input class="native-control" aria-label="任务申请人" bind:value={owner} placeholder="subject" /></label>
    <label>Operation ID<input class="native-control" aria-label="任务操作标识" bind:value={operation} placeholder="可选" /></label>
  </div>
  <IrisButton size="sm" variant="outline" disabled={loading} onclick={() => { appliedOwner = owner.trim(); appliedOperation = operation.trim(); refresh += 1 }}>刷新任务</IrisButton>
  {#if filtersDirty}<p role="status" class="muted-copy">筛选已修改；点击刷新任务应用，分页将从第一页重新开始。</p>{/if}
  <ErrorNotice {error} onRetry={() => void load(cursor)} />
  <p class="muted-copy">{loading ? '读取任务…' : `当前筛选共 ${total} 条，当前页 ${items.length} 条`}。运行列表实时更新，不是证据快照。</p>
  {#if !loading && !error && !items.length}<p role="status">当前条件下没有任务。</p>{/if}
  <table class="native-table">
    <thead><tr><th>任务 ID</th><th>申请人</th><th>创建时间</th><th>状态</th>{#if actions}<th>本人操作</th>{/if}</tr></thead>
    <tbody>{#each items as item (item.id)}<tr>
      <td><a href={evidenceHash(kind, item.id)}>{item.id}</a></td><td>{requester(item)}</td><td>{formatDate(item.created_at)}</td><td><StatusBadge value={item.status} />
        {#if kind === 'exports' && typeof item.failure_code === 'string'}<p class="muted-copy">{exportFailureLabel(item.failure_code)}</p>{/if}
        {#if kind === 'exports' && typeof item.retry_of === 'string'}<a href={evidenceHash('exports', item.retry_of)}>原任务</a>{/if}
      </td>{#if actions}<td>{@render actions(item)}</td>{/if}
    </tr>{/each}</tbody>
  </table>
  <div class="filter-footer"><span>第 {pageIndex + 1} 页</span><div class="button-row"><IrisButton size="sm" variant="outline" disabled={loading || filtersDirty || pageIndex === 0} onclick={previous}>上一页</IrisButton><IrisButton size="sm" variant="outline" disabled={loading || filtersDirty || !nextCursor} onclick={next}>下一页</IrisButton></div></div>
  {#if kind === 'exports'}<p class="muted-copy">列表只显示运行摘要；证据范围、事件数量、摘要及下载须再次通过法律保留与租户校验。</p>{/if}
</IrisCard>
