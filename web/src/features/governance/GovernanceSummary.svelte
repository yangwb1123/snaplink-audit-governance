<script lang="ts">
  import { onDestroy, untrack } from 'svelte'
  import { IrisCard } from '@iris-ui-kit/svelte'
  import { useAuditApi } from '../../api/useAuditApi'
  import { RequestScope } from '../../api/requestScope'
  import type { GovernanceStatus } from '../../api/types'
  import { can } from '../../auth/claims'
  import { authState } from '../../auth/store'
  import ErrorNotice from '../../components/ErrorNotice.svelte'
  import LoadingBlock from '../../components/LoadingBlock.svelte'
  import { formatDate } from '../../components/format'

  let { tenantId, revision = 0 }: { tenantId?: string; revision?: number } = $props()
  const api = useAuditApi()
  const requests = new RequestScope()
  let status = $state<GovernanceStatus | null>(null)
  let error = $state<unknown>(null)
  let loading = $state(false)
  const exportReadable = $derived(Boolean($authState.session && can($authState.session, 'audit:export:create')))
  const restoreReadable = $derived(Boolean($authState.session && can($authState.session, 'audit:operation:read')))
  const holdReadable = $derived(Boolean($authState.session && can($authState.session, 'audit:legal_hold:manage')))
  const policyReadable = $derived(Boolean($authState.session && can($authState.session, 'audit:policy:read')))
  const readable = $derived(exportReadable || restoreReadable || holdReadable || policyReadable)
  const scopedTenant = $derived($authState.session?.platform ? tenantId : $authState.session?.tenantId)
  // In-memory only; never put the token/identity binding in DOM or storage.
  const identityKey = $derived($authState.session ? JSON.stringify([
    $authState.session.subject, $authState.session.platform, $authState.session.tenantId,
    [...$authState.session.permissions].sort(), $authState.session.accessToken,
  ]) : '')

  async function load(scopeTenant: string | undefined, identity: string, permitted: boolean): Promise<void> {
    const ticket = requests.next()
    status = null
    error = null
    loading = false
    if (!permitted || !scopeTenant?.trim()) { ticket.finish(); return }
    loading = true
    try {
      const next = await api.getGovernanceStatus(scopeTenant, ticket.signal)
      if (!ticket.current() || identity !== identityKey || scopeTenant !== scopedTenant) return
      if (next.tenant_id !== scopeTenant || next.source !== 'control' || next.completeness !== 'not_asserted' ||
          (next.retention?.policy && next.retention.policy.tenant_id !== scopeTenant) ||
          (next.restores && next.restores.execution !== 'not_tracked') ||
          (exportReadable && next.exports && (!Number.isSafeInteger(next.exports.cancelled) || next.exports.cancelled < 0))) {
        throw new Error('治理摘要与当前租户或数据语义不匹配')
      }
      status = next
    } catch (cause) {
      if (ticket.current() && identity === identityKey && scopeTenant === scopedTenant) error = cause
    } finally {
      if (ticket.current() && identity === identityKey && scopeTenant === scopedTenant) loading = false
      ticket.finish()
    }
  }

  $effect(() => {
    const scopeTenant = scopedTenant
    const identity = identityKey
    const permitted = readable
    void tenantId
    void revision
    untrack(() => void load(scopeTenant, identity, permitted))
    return () => requests.invalidate()
  })
  onDestroy(() => requests.dispose())
</script>

{#if readable}
  <section aria-label="租户治理摘要">
    <h2 class="card-heading">租户治理摘要</h2>
    <p class="card-copy">当前保留的治理元数据，不限最近 24 小时。不是账本、发布、投影或业务执行的完整性证明。</p>
    <ErrorNotice {error} onRetry={() => void load(scopedTenant, identityKey, readable)} />
    {#if !scopedTenant?.trim()}
      <p class="card-copy">请先选择一个已授权租户。</p>
    {:else if loading}
      <LoadingBlock />
    {:else if status}
      <p class="card-copy">观察时间：{formatDate(status.observed_at)} · 来源：控制面 · 完整性未声明</p>
      <div class="dashboard-grid">
        {#if exportReadable && status.exports}
          <IrisCard variant="outline" style="" header={undefined} footer={undefined}>
            <h3 class="card-heading">导出作业</h3>
            <p>排队 {status.exports.pending} · 运行 {status.exports.running} · 已完成 {status.exports.completed} · 失败 {status.exports.failed} · 已取消 {status.exports.cancelled}</p>
            <p class="card-copy">租户排队及运行上限 {status.exports.tenant_queue_limit}，运行上限 {status.exports.tenant_running_limit}；不是当前可用配额。</p>
            {#if status.exports.oldest_pending_at}<p class="card-copy">最早排队创建时间：{formatDate(status.exports.oldest_pending_at)}</p>{/if}
            {#if status.exports.unknown > 0}<p>未知作业状态 {status.exports.unknown}，需核对。</p>{/if}
            <p class="card-copy">完成不代表可下载，详情及下载仍须通过 Legal Hold 和证据认证。</p>
            <a href="#evidence/exports">查看导出工作台</a>
          </IrisCard>
        {/if}
        {#if restoreReadable && status.restores}
          <IrisCard variant="outline" style="" header={undefined} footer={undefined}>
            <h3 class="card-heading">恢复治理决策</h3>
            <p>待审批 {status.restores.pending_approval} · 已批准 {status.restores.approved} · 已拒绝 {status.restores.rejected}</p>
            <p>我的待审批申请 {status.restores.mine_pending_approval}{#if holdReadable && status.restores.actionable !== undefined} · 待我决策 {status.restores.actionable}{/if}</p>
            {#if status.restores.unknown > 0}<p>未知决策状态 {status.restores.unknown}，需核对。</p>{/if}
            <p class="card-copy">批准不是业务执行成功；本接口不跟踪业务执行。待决策计数只是提示，最终审批仍检查权限与职责分离。</p>
            <a href="#evidence/restores">查看恢复工作台</a>
          </IrisCard>
        {/if}
        {#if holdReadable && status.legal_holds}
          <IrisCard variant="outline" style="" header={undefined} footer={undefined}>
            <h3 class="card-heading">Legal Hold</h3>
            <p>有效 {status.legal_holds.active} · 已释放 {status.legal_holds.released}</p>
            <p class="card-copy">治理记录数不代表受保护事件或对象版本数，也不推断哪些导出会被阻断。</p>
            <a href="#evidence/holds">查看保全工作台</a>
          </IrisCard>
        {/if}
        {#if policyReadable && status.retention}
          <IrisCard variant="outline" style="" header={undefined} footer={undefined}>
            <h3 class="card-heading">留存策略配置</h3>
            {#if status.retention.configured && status.retention.policy}
              <p>类别 {status.retention.policy.retention_class} · Hot {status.retention.policy.hot_days} 天 · Warm {status.retention.policy.warm_days} 天 · Archive {status.retention.policy.archive_days} 天</p>
            {:else if !status.retention.configured}
              <p>未配置留存策略，不推断默认期限。</p>
            {:else}
              <p>策略已配置但详情不可用，请刷新核对。</p>
            {/if}
            <p class="card-copy">只是配置，不是生命周期执行、对象 COMPLIANCE 留存到期或可删除证明。</p>
          </IrisCard>
        {/if}
      </div>
    {/if}
  </section>
{/if}
