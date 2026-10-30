<script lang="ts">
  import { onDestroy, untrack } from 'svelte'
  import { IrisButton, IrisCard, IrisFormField, IrisInput } from '@iris-ui-kit/svelte'
  import { useAuditApi } from '../../api/useAuditApi'
  import { RequestScope } from '../../api/requestScope'
  import type { IntegrityResult } from '../../api/types'
  import ErrorNotice from '../../components/ErrorNotice.svelte'
  import StatusBadge from '../../components/StatusBadge.svelte'
  import { formatDate } from '../../components/format'
  import { isIntegrityResult } from './integrityResult'

  const invalidResult = '完整性结果无法核对'
  let { tenantId }: { tenantId?: string } = $props()
  const api = useAuditApi()
  const requests = new RequestScope()
  let lastTenant = untrack(() => tenantId)
  let streamId = $state('')
  let result = $state<IntegrityResult | null>(null)
  let error = $state<unknown>(null)
  let busy = $state(false)
  const announcement = $derived(
    busy
      ? '完整性验证中…'
      : error
        ? '请求失败'
        : result
          ? result.valid
            ? '验证通过'
            : '发现完整性问题'
          : '尚未验证',
  )

  $effect(() => {
    const currentTenant = tenantId
    if (currentTenant === lastTenant) return () => requests.invalidate()
    lastTenant = currentTenant
    requests.invalidate()
    streamId = ''
    result = null
    error = null
    busy = false
    return () => requests.invalidate()
  })
  onDestroy(() => requests.dispose())

  function changeStream(value: string): void {
    requests.invalidate()
    streamId = value
    result = null
    error = null
    busy = false
  }

  function submit(event: SubmitEvent): void {
    event.preventDefault()
    void verify()
  }

  function cancel(): void {
    if (!busy) return
    // Abort the transport and advance the generation so a fetcher that ignores
    // AbortSignal still cannot publish its late success, failure or finish.
    requests.invalidate()
    busy = false
    result = null
    error = null
  }

  async function verify(): Promise<void> {
    // Form submits, Enter and stale DOM events all share this single guard.
    if (busy) return
    const ticket = requests.next()
    const scopeTenant = tenantId
    const scopeStream = streamId.trim()
    busy = true
    result = null
    error = null
    try {
      const next = await api.verifyIntegrity(scopeStream, scopeTenant, ticket.signal)
      if (!ticket.current()) return
      if (!isIntegrityResult(next, scopeTenant, scopeStream)) throw new Error(invalidResult)
      result = next
    } catch (cause) {
      if (ticket.current()) error = cause
    } finally {
      if (ticket.current()) busy = false
      ticket.finish()
    }
  }
</script>

<IrisCard variant="outline" class="section-card" style="" header={undefined} footer={undefined}>
  <h2 class="card-heading">账本完整性验证</h2>
  <p class="card-copy">留空验证整个租户，或指定 stream_id。有效证明已入账事实的完整性，不代表所有业务事实都已经到达。</p>
  <form class="inline-form" aria-label="账本完整性验证" aria-busy={busy} onsubmit={submit}>
    <IrisFormField label="Stream ID"><IrisInput value={streamId} placeholder="可选" oninput={(event) => changeStream(event.currentTarget.value)} /></IrisFormField>
    <IrisButton type="submit" variant="solid" disabled={busy}>{busy ? '验证中…' : '开始验证'}</IrisButton>
    {#if busy}<IrisButton type="button" variant="outline" onclick={cancel}>取消等待</IrisButton>{/if}
  </form>
  <p class="muted-copy">取消等待只停止本页等待并丢弃晚到结果，不撤销服务端验证或必要的自审计事实，也不会自动重新验证。</p>
  <p class="muted-copy" role="status" aria-live="polite" aria-atomic="true">{announcement}</p>
  <ErrorNotice {error} />
  {#if result}
    <div class="result-panel">
      <StatusBadge value={result.valid} />
      <strong>{result.valid ? '验证通过' : '发现完整性问题'}</strong>
      <span>事件 {result.event_count} · 分段 {result.segment_count}</span>
      <span>范围：{result.stream_id ? `Stream ID ${result.stream_id}` : '整个租户'}</span>
      <span>检查时间：<time datetime={result.checked_at}>{formatDate(result.checked_at)}</time></span>
    </div>
    <p class="muted-copy">计数是本次核对范围，不单独证明完整性。</p>
    {#if result.errors?.length}<ul>{#each result.errors as item, index (index)}<li>{item}</li>{/each}</ul>{/if}
  {/if}
</IrisCard>
