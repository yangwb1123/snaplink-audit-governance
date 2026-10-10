<script lang="ts">
  import { onDestroy, untrack } from 'svelte'
  import { IrisButton, IrisCard, IrisFormField, IrisInput } from '@iris-ui-kit/svelte'
  import { useAuditApi } from '../../api/useAuditApi'
  import { RequestScope } from '../../api/requestScope'
  import type { IntegrityResult } from '../../api/types'
  import ErrorNotice from '../../components/ErrorNotice.svelte'
  import StatusBadge from '../../components/StatusBadge.svelte'

  let { tenantId }: { tenantId?: string } = $props()
  const api = useAuditApi()
  const requests = new RequestScope()
  let lastTenant = untrack(() => tenantId)
  let streamId = $state('')
  let result = $state<IntegrityResult | null>(null)
  let error = $state<unknown>(null)
  let busy = $state(false)

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

  async function verify(): Promise<void> {
    const ticket = requests.next()
    const scopeTenant = tenantId
    busy = true
    result = null
    error = null
    try {
      const next = await api.verifyIntegrity(streamId.trim(), scopeTenant, ticket.signal)
      if (!ticket.current()) return
      if (scopeTenant && next.tenant_id !== scopeTenant) throw new Error('完整性结果与当前租户不匹配')
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
  <div class="inline-form">
    <IrisFormField label="Stream ID"><IrisInput value={streamId} placeholder="可选" oninput={(event) => changeStream(event.currentTarget.value)} /></IrisFormField>
    <IrisButton variant="solid" disabled={busy} onclick={() => void verify()}>{busy ? '验证中…' : '开始验证'}</IrisButton>
  </div>
  <ErrorNotice {error} />
  {#if result}
    <div class="result-panel"><StatusBadge value={result.valid} /><span>事件 {result.event_count ?? 0} · 分段 {result.segment_count ?? 0}</span></div>
    {#if result.errors?.length}<ul>{#each result.errors as item (item)}<li>{item}</li>{/each}</ul>{/if}
  {/if}
</IrisCard>
