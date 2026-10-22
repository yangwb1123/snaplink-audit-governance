<script lang="ts">
  import { onDestroy, untrack } from 'svelte'
  import { IrisButton, IrisDescriptions, IrisDrawer, IrisDrawerContent, IrisDrawerTitle } from '@iris-ui-kit/svelte'
  import { ApiError } from '../api/client'
  import { RequestScope } from '../api/requestScope'
  import { useAuditApi } from '../api/useAuditApi'
  import type { AuditEvent, EventReceipt } from '../api/types'
  import { can } from '../auth/claims'
  import { authState } from '../auth/store'
  import FieldDiff from '../components/FieldDiff.svelte'
  import ReadState from '../components/ReadState.svelte'
  import { formatDate } from '../components/format'
  import { eventHash } from './navigation'
  import { navigateInvestigation } from './investigationNavigation'

  let { eventId, tenantId, onClose }: { eventId: string; tenantId?: string; onClose: () => void } = $props()
  const session = $derived($authState.session)
  const allowed = $derived(Boolean(session && can(session, 'audit:event:read') && (session.platform ? tenantId : session.tenantId && (!tenantId || tenantId === session.tenantId))))
  const operationAllowed = $derived(Boolean(session && can(session, 'audit:operation:read')))
  const eventReads = new RequestScope()
  const receiptReads = new RequestScope()
  let selected = $state<AuditEvent | null>(null)
  let receipt = $state<EventReceipt | null>(null)
  let eventLoading = $state(false)
  let receiptLoading = $state(false)
  let eventError = $state<unknown>(null)
  let receiptError = $state<unknown>(null)
  let notFound = $state(false)
  onDestroy(() => { eventReads.dispose(); receiptReads.dispose() })

  function begin(scope: RequestScope) {
    const request = scope.next()
    const expectedSession = session, expectedTenant = tenantId, expectedId = eventId
    return { ...request, current: () => request.current() && expectedSession === session && expectedTenant === tenantId && expectedId === eventId && allowed }
  }

  async function loadEvent(): Promise<void> {
    if (!allowed || eventHash(eventId) === '#events') return
    const request = begin(eventReads)
    eventLoading = true; selected = null; eventError = null; notFound = false
    try {
      const event = await useAuditApi().getEvent(eventId, tenantId, request.signal)
      if (!request.current()) return
      if (event.event_id !== eventId || event.tenant_id !== (tenantId ?? session?.tenantId)) throw new Error('事件与当前租户或 ID 不匹配')
      selected = event
    } catch (cause) {
      if (!request.current()) return
      notFound = cause instanceof ApiError && cause.status === 404 && cause.code === 'not_found'
      eventError = notFound ? null : cause
    } finally { if (request.current()) eventLoading = false; request.finish() }
  }

  async function loadReceipt(): Promise<void> {
    if (!allowed || eventHash(eventId) === '#events') return
    const request = begin(receiptReads)
    receiptLoading = true; receipt = null; receiptError = null
    try {
      const next = await useAuditApi().getReceipt(eventId, tenantId, request.signal)
      if (!request.current()) return
      if (next.event_id !== eventId || next.tenant_id !== (tenantId ?? session?.tenantId)) throw new Error('回执与当前租户或 ID 不匹配')
      receipt = next
    } catch (cause) {
      if (request.current()) receiptError = cause
    } finally { if (request.current()) receiptLoading = false; request.finish() }
  }

  $effect(() => {
    void session; void tenantId; void eventId
    untrack(() => {
      eventReads.invalidate(); receiptReads.invalidate()
      selected = null; receipt = null; eventError = null; receiptError = null
      eventLoading = false; receiptLoading = false; notFound = false
      void loadEvent(); void loadReceipt()
    })
    return () => { eventReads.invalidate(); receiptReads.invalidate() }
  })
</script>

<IrisDrawer open={Boolean(eventId)} onOpenChange={(open) => !open && onClose()} size="min(720px, 92vw)">
  <IrisDrawerContent>
    <IrisDrawerTitle>审计事件详情</IrisDrawerTitle>
    <div class="drawer-body">
      {#if !allowed}<p role="alert">当前会话没有事件读取权限或有效租户，不会发出请求。</p>
      {:else}
        <ReadState loading={eventLoading} error={eventError} empty={notFound} loadingLabel="正在读取认证事件…" emptyLabel="未找到当前租户的事件" onRetry={() => void loadEvent()}>
          {#if selected}
            <IrisDescriptions style="" bordered columns={1} items={[
              { label: '事件 ID', value: selected.event_id },
              { label: '租户', value: selected.tenant_id },
              { label: '来源 / 类型', value: `${selected.source_system} / ${selected.event_type}` },
              { label: 'Schema ID (schema_id)', value: selected.schema_id || '—' },
              { label: 'Schema 版本 (schema_version)', value: selected.schema_version ?? '—' },
              { label: '留存类别 (retention_class)', value: selected.retention_class || '—' },
              { label: '数据分级 (data_classification)', value: selected.data_classification || '—' },
              { label: '操作 / 结果', value: `${selected.action} / ${selected.outcome}` },
              { label: '操作者', value: selected.actor?.name || selected.actor?.id || '—' },
              { label: '时间', value: formatDate(selected.occurred_at) },
              { label: '操作 / 因果 / 关联', value: `${selected.operation_id || '—'} / ${selected.causation_id || '—'} / ${selected.correlation_id || '—'}` },
              { label: 'Trace / Span', value: `${selected.trace_id || '—'} / ${selected.span_id || '—'}` },
              { label: '聚合对象 / 版本', value: `${selected.aggregate_type || '—'}:${selected.aggregate_id || '—'} / ${selected.aggregate_version ?? '—'}` },
              { label: '流 / 序号', value: `${selected.stream_id || '—'} / ${selected.sequence ?? '—'}` },
            ]} />
            {#if operationAllowed && (selected.operation_id || (selected.aggregate_type && selected.aggregate_id))}
              <div class="button-row">
                {#if selected.operation_id}<IrisButton size="sm" variant="outline" onclick={() => selected?.operation_id && navigateInvestigation({ mode: 'operation', operationId: selected.operation_id })}>查看操作时间线</IrisButton>{/if}
                {#if selected.aggregate_type && selected.aggregate_id}<IrisButton size="sm" variant="outline" onclick={() => selected?.aggregate_type && selected?.aggregate_id && navigateInvestigation({ mode: 'aggregate', aggregateType: selected.aggregate_type, aggregateId: selected.aggregate_id })}>查看聚合历史</IrisButton>{/if}
              </div>
            {/if}
            <h3>Payload</h3><FieldDiff changes={selected.changed_fields} />
            <pre class="json-view">{JSON.stringify(selected.payload ?? {}, null, 2)}</pre>
            <h3>Hash</h3><code class="hash-value">{selected.hash || '—'}</code>
          {/if}
        </ReadState>
        <h3>事件回执</h3>
        <ReadState loading={receiptLoading} error={receiptError} loadingLabel="正在读取回执元数据…" onRetry={() => void loadReceipt()}>
          {#if receipt}
            <IrisDescriptions style="" bordered columns={1} items={[
              { label: '回执状态', value: receipt.status },
              { label: '回执流 / 序号', value: `${receipt.stream_id ?? '—'} / ${receipt.sequence ?? '—'}` },
              { label: '账本 / 索引 / 归档', value: `${formatDate(receipt.ledgered_at)} / ${formatDate(receipt.indexed_at)} / ${formatDate(receipt.archived_at)}` },
            ]} />
            <p class="muted-copy">回执是元数据观察，不替代事件正文认证或完整性验证。</p>
          {/if}
        </ReadState>
      {/if}
    </div>
  </IrisDrawerContent>
</IrisDrawer>
