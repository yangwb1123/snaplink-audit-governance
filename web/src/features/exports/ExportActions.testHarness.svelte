<script lang="ts">
  import { IrisProvider, SkinProvider } from '@iris-ui-kit/svelte'
  import { skinEngine } from '../../skin'
  import type { ExportSummary } from '../../api/types'
  import ExportActions from './ExportActions.svelte'
  let { status = 'pending', requester = 'alice', onCommitted, onRefresh }: {
    status?: string
    requester?: string
    onCommitted: (summary: ExportSummary) => void
    onRefresh: (id: string) => Promise<void>
  } = $props()
  let tenantId = $state('tenant-a')
  let id = $state('export-source')
  const job = $derived({ id, tenant_id: tenantId, requested_by: requester, status })
</script>

<SkinProvider engine={skinEngine}>
  <IrisProvider locale="zh-CN">
    <button onclick={() => (tenantId = 'tenant-b')}>选择 B</button>
    <button onclick={() => (tenantId = 'tenant-a')}>选择 A</button>
    <button onclick={() => (id = 'export-other')}>选择其他任务</button>
    <ExportActions {job} {tenantId} {onCommitted} {onRefresh} />
  </IrisProvider>
</SkinProvider>
