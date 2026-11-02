<script lang="ts">
  import { untrack } from 'svelte'
  import OperationHistory from './OperationHistory.svelte'

  let { tenantId = 'tenant-a', initialId = 'op-a' }: { tenantId?: string; initialId?: string } = $props()
  let currentTenant = $state(untrack(() => tenantId))
  let currentInitialId = $state(untrack(() => initialId))
</script>

<div class="harness-controls">
  <button type="button" onclick={() => (currentTenant = 'tenant-b')}>切换到租户 B</button>
  <button type="button" onclick={() => (currentTenant = 'tenant-a')}>切换到租户 A</button>
  <button type="button" onclick={() => (currentInitialId = 'op-b')}>初始 ID 改为 B</button>
  <button type="button" onclick={() => (currentInitialId = 'op-a')}>初始 ID 恢复 A</button>
</div>
<OperationHistory tenantId={currentTenant} initialId={currentInitialId} />
