<script lang="ts">
  import { onDestroy, untrack } from 'svelte'
  import HoldsPanel from './HoldsPanel.svelte'
  import { holdCreateSession } from './createOwner.svelte'

  let { initialTenant = 'tenant-a' }: { initialTenant?: string } = $props()
  let tenantId = $state(untrack(() => initialTenant))
  const creates = holdCreateSession()
  onDestroy(() => creates.dispose())
</script>

<button onclick={() => (tenantId = 'tenant-a')}>租户 A</button>
<button onclick={() => (tenantId = 'tenant-b')}>租户 B</button>
<button onclick={() => (tenantId = '')}>未确认租户</button>
<HoldsPanel {tenantId} createOwner={creates.owner} />
