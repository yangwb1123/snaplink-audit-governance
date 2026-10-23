<script lang="ts">
  import AdminActionsPage from './AdminActionsPage.svelte'
  let tenantId = $state<string | undefined>('tenant-a')
</script>

<button onclick={() => (tenantId = 'tenant-a')}>租户 A</button>
<button onclick={() => (tenantId = 'tenant-b')}>租户 B</button>
<button onclick={() => (tenantId = undefined)}>清除租户</button>
<AdminActionsPage {tenantId} />
