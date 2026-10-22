<script lang="ts">
  import CatalogPage from './CatalogPage.svelte'
  let tenantId = $state('tenant-a')
</script>
<button onclick={() => (tenantId = 'tenant-a')}>选择 A</button>
<button onclick={() => (tenantId = 'tenant-b')}>选择 B</button>
<CatalogPage {tenantId} />
