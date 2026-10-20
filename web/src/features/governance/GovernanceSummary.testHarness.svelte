<script lang="ts">
  import type { Writable } from 'svelte/store'
  import GovernanceSummary from './GovernanceSummary.svelte'
  let { scope }: { scope: Writable<{ tenantId?: string; revision: number }> } = $props()
</script>

<GovernanceSummary tenantId={$scope.tenantId} revision={$scope.revision} />
