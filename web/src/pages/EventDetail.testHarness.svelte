<script lang="ts">
  import EventDetail from './EventDetail.svelte'
  let tenantId = $state('tenant-a')
  let eventId = $state('event-a')
</script>
<button onclick={() => (tenantId = 'tenant-a')}>租户 A</button>
<button onclick={() => (tenantId = 'tenant-b')}>租户 B</button>
<button onclick={() => (eventId = 'event-a')}>事件 A</button>
<button onclick={() => (eventId = 'event-b')}>事件 B</button>
<EventDetail {eventId} {tenantId} onClose={() => (eventId = '')} />
