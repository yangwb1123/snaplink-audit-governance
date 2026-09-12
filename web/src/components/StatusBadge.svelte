<script lang="ts">
  import { IrisBadge, type IrisBadgeTone } from '@iris-ui-kit/svelte'

  let { value }: { value: string | boolean } = $props()

  const successStates = new Set(['success', 'succeeded', 'completed', 'approved', 'valid', 'ledgered'])
  const dangerStates = new Set(['failed', 'rejected', 'invalid', 'conflict'])
  const warningStates = new Set(['pending', 'pending_approval', 'accepted', 'running'])

  function tone(): IrisBadgeTone {
    const text = String(value).toLowerCase()
    if (successStates.has(text) || value === true) return 'success'
    if (dangerStates.has(text) || value === false) return 'danger'
    if (warningStates.has(text)) return 'warning'
    return 'neutral'
  }
</script>

<IrisBadge tone={tone()} variant="subtle">{String(value)}</IrisBadge>
