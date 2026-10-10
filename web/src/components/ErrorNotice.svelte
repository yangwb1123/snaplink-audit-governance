<script lang="ts">
  import { IrisAlert, IrisButton } from '@iris-ui-kit/svelte'
  import { ApiError } from '../api/client'
  import { errorPresentation } from '../api/errorPresentation'

  let { error, onRetry }: { error: unknown; onRetry?: () => void } = $props()

  const presentation = $derived(errorPresentation(error))

  function detail(): string | null {
    if (!(error instanceof ApiError)) return null
    return `HTTP ${error.status} · ${error.code}${error.requestId ? ` · request_id=${error.requestId}` : ''}`
  }
</script>

{#if error}
  <IrisAlert
    tone={presentation.warning ? 'warning' : 'danger'}
    title={presentation.title}
    onclose={undefined}
    style=""
    icon={undefined}
    titleSlot={undefined}
  >
    <p>{presentation.guidance}</p>
    <div class="alert-detail">
      {#if detail()}<span>{detail()}</span>{/if}
      {#if onRetry && presentation.retryable}
        <IrisButton size="sm" variant="outline" onclick={onRetry}>重试</IrisButton>
      {/if}
    </div>
  </IrisAlert>
{/if}
