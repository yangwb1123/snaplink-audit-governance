<script lang="ts">
  import { IrisAlert, IrisButton } from '@iris-ui-kit/svelte'
  import { ApiError } from '../api/client'

  let { error, onRetry }: { error: unknown; onRetry?: () => void } = $props()

  function message(): string {
    return error instanceof Error ? error.message : '请求失败'
  }

  function detail(): string | null {
    if (!(error instanceof ApiError)) return null
    return `HTTP ${error.status} · ${error.code}${error.requestId ? ` · request_id=${error.requestId}` : ''}`
  }
</script>

{#if error}
  <IrisAlert
    tone={error instanceof ApiError && error.status === 403 ? 'warning' : 'danger'}
    title={message()}
    onclose={undefined}
    style=""
    icon={undefined}
    titleSlot={undefined}
  >
    <div class="alert-detail">
      {#if detail()}<span>{detail()}</span>{/if}
      {#if onRetry}
        <IrisButton size="sm" variant="outline" onclick={onRetry}>重试</IrisButton>
      {/if}
    </div>
  </IrisAlert>
{/if}
