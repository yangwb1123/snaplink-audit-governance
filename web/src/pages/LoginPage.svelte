<script lang="ts">
  import { IrisAlert, IrisButton, IrisCard, IrisIcon, IrisSpinner } from '@iris-ui-kit/svelte'
  import { authState, clearAuthError, login } from '../auth/store'
</script>

<main class="login-page">
  <IrisCard variant="outline" padding="lg" class="login-card" style="" header={undefined} footer={undefined}>
    <div class="login-brand" aria-hidden="true"><IrisIcon name="shield" size={24} /></div>
    <p class="eyebrow">SNAPLINK PLATFORM</p>
    <h1>Audit Governance</h1>
    <p class="login-copy">
      通过 Snaplink 统一登录。浏览器使用 Authorization Code + PKCE，访问令牌只保存在当前页面内存中。
    </p>
    {#if $authState.error}
      <IrisAlert
        tone="danger"
        title="登录未完成"
        closable
        onclose={clearAuthError}
        style=""
        icon={undefined}
        titleSlot={undefined}
      >
        {$authState.error}
      </IrisAlert>
    {/if}
    <IrisButton
      variant="solid"
      size="lg"
      class="login-button"
      disabled={$authState.status === 'checking'}
      onclick={() => void login()}
    >
      {#if $authState.status === 'checking'}
        <IrisSpinner size="sm" style="" />正在连接 Snaplink…
      {:else}
        <IrisIcon name="user" size={18} />使用 Snaplink 登录
      {/if}
    </IrisButton>
    <p class="login-footnote">resource: audit-governance · scope: openid profile</p>
  </IrisCard>
</main>
