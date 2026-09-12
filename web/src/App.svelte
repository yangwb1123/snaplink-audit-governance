<script lang="ts">
  import { IrisProvider, IrisSpinner, SkinProvider } from '@iris-ui-kit/svelte'
  import AuthProvider from './auth/AuthProvider.svelte'
  import { authState } from './auth/store'
  import { skinEngine } from './skin'
  import LoginPage from './pages/LoginPage.svelte'
  import Shell from './Shell.svelte'

  const loadingLabel = '正在校验 Snaplink 登录回调…'
</script>

<SkinProvider engine={skinEngine}>
  <IrisProvider locale="zh-CN">
    <AuthProvider>
      {#if $authState.status === 'checking'}
        <main class="bootstrap-page" role="status">
          <IrisSpinner size="lg" style="" />
          <span>{loadingLabel}</span>
        </main>
      {:else if $authState.session}
        <Shell />
      {:else}
        <LoginPage />
      {/if}
    </AuthProvider>
  </IrisProvider>
</SkinProvider>
