<script lang="ts">
  import { onMount, untrack } from 'svelte'
  import {
    IrisAdminLayout,
    IrisAlert,
    IrisBadge,
    IrisButton,
    IrisIcon,
    IrisToastViewport,
    useSkin,
  } from '@iris-ui-kit/svelte'
  import { useAuditApi } from './api/useAuditApi'
  import type { Tenant } from './api/types'
  import { can } from './auth/claims'
  import { authState, logout } from './auth/store'
  import type { UserSession } from './auth/types'
  import CatalogPage from './pages/CatalogPage.svelte'
  import EventsPage from './pages/EventsPage.svelte'
  import EvidencePage from './pages/EvidencePage.svelte'
  import OperationsPage from './pages/OperationsPage.svelte'
  import OverviewPage from './pages/OverviewPage.svelte'
  import { resolveTenantSelection, TENANTS_CHANGED_EVENT } from './tenantScope'

  interface PageDefinition {
    key: string
    title: string
    icon: string
    permission?: string
  }

  const pageDefinitions: PageDefinition[] = [
    { key: 'overview', title: '审计态势', icon: 'home' },
    { key: 'events', title: '审计事件', icon: 'table', permission: 'audit:event:read' },
    { key: 'operations', title: '操作时间线', icon: 'clock', permission: 'audit:operation:read' },
    { key: 'evidence', title: '证据与治理', icon: 'shield', permission: 'audit:integrity:verify' },
    { key: 'catalog', title: '治理目录', icon: 'settings', permission: 'audit:policy:read' },
  ]

  const api = useAuditApi()
  const { skin, setSkin, setMode } = useSkin()
  let route = $state(readRoute())
  let tenantId = $state($authState.session?.platform ? '' : $authState.session?.tenantId ?? '')
  let tenants = $state<Tenant[]>([])
  let tenantLoading = $state(false)
  let tenantLoadError = $state<unknown>(null)
  let tenantListSessionKey = $state('')
  let tenantRequestGeneration = 0
  let lastTenantListSessionKey = ''

  const visiblePages = $derived(
    pageDefinitions.filter(
      (page) => !page.permission || ($authState.session && can($authState.session, page.permission)),
    ),
  )
  const menus = $derived(
    visiblePages.map((page, index) => ({ ...page, order: index + 1 })),
  )
  const activeRoute = $derived(
    visiblePages.some((page) => page.key === route) ? route : 'overview',
  )
  const isDark = $derived($skin.type === 'dark')
  const sessionScopeKey = $derived(
    $authState.session
      ? JSON.stringify([
          $authState.session.subject,
          $authState.session.platform,
          $authState.session.tenantId,
        ])
      : '',
  )
  // Readiness tracks the last-known-good platform scope, never the transient
  // refresh flags: reloading an established scope must not unmount the routed
  // page (and therefore must not drop the page's drafts or re-run its queries).
  const platformScopeEstablished = $derived(
    Boolean(
      $authState.session?.platform &&
        tenantListSessionKey === sessionScopeKey &&
        tenants.some((tenant) => tenant.id === tenantId),
    ),
  )
  const tenantScopeReady = $derived(
    $authState.session?.platform
      ? platformScopeEstablished
      : Boolean($authState.session?.tenantId && tenantId === $authState.session.tenantId),
  )
  const platformCatalogBootstrap = $derived(
    Boolean($authState.session?.platform && activeRoute === 'catalog' && !tenantId),
  )

  function readRoute(): string {
    return window.location.hash.replace(/^#\/?/, '') || 'overview'
  }

  function navigate(next: string): void {
    window.location.hash = next
  }

  function toggleTheme(): void {
    setMode('fixed')
    setSkin(isDark ? 'sunrise' : 'ocean')
  }

  $effect(() => {
    const current = activeRoute
    if (route !== current) navigate(current)
  })

  async function loadTenants(session: UserSession | null): Promise<void> {
    const generation = ++tenantRequestGeneration
    const scopeKey = session
      ? JSON.stringify([session.subject, session.platform, session.tenantId])
      : ''

    if (!session) {
      tenants = []
      tenantId = ''
      tenantLoading = false
      tenantLoadError = null
      tenantListSessionKey = ''
      lastTenantListSessionKey = ''
      return
    }

    if (!session.platform) {
      tenants = []
      tenantId = session.tenantId
      tenantLoading = false
      tenantLoadError = null
      tenantListSessionKey = scopeKey
      lastTenantListSessionKey = scopeKey
      return
    }

    // A refresh keeps the established scope (list, selection, session keys) until
    // the new list resolves; only an initial load or a session-scope change wipes it.
    const refreshingScope = lastTenantListSessionKey === scopeKey
    const scopeEstablished =
      refreshingScope && untrack(() => tenants.some((tenant) => tenant.id === tenantId))
    const currentSelection = refreshingScope ? untrack(() => tenantId) : ''
    if (!scopeEstablished) {
      tenantListSessionKey = ''
      lastTenantListSessionKey = ''
    }
    tenantLoading = true
    tenantLoadError = null
    try {
      const result = await api.listTenants()
      if (generation !== tenantRequestGeneration) return
      const available = [...result.items].sort(
        (left, right) => left.name.localeCompare(right.name, 'zh-CN') || left.id.localeCompare(right.id),
      )
      tenants = available
      tenantId = resolveTenantSelection(available, currentSelection, session.tenantId)
      tenantListSessionKey = scopeKey
      lastTenantListSessionKey = scopeKey
    } catch (cause) {
      if (generation !== tenantRequestGeneration) return
      tenantLoadError = cause
      if (scopeEstablished) return
      tenants = []
      tenantId = ''
      tenantListSessionKey = ''
      lastTenantListSessionKey = ''
    } finally {
      if (generation === tenantRequestGeneration) tenantLoading = false
    }
  }

  function refreshTenants(): void {
    void loadTenants($authState.session)
  }

  $effect(() => {
    const session = $authState.session
    void session?.subject
    void session?.platform
    void session?.tenantId
    void loadTenants(session)
    return () => {
      tenantRequestGeneration += 1
    }
  })

  onMount(() => {
    const update = () => (route = readRoute())
    const updateTenants = () => refreshTenants()
    window.addEventListener('hashchange', update)
    window.addEventListener(TENANTS_CHANGED_EVENT, updateTenants)
    return () => {
      window.removeEventListener('hashchange', update)
      window.removeEventListener(TENANTS_CHANGED_EVENT, updateTenants)
    }
  })
</script>

{#snippet toolbar()}
  {#if $authState.session?.platform}
    <div class="tenant-picker">
      <label for="tenant-scope">租户范围</label>
      <select
        id="tenant-scope"
        bind:value={tenantId}
        aria-label="租户范围"
        disabled={tenantLoading || Boolean(tenantLoadError) || tenants.length === 0}
      >
        <option value="" disabled>请选择租户</option>
        {#each tenants as tenant (tenant.id)}
          <option value={tenant.id}>{tenant.name} ({tenant.id})</option>
        {/each}
      </select>
      {#if tenantLoading}
        <small role="status">读取租户…</small>
      {:else if tenantLoadError}
        <small role="alert">租户列表读取失败</small>
      {:else if tenants.length === 0}
        <small role="status">没有可用租户</small>
      {/if}
      <IrisButton variant="outline" size="sm" disabled={tenantLoading} onclick={refreshTenants}>刷新租户</IrisButton>
    </div>
  {:else}
    <IrisBadge tone="neutral">tenant: {$authState.session?.tenantId || '未设置'}</IrisBadge>
  {/if}
  <IrisButton variant="outline" size="sm" aria-label="切换主题" onclick={toggleTheme}>
    <IrisIcon name={isDark ? 'sun' : 'moon'} size={16} />
  </IrisButton>
  <div class="session-summary">
    <span>{$authState.session?.displayName}</span>
    <small>{$authState.session?.roles.join(', ') || '无角色'}</small>
  </div>
  <IrisButton variant="outline" size="sm" onclick={() => void logout()}>退出</IrisButton>
{/snippet}

{#snippet footer()}
  <div class="shell-footer">
    <span>Snaplink 认证授权 · Audit Governance 不可变账本</span>
    <span>令牌仅保存在页面内存</span>
  </div>
{/snippet}

{#snippet children(_layout: { activeKey: string })}
  {#if !tenantScopeReady && !platformCatalogBootstrap}
    {#if $authState.session?.platform && tenantLoading}
      <IrisAlert tone="info" title="读取可用租户" onclose={undefined} style="" icon={undefined} titleSlot={undefined}>
        正在确认租户范围；完成前不会发出租户级审计请求。
      </IrisAlert>
    {:else if $authState.session?.platform && tenantLoadError}
      <IrisAlert tone="danger" title="无法读取可用租户" onclose={undefined} style="" icon={undefined} titleSlot={undefined}>
        租户范围验证失败，已暂停租户级请求。<IrisButton size="sm" variant="outline" onclick={refreshTenants}>重试</IrisButton>
      </IrisAlert>
    {:else if $authState.session?.platform && tenants.length === 0}
      <IrisAlert tone="warning" title="没有可用租户" onclose={undefined} style="" icon={undefined} titleSlot={undefined}>
        创建或授权至少一个租户后，才能访问租户范围内的审计页面。
      </IrisAlert>
    {:else if $authState.session?.platform}
      <IrisAlert tone="warning" title="请选择租户" onclose={undefined} style="" icon={undefined} titleSlot={undefined}>
        当前 API 路由要求明确的 tenant_id；请选择租户后再加载页面。
      </IrisAlert>
    {:else}
      <IrisAlert tone="danger" title="会话缺少租户范围" onclose={undefined} style="" icon={undefined} titleSlot={undefined}>
        当前会话没有有效的 tenant_id，已停止发出审计请求。请重新登录或联系管理员。
      </IrisAlert>
    {/if}
  {:else}
    {#if $authState.session?.platform && tenantLoadError && platformScopeEstablished}
      <IrisAlert tone="warning" title="租户列表刷新失败" onclose={undefined} style="" icon={undefined} titleSlot={undefined}>
        仍在使用上次成功读取的租户范围，页面保持可用。<IrisButton size="sm" variant="outline" onclick={refreshTenants}>重试</IrisButton>
      </IrisAlert>
    {/if}
    {#key `${activeRoute}:${tenantId}`}
      {#if activeRoute === 'events'}
        <EventsPage tenantId={tenantId || undefined} />
      {:else if activeRoute === 'operations'}
        <OperationsPage tenantId={tenantId || undefined} />
      {:else if activeRoute === 'evidence'}
        <EvidencePage tenantId={tenantId || undefined} />
      {:else if activeRoute === 'catalog'}
        <CatalogPage tenantId={tenantId || undefined} />
      {:else}
        <OverviewPage tenantId={tenantId || undefined} />
      {/if}
    {/key}
  {/if}
{/snippet}

<IrisAdminLayout
  menus={menus}
  activeKey={activeRoute}
  onActiveKeyChange={navigate}
  appTitle="Audit Governance"
  toolbar={toolbar}
  footer={footer}
  showTabs={false}
  contentWidth="fluid"
>
  {@render children({ activeKey: activeRoute })}
</IrisAdminLayout>
<IrisToastViewport position="bottom-right" />
