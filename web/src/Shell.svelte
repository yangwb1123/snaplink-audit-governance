<script lang="ts">
  import { onMount } from 'svelte'
  import {
    IrisAdminLayout,
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
  import CatalogPage from './pages/CatalogPage.svelte'
  import EventsPage from './pages/EventsPage.svelte'
  import EvidencePage from './pages/EvidencePage.svelte'
  import OperationsPage from './pages/OperationsPage.svelte'
  import OverviewPage from './pages/OverviewPage.svelte'

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
  let tenantId = $state($authState.session?.tenantId ?? '')
  let tenants = $state<Tenant[]>([])

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

  $effect(() => {
    const session = $authState.session
    const platform = Boolean(session?.platform)
    if (!platform) {
      tenants = []
      return
    }
    let active = true
    void api
      .listTenants()
      .then((result) => {
        if (active) tenants = result.items
      })
      .catch(() => {
        if (active) tenants = []
      })
    return () => {
      active = false
    }
  })

  onMount(() => {
    const update = () => (route = readRoute())
    window.addEventListener('hashchange', update)
    return () => window.removeEventListener('hashchange', update)
  })
</script>

{#snippet toolbar()}
  {#if $authState.session?.platform}
    <label class="tenant-picker">
      <span>租户范围</span>
      <select bind:value={tenantId} aria-label="租户范围">
        <option value="">全部租户 / 未指定</option>
        {#each tenants as tenant (tenant.id)}
          <option value={tenant.id}>{tenant.name} ({tenant.id})</option>
        {/each}
      </select>
    </label>
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
