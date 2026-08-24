import * as React from 'react'
import {
  IrisAdminLayout,
  IrisBadge,
  IrisButton,
  IrisIcon,
  IrisToastViewport,
  useSkin,
  type NavNode,
} from '@iris-ui-kit/react'
import { useAuditApi } from './api/useAuditApi'
import type { Tenant } from './api/types'
import { can } from './auth/claims'
import { useAuth } from './auth/AuthProvider'
import { LoadingBlock } from './components/Page'
import { useHashRoute } from './router'

const CatalogPage = React.lazy(() =>
  import('./pages/CatalogPage').then((module) => ({ default: module.CatalogPage })),
)
const EventsPage = React.lazy(() =>
  import('./pages/EventsPage').then((module) => ({ default: module.EventsPage })),
)
const EvidencePage = React.lazy(() =>
  import('./pages/EvidencePage').then((module) => ({ default: module.EvidencePage })),
)
const OperationsPage = React.lazy(() =>
  import('./pages/OperationsPage').then((module) => ({ default: module.OperationsPage })),
)
const OverviewPage = React.lazy(() =>
  import('./pages/OverviewPage').then((module) => ({ default: module.OverviewPage })),
)

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

function PageHost({ route, tenantId }: { route: string; tenantId?: string }) {
  let page: React.ReactNode
  switch (route) {
    case 'events':
      page = <EventsPage tenantId={tenantId} />
      break
    case 'operations':
      page = <OperationsPage tenantId={tenantId} />
      break
    case 'evidence':
      page = <EvidencePage tenantId={tenantId} />
      break
    case 'catalog':
      page = <CatalogPage tenantId={tenantId} />
      break
    default:
      page = <OverviewPage tenantId={tenantId} />
  }
  return <React.Suspense fallback={<LoadingBlock label="正在加载页面…" />}>{page}</React.Suspense>
}

export function Shell() {
  const { session, logout } = useAuth()
  const api = useAuditApi()
  const { skin, setSkin, setMode } = useSkin()
  const { route, navigate } = useHashRoute()
  const [tenantId, setTenantId] = React.useState(session?.tenantId ?? '')
  const [tenants, setTenants] = React.useState<Tenant[]>([])

  const pages = React.useMemo(
    () =>
      pageDefinitions.filter(
        (page) => !page.permission || (session && can(session, page.permission)),
      ),
    [session],
  )
  const menus = React.useMemo<NavNode[]>(
    () => pages.map((page, index) => ({ ...page, order: index + 1 })),
    [pages],
  )
  const activeRoute = pages.some((page) => page.key === route) ? route : 'overview'

  React.useEffect(() => {
    if (route !== activeRoute) navigate(activeRoute)
  }, [activeRoute, navigate, route])

  React.useEffect(() => {
    let active = true
    if (!session?.platform) return
    api
      .listTenants()
      .then((result) => {
        if (active) setTenants(result.items)
      })
      .catch(() => {
        if (active) setTenants([])
      })
    return () => {
      active = false
    }
  }, [api, session?.platform])

  const toggleTheme = () => {
    setMode('fixed')
    setSkin(skin.type === 'dark' ? 'light' : 'dark')
  }

  const toolbar = (
    <div className="shell-toolbar">
      {session?.platform ? (
        <label className="tenant-picker">
          <span>租户范围</span>
          <select value={tenantId} onChange={(event) => setTenantId(event.target.value)}>
            <option value="">全部租户 / 未指定</option>
            {tenants.map((tenant) => (
              <option key={tenant.id} value={tenant.id}>
                {tenant.name} ({tenant.id})
              </option>
            ))}
          </select>
        </label>
      ) : (
        <IrisBadge tone="neutral">tenant: {session?.tenantId || '未设置'}</IrisBadge>
      )}
      <IrisButton variant="outline" size="sm" aria-label="切换主题" onClick={toggleTheme}>
        <IrisIcon name={skin.type === 'dark' ? 'sun' : 'moon'} size={16} />
      </IrisButton>
      <div className="session-summary">
        <span>{session?.displayName}</span>
        <small>{session?.roles.join(', ') || '无角色'}</small>
      </div>
      <IrisButton variant="outline" size="sm" onClick={() => void logout()}>
        退出
      </IrisButton>
    </div>
  )

  return (
    <>
      <IrisAdminLayout
        menus={menus}
        activeKey={activeRoute}
        onActiveKeyChange={navigate}
        appTitle="Audit Governance"
        toolbar={toolbar}
        showTabs={false}
        contentWidth="fluid"
        footer={
          <div className="shell-footer">
            <span>Snaplink 认证授权 · Audit Governance 不可变账本</span>
            <span>令牌仅保存在页面内存</span>
          </div>
        }
      >
        <PageHost route={activeRoute} tenantId={tenantId || undefined} />
      </IrisAdminLayout>
      <IrisToastViewport position="bottom-right" />
    </>
  )
}
