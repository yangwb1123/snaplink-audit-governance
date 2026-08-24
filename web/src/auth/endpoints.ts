import type { RuntimeConfig } from '../config'

export function browserEndpoint(endpoint: string, config: RuntimeConfig): string {
  if (!config.snaplinkApiBaseUrl) return endpoint
  const upstream = new URL(endpoint)
  const transport = new URL(config.snaplinkApiBaseUrl, window.location.origin)
  transport.pathname = `${transport.pathname.replace(/\/$/, '')}${upstream.pathname}`
  transport.search = upstream.search
  return transport.toString()
}
