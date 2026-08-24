import * as React from 'react'

export function useHashRoute(fallback = 'overview') {
  const read = React.useCallback(
    () => window.location.hash.replace(/^#\/?/, '') || fallback,
    [fallback],
  )
  const [route, setRoute] = React.useState(read)
  React.useEffect(() => {
    const update = () => setRoute(read())
    window.addEventListener('hashchange', update)
    return () => window.removeEventListener('hashchange', update)
  }, [read])
  const navigate = React.useCallback((next: string) => {
    window.location.hash = next
  }, [])
  return { route, navigate }
}
