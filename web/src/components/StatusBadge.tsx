import { IrisBadge, type IrisBadgeTone } from '@iris-ui-kit/react'

const successStates = new Set([
  'success',
  'succeeded',
  'completed',
  'approved',
  'valid',
  'ledgered',
])
const dangerStates = new Set(['failed', 'rejected', 'invalid', 'conflict'])
const warningStates = new Set(['pending', 'pending_approval', 'accepted', 'running'])

export function StatusBadge({ value }: { value: string | boolean }) {
  const text = String(value)
  const normalized = text.toLowerCase()
  let tone: IrisBadgeTone = 'neutral'
  if (successStates.has(normalized) || value === true) tone = 'success'
  else if (dangerStates.has(normalized) || value === false) tone = 'danger'
  else if (warningStates.has(normalized)) tone = 'warning'
  return (
    <IrisBadge tone={tone} variant="subtle">
      {text}
    </IrisBadge>
  )
}
