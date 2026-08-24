import {
  IrisButton,
  IrisDialog,
  IrisDialogContent,
  IrisDialogDescription,
  IrisDialogTitle,
} from '@iris-ui-kit/react'

export function ConfirmDialog({
  open,
  title,
  description,
  confirmLabel,
  busy = false,
  onOpenChange,
  onConfirm,
}: {
  open: boolean
  title: string
  description: string
  confirmLabel: string
  busy?: boolean
  onOpenChange: (open: boolean) => void
  onConfirm: () => Promise<void>
}) {
  return (
    <IrisDialog open={open} onOpenChange={onOpenChange} closeOnOutsideClick={!busy}>
      <IrisDialogContent className="confirm-dialog">
        <IrisDialogTitle>{title}</IrisDialogTitle>
        <IrisDialogDescription>{description}</IrisDialogDescription>
        <div className="dialog-actions">
          <IrisButton variant="outline" disabled={busy} onClick={() => onOpenChange(false)}>
            取消
          </IrisButton>
          <IrisButton variant="solid" disabled={busy} onClick={() => void onConfirm()}>
            {busy ? '提交中…' : confirmLabel}
          </IrisButton>
        </div>
      </IrisDialogContent>
    </IrisDialog>
  )
}
