<script lang="ts">
  import { IrisButton, IrisDialog, IrisDialogContent, IrisDialogDescription, IrisDialogTitle } from '@iris-ui-kit/svelte'

  let {
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
  } = $props()
</script>

<IrisDialog {open} onOpenChange={onOpenChange} closeOnOutsideClick={!busy}>
  <IrisDialogContent class="confirm-dialog">
    <IrisDialogTitle>{title}</IrisDialogTitle>
    <IrisDialogDescription>{description}</IrisDialogDescription>
    <div class="dialog-actions">
      <IrisButton variant="outline" disabled={busy} onclick={() => onOpenChange(false)}>取消</IrisButton>
      <IrisButton variant="solid" disabled={busy} loading={busy} onclick={() => void onConfirm()}>
        {busy ? '提交中…' : confirmLabel}
      </IrisButton>
    </div>
  </IrisDialogContent>
</IrisDialog>
