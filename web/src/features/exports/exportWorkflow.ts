export function exportFailureLabel(code: string): string {
  switch (code) {
    case 'execution_failed': return '执行失败；可创建关联新任务，仍会重新检查证据及 Legal Hold。'
    case 'legal_hold': return '执行时被 Legal Hold 阻断；重试不能绕过当前保全。'
    case 'lease_expired': return '运行租约已过期；不推断外部写入已停止或回滚。'
    case 'legacy_stuck': return '旧任务按运行年龄恢复为失败；没有心跳进度证明。'
    case 'cancelled': return '控制面已取消；不删除 WORM，也不证明外部写入已停止。'
    default: return '未识别的失败类别，请刷新核对；不推断成功或安全重试。'
  }
}
