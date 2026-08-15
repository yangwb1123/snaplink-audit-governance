#!/bin/bash
# Continue the snaplink-audit-governance improvement campaign with ai-batch-runner.
# Waits for a currently-running campaign round (optional PID arg) to release the
# lock, then loops --retry-failed rounds until a round passes with zero failures.
# Usage: nohup bash scripts/continue-campaign.sh [wait-pid] > logs/campaign-supervisor.out 2>&1 &
set -u
cd "$(dirname "$0")/.." || exit 1

WAIT_PID="${1:-}"
if [ -n "$WAIT_PID" ] && kill -0 "$WAIT_PID" 2>/dev/null; then
  echo "[supervisor] $(date +%H:%M:%S) waiting for campaign pid $WAIT_PID to exit (lock release)"
  while kill -0 "$WAIT_PID" 2>/dev/null; do sleep 60; done
  echo "[supervisor] $(date +%H:%M:%S) previous round exited; starting continuation"
fi

round=1
while true; do
  echo "[supervisor] $(date +%H:%M:%S) ===== continuation round $round ====="
  python3 ~/ai-batch-runner/pi-batch.py campaign \
    --config .pi-batch/campaign.yaml \
    --retry-failed \
    --rounds 1 \
    --round-delay 120 \
    --log-file logs/maintenance-r7.log >> logs/maintenance-r7.out 2>&1
  rc=$?
  echo "[supervisor] $(date +%H:%M:%S) round $round exited rc=$rc"
  if [ "$rc" -eq 0 ]; then
    echo "[supervisor] $(date +%H:%M:%S) all directions passed; campaign complete"
    break
  fi
  if [ "$rc" -eq 130 ]; then
    echo "[supervisor] $(date +%H:%M:%S) interrupted (Ctrl-C); stopping"
    break
  fi
  round=$((round + 1))
  echo "[supervisor] $(date +%H:%M:%S) retrying failed directions in 120s"
  sleep 120
done
