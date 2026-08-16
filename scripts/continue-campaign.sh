#!/bin/bash
# Continue the snaplink-audit-governance improvement campaign with ai-batch-runner.
# Loops --retry-failed rounds until a round passes with zero failures.
#
# Quota awareness: when the provider reports a hard quota/billing block
# (Weekly usage limit / Insufficient balance / CreditsError), the supervisor
# sleeps 1h between attempts instead of spinning every 120s — the block is
# time-based (resets in ~22h) and retries cannot clear it.
#
# Usage: nohup bash scripts/continue-campaign.sh > logs/campaign-supervisor.out 2>&1 &
set -u
cd "$(dirname "$0")/.." || exit 1

LOG="logs/maintenance-r7.log"
sleep_long=3600   # hard quota block: re-check hourly
sleep_short=120   # ordinary failure: quick retry

quota_blocked() {
  tail -200 "$LOG" 2>/dev/null | grep -qE "GoUsageLimitError|Weekly usage limit|Insufficient balance|CreditsError"
}

round=1
while true; do
  echo "[supervisor] $(date +%H:%M:%S) ===== continuation round $round ====="
  # --jobs 1: serial analysis (fewer concurrent provider connections; the
  # box often runs several sibling pi-batch campaigns that exhaust quota).
  python3 ~/ai-batch-runner/pi-batch.py campaign \
    --config .pi-batch/campaign.yaml \
    --retry-failed \
    --rounds 1 \
    --round-delay 120 \
    --jobs 1 \
    --log-file "$LOG" >> logs/maintenance-r7.out 2>&1
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
  if quota_blocked; then
    echo "[supervisor] $(date +%H:%M:%S) provider quota/billing block detected; re-checking in ${sleep_long}s"
    sleep "$sleep_long"
  else
    echo "[supervisor] $(date +%H:%M:%S) retrying failed directions in ${sleep_short}s"
    sleep "$sleep_short"
  fi
done
