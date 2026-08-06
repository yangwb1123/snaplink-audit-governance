# Release Notes

## 2026-08-06 — Archive enablement keyed on the configured Store

**Behavior change (fix):** archiving is now enabled based on the configured
`archive.Store`, not the `ArchiveDir` string. In S3-only deployments (an S3
store injected via `external.Archive()` while `Config.ArchiveDir` stays empty)
the ingest gate now archives events and `ArchivePending` retries pending
receipts. Previously both treated the destination as unconfigured: ingest
skipped archiving and receipts stalled at `StatusIndexed` forever.

Details:

- New `archive.Configured(Store)` predicate (nil-safe; empty-dir `FileStore` →
  unconfigured, S3 and injected stores → configured), shared by the ingest
  gate, `ArchivePending`, and the readyz probe so the call sites cannot
  re-diverge.
- `FileStore.Put` now rejects an empty `Dir` before any filesystem access
  (same message as `Ready`) — previously it silently wrote into the process
  CWD.
- readyz now skips unconfigured stores instead of panicking on a nil store.

Only behavior flips: the S3-only bug itself, and the broken split-brain
configuration `&FileStore{Dir:""}` + `ArchiveDir:"/x"` (silent CWD writes →
loud `ErrInvalid`). Local file-mode behavior is unchanged. No migration
needed; rollback = redeploy the previous binary.

## 2026-08-06 — FileStore.Put fails loudly on corruption instead of reporting archived

**Behavior change (fix):** `archive.FileStore.Put` no longer treats *any*
pre-existing path as "already archived". Previously a write/sync failure
mid-Put left a truncated partial object at the final key, every later retry
hit EEXIST and returned nil, and the receipt was marked `StatusArchived` —
a permanently corrupt object in a WORM compliance archive with no repair
path. The same EEXIST branch silently accepted symlinks and directories.

New `FileStore.Put` contract (no signature/API/route changes):

- A pre-existing path that is not a regular file (symlink, directory,
  device) is an error.
- An existing regular file is only "already archived" when it is
  byte-identical to the new payload; a mismatch — or a file that cannot be
  read, and therefore cannot be verified — is an error, and the pre-existing
  object is never modified or removed.
- Any Write/Sync/Close error after the object was created removes it
  best-effort, keeping the invariant "Put returned an error ⇒ no object at
  the key". O_EXCL/`0o440` and the idempotent byte-identical retry behavior
  are unchanged.

**Impact:** identical retries (the normal replay path) still return nil;
only previously-silent corruption becomes loud. Ingest degrades such
failures to `StatusIndexed` (unchanged service behavior), so receipts stay
retryable via `ArchivePending` — operators must clear the blocking object
first. S3Store is unaffected. No data/config migration; rolling deploy with
a transient window where old and new binaries disagree on mismatch verdicts
(old: nil, new: error); rollback = redeploy the previous binary.

## Operator runbook — archive mismatch errors

**Symptoms:** `ArchivePending` returns `archive path ... already exists with
different content` (or `... cannot be verified`); affected receipts remain
`StatusIndexed` and are retried on every `ArchivePending` run.

**Diagnosis:** for the failing key (see `archiveEvent`/`archiveSegment` key
formats in `internal/service/service.go`), compare the object on disk with
the expected canonical JSON:

```sh
ls -la <archive-dir>/<key>
# regenerate the expected payload, e.g. for an event:
#   jq -S . <(git show HEAD:internal/service/service_test.go)  # reference only
od -c <archive-dir>/<key>   # truncated/corrupt content = crash-leftover partial
```

**Cause classification:**

1. Crash- or error-leftover partial object from a failed Put under the old
   binary (pre-deploy). The object is *not* the true event payload → safe
   to remove, then re-run `ArchivePending`.
2. Tampered object (an external process wrote the path). Removing it is a
   deliberate WORM exception — confirm with the tenant/compliance owner
   first; the re-archived payload is re-verified by `VerifyIntegrity`.
3. Genuine key collision (different payload, same key). Do **not** remove:
   investigate the producer; the receipt correctly refuses to mark it
   archived.

**Repair:** after removing a confirmed-partial object, re-run
`ArchivePending <tenant>` (or `audit-governance-worker`'s pending-archive
job); the receipt converges to `StatusArchived` only after a byte-identical
write. If `os.Remove` itself fails (read-only filesystem, permissions),
fix the destination and retry — the receipt stays `StatusIndexed` and is
never falsely reported archived.

**Verification:** `python3 cli.py check`; archive unit tests
`go test ./internal/archive/` cover the mismatch, unreadable, non-regular,
and write-fault (EFBIG/read-only) legs.
