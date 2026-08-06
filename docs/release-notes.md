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
