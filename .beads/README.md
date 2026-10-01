# NBNv2 issue tracker

This repository retains its existing Beads 0.47.1 SQLite tracker and the versioned `issues.jsonl`. Do not initialize another tracker or run an installer against this directory.

The 2026-10-01 reconciliation is complete in both stores: 296 issues, 516 comments, 321 dependencies, 594 labels, 36 historical close reasons, and the deletion provenance of tombstone `NBNv2-2qo`. The database also retains all 1,505 historical events and its existing configuration and metadata.

The repaired JSONL SHA-256 is `5D78399EF6D9A6B80DEE654AD0C6576A4A8B9686D6622E4067C3DD7720322157`. The local evidence directory is `<CODEX_HOME>/state/beads-recovery/nbnv2-20261001-01a0f8a3/`, including pre-repair SQLite online backups, byte-exact JSONL backups, the isolated rehearsal, and a report identifying the only 40 changed database cells. The transaction changed no IDs, comments, owners, content hashes, timestamps, dependencies, labels, or historical events.

## Operating limits

- The shared activation audit classifies this backend as legacy and unsupported for ordinary Beads lifecycle mutations. Use the shared durable fallback for new tracker work until a separately reviewed migration qualifies a replacement.
- The tracked pre-commit hook is retained and disabled by default. Render documentation explicitly and run the freshness check; the pre-push documentation check remains active.
- Do not run `bd import`, `bd export`, `bd sync`, `bd sync --flush-only`, or opt into the pre-commit hook against this tracker without a new complete preservation proof. The 0.47.1 importer skips tombstones and omits these provenance columns from existing-issue updates; its explicit exporter omits comments. A 1.3.1 import also skips tombstones, so it is not a faithful replacement for this data.
- Read-only investigation uses direct SQLite read-only connections or Beads with `--readonly --no-daemon --no-auto-import --no-auto-flush --allow-stale` and the explicit root database path. `bd show` hides tombstones; absence there is not evidence of deletion from the database.

A future migration must prove preservation of issue identities, comments, dependency and label records, closure/deletion provenance, timestamps, events, and configuration before switching the canonical backend. Keep the recovery artifacts until that proof and an operational backup/restore check succeed. This local repair does not establish cross-host tracker parity.
