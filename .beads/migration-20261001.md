# Beads backend migration — 2026-10-01

Canonical tracker: this repository's `.beads`, prefix `NBNv2`. Beads 1.3.1,
matching pinned Dolt 2.2.0, embedded single-writer mode. Viewer 0.25.2 consumes the explicit JSONL export.

Preserved source counts: 296 issues (1 tombstones),
516 comments, 321 original dependency rows,
594 labels and 1505 historical events.
Source semantic snapshot: `502b828ca68074f8855468e86538ac0430a9a48e47b78e27033ddece838524fb`.

Original SQLite files, runtime files and config are retained outside the repository under
`<CODEX_HOME>/state/beads-upgrade/20261001-01a0f8a3/cutover-baselines/nbnv2`.
Full local native backup destination: `<CODEX_HOME>/state/beads-backups/nbnv2`.
Isolated migration, full-backup restore, restarted create/comment/update/close/read,
and tombstone export checks passed before installation. Synthetic proof records are not installed.
Native/archive verification SHA-256: `5b27c7bc512477016efc3896400ce6e8c8091c1c538ede0f14fea0646252c678`.

All original tables, schemas, rows, raw source JSONL and ID mappings are retained in the
ignored Dolt `legacy_beads_*` tables and full backup. Original exact timestamp strings,
deletion provenance, `crystallizes` and `quality_score` are also retained in per-issue
`metadata.codex_legacy_beads_v1`; native SQL dates have UTC whole-second precision.
This is a preservation migration, not a claim of an unchanged legacy schema.

Four duplicate blocks/parent-child pairs project to parent-child; both original edges remain archived. The native distinct hard-block badge/edge count differs. The previously repaired deletion/close provenance is retained. The tracked commit hook remains disabled by default.

No Dolt cloud remote or off-host parity is asserted. JSONL is not a complete backup;
follow README.md for explicit export, local backup and safe task takeover. Migration
evidence and live acceptance are recorded in the local upgrade CHECKPOINT.md.
