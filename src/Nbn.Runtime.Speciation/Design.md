# Nbn.Runtime.Speciation

Owns the runtime speciation control plane: taxonomy epochs, species membership persistence, lineage edges, and startup reconciliation for known brains.

## Stable responsibilities

- Single-writer `SpeciationManagerActor` serializes membership/epoch decisions.
- SQLite persistence keeps immutable membership decisions within an epoch and retains historical epochs after reset.
- Startup loads persisted epoch state, then reconciles missing memberships for currently registered brains from SettingsMonitor.
- Handles canonical `nbn.speciation` proto contracts for status/config/evaluate/assign/list/query/history and batch evaluate/apply.
- Supports explicit epoch-history hygiene controls via proto: clear-all reset (wipe all persisted epochs/history then seed a new epoch) and delete-specific-historical-epoch (current epoch deletion rejected).
- Enforces contract-level dry-run versus commit semantics: dry-run never mutates state; commit accepts `brain_id`, artifact-ref, and artifact-URI candidates while preserving per-epoch membership immutability.
- Artifact candidates commit with deterministic synthetic membership identity derived from candidate reference/URI so commit does not require child spawn.
- Accepts parent references in both `brain_id` and artifact (`ArtifactRef`/URI) forms for assignment evidence and decision provenance.
- Applies a deterministic lineage-aware assignment policy driven by `SpeciationRuntimeConfig.config_snapshot_json` (`assignment_policy` block): `lineage_match_threshold`, `lineage_split_threshold`, `parent_consensus_threshold`, `lineage_hysteresis_margin`, and derived-species controls.
- Derives assignment outcomes from parent membership evidence plus optional similarity metrics in `decision_metadata_json`, prioritizing lineage-scoped similarity (`lineage.lineage_similarity_score` / `lineage_similarity_score`) before legacy generic `similarity_score`, with deterministic tie-breaking and hysteresis-band hold behavior.
- Persists enriched decision provenance (`assignment_strategy`, policy snapshot, lineage inputs, and parsed scores) in `decision_metadata_json` for evaluate/assign/batch flows.
- Ingests lineage edges atomically on committed membership creation whenever parent references are provided (`brain_id` directly, or artifact refs/URIs via deterministic synthetic parent identities), so lineage history and membership decisions stay in sync.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
