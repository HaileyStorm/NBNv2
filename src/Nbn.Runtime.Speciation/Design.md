# Nbn.Runtime.Speciation

Owns the runtime speciation control plane: taxonomy epochs, species membership persistence, lineage edges, and startup reconciliation for known brains.

## Stable responsibilities

- Single-writer `SpeciationManagerActor` serializes membership/epoch decisions.
- SQLite persistence keeps membership sticky within an epoch and retains historical epochs after reset. By default assignments are immutable; a bounded manager-only founder-similarity-band path may reassign recent source-species members to a just-created derived species and may reuse that same derived species for nearby post-split source members when strict policy predicates pass.
- Startup loads persisted epoch state, then reconciles missing memberships for currently registered brains from SettingsMonitor.
- Runtime startup policy/default species config is sourced from SettingsMonitor `workbench.speciation.*` keys; speciation-policy CLI/env overrides are not used.
- Handles canonical `nbn.speciation` proto contracts for status/config/evaluate/assign/list/query/history and batch evaluate/apply.
- Supports explicit epoch-history hygiene controls via proto: clear-all reset (wipe all persisted epochs/history, reset epoch/decision autoincrement counters, then seed a new epoch) and delete-specific-historical-epoch (current epoch deletion rejected).
- Enforces contract-level dry-run versus commit semantics: dry-run never mutates state; commit accepts `brain_id`, artifact-ref, and artifact-URI candidates while preserving per-epoch membership immutability.
- Artifact candidates commit with deterministic synthetic membership identity derived from candidate reference/URI so commit does not require child spawn.
- Accepts parent references in both `brain_id` and artifact (`ArtifactRef`/URI) forms for assignment evidence and decision provenance.
- Applies a deterministic lineage-aware assignment policy driven by `SpeciationRuntimeConfig.config_snapshot_json` (`assignment_policy` block): `lineage_match_threshold`, `lineage_split_threshold`, `parent_consensus_threshold`, `lineage_hysteresis_margin`, split-guard controls (`lineage_split_guard_margin`, `lineage_min_parent_memberships_before_split`), bounded recent-split realignment controls (`lineage_realign_parent_membership_window`, `lineage_realign_match_margin`), bounded hindsight controls (`lineage_hindsight_reassign_commit_window`, `lineage_hindsight_similarity_margin`), and derived-species controls.
- Runtime default lineage policy is intentionally conservative against premature merge/re-split churn: `lineage_match_threshold=0.92`, `lineage_split_threshold=0.88`, `lineage_hysteresis_margin=0.04`, `lineage_split_guard_margin=0.02` (effective split `0.86`), `lineage_min_parent_memberships_before_split=1`, and `parent_consensus_threshold=0.70`.
- Derives assignment outcomes from parent membership evidence plus optional similarity metrics in `decision_metadata_json`, keeping source-species similarity and assigned-species similarity distinct: best-fit parent-species pairwise evidence drives split/hindsight decisions, and runtime only falls back to a lineage source when the parent lineage is otherwise unambiguous; assigned-species pairwise evidence drives intra-species cohesion.
- Split classification now uses a dynamic effective split threshold: `max(policy_effective_split_threshold, species_floor_similarity - lineage_hysteresis_margin)`, where `species_floor_similarity` is the persisted minimum intra-species pairwise similarity estimate for that species.
- Floor sampling records `lineage.intra_species_similarity_sample` at commit time from assigned-species evidence; split founders seed a singleton sample of `1.0`, and subsequent in-species pairwise evidence can only relax that floor downward. This avoids using cross-species aggregate similarity to lower species floors.
- Founder-similarity clustering is symmetric: hindsight/reuse only pulls source-species members into a newborn derived species when their source-species similarity lies within `founder_similarity +/- lineage_hindsight_similarity_margin`, preventing much more divergent members from collapsing into the same new species.
- Mixed-parent candidates therefore follow the best-fitting parent species directly when parent-specific pairwise similarity is available, even when that species is not the count/recency leader in parent memberships.
- New derived species therefore start with high internal similarity by construction (singleton floor `1.0`) and only relax downward as accepted in-species variety grows; low-similarity follow-on members split again or land elsewhere instead of being held in-species until a later sample appears.
- Derived species IDs remain deterministic (`<source>-<prefix>-<hash>`); derived display names now use lineage-path suffix notation (`<source display> [A]`, then `[AB]`, etc.) so each divergence appends one deterministic branch letter while preserving ancestry readability.
- Persists enriched decision provenance (`assignment_strategy`, policy snapshot, lineage inputs, parsed scores, assigned-species similarity, canonical `lineage.source_species_*` fields, legacy-compatible `lineage.dominant_species_*` aliases, intra-species sample evidence, dynamic/policy split thresholds, split-threshold source, and split proximity deltas) in `decision_metadata_json` for evaluate/assign/batch flows.
- NBNv2-6mp alternatives evaluated:
  - Keep static policy-only split thresholds: rejected because newborn species routinely formed at threshold-adjacent similarity and did not encode species-internal cohesion.
  - Full species-centroid/pairwise matrix distance at assignment time: rejected for runtime/storage cost and nondeterministic replay risk without additional artifact loading/state.
  - Chosen: persisted intra-species minimum-lineage floor with deterministic threshold relaxation, which preserves single-writer determinism and restart-stable behavior.
- Ingests lineage edges atomically on committed membership creation whenever parent references are provided (`brain_id` directly, or artifact refs/URIs via deterministic synthetic parent identities), so lineage history and membership decisions stay in sync.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
