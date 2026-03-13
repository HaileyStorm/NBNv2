# Nbn.Runtime.RegionHost

Owns RegionShard execution, signal batch handling, and backend dispatch behavior.

Region shard startup may selectively fetch the assigned `.nbn` region section when artifact manifests provide region-index metadata and the store supports range reads. That optimization is definition-only today: `.nbs` snapshots still load through the full-artifact path because snapshot validation expects whole-brain coverage plus overlay parsing before the shard state is materialized.

RegionHost consumes non-file `store_uri` values through the shared artifact resolver, including the built-in HTTP(S) artifact-service backend. Indexed `.nbn` loads issue HTTP `Range` requests when available, but mismatched region-index metadata, unsupported remote range reads (`405`/`501`), or missing range responses fall back to full definition reads instead of changing validation semantics.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
