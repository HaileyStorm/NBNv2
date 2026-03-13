# Nbn.Runtime.RegionHost

Owns RegionShard execution, signal batch handling, and backend dispatch behavior.

Region shard startup may selectively fetch the assigned `.nbn` region section when artifact manifests provide region-index metadata and the store supports range reads. That optimization is definition-only today: `.nbs` snapshots still load through the full-artifact path because snapshot validation expects whole-brain coverage plus overlay parsing before the shard state is materialized.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
