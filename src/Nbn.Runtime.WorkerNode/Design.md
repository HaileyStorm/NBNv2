# Nbn.Runtime.WorkerNode

Owns worker node inventory, placement execution endpoints, and lifecycle participation.

## Stable behavior notes

- Region-shard placement is artifact-backed. If base artifact metadata is unavailable at assignment time, WorkerNode returns a retryable `PlacementFailureWorkerUnavailable` ack instead of hosting a synthetic shard.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
