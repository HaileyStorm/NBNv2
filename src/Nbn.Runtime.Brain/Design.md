# Nbn.Runtime.Brain

Owns BrainRoot/BrainSignalRouter coordination and tick-phase routing semantics.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.

## Stable behavior notes

- BrainSignalRouter accepts shard delivery acknowledgements from PID-equivalent endpoints for the same actor id, not just byte-for-byte address matches, so mixed-topology hostname/IP/wildcard differences do not stall `TickDeliver`.
