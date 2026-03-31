# Nbn.Runtime.IO

Owns IO Gateway and per-brain coordinator integration paths for external command and output subscriptions.

## Stable responsibilities

- `IoGatewayActor` is intentionally split by responsibility: discovery, reproduction/speciation forwarding, client lifecycle, per-brain registration/artifact state, coordinator routing, and energy/config commands live in separate partial files so refactors can stay file-bounded without changing gateway behavior.
- Exposes canonical IO/proto wrappers for external runtime operations, including spawn/kill lifecycle forwarding, separate queued-spawn and placed-and-visible wait acknowledgements, reproduction/speciation contract calls, global default and per-brain output-vector-source selection, acknowledged whole-brain runtime state reset, and placement-worker capacity snapshots forwarded from HiveMind.
- Normalizes upstream service failures into explicit, stable contract reason codes (unavailable, empty response, request failed) before returning to callers.
- Forwards speciation stateful and batch requests to the dedicated Speciation manager endpoint without embedding assignment policy in the gateway.
- Tracks coordinator PID ownership/location metadata from HiveMind so input/output traffic can route to worker-hosted coordinators and preserve subscriptions/pending input state across coordinator moves.
- Resolves missing `BrainInfo` entries through HiveMind metadata bootstrap with a tolerance for moderately slow metadata replies, while still falling back to a default empty `BrainInfo` when HiveMind does not answer in time. `AwaitSpawnPlacementViaIO` uses that same metadata visibility path so the wait response only reports success once the placed brain is queryable through IO.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
