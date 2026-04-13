# Nbn.Runtime.IO

Owns IO Gateway and per-brain coordinator integration paths for external command and output subscriptions.

## Stable responsibilities

- `IoGatewayActor` is intentionally split by responsibility: discovery, reproduction/speciation forwarding, client lifecycle, per-brain registration/artifact state, coordinator routing, and energy/config commands live in separate partial files so refactors can stay file-bounded without changing gateway behavior.
- Exposes canonical IO/proto wrappers for external runtime operations, including spawn/kill lifecycle forwarding, pause/resume brain control forwarding, separate queued-spawn and placed-and-visible wait acknowledgements, reproduction/speciation contract calls, global default and per-brain output-vector-source selection, barrier-coordinated whole-brain runtime state reset, and placement-worker capacity snapshots forwarded from HiveMind.
- Normalizes upstream service failures into explicit, stable contract reason codes (unavailable, empty response, request failed) before returning to callers.
- Forwards speciation stateful and batch requests to the dedicated Speciation manager endpoint without embedding assignment policy in the gateway.
- Tracks coordinator PID ownership/location metadata from HiveMind so input/output traffic can route to worker-hosted coordinators and preserve subscriptions/pending input state across coordinator moves.
- Keeps output subscriptions exact by default while allowing opt-in `latest_only` fan-out coalescing for callers that need bounded-memory behavior under slow-subscriber pressure; subscriber termination prunes both coordinator fan-out and gateway replay state.
- Bounds missing-brain output-subscription replay and stale router-registration caches so malformed or abandoned brain IDs cannot grow gateway bookkeeping without limit. Registered-brain and live-subscriber entries remain correctness state and are not evicted by those caps.
- Resolves missing `BrainInfo` entries through HiveMind metadata bootstrap with a tolerance for moderately slow metadata replies, while still falling back to a default empty `BrainInfo` when HiveMind does not answer in time. `AwaitSpawnPlacementViaIO` uses that same metadata visibility path so the wait response only reports success once the placed brain is queryable through IO, and the metadata-bootstrap portion of that wait now consumes the caller's remaining placement budget rather than a separate fixed short timeout.
- Emits IO placement-wait telemetry for `AwaitSpawnPlacementViaIO` covering total wait, HiveMind wait, metadata-visibility wait, and success/failure outcomes so placement-visible vs metadata-visible delays are observable without parsing console logs.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
