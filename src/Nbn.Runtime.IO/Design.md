# Nbn.Runtime.IO

Owns IO Gateway and per-brain coordinator integration paths for external command and output subscriptions.

## Stable responsibilities

- Exposes canonical IO/proto wrappers for external runtime operations, including reproduction and speciation contract calls.
- Normalizes upstream service failures into explicit, stable contract reason codes (unavailable, empty response, request failed) before returning to callers.
- Forwards speciation stateful and batch requests to the dedicated Speciation manager endpoint without embedding assignment policy in the gateway.
- Tracks coordinator PID ownership/location metadata from HiveMind so input/output traffic can route to worker-hosted coordinators and preserve subscriptions/pending input state across coordinator moves.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
