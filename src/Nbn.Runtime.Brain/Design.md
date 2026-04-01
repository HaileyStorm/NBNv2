# Nbn.Runtime.Brain

Owns BrainRoot/BrainSignalRouter coordination and tick-phase routing semantics.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.

## Stable behavior notes

- BrainSignalRouter accepts shard delivery acknowledgements from PID-equivalent endpoints for the same actor id, not just byte-for-byte address matches, so mixed-topology hostname/IP/wildcard differences do not stall `TickDeliver`.
- BrainRoot replays the cached routing snapshot and cached IO gateway registration whenever it attaches or respawns a signal router, so router replacement does not require callers to re-seed control-plane state.
- BrainSignalRouter uses `RegisterIoGateway.input_coordinator_mode`, `RegisterIoGateway.input_tick_drain_armed`, and observed `InputWrite`/`InputVector` traffic to suppress idle `DrainInputs` requests for `DirtyOnChange` brains; `ReplayLatestVector` brains still drain every tick so replay-latest semantics stay intact.
- Brain actor helper placement stays split by responsibility: `BrainRootActor` owns lifecycle/control forwarding, `BrainSignalRouterActor.Inputs.cs` owns IO drain/runtime input handling, and `BrainSignalRouterActor.Delivery.cs` plus `BrainSignalRouterActor.PidMatching.cs` own delivery completion and ACK trust rules.
