# Nbn.Runtime.BrainHost

Owns brain host process wiring and runtime hosting orchestration where applicable.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.

## Stable behavior notes

- `Program.cs` owns BrainHost bootstrap and the explicit `BrainSignalRouterActor` plus `BrainRootActor` wiring; `BrainTelemetrySession` owns optional Brain meter exporters so host startup logic stays focused on runtime wiring.
