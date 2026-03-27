# Nbn.Runtime.Observability

Owns DebugHub/VisualizationHub behavior and telemetry integration surfaces.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.

## Stable behavior notes

- DebugHub and VisualizationHub key subscribers by normalized PID string and share the same PID parsing rules for subscribe, unsubscribe, and termination cleanup.
- Observability meter names, activity source names, publish span names, and `nbn.debug.*` / `nbn.viz.*` metric names are stable tooling contracts and should not be renamed without an explicit migration.
