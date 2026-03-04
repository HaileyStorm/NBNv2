# Nbn.Runtime.SettingsMonitor

Owns node registry, settings distribution, capability heartbeats, and status query contracts.

## Stable behavior boundaries

- Seeds canonical default settings for shared runtime/operator policy keys, including reproduction profile keys (`repro.config.*`) consumed by Workbench Reproduction and EvolutionSim.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
