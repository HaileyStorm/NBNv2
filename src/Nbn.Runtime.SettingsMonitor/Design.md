# Nbn.Runtime.SettingsMonitor

Owns node registry, settings distribution, capability heartbeats, and status query contracts.

## Stable behavior boundaries

- Seeds canonical default settings for shared runtime/operator policy keys, including reproduction profile keys (`repro.config.*`) consumed by Workbench Reproduction and EvolutionSim.
- Persists worker capability heartbeats as the canonical source for placement-facing CPU/RAM/storage/GPU/ILGPU telemetry, explicit CPU/GPU score rows, explicit worker limit percentages, and worker pressure/load fields; SettingsMonitor does not fabricate missing capability values, and freshness filtering remains a HiveMind placement concern layered on top of the stored snapshot.
- Records node/controller freshness in SettingsMonitor's own observation clock domain when heartbeats arrive, so mixed-machine deployments do not inherit worker/controller liveness skew from unsynchronized host clocks.
- Runtime remoting defaults to binding all interfaces and auto-resolving a non-loopback advertised host when none is supplied, so a default-launched SettingsMonitor is reachable from peer machines unless the operator explicitly narrows `--bind-host`.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
