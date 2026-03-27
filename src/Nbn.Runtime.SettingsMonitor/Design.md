# Nbn.Runtime.SettingsMonitor

Owns node registry, settings distribution, capability heartbeats, and status query contracts.

## Stable behavior boundaries

- Seeds canonical default settings for shared runtime/operator policy keys, including reproduction profile keys (`repro.config.*`) consumed by Workbench Reproduction and EvolutionSim.
- Persists worker capability heartbeats as the canonical source for placement-facing CPU/RAM/storage/GPU/ILGPU telemetry, explicit CPU/GPU score rows, explicit worker limit percentages, and worker pressure/load fields; SettingsMonitor does not fabricate missing capability values, and freshness filtering remains a HiveMind placement concern layered on top of the stored snapshot.
- Records node/controller freshness from message timestamps when senders provide them, falling back to SettingsMonitor's local observation clock only when messages omit time fields; mixed-machine deployments should therefore preserve sender clock quality for liveness data.
- Owns canonical `service.endpoint.*` settings rows and watcher notifications, while `ServiceEndpointDiscoveryClient` remains a transport/parsing helper and HiveMind remains responsible for applying endpoint changes to placement/runtime behavior.
- Treats `SettingsMonitorReporter` as the shared liveness/capability publisher boundary; reporter cadence and fallback capability sampling live in the helper, but persisted settings/node schema ownership stays in SettingsMonitor.
- Runtime remoting defaults to binding all interfaces and auto-resolving a non-loopback advertised host when none is supplied, so a default-launched SettingsMonitor is reachable from peer machines unless the operator explicitly narrows `--bind-host`.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
