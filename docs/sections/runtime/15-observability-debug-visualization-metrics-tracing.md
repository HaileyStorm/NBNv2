## 15. Observability: debug, visualization, metrics, tracing

### 15.1 Debug and visualization streams

Debug and visualization can be disabled entirely:

* if disabled, hubs may not run, and actors drop debug/viz events at source.

Debug level is configurable in Workbench and/or settings:

* severity threshold
* context filters
* throttling rates
* emitter-side enable/min-severity gates (`debug.stream.enabled`, `debug.stream.min_severity`)

Runtime behavior:

* HiveMind applies `debug.stream.enabled` + `debug.stream.min_severity` before sending `DebugOutbound`.
* HiveMind propagates those gates to RegionShards via `UpdateShardRuntimeConfig` (`debug_enabled`, `debug_min_severity`).
* RegionShards apply the same emitter-side gate before sending debug events to DebugHub.
* Visualization cadence is settings-backed:
* `viz.tick.min_interval_ms` throttles HiveMind `VizTick` emissions.
* `viz.stream.min_interval_ms` throttles RegionShard visualization stream collection/emission.
* Workbench visualizer cadence controls consume SettingsMonitor snapshots plus `SettingChanged` feeds, including external Settings DB value updates detected and published by SettingsMonitor for existing setting rows, so the operator control target tracks the authoritative settings value without reconnecting.
* When `tick.cadence.hz` changes externally, Workbench re-queries HiveMind status so the visualizer continues to show both the configured cadence target and the current authoritative runtime target when they temporarily diverge.
* When stream throttling is active (`target tick cadence faster than configured stream interval`), RegionShards sample visualization work in deterministic region phases across ticks to spread CPU cost without changing simulation compute/deliver semantics.
* Workbench Orchestrator exposes local worker count for WorkerNode launch, settings-backed worker policy controls for capability refresh cadence and pressure-rebalance thresholds, node-scoped resource-usage summaries derived from SettingsMonitor worker capability snapshots (workers grouped by address host, including multiple worker roots that share one port), recent worker-pressure/tick-health summaries from HiveMind status, and `Profile Current System` for attached perf-probe runs against the currently running deployment.
* Workbench Debug mirrors that same node-scoped system-load summary so operators can correlate debug streams with current node resource usage, recent worker pressure, and automatic tick-cadence backpressure without leaving the debug surface.
* Workbench Speciation surfaces total epoch count, current active epoch, and a pane-wide epoch selector so memberships and history visualizations stay aligned to one epoch scope when the operator drills into historical taxonomy state.

### 15.2 OpenTelemetry (NBN-managed)

When enabled, NBN configures and runs OpenTelemetry exporters. Configuration can be:

* file-based settings (SQLite settings)
* environment variables
* Workbench controls (dev mode)

Metrics include:

* tick durations (compute phase, deliver phase)
* timeout counts
* late arrival counts
* shard compute ms, deliver ms
* reschedule events
* recovery request/completion/failure counts
* per-brain cost and energy
* per-brain tick-cost totals
* energy depletion counts
* plasticity strength-code mutation counts
* snapshot/rebase overlay record counts
* signal batch sizes and counts
* worker capability refresh/reschedule thresholds
* worker pressure snapshots and pressure-triggered reschedule events
* speciation startup-reconcile runs plus added/existing membership counts
* speciation assignment decision counts and latencies by apply mode / candidate mode / decision reason / failure reason
* speciation epoch-transition counts (initialize, start-new-epoch, delete-epoch, reset-all)
* speciation status snapshot membership/species/lineage-edge counts

Traces include:

* tick phase spans
* reschedule/recovery spans
* artifact fetch spans
* energy depletion milestones
* plasticity mutation milestones
* speciation assignment spans
* speciation startup-reconcile spans
* speciation epoch-transition spans

### 15.3 Lateness accounting

Both are recorded:

* timeouts (deadline exceeded)
* late arrivals (received after timeout or after tick advancement)

Recovery-specific operator events:

* `brain.recovering`
* `brain.recovered`
* `brain.recovery.failed`

---
