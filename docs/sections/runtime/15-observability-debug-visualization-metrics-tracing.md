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
* When stream throttling is active (`target tick cadence faster than configured stream interval`), RegionShards sample visualization work in deterministic region phases across ticks to spread CPU cost without changing simulation compute/deliver semantics.

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
* per-brain cost and energy
* per-brain tick-cost totals
* energy depletion counts
* plasticity strength-code mutation counts
* snapshot/rebase overlay record counts
* signal batch sizes and counts

Traces include:

* tick phase spans
* reschedule/recovery spans
* artifact fetch spans
* energy depletion milestones
* plasticity mutation milestones

### 15.3 Lateness accounting

Both are recorded:

* timeouts (deadline exceeded)
* late arrivals (received after timeout or after tick advancement)

---
