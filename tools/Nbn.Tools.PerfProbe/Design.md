# Nbn.Tools.PerfProbe

Owns placement-facing and runtime-facing NBN performance probing and report generation.

## Stable responsibilities

- Expose two benchmark surfaces on shared reporting artifacts:
  - `worker-profile` for placement-facing capability and planner profiling.
  - `localhost-stress` for deeper localhost runtime limit probing.
- Expose a non-invasive `current-system` profile mode that attaches to the currently running runtime through SettingsMonitor discovery and captures service-discovery, worker-inventory, and HiveMind placement/tick snapshots without assuming localhost ownership.
- Surface worker capability limits and pressure metadata in the human-readable reports so worker-node profiling can be used both for HiveMind placement validation and for deeper attached-system diagnostics.
- Emit report artifacts as JSON, CSV, Markdown, and HTML, with both tabular summaries and chart output.
- Run real CPU and GPU placement-planner preference profiles in `worker-profile` when local capability telemetry and workload size qualify for the planner's GPU path.
- Keep `localhost-stress` GPU runtime benchmark rows present as explicit skips until the dedicated runtime perf follow-up for the landed RegionShard GPU backend is wired in; do not fake GPU runtime execution.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
