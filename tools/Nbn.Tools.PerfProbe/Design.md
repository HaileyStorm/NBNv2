# Nbn.Tools.PerfProbe

Owns CPU-first NBN performance probing and report generation.

## Stable responsibilities

- Expose two benchmark surfaces on shared reporting artifacts:
  - `worker-profile` for placement-facing capability and planner profiling.
  - `localhost-stress` for deeper localhost runtime limit probing.
- Expose a non-invasive `current-system` profile mode that attaches to the currently running runtime through SettingsMonitor discovery and captures service-discovery, worker-inventory, and HiveMind placement/tick snapshots without assuming localhost ownership.
- Emit report artifacts as JSON, CSV, Markdown, and HTML, with both tabular summaries and chart output.
- Keep GPU runtime benchmark rows present as explicit skips until the RegionShard GPU backend from `NBNv2-zwv.5` exists; do not fake GPU runtime execution.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
