# Nbn.Tools.PerfProbe

Owns placement-facing and runtime-facing NBN performance probing and report generation.

## Stable responsibilities

- Expose two benchmark surfaces on shared reporting artifacts:
  - `worker-profile` for placement-facing capability and planner profiling.
  - `localhost-stress` for deeper localhost runtime limit probing.
- Expose a non-invasive `current-system` profile mode that attaches to the currently running runtime through SettingsMonitor discovery and captures service-discovery, worker-inventory, and HiveMind placement/tick snapshots without assuming localhost ownership.
- Surface worker capability limits and pressure metadata in the human-readable reports so worker-node profiling can be used both for HiveMind placement validation and for deeper attached-system diagnostics.
- Emit report artifacts as JSON, CSV, Markdown, and HTML, with both tabular summaries and chart output.
- Place default CLI report output under `./perf-probe/perf-probe-<timestamp>` unless the caller overrides `--output-dir`, so probe runs do not scatter timestamped folders in the launch directory.
- Run real CPU and GPU placement-planner preference profiles in `worker-profile` when local capability telemetry and workload size qualify for the planner's GPU path.
- Run real CPU and GPU runtime rows in `localhost-stress` when compatible ILGPU hardware is present; skip only when no compatible GPU/runtime path exists.
- Treat completed localhost-stress measurements as report data, not hard pass/fail performance gates for a specific machine. Rows fail only when runtime execution or backend verification fails; sub-target results remain passed measurements with the observed ceiling in the metrics/summary.
- Host the localhost stress harness with stable named local `HiveMind` and worker actors so placement, routing, and tick-control PIDs stay valid inside the in-process probe runtime.
- Apply benchmark-safe runtime config through the real IO/Hive control path before measuring localhost runtime rows: perf brains disable plasticity and homeostasis so explicit GPU runs benchmark the supported compute path instead of falling back to CPU on unsupported runtime features.
- Keep the original localhost rows as input/control-plane validations, but add a dedicated compute-dominant recurrent row that seeds hidden-region activity through runtime pulses and removes per-tick input routing so end-to-end CPU/GPU throughput differences remain visible in the runtime artifacts.

## Code organization

- `PerfProbeRunner.cs` owns worker-profile orchestration plus shared timing, environment, and GPU-skip helpers.
- `PerfProbeRunner.LocalRuntimeHarness.cs` owns the in-process localhost harness, including stable actor naming, IO bootstrap handoff, benchmark-safe runtime config, and backend verification.
- `PerfProbeRunner.CurrentSystem.cs` owns the non-invasive attached-runtime snapshot path and service-endpoint discovery handling.
- `PerfProbeModels.cs` owns stable configuration, scenario, and report contracts; keep report row identities and metric-label selection compatible.
- `PerfReportWriter.cs` owns the JSON/CSV/Markdown/HTML artifact envelope and must preserve file names and row/column shapes.
- `Program.cs` owns CLI command routing and output-directory/report-opening behavior.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
