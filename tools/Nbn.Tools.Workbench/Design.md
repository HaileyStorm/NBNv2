# Nbn.Tools.Workbench

Owns desktop orchestration/visualization/debug UX and deterministic view-model dispatch behavior.

## Stable responsibilities

- Host operator workflows over IO/debug/viz and reproduction surfaces without exposing actor placement details.
- Keep command/view-model correctness independent of Avalonia loop availability (dispatcher wrappers execute inline when no UI lifetime exists).
- Keep visualization semantics deterministic across rendering modes.
- Treat `DebugHub` and `VisualizationHub` as Observability-owned actor names resolved at the configured Observability host/port (not Workbench-owned services).
- Surface Observability connection state from runtime reachability/availability signals, not from endpoint text configuration alone.
- Launch runtime services only from explicit Local Launch commands (`Start` / `Start All`); connection attempts never auto-start runtimes.
- Treat spawn success as visualization-ready only after SettingsMonitor reports a live brain controller heartbeat and placement lifecycle reaches `Running` with registered shards for the spawned brain.
- Drive worker endpoint visibility from SettingsMonitor heartbeat/registration data as a multi-worker list (`active`, `degraded`, `failed`) with transient retention before removal; include compact per-host brain hints from placement/controller ownership when available, and do not infer worker availability from local endpoint text fields alone.
- Continuous input-vector mode is tick-driven from `VizTick` cadence (using selected-brain + tick de-duplication) rather than output-vector callbacks, so input injection remains active even when output streams are sparse; vector sends are client-validated for parse correctness and selected-brain input width before dispatch.
- Random brain generation applies activity guardrails in non-fixed modes: input-region neurons are normalized to a signal-responsive profile (`ACT_IDENTITY`, `RESET_ZERO`, `ACCUM_SUM`, zero thresholds), output-region neurons use bounded defaults (`RESET_ZERO`, `ACCUM_MAX`, activation subset `ACT_CLAMP|ACT_TANH|ACT_SIG`, tighter threshold caps) to keep output dynamics near operational IO ranges, `ACCUM_NONE` is excluded from random weighted picks, and random threshold ranges are capped to avoid dead-on-arrival or runaway defaults.

## Visualization layout decisions

- Baseline full-brain map uses axial 2D layout.
- Optional `Projected 3D (R&D)` mode is allowed for region-map rendering only; focus-mode neuron layouts remain 2D.
- Projected mode uses axial slice depth (`z=-3..+3`) and deterministic lane offsets, then clamps to canvas safe bounds.
- Projection must fall back to axial 2D when region data is invalid/empty, geometry is non-finite, or spread is below minimum usability thresholds.
- Hover/select/pin/tooltips and keyboard navigation operate on final 2D coordinates and existing spatial indexing regardless of projection mode.
- Focus-mode gateway placement applies deterministic de-overlap search so gateway region nodes do not stack on top of each other.
- Viz canvas empty-space double-click resets camera to default center; when already at default center in region-focus mode, it returns to full-brain view.
- Layout cost for projected region-map mode is `O(region_count)` and bounded by NBN region count (`<= 32`); no GPU dependency is introduced.
- Viz panel exposes an optional compact top-N activity chart as an in-canvas top-right overlay (below canvas toolbar/status text): full-brain mode ranks regions with score `1 + |value| + |strength|` and log y-axis (`log(1+score)`), while focus mode ranks neurons in the focused region (including output region `31` neurons when focused) using signed `VizNeuronBuffer` values on a linear y-axis; chart history range is configured in seconds (default `~3s`) then converted to a bounded tick window from current cadence, and chart/canvas projection history uses a larger internal retention buffer than the visible stream table to avoid short-window resets under high event throughput.
- Buffer monitoring uses sparse `VizNeuronBuffer` events (initial sample plus value-change updates) and chart projection carries forward last-known neuron buffer samples across global tick progression, so low-activity windows keep advancing on the x-axis without flooding unchanged samples.
- Canvas color modes include two energy-focused variants that preserve the existing colorblind-safe blue/orange paradigm: `Energy: Reserve` uses latest buffer/value reserve sign+magnitude, and `Energy: Cost pressure` visualizes estimated burn pressure from activity/recency/fanout relative to reserve.
- Default/fit recenter operations apply a leftward visual bias while the mini chart overlay is enabled so core graph content remains readable under the top-right overlay.
- Focus-mode concentric/gateway placement and right-side region lanes apply a deterministic avoidance bias for the top-right overlay zone to reduce node overlap near the chart.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
