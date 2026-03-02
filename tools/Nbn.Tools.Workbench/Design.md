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
- Continuous input-vector mode is tick-driven from selected-brain output-vector callbacks (with selected-brain + tick de-duplication), not `VizTick`, so input replay remains tied to real simulation ticks even when visualization stream cadence is throttled.
- Visualizer `Inputs` and `Vector Outputs` cards surface system IO coordinator policies backed by SettingsMonitor (`io.input_coordinator.mode`, `io.output.vector_source`); UI selection writes canonical values to SettingsMonitor and consumes setting-change feeds so controls stay synchronized with external updates and Orchestrator settings workflows.
- Visualizer cadence controls are SettingsMonitor-backed: tick override (`tick.override.hz`) and visualization cadence (`viz.tick.min_interval_ms`, `viz.stream.min_interval_ms`) are written via settings and reflected back through setting-change feeds, so Orchestrator `Settings` remains the single live source of truth.
- Random brain generation applies activity guardrails in non-fixed modes: input-region neurons are normalized to a signal-responsive profile (`ACT_IDENTITY`, `RESET_ZERO`, `ACCUM_SUM`, zero thresholds), internal neurons use a bounded/stable activation subset (`ACT_IDENTITY|ACT_STEP_*|ACT_CLAMP|ACT_SIN|ACT_TANH|ACT_SIG|ACT_PCLAMP`) with tighter threshold caps (pre-activation `<=36`, activation `<=40`) and parameter clamps near decoded `[-1,+1]`, output-region neurons use bounded defaults (`ACCUM_SUM`, activation subset `ACT_CLAMP|ACT_TANH|ACT_SIG`, tighter threshold caps) and non-fixed I/O reset behavior is pinned to `RESET_ZERO`; non-fixed internal reset selection uses a curated fading-memory subset (`RESET_HOLD` 35%, `RESET_HALF` 30%, `RESET_TENTH` 20%, `RESET_HUNDREDTH` 10%, `RESET_ZERO` 5%), non-fixed accumulation weights de-emphasize `ACCUM_PRODUCT`, `ACCUM_NONE` is excluded from random weighted picks, generation biases dense intra-region / sparse inter-region topology, and generation seeds an input-to-output influence path plus high-likelihood output->input-endpoint recurrence bridges (outside region `0`) with low target sharing preference.

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
- Viz canvas color transfer supports both `Linear` and `Perceptual log/symlog` curves; perceptual mode uses `log1p` for non-negative activity-like signals and symmetric log for signed value/reserve signals to improve low-mid contrast without flipping sign semantics.
- Color-mode and transfer-curve selectors expose detailed tooltip legend text with concrete palette meanings (for example, warm orange positive vs cool blue negative) so operators can interpret node/edge color intent without reading implementation details.
- Default/fit recenter operations apply a leftward visual bias while the mini chart overlay is enabled so core graph content remains readable under the top-right overlay.
- Focus-mode concentric/gateway placement and right-side region lanes apply a deterministic avoidance bias for the top-right overlay zone to reduce node overlap near the chart.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
