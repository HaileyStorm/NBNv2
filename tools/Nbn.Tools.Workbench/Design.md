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

## Visualization layout decisions

- Baseline full-brain map uses axial 2D layout.
- Optional `Projected 3D (R&D)` mode is allowed for region-map rendering only; focus-mode neuron layouts remain 2D.
- Projected mode uses axial slice depth (`z=-3..+3`) and deterministic lane offsets, then clamps to canvas safe bounds.
- Projection must fall back to axial 2D when region data is invalid/empty, geometry is non-finite, or spread is below minimum usability thresholds.
- Hover/select/pin/tooltips and keyboard navigation operate on final 2D coordinates and existing spatial indexing regardless of projection mode.
- Focus-mode gateway placement applies deterministic de-overlap search so gateway region nodes do not stack on top of each other.
- Viz canvas empty-space double-click resets camera to default center; when already at default center in region-focus mode, it returns to full-brain view.
- Layout cost for projected region-map mode is `O(region_count)` and bounded by NBN region count (`<= 32`); no GPU dependency is introduced.
- Viz panel exposes an optional compact top-N activity chart as an in-canvas top-right overlay (below canvas toolbar/status text): full-brain mode ranks regions, focus mode ranks neurons in the focused region (including output region `31` neurons when focused), per-tick score is deterministically `1 + |value| + |strength|`, and chart history range is configured in seconds (default `~3s`) then converted to a bounded tick window from current cadence.
- Default/fit recenter operations apply a leftward visual bias while the mini chart overlay is enabled so core graph content remains readable under the top-right overlay.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
