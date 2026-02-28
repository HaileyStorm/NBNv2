# Nbn.Tools.Workbench

Owns desktop orchestration/visualization/debug UX and deterministic view-model dispatch behavior.

## Stable responsibilities

- Host operator workflows over IO/debug/viz and reproduction surfaces without exposing actor placement details.
- Keep command/view-model correctness independent of Avalonia loop availability (dispatcher wrappers execute inline when no UI lifetime exists).
- Keep visualization semantics deterministic across rendering modes.
- Treat `DebugHub` and `VisualizationHub` as Observability-owned actor names resolved at the configured Observability host/port (not Workbench-owned services).
- Surface Observability connection state from runtime reachability/availability signals, not from endpoint text configuration alone.
- Launch runtime services only from explicit Local Launch commands (`Start` / `Start All`); connection attempts never auto-start runtimes.

## Visualization layout decisions

- Baseline full-brain map uses axial 2D layout.
- Optional `Projected 3D (R&D)` mode is allowed for region-map rendering only; focus-mode neuron layouts remain 2D.
- Projected mode uses axial slice depth (`z=-3..+3`) and deterministic lane offsets, then clamps to canvas safe bounds.
- Projection must fall back to axial 2D when region data is invalid/empty, geometry is non-finite, or spread is below minimum usability thresholds.
- Hover/select/pin/tooltips and keyboard navigation operate on final 2D coordinates and existing spatial indexing regardless of projection mode.
- Focus-mode gateway placement applies deterministic de-overlap search so gateway region nodes do not stack on top of each other.
- Viz canvas empty-space double-click resets camera to default center; when already at default center in region-focus mode, it returns to full-brain view.
- Layout cost for projected region-map mode is `O(region_count)` and bounded by NBN region count (`<= 32`); no GPU dependency is introduced.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
