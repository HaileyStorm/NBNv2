# Visualizer 3D Layout R&D Notes

## Scope
- Adds an **optional** `Projected 3D (R&D)` region layout mode for full-brain region map rendering.
- Focus-mode neuron view remains 2D and explicitly reports fallback in legend text.

## Coordinate Model
- Uses NBN axial slice semantics (`z = -3..+3`) as depth.
- Applies a lightweight 3D-to-2D projection:
  - depth shifts X and Y (`depthX`, `depthY`) from axial slice value
  - in-slice regions are spread with deterministic lane offsets
- Output is clamped to canvas safe padding bounds to keep interaction stable.

## Guardrails and Fallback
- 3D mode only attempts region-map projection; focus mode always falls back to 2D.
- Projection aborts and falls back to 2D if:
  - region set is invalid/empty
  - computed spread collapses below minimum usability thresholds
  - non-finite geometry is detected
- Legend reports whether 3D projection succeeded or fallback was used.

## Interaction Semantics
- Hover/select/pin/tooltips remain coherent because interaction still runs on final 2D coordinates.
- Hit-testing and keyboard navigation continue to use the existing viewmodel spatial index.

## Performance Notes
- Region-map 3D projection cost is O(region_count) and bounded (`<=32` regions).
- No GPU dependency or new rendering backend required.
- Rendering still uses the same canvas draw pipeline and diffed snapshots.

## Go / No-Go Criteria
- **Go** if:
  - full-brain region readability improves (depth cues without overlap regressions)
  - input responsiveness remains smooth under sustained stream updates
  - no regression in hover/select/pin consistency
- **No-Go** if:
  - fallback triggers frequently in normal scenarios
  - perceived readability worsens vs axial 2D baseline
  - layout instability introduces interaction ambiguity
