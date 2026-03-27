# Nbn.Tests

Owns solution-level regression coverage for shared contracts, runtime invariants, and operator/tooling behavior.

## Stable responsibilities

- Protect canonical file/proto/shared-contract behavior through the `Format`, `Proto`, and `Shared` suites.
- Protect runtime/control-plane invariants through the subsystem suites (`Brain`, `HiveMind`, `RegionHost`, `WorkerNode`, `SettingsMonitor`, `Reproduction`, `Speciation`, `Observability`, and `RuntimeArtifacts`).
- Protect operator-tooling behavior through the `Workbench` and `Tools` suites, including headless-safe dispatch, local-vs-installed launch resolution, artifact-store promotion, and view-model/service boundaries.
- Keep reusable harnesses in `TestSupport/*` and keep scenario-specific assertions in the suite that owns the behavior under test.

## Source layout

- Top-level test folders mirror repo subsystem ownership so behavior changes can be verified close to the owning project boundary.
- `Proto/ProtoCompatibilityTests.cs` guards shared `.proto` drift.
- `Workbench/UiDispatcherTests.cs` and `Workbench/LocalProjectLaunchPreparerTests.cs` guard the headless dispatcher contract and Workbench's local-vs-installed launch boundary.
- `TestSupport/*` owns async wait helpers, temporary path/database scopes, artifact-store harnesses, HTTP test servers, and repeated actor/bootstrap helpers.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
