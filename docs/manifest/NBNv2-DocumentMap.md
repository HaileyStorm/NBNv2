# NBNv2 documentation map

Defines canonical ownership and include order used by the INDEX template and render pipeline.

## Canonical locations

- Cross-cutting sections: `docs/sections/*`
- Component notes near code: `src/*/Design.md`, `tools/*/Design.md`, `tests/*/Design.md` (canonical docs, not assembled into `docs/NBNv2.md`)
- `Docs/` or `docs/` project subfolders are non-canonical.
- Template entrypoint: `docs/INDEX.md`
- Generated full document: `docs/NBNv2.md`

## Assembly order (INDEX include sequence)

1. docs/sections/overview/00-front-matter.md
2. docs/sections/overview/01-purpose-and-scope.md
3. docs/sections/overview/02-technology-stack-and-solution-structure.md
4. docs/sections/runtime/03-distributed-architecture-and-service-topology.md
5. docs/sections/runtime/04-identifiers-addressing-and-ids.md
6. docs/sections/runtime/05-simulation-model-and-tick-semantics.md
7. docs/sections/runtime/06-global-tick-engine-backpressure-and-scheduling.md
8. docs/sections/runtime/07-cost-and-energy-model.md
9. docs/sections/runtime/08-distance-model-region-and-neuron.md
10. docs/sections/runtime/09-sharding-and-placement.md
11. docs/sections/runtime/10-regionshard-compute-backends.md
12. docs/sections/runtime/11-plasticity-axon-strength-adaptation.md
13. docs/sections/runtime/12-brain-lifecycle-failure-recovery-and-snapshots.md
14. docs/sections/runtime/13-io-architecture-and-external-world-interface.md
15. docs/sections/runtime/14-reproduction-and-evolution.md
16. docs/sections/runtime/15-observability-debug-visualization-metrics-tracing.md
17. docs/sections/formats/16-artifact-storage-and-deduplication.md
18. docs/sections/formats/17-file-formats-nbn-and-nbs.md
19. docs/sections/runtime/18-database-schemas-sqlite.md
20. docs/sections/protocols/19-protocol-schemas-proto.md
21. docs/sections/overview/20-implementation-roadmap.md
22. docs/sections/reference/A-defaults-and-constants.md
23. docs/sections/reference/B-function-catalog.md
24. docs/sections/reference/C-region-axial-map-and-distance-examples.md

## Include marker contract

`<!-- NBN:INCLUDE path="..." -->`

All paths are repository-root relative. Missing include targets are hard failures.

## Render commands

- Windows render: `pwsh -NoProfile -File tools/docs/render-nbnv2-docs.ps1`
- Linux/macOS render: `bash tools/docs/render-nbnv2-docs.sh`
- Windows freshness check: `pwsh -NoProfile -File tools/docs/render-nbnv2-docs.ps1 -Check`
- Linux/macOS freshness check: `bash tools/docs/render-nbnv2-docs.sh --check`

## Freshness policy

- Policy: CI fails when `docs/NBNv2.md` is stale relative to `docs/INDEX.md` includes.
- Determinism rules enforced by renderer:
  - include markers must match contract exactly
  - include paths resolve relative to repository root
  - missing/invalid include targets fail hard
  - include order in `docs/INDEX.md` must exactly match this manifest
  - rendered output is normalized to UTF-8 with LF line endings
- Commit/push automation:
  - one-time setup: `git config core.hooksPath .githooks`
  - `.githooks/pre-commit` renders and stages `docs/NBNv2.md`
  - `.githooks/pre-push` runs freshness check

## CI and local runner guidance

- GitHub Actions workflow: `.github/workflows/docs-render.yml` (runs on `push` and `pull_request` on Windows + Linux).
- Single command to reproduce CI freshness check locally:
  - `bash tools/docs/render-nbnv2-docs.sh --check`
- Windows self-hosted runner equivalent:
  - `pwsh -NoProfile -File tools/docs/render-nbnv2-docs.ps1 -Check`
- Local `act` example (Linux job): `act pull_request -W .github/workflows/docs-render.yml -j render-and-validate`
