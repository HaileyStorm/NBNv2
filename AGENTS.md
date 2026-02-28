# NBNv2 Agent Operating Guide

## Scope and precedence

- Repo-local guide for coding agents in this repository.
- Global baseline remains `C:\Users\Haile\.codex\AGENTS.md`.
- If rules conflict, this file wins for this repo.

## NBN in one minute

- NBN is a distributed, tick-based neural simulation system where brains are split into regions and region shards across worker processes.
- Runtime coordination is actor-based (Proto.Actor remoting over gRPC) with protobuf contracts and deterministic tick barriers.
- Brain definition/state artifacts use `.nbn` (definition) and `.nbs` (snapshot/overlay) formats.
- Reproduction/evolution and observability are first-class runtime features.
- NBN is not a gradient-descent/backprop training framework.

## Canonical documentation workflow

- Template and reader entrypoint: `docs/INDEX.md`.
- Generated full specification: `docs/NBNv2.md`.
- Include/order manifest: `docs/manifest/NBNv2-DocumentMap.md`.
- Canonical split-doc locations:
  - cross-cutting sections in repo-root `docs/sections/*`
  - project docs near code in `src/*/Design.md`, `tools/*/Design.md`, `tests/*/Design.md`
- Do not create `Docs/` or `docs/` subfolders under `src/*`, `tools/*`, or `tests/*`; project docs must live at project root.

## Documentation maintenance policy (required)

When making code changes, update relevant canonical docs if behavior, decisions, ownership boundaries, invariants, or externally-visible contracts changed.

Keep docs concise and high-value:

1. Add or update only stable and reusable information.
2. Prefer editing or replacing outdated text over appending history.
3. Avoid transient run logs, speculative ideas, or local debugging transcripts.
4. If detail is useful but too large/noisy, create a focused follow-up issue instead of bloating canonical docs.

## Include marker contract

- Syntax: `<!-- NBN:INCLUDE path="..." -->`
- Paths are repository-root relative.
- Deterministic order comes from `docs/manifest/NBNv2-DocumentMap.md`.

## Documentation render automation (required)

- Renderer implementation: `tools/docs/render_nbnv2_docs.py`.
- OS wrappers:
  - Windows: `pwsh -NoProfile -File tools/docs/render-nbnv2-docs.ps1`
  - Linux/macOS: `bash tools/docs/render-nbnv2-docs.sh`
- Freshness check (same policy as CI):
  - Windows: `pwsh -NoProfile -File tools/docs/render-nbnv2-docs.ps1 -Check`
  - Linux/macOS: `bash tools/docs/render-nbnv2-docs.sh --check`
- Freshness policy: CI fails when `docs/NBNv2.md` is stale relative to `docs/INDEX.md` includes.
- Assembly scope: `docs/NBNv2.md` includes the spec template sections/appendices from `docs/sections/*` only; project `*/Design.md` docs remain standalone.
- Commit/push hook policy:
  - Canonical hooks live in `.githooks/pre-commit` and `.githooks/pre-push`.
  - One-time setup command: `git config core.hooksPath .githooks`.
  - `pre-commit` renders and stages `docs/NBNv2.md`; `pre-push` runs freshness check.
- CI workflow: `.github/workflows/docs-render.yml` runs the check on `push` and `pull_request` for Windows and Linux.

## Architecture snapshot

- Shared/contracts: `Nbn.Shared`
- Runtime services: `Nbn.Runtime.SettingsMonitor`, `Nbn.Runtime.HiveMind`, `Nbn.Runtime.IO`, `Nbn.Runtime.Reproduction`, `Nbn.Runtime.Observability`, `Nbn.Runtime.Artifacts`
- Runtime execution: `Nbn.Runtime.Brain`, `Nbn.Runtime.BrainHost`, `Nbn.Runtime.RegionHost`, `Nbn.Runtime.WorkerNode`
- Tools: `Nbn.Tools.Workbench`, `Nbn.Tools.DemoHost`
- Tests: `Nbn.Tests`

## Runtime flow (how components connect)

1. External clients interact through `Nbn.Runtime.IO` (IO Gateway), not directly with shards.
2. Spawn/control requests flow to `Nbn.Runtime.HiveMind`, which owns global tick pacing and brain lifecycle.
3. HiveMind coordinates `Nbn.Runtime.Brain` actors (BrainRoot/SignalRouter) and placement onto `Nbn.Runtime.RegionHost` shards, typically via worker processes (`Nbn.Runtime.WorkerNode`).
4. Each tick runs compute then deliver globally; region outputs from tick `N` are routed and become input to compute on tick `N+1`.
5. Shared contracts, addressing, and format/proto helpers come from `Nbn.Shared`; artifacts/snapshots are managed through artifact/runtime services.
6. Observability streams and metrics flow through `Nbn.Runtime.Observability`; Workbench and DemoHost are operator/tooling surfaces over these runtime APIs.

## Critical invariants (do not break)

- Input region is always `0`; output region is always `31`.
- No axon may target region `0`.
- Region `31` may emit axons but may not target region `31`.
- For one source neuron, duplicate target `(target_region_id, target_neuron_id)` is invalid.
- Global tick is two-phase: compute then deliver.
- Signals from tick `N` are visible at compute of tick `N+1`.
- Do not dispatch compute `N+1` until deliver `N` completes (or timeout policy finalizes it).
- Snapshots are taken at tick boundaries; recovery restores full brain from `.nbn + .nbs`.
- Reproduction cannot add/remove neurons in regions `0` or `31`.

## Build/test defaults (required)

- `dotnet build -c Release --disable-build-servers`
- `dotnet test -c Release --disable-build-servers`
- If binaries are locked:
  - `dotnet build -c Release --disable-build-servers --artifacts-path .artifacts-temp`
  - `dotnet test -c Release --disable-build-servers --artifacts-path .artifacts-temp`

## Workbench UI dispatch rule (required)

- View-model command results must be correct when no Avalonia UI loop is running.
- Dispatcher wrappers execute inline when no active UI lifetime exists.
- Correctness must not depend on eventual `Dispatcher.UIThread.Post(...)` drain.

## Beads rules (repo-specific)

- Canonical tracker: repo-root `.beads`.
- Do not initialize per-subfolder trackers.
- Run lifecycle commands from repo root (or pass explicit DB path).
- Use `bd where` before lifecycle operations.
- Use `bv --robot-*` for automation.
- Avoid git worktrees for Beads-governed work in this repo.
