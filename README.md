![NBN](docs/branding/png/nbn-soft-gold-right-n-logo.png)

# NBN

This repository contains the NBN codebase and retains the historical `NBNv2` name in some paths and files.

NBN is a distributed, tick-based neural simulation system that runs brains across
multiple runtime services and worker processes.

NBN is not a gradient-descent/backprop training framework.

## Start Here

- Full specification: [`docs/NBNv2.md`](docs/NBNv2.md)
- Spec assembly source template: [`docs/INDEX.md`](docs/INDEX.md)
- Workbench operator surface: [`tools/Nbn.Tools.Workbench`](tools/Nbn.Tools.Workbench)
- Reproduction operator runbook: [`tools/demo/reproduction_operator_runbook.md`](tools/demo/reproduction_operator_runbook.md)
- Speciation operator runbook: [`tools/demo/speciation_operator_runbook.md`](tools/demo/speciation_operator_runbook.md)

## Goals

- Simulate brains with deterministic global tick semantics.
- Scale region shards across worker processes and hosts.
- Keep runtime operations observable (debug, visualization, metrics, tracing).

## Architecture Snapshot

1. External clients interact through `Nbn.Runtime.IO` (IO Gateway).
2. `Nbn.Runtime.HiveMind` controls global tick pacing and brain lifecycle.
3. HiveMind coordinates `Nbn.Runtime.Brain`, `Nbn.Runtime.RegionHost`, and `Nbn.Runtime.WorkerNode`.
4. Tick execution is two-phase: compute then deliver; outputs from tick `N` are inputs for compute on tick `N+1`.
5. Shared contracts and formats live in `Nbn.Shared`.
6. Tooling surfaces include `Nbn.Tools.Workbench` plus targeted CLI tools under `tools/`.

## Repository Layout

- `src/`
  - runtime services: `Nbn.Runtime.*`
  - shared contracts and helpers: `Nbn.Shared`
- `tools/`
  - `Nbn.Tools.Workbench` (Avalonia UI)
  - `Nbn.Tools.DemoHost` (targeted scenario CLI)
  - `demo/` runbooks
- `tests/`
  - `Nbn.Tests`

## Build And Test

```bash
dotnet build NBNv2.sln -c Release --disable-build-servers
dotnet test NBNv2.sln -c Release --disable-build-servers
```

If binaries are locked on your machine:

```bash
dotnet build NBNv2.sln -c Release --disable-build-servers --artifacts-path .artifacts-temp
dotnet test NBNv2.sln -c Release --disable-build-servers --artifacts-path .artifacts-temp
```

## Local Launch

```bash
dotnet run --project tools/Nbn.Tools.Workbench -c Release
```

In `Orchestrator`, use `Start All`, then `Spawn Sample Brain` for the current end-to-end local validation path.

## Status / Roadmap

- Documentation is now canonically assembled into `docs/NBNv2.md` from `docs/INDEX.md`.
- Active areas include runtime orchestration, observability, reproduction, and operator tooling.

## Contributing Notes

- Keep detailed behavior and contracts in [`docs/NBNv2.md`](docs/NBNv2.md); keep this README concise.
- Cross-cutting docs live in repo-root `docs/sections/*`.
- Project-specific docs belong next to code as `src/*/Design.md`, `tools/*/Design.md`, and `tests/*/Design.md`.
- Do not create project-level `Docs/` or `docs/` subfolders under `src/*`, `tools/*`, or `tests/*`.
