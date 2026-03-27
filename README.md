![NBN](docs/branding/png/nbn-soft-gold-right-n-logo.png)

# NBN

This repository contains the NBN codebase. There were previous attempts in other repository, thus the NBNv2 moniker and v2.0.0 initial release tag.

NBN is a distributed, tick-based neural simulation system that runs brains across
multiple runtime services and worker processes.

NBN is not a gradient-descent/backprop training framework.

## Start Here

- Full specification: [`docs/NBNv2.md`](docs/NBNv2.md)
- Spec assembly source template: [`docs/INDEX.md`](docs/INDEX.md)
- Release / installer guide: [`tools/dist/Design.md`](tools/dist/Design.md)
- Release version line: [`release/version.json`](release/version.json)
- Local release entrypoints: [`tools/dist/release.sh`](tools/dist/release.sh) and [`tools/dist/release.ps1`](tools/dist/release.ps1)
- Workbench operator surface: [`tools/Nbn.Tools.Workbench`](tools/Nbn.Tools.Workbench)
- Reproduction operator runbook: [`docs/runbooks/reproduction_operator_runbook.md`](docs/runbooks/reproduction_operator_runbook.md)
- Speciation operator runbook: [`docs/runbooks/speciation_operator_runbook.md`](docs/runbooks/speciation_operator_runbook.md)

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
- `docs/runbooks/`
  - operator runbooks
- `tests/`
  - `Nbn.Tests`

## Installation

- Full-suite artifacts are `nbn-suite-<version>-win-x64-setup.exe`, `nbn-suite_<version>_amd64.deb`, and `nbn-suite-<version>-linux-x64-installer.run`.
- Worker-only artifacts are `nbn-worker-<version>-win-x64-setup.exe`, `nbn-worker_<version>_amd64.deb`, `nbn-worker-<version>-linux-x64-installer.run`, `nbn-worker-<version>-win-x64-portable.zip`, and `nbn-worker-<version>-linux-x64-portable.tar.gz`.
- Full-suite installers place files under `C:\Program Files\NBN\` on Windows and `/opt/nbn/` on Linux. Worker-only installers use `C:\Program Files\NBN Worker\` and `/opt/nbn-worker/`.
- Installers place one `bin` directory on `PATH`. Portable worker archives do not modify `PATH`; run `bin/nbn-worker` from the extracted directory.
- Windows installers and installed Windows executables use the primary NBN icon. Linux full-suite installs also register a branded `NBN Workbench` desktop entry/icon where the target desktop environment honors standard application entries.
- Full-suite installed aliases are `nbn-workbench`, `nbn-settings`, `nbn-hivemind`, `nbn-io`, `nbn-repro`, `nbn-speciation`, `nbn-observability`, `nbn-worker`, `nbn-brainhost`, `nbn-regionhost`, `nbn-evolution-sim`, and `nbn-perf-probe`. Worker-only installs expose `nbn-worker`.
- Installed Workbench resolves managed runtime launches from `runtime-manifest.json` first and PATH aliases second. It does not require a source checkout or the .NET SDK for `Start` / `Start All`.
- `nbn-settings` is the installed alias for SettingsMonitor. Its key options are `--db <path>`, `--bind-host <host>`, `--port <port>`, `--advertise-host <host>`, and `--advertise-port <port>`.
- For SettingsMonitor and other remoted services, `--bind-host` / `--port` describe the local listening socket, while `--advertise-host` / `--advertise-port` describe the address other nodes should discover and use. For a multi-machine non-local-network run, the advertised host should be the discoverable IP or DNS name, not `127.0.0.1`.
- Example SettingsMonitor launch: `nbn-settings --db ./settingsmonitor.db --bind-host 0.0.0.0 --port 12010 --advertise-host 10.20.30.40 --advertise-port 12010`
- `nbn-worker` is the installed alias for WorkerNode. Its core connectivity options are `--bind-host`, `--port`, `--advertise-host`, `--advertise-port`, `--settings-host`, `--settings-port`, `--settings-name`, `--logical-name`, and `--root-name`.
- Worker resource quota flags are `--cpu-pct`, `--ram-pct`, `--storage-pct`, `--gpu-compute-pct`, and `--gpu-vram-pct` (or legacy `--gpu-pct`). These percentages define the worker availability it advertises for placement; lower values intentionally reserve headroom instead of offering the full machine.
- Example worker launch: `nbn-worker --bind-host 0.0.0.0 --port 12041 --advertise-host 10.20.30.41 --advertise-port 12041 --settings-host 10.20.30.40 --settings-port 12010 --settings-name SettingsMonitor --logical-name nbn.worker.lab-a --root-name worker-node --cpu-pct 75 --ram-pct 75 --storage-pct 80 --gpu-compute-pct 70 --gpu-vram-pct 70`

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

In `Orchestrator`, use `Start All`, then in `Designer` use `Generate Random Brain` and `Spawn Brain` for the current end-to-end local validation path.

## Manual Release Flow

- Releases are manual and tag-triggered; there is no every-push release path.
- Linux/macOS: `bash tools/dist/release.sh --confirm`
- Windows: `powershell -NoProfile -File tools/dist/release.ps1 -ConfirmRelease`
- The release scripts validate docs freshness, build/test in `Release`, compute the next `2.<minor>.<patch>` tag from `release/version.json`, create the tag, and push it so GitHub Actions can publish the eight release artifacts.
- The pushed tag triggers [`.github/workflows/release.yml`](.github/workflows/release.yml).

## Status / Roadmap

- Documentation is now canonically assembled into `docs/NBNv2.md` from `docs/INDEX.md`.
- Active areas include runtime orchestration, observability, reproduction, and operator tooling.

## Contributing Notes

- Keep detailed behavior and contracts in [`docs/NBNv2.md`](docs/NBNv2.md); keep this README concise.
- Cross-cutting docs live in repo-root `docs/sections/*`; operator runbooks live in `docs/runbooks/*`.
- Project-specific docs belong next to code as `src/*/Design.md`, `tools/*/Design.md`, and `tests/*/Design.md`.
- Do not create project-level `Docs/` or `docs/` subfolders under `src/*`, `tools/*`, or `tests/*`.
