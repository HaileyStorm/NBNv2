# tools/dist

Owns the checked-in distribution helper scripts and the packaging boundary they share with Workbench installed-mode launch resolution.

## Stable responsibilities

- Keep the Windows and Linux WorkerNode publish helpers behaviorally aligned.
- Produce manual WorkerNode publish outputs for supported RIDs without claiming repo-local installer or release automation contracts that do not exist.
- Document the handoff boundary between `tools/dist` and `tools/Nbn.Tools.Workbench`: Workbench can consume installed commands or a `runtime-manifest.json`, but this folder does not generate a suite-wide package today.

## Current scope

- `publish_worker_node.ps1` and `publish_worker_node.sh` are the only checked-in distribution entrypoints in this directory.
- Both scripts publish `src/Nbn.Runtime.WorkerNode/Nbn.Runtime.WorkerNode.csproj` in `Release` with `--disable-build-servers`.
- Default output root is `artifacts/dist/worker-node/<rid>`.
- Default target RIDs are `win-x64` and `linux-x64`.
- Publish outputs are single-file, self-extracting native-library bundles with trimming disabled and debug symbols omitted.
- The PowerShell helper defaults to self-contained publish and exposes `-FrameworkDependent` to opt out; the bash helper defaults to self-contained publish and accepts `SELF_CONTAINED=false`.
- The scripts are intended for manual publish/test/distribution flows. They do not create installers, tags, release notes, GitHub Releases, or version metadata files.

## Workbench Boundary

- Installed-mode command discovery is owned by `tools/Nbn.Tools.Workbench/Services/LocalProjectLaunchPreparer.cs` and verified by `tests/Nbn.Tests/Workbench/LocalProjectLaunchPreparerTests.cs`.
- Workbench can resolve installed commands from a nearby `runtime-manifest.json` or from PATH aliases when an external package provides them.
- `tools/dist` does not currently generate `runtime-manifest.json`, package layouts, or canonical alias shims for the full suite. Any future packaging work must stay aligned with the command labels and local-vs-installed launch contract already documented in `tools/Nbn.Tools.Workbench/Design.md`.

## Source layout

- `publish_worker_node.ps1` owns the Windows-oriented publish entrypoint and framework-dependent opt-out switch.
- `publish_worker_node.sh` owns the POSIX publish entrypoint and environment-variable configuration surface.

## Maintenance guidance

Keep this file concise and current. Update it when the checked-in publish helpers, supported publish shape, or Workbench packaging handoff changes. Do not document speculative installer or release pipelines here until the corresponding scripts or automation are actually present in the repo.
