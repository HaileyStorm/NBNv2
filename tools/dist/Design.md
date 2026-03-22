# tools/dist

Owns distribution packaging, installer layout, release automation, versioning, and the canonical shipped-binary contract for NBN.

## Stable responsibilities

- Define the canonical release artifact matrix, install layouts, alias contract, and versioning policy for shipped NBN binaries.
- Define how Workbench-installed launches find packaged binaries without depending on a source checkout.
- Keep release automation manual and tag-triggered, with GitHub Releases built from the tagged commit.

## Decisions

- Releases are manual and tag-triggered. They are not created on every push.
- The canonical release flow is: local release command -> validation -> git tag push -> GitHub Actions builds artifacts -> GitHub Release is created from the tag.
- Windows installers use Inno Setup.
- Linux ships two installer forms for each supported bundle: `.deb` and a distro-agnostic self-extracting installer.
- Worker-only also ships portable archives for Windows and Linux.
- Version format is `2.<minor>.<patch>`.
- `major` stays `2` for this repository. A major change means a new repository line, not an in-place release bump.
- `minor` is manual.
- `patch` is automatic and increments from the latest existing tag in the current `2.<minor>.x` line.
- Workbench supports two launch modes:
  - local source-checkout mode
  - installed-binary mode
- Workbench must never use `dotnet run` or source-project builds when it is running as an installed binary.

## Why This Exists

Current NBN release/install behavior is not productized yet.

Evidence from the current repository:

- Workbench local launch currently preflights a Release build and can fall back to `dotnet run --project ... --no-build` from source directories. See `tools/Nbn.Tools.Workbench/Services/LocalProjectLaunchPreparer.cs`.
- Workbench design docs currently define Local Launch as a source-oriented Release-build flow rather than an installed-package flow. See `tools/Nbn.Tools.Workbench/Design.md`.
- The only existing distribution helper is worker-only publishing. See `tools/dist/publish_worker_node.ps1` and `tools/dist/publish_worker_node.sh`.
- Current GitHub Actions automation only validates docs freshness. See `.github/workflows/docs-render.yml`.
- NBN itself already requires cross-platform execution and explicitly supports worker-only processes. See `docs/NBNv2.md`.

This design closes those gaps with one coherent release model.

## Supported Release Artifacts

Every release publishes these eight assets:

1. `nbn-suite-<version>-win-x64-setup.exe`
2. `nbn-worker-<version>-win-x64-setup.exe`
3. `nbn-suite_<version>_amd64.deb`
4. `nbn-worker_<version>_amd64.deb`
5. `nbn-suite-<version>-linux-x64-installer.run`
6. `nbn-worker-<version>-linux-x64-installer.run`
7. `nbn-worker-<version>-win-x64-portable.zip`
8. `nbn-worker-<version>-linux-x64-portable.tar.gz`

There is no portable full-suite artifact in v1.

## Bundle Contents

### Full suite bundle

The full suite includes every current executable project:

- `Nbn.Tools.Workbench`
- `Nbn.Runtime.SettingsMonitor`
- `Nbn.Runtime.HiveMind`
- `Nbn.Runtime.IO`
- `Nbn.Runtime.Reproduction`
- `Nbn.Runtime.Speciation`
- `Nbn.Runtime.Observability`
- `Nbn.Runtime.WorkerNode`
- `Nbn.Runtime.BrainHost`
- `Nbn.Runtime.RegionHost`
- `Nbn.Tools.EvolutionSim`
- `Nbn.Tools.PerfProbe`

### Worker-only bundle

The worker-only bundle includes:

- `Nbn.Runtime.WorkerNode`
- worker-only support files needed by that publish output
- worker-only README / quick-start text

The worker-only bundle does not include Workbench or the rest of the runtime service/tool suite.

## Command Alias Contract

Installers place one `bin` directory on `PATH`. That `bin` directory exposes these commands.

Full suite aliases:

- `nbn-workbench`
- `nbn-settings`
- `nbn-hivemind`
- `nbn-io`
- `nbn-repro`
- `nbn-speciation`
- `nbn-observability`
- `nbn-worker`
- `nbn-brainhost`
- `nbn-regionhost`
- `nbn-evolution-sim`
- `nbn-perf-probe`

Worker-only aliases:

- `nbn-worker`

Portable worker archives do not modify `PATH`. The user runs the worker binary from the extracted directory.

## Install Layout

The installer layout is normalized around a manifest and a single PATH directory.

Windows full suite root:

- `C:\Program Files\NBN\`
- `bin\` contains shims or launchers for the aliases above
- `apps\workbench\`
- `services\settings\`
- `services\hivemind\`
- `services\io\`
- `services\reproduction\`
- `services\speciation\`
- `services\observability\`
- `services\worker\`
- `services\brainhost\`
- `services\regionhost\`
- `tools\evolution-sim\`
- `tools\perf-probe\`
- `share\runbooks\`
- `runtime-manifest.json`

Linux full suite root:

- `/opt/nbn/`
- same internal structure as the Windows install root

Worker-only roots:

- Windows: `C:\Program Files\NBN Worker\`
- Linux: `/opt/nbn-worker/`

`runtime-manifest.json` is the authoritative installed-layout map that Workbench and generated shims use to locate binaries.

## Workbench Launch Behavior

Workbench now has a fixed two-mode contract.

### Local source-checkout mode

Workbench uses local mode only when it can positively identify that it is running from a repository checkout.

Detection requirements:

- find repo markers relative to the current executable location
- markers must include `NBNv2.sln`, `Directory.Build.props`, and the expected project roots for Workbench plus the launched services

Behavior in local mode:

- keep relative-path source-project discovery
- build target projects in Release before launch
- run the freshly built sibling executable when present
- keep the current local development safety rule that source edits must not silently reuse stale binaries
- `dotnet run --no-build` remains a local-only fallback when an executable output is not available after a successful build

### Installed-binary mode

Workbench uses installed mode whenever local repo detection fails.

Behavior in installed mode:

- never build source projects
- never call `dotnet run`
- resolve service/tool commands from the installed `runtime-manifest.json`
- if the manifest is unavailable, fall back to PATH aliases such as `nbn-settings`, `nbn-worker`, and `nbn-observability`
- launch only installed sibling binaries or PATH-resolved installed commands

The installed-mode command mapping must cover at least the services Workbench currently manages in Orchestrator `Start` / `Start All`:

- SettingsMonitor
- HiveMind
- IO
- Reproduction
- Speciation
- WorkerNode
- Observability

This preserves the current SettingsMonitor-first bootstrap model while removing the source-tree dependency from installed Workbench.

## Versioning Policy

Release tags use `v<major>.<minor>.<patch>`, for example `v2.0.0`.

Tracked version control file:

- `release/version.json`

Schema:

```json
{
  "major": 2,
  "minor": 0
}
```

Rules:

- `major` is fixed to `2` in this repository and is not changed during normal release work
- `minor` is changed manually by a human or agent in `release/version.json`
- `patch` is computed automatically by scanning existing tags matching `v2.<minor>.*`
- if no tags exist for the current line, the first release is `.0`
- the release command aborts if the computed tag already exists

The resolved version is stamped into:

- release asset names
- installer display version
- GitHub Release title
- publish metadata for shipped executables

## Release Workflow

Canonical entrypoints:

- Windows: `tools/release/release.ps1`
- Linux: `tools/release/release.sh`

Release command responsibilities:

1. verify clean git state
2. read `release/version.json`
3. compute next patch version from existing tags
4. run docs freshness check
5. run Release build
6. run Release tests
7. optionally run targeted publish smoke checks
8. create tag `v<version>`
9. push the tag

GitHub Actions responsibilities after tag push:

1. validate docs freshness
2. build and test on Windows and Ubuntu
3. publish RID-specific outputs
4. assemble Windows Inno Setup installers
5. assemble Linux `.deb` packages
6. assemble Linux distro-agnostic `.run` installers
7. assemble worker-only portable archives
8. create the GitHub Release and attach all eight artifacts

The release workflow triggers from pushed tags matching `v*`.

## Packaging Strategy

### Publish outputs

Each shipped executable gets a dedicated publish output for its target RID.

Windows and Linux runtime targets:

- `win-x64`
- `linux-x64`

Worker-only portable outputs stay self-contained and single-file where supported.

Full-suite packaging uses per-application publish directories plus generated alias shims. This is more robust than assuming every shipped executable must be flattened into one directory.

### Windows packaging

Windows installer implementation is Inno Setup.

The Windows packaging stage:

- collects published outputs into the install layout
- generates or stages command shims in `bin`
- writes `runtime-manifest.json`
- compiles `nbn-suite.iss`
- compiles `nbn-worker.iss`

The installers:

- add the install `bin` directory to PATH
- add Start Menu entries for Workbench and uninstallers
- optionally add a desktop shortcut for Workbench in the full suite installer
- support uninstall and upgrade-in-place

### Linux packaging

Linux ships two installer forms per bundle.

`.deb` packaging:

- installs into `/opt/nbn` or `/opt/nbn-worker`
- places symlinks or wrapper scripts in `/usr/local/bin` or the distro-appropriate command location
- registers uninstall cleanly through the package manager

Distro-agnostic `.run` packaging:

- extracts and installs into `/opt/nbn` or `/opt/nbn-worker`
- creates command wrappers in a system bin location or prints a post-install command to do so
- provides an uninstall script in the install root

Portable worker archives:

- extract to any directory
- run `nbn-worker` from that directory
- do not write PATH or system locations

## End-User Flows

### Full suite on Windows

1. Download `nbn-suite-<version>-win-x64-setup.exe`
2. Run the installer
3. Launch Workbench from Start Menu or `nbn-workbench`
4. Use `Start All` without needing the .NET SDK or a source checkout

### Full suite on Linux

1. Download either the `.deb` or `.run` installer
2. Install it
3. Launch Workbench from the desktop menu if available or `nbn-workbench`
4. Use `Start All` without needing the .NET SDK or a source checkout

### Worker-only

1. Install the worker package or extract the portable archive
2. Run `nbn-worker --help` or invoke it with the normal worker options and environment variables
3. Connect it to the existing SettingsMonitor bootstrap coordinates

## Verification Requirements

Every release candidate must verify:

- docs freshness passes
- solution Release build passes
- solution Release tests passes
- Workbench local mode still works from a source checkout
- installed-mode Workbench never attempts source builds
- installed-mode Workbench `Start All` resolves installed commands correctly
- worker-only Windows installer installs and runs
- worker-only Linux `.deb` installs and runs
- worker-only Linux `.run` installs and runs
- worker-only portable archives launch successfully
- full-suite Windows installer launches Workbench and managed services
- full-suite Linux `.deb` and `.run` installers launch Workbench and managed services

## AGENTS.md Follow-Up

Repo-root `AGENTS.md` must gain a release section that tells agents:

- releases are manual and require explicit human confirmation
- the canonical release entrypoint is the repo release script
- tag push triggers the actual packaging and GitHub Release workflow
- agents may suggest that a release is appropriate, but must wait for confirmation before running the release flow

## Implementation Order

1. Add Workbench local-vs-installed launch mode abstraction and manifest/PATH-based installed resolution.
2. Generalize publish scripts from worker-only to all shipped executables.
3. Add release metadata files under `release/`.
4. Add Windows Inno Setup scripts and staging logic.
5. Add Linux `.deb` packaging scripts.
6. Add Linux distro-agnostic `.run` packaging scripts.
7. Add worker-only portable archive packaging.
8. Add tag-triggered GitHub Actions release workflow.
9. Add local release entry scripts.
10. Add release/install documentation and `AGENTS.md` release guidance.
11. Add smoke tests or scripted verification for installed-mode Workbench and packaged worker launch.

## Source Evidence

Repository evidence used for this plan:

- `tools/Nbn.Tools.Workbench/Services/LocalProjectLaunchPreparer.cs`
- `tools/Nbn.Tools.Workbench/ViewModels/OrchestratorPanelViewModel.cs`
- `tools/Nbn.Tools.Workbench/Design.md`
- `src/Nbn.Runtime.WorkerNode/WorkerNodeOptions.cs`
- `tools/dist/publish_worker_node.ps1`
- `tools/dist/publish_worker_node.sh`
- `.github/workflows/docs-render.yml`
- `README.md`
- `docs/NBNv2.md`

External reference points:

- .NET deployment overview: <https://learn.microsoft.com/en-us/dotnet/core/deploying/>
- .NET single-file deployment: <https://learn.microsoft.com/en-us/dotnet/core/deploying/single-file/overview>
- GitHub Actions tag and manual workflow docs: <https://docs.github.com/en/actions/reference/events-that-trigger-workflows>
- GitHub CLI release creation: <https://cli.github.com/manual/gh_release_create>
- Inno Setup compiler CLI: <https://jrsoftware.org/ishelp/index.php?topic=compilercmdline>

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable release behavior, artifact shapes, install layout, alias names, versioning rules, or automation ownership changes. Keep Workbench-specific launch invariants aligned with `tools/Nbn.Tools.Workbench/Design.md` instead of redefining UI/runtime semantics here.
