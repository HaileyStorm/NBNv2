# tools/dist

Owns distribution packaging, release metadata, tag-triggered release automation, and the shipped-binary contract Workbench uses in installed mode.

## Stable responsibilities

- Define the supported release artifact matrix, install layouts, command aliases, and versioning policy for shipped NBN binaries.
- Keep local release entrypoints manual and explicit-confirmation gated.
- Generate install layouts and `runtime-manifest.json` values that stay aligned with `tools/Nbn.Tools.Workbench/Services/LocalProjectLaunchPreparer.cs`.
- Build package artifacts from the tagged commit only; there is no every-push release path.

## Checked-in entrypoints

- `build-release.py`
  - shared release builder for version resolution, publish staging, runtime-manifest generation, command wrappers, and platform package assembly
- `release.sh`
  - Linux/macOS manual release entrypoint; requires `--confirm`
- `release.ps1`
  - Windows manual release entrypoint; requires `-Confirm`
- `release-config.json`
  - canonical bundle/application inventory and alias contract
- `packaging/windows/*.iss`
  - Inno Setup templates for suite and worker installers
- `packaging/linux/install-template.sh`
  - self-extracting `.run` installer stub used by the Linux release builder
- `.github/workflows/release.yml`
  - tag-triggered build/package/publish workflow for GitHub Releases
- `publish_worker_node.ps1` and `publish_worker_node.sh`
  - retained worker-only publish helpers for targeted manual publish/debug loops

## Release contract

- Releases are manual and tag-triggered.
- The canonical flow is:
  1. run `tools/dist/release.sh --confirm` or `tools/dist/release.ps1 -ConfirmRelease`
  2. validate docs freshness, build, and full tests
  3. compute the next `2.<minor>.<patch>` version from `release/version.json` plus existing tags
  4. create and push tag `v<version>`
  5. GitHub Actions builds, packages, and publishes the GitHub release from that tag
- The local release scripts refuse to tag or push without explicit confirmation.
- The published GitHub Release description must be curated and meaningful. A tag-only or generic placeholder description is not sufficient for an operator-facing release.
- Release descriptions should call out the major shipped changes, installation/runtime notes, validation performed, and any important caveats for operators.

## Supported artifacts

Every release publishes these eight assets:

1. `nbn-suite-<version>-win-x64-setup.exe`
2. `nbn-worker-<version>-win-x64-setup.exe`
3. `nbn-suite_<version>_amd64.deb`
4. `nbn-worker_<version>_amd64.deb`
5. `nbn-suite-<version>-linux-x64-installer.run`
6. `nbn-worker-<version>-linux-x64-installer.run`
7. `nbn-worker-<version>-win-x64-portable.zip`
8. `nbn-worker-<version>-linux-x64-portable.tar.gz`

## Bundle contents and aliases

Full suite bundle:

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

Worker-only bundle:

- `nbn-worker`

Installers put one `bin` directory on `PATH`. Worker portable archives do not modify `PATH`; run the command from the extracted directory instead.

## Install layout

Full suite roots:

- Windows: `C:\Program Files\NBN\`
- Linux: `/opt/nbn/`

Worker-only roots:

- Windows: `C:\Program Files\NBN Worker\`
- Linux: `/opt/nbn-worker/`

Shared layout rules:

- `bin/` contains user-facing command wrappers
- `apps/workbench/` contains the Workbench binary
- `services/*/` contains managed runtime service binaries
- `tools/*/` contains standalone CLI tools
- `share/runbooks/` carries checked-in operator runbooks in the full suite bundle
- `runtime-manifest.json` at the install root is the authoritative installed-layout map for Workbench installed-mode launches

## Workbench handoff

- Installed-mode command discovery is owned by `tools/Nbn.Tools.Workbench/Services/RepoLocator.cs` plus `tools/Nbn.Tools.Workbench/Services/LocalProjectLaunchPreparer.cs`.
- `runtime-manifest.json` entries must map aliases to executable paths relative to the install root because Workbench resolves relative manifest paths from the manifest directory.
- If the manifest is unavailable, Workbench falls back to the PATH aliases above.
- Installed Workbench must never build source projects or call `dotnet run`.

## Versioning

- Version format is `2.<minor>.<patch>`.
- `major` stays `2` in this repository.
- `minor` is changed manually in `release/version.json`.
- `patch` is computed automatically from existing `v2.<minor>.*` tags.
- Release tags use `v<version>`.

## Platform packaging

- Windows uses Inno Setup for full-suite and worker installers.
- Linux uses `dpkg-deb` for `.deb` packages and the checked-in `.run` installer stub for distro-agnostic installers.
- Worker portable archives reuse the staged worker install layout without writing system paths.

## Maintenance guidance

Keep this file concise and current. Update it when release entrypoints, artifact names, alias contracts, install layout, or Workbench packaging handoff change.
