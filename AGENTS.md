# NBNv2 Agent Operating Guide

## Scope and precedence

- Repo-local guide for coding agents in this repository.
- Global baseline remains `~/.codex/AGENTS.md`.
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
  - operator runbooks in repo-root `docs/runbooks/*`
  - project docs near code in `src/*/Design.md`, `tools/*/Design.md`, `tests/*/Design.md`
- Do not create `Docs/` or `docs/` subfolders under `src/*`, `tools/*`, or `tests/*`; project docs must live at project root.

## Codex multi-agent doc-first workflow (required)

- For non-trivial code/behavior tasks, start with `nbn_spec_guard` or an equivalent scout that reads `docs/NBNv2.md` before deep code analysis.
- For clearly bounded mechanical or docs-only edits, that full spec pass is optional when the task does not depend on spec details.
- Default repo fan-out for behavior work:
  1. `nbn_spec_guard` for expected behavior, ownership boundaries, and doc anchors
  2. `nbn_runtime_invariants` for region/tick/snapshot/reproduction rules
  3. `test_mapper` or `verifier` for regression surface and commands
- Prefer multiple narrow agents over one broad worker; keep final synthesis and merge decisions in the main thread.
- When the spec or code surface is large, split it across fresh read-only agents and compress the findings with `packetizer` before editing.
- After any failed, aborted, or partially-applied edit attempt, immediately re-read the affected file(s) from disk and inspect `git diff` before making the next edit. Never assume a large patch landed exactly as intended.
- For large-file edits, prefer smaller verified patches over one broad rewrite, and verify exact anchor text before every scripted replacement.
- Continue to refer back to `docs/NBNv2.md` and nearby `Design.md` docs throughout edits, verification, and handoff; re-run targeted repo agents when scope shifts.

## Repo-specific agent roles

- `nbn_spec_guard`: reads `docs/NBNv2.md` and relevant `Design.md` files, then maps expected behavior, ownership, and doc impact.
- `nbn_runtime_invariants`: checks region `0`/`31`, tick barrier ordering, snapshot/recovery, and reproduction protections against the assigned change.
- `nbn_docs_guard`: decides whether canonical docs or render/check commands must change, and points to the exact doc files.

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
  - Windows: `powershell -NoProfile -File tools/docs/render-nbnv2-docs.ps1`
  - Linux/macOS: `bash tools/docs/render-nbnv2-docs.sh`
- Freshness check (same policy as CI):
  - Windows: `powershell -NoProfile -File tools/docs/render-nbnv2-docs.ps1 -Check`
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
- Tools: `Nbn.Tools.Workbench`, `Nbn.Tools.EvolutionSim`, `Nbn.Tools.PerfProbe`
- Tests: `Nbn.Tests`

## Runtime flow (how components connect)

1. External clients interact through `Nbn.Runtime.IO` (IO Gateway), not directly with shards.
2. Spawn/control requests flow to `Nbn.Runtime.HiveMind`, which owns global tick pacing and brain lifecycle.
3. HiveMind coordinates `Nbn.Runtime.Brain` actors (BrainRoot/SignalRouter) and placement onto `Nbn.Runtime.RegionHost` shards, typically via worker processes (`Nbn.Runtime.WorkerNode`).
4. Each tick runs compute then deliver globally; region outputs from tick `N` are routed and become input to compute on tick `N+1`.
5. Shared contracts, addressing, and format/proto helpers come from `Nbn.Shared`; artifacts/snapshots are managed through artifact/runtime services.
6. Observability streams and metrics flow through `Nbn.Runtime.Observability`; Workbench and standalone tools are operator/tooling surfaces over these runtime APIs.

## Critical invariants (do not break)

- Input region is always `0`; output region is always `31`.
- No axon may target region `0`.
- Region `31` may emit axons but may not target region `31`.
- For one source neuron, duplicate target `(target_region_id, target_neuron_id)` is invalid.
- Global tick is two-phase: compute then deliver.
- Signals from tick `N` are visible at compute of tick `N+1`.
- Do not dispatch compute `N+1` until deliver `N` completes (or timeout policy finalizes it).
- Snapshots are taken at tick boundaries; recovery restores full brain from `.nbn + .nbs`.
- Reproduction defaults to protecting neuron counts in regions `0` and `31`.
- When explicitly requested by an external reproduction caller, manual neuron additions/removals in regions `0` and `31` are allowed; IO-region axon invariants still apply.

## Build/test defaults (required)

- `dotnet build -c Release --disable-build-servers`
- `dotnet test -c Release --disable-build-servers`
- Use filtered `dotnet test --filter ...` runs during development when they materially shorten iteration.
- Before completing a turn, run the full suite with `dotnet test -c Release --disable-build-servers`.
- Do not run a filtered set as a gate immediately before the full-suite run. If the next step is the full suite, run the full suite directly.
- If binaries are locked:
  - `dotnet build -c Release --disable-build-servers --artifacts-path .artifacts-temp`
  - `dotnet test -c Release --disable-build-servers --artifacts-path .artifacts-temp`
- When tests create temporary SQLite-backed artifact stores on Windows, clear pools before deleting the temp directory (`SqliteConnection.ClearAllPools()`) or cleanup may fail on `artifacts.db` locks.

## Artifact store bootstrap

- Non-file `store_uri` values must resolve through the shared artifact resolver, not local-path fallback.
- Runtime fetch/store callers that honor this path include `Nbn.Runtime.HiveMind`, `Nbn.Runtime.RegionHost`, `Nbn.Runtime.WorkerNode`, and `Nbn.Runtime.Reproduction`.
- When no in-process adapter registration path is available, bootstrap exact mappings through `NBN_ARTIFACT_STORE_URI_MAP`.
- Expected format: JSON object mapping exact `store_uri` strings to backing roots or adapter targets.
- Example: `{"memory+prod://artifact-store/main":"D:\\nbn\\artifact-mirror","s3+cache://cluster-a/artifacts":"E:\\nbn\\s3-cache"}`

## Workbench UI dispatch rule (required)

- View-model command results must be correct when no Avalonia UI loop is running.
- Dispatcher wrappers execute inline when no active UI lifetime exists.
- Correctness must not depend on eventual `Dispatcher.UIThread.Post(...)` drain.

## Beads rules (repo-specific)

- Beads/BV lifecycle rules are defined in `~/.codex/AGENTS.md`.
- This repo does not add Beads-specific overrides.

## Release automation

- The release flow is manual and tag-triggered; do not add or run an every-push release path.
- Canonical local entrypoints are `tools/dist/release.sh --confirm` and `tools/dist/release.ps1 -ConfirmRelease`.
- Those scripts validate docs freshness, build/test, compute the next version from `release/version.json`, create the tag, and push it.
- The pushed tag triggers `.github/workflows/release.yml`, which builds installers/packages and publishes the GitHub Release.
- When a release is performed, agents must ensure the final GitHub Release title/body is meaningful and operator-facing. Do not leave placeholder text, tag-only text, or generic one-line workflow text in the published release.
- A proper release description should summarize the important operator-visible changes, relevant installation/runtime notes, validation performed, and any caveats or follow-up attention items.
- If automation creates only a placeholder release shell, update it afterward with `gh release edit` (or equivalent) before considering the release complete.
- Agents must not run the release scripts, create release tags, or push release tags without explicit human confirmation in the active session.
- Package/install contract details live in `tools/dist/Design.md`; keep release automation, alias names, and Workbench installed-mode behavior aligned with that file.

## Landing the Plane (Session Completion)

When ending a work session, complete the full landing flow. Work is not complete until changes are committed and pushed.

1. File follow-up issues for remaining work.
2. Run quality gates for changed code.
3. Update issue status.
4. Push to remote:
   `git pull --rebase`
   `bd sync`
   `git push`
   `git status` must show up to date with origin.
5. Clean up temporary state you created.
6. Verify intended changes are committed and pushed.
7. Hand off context for the next session.

Critical rules:
- Do not stop at "ready to push".
- If push fails, resolve it and retry.
