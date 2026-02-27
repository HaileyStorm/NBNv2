## 2. Technology stack and solution structure

### 2.1 Locked stack (v2)

* **Language:** C#
* **Runtime:** .NET (current LTS recommended)
* **Actor runtime:** Proto.Actor (.NET)
* **Remoting transport:** Proto.Remote over gRPC
* **Wire/message schema:** Protobuf (`.proto`)
* **Cross-platform GUI:** Avalonia UI (single â€œWorkbenchâ€ app)
* **Settings/metadata store:** SQLite + Dapper
* **Telemetry:** OpenTelemetry (metrics/traces/logs)
* **GPU compute (optional):** ILGPU (CUDA first; OpenCL if feasible), CPU fallback always
* **No native CUDA/HIP / no C++ requirement**

### 2.2 Solution layout (high-level)

Keep this intentionally high-level; the codebase is the source of truth for exact file ownership.

* `Nbn.Shared`: shared contracts/helpers (addressing, quantization, generated proto code, validation).
* Runtime service roots: `Nbn.Runtime.SettingsMonitor`, `Nbn.Runtime.HiveMind`, `Nbn.Runtime.Reproduction`, `Nbn.Runtime.IO`, `Nbn.Runtime.Observability`, `Nbn.Runtime.Artifacts`.
* Runtime execution: `Nbn.Runtime.Brain` (BrainRoot/BrainSignalRouter) and `Nbn.Runtime.RegionHost` (RegionShard workers, optional debug mirrors).
* Tools/UI: `Nbn.Tools.Workbench` (orchestrator, designer, visualizer, debug/IO/energy/reproduction consoles).
* Tests: `Nbn.Tests` (format, simulation, parity, reproduction).

### 2.3 Project tooling (Beads)

Beads usage constraints for this repository:

* Canonical tracker is repo-root `.beads/` only; do not initialize per-subfolder trackers.
* Run lifecycle commands from repo root (or pass `--db <repo>/.beads/beads.db`).
* Use `bd where` before lifecycle commands.
* Retire legacy project-local `.beads/` directories so state is not split.
* Use `bv --robot-*` for automation; avoid interactive `bv` in automated sessions.
* Avoid git worktrees with Beads in this repo.

### 2.4 Build/test defaults (required)

Always use these flags for local and CI builds/tests:

* **Configuration:** `-c Release`
* **Disable build servers:** `--disable-build-servers` for both `dotnet build` and `dotnet test`

Preferred commands:

* `dotnet build -c Release --disable-build-servers`
* `dotnet test -c Release --disable-build-servers` (add `--no-build` only if you already built with the same config)
* If local running processes are locking Release binaries, use an isolated output root:
  * `dotnet test -c Release --disable-build-servers --artifacts-path .artifacts-temp`
  * (same pattern applies to `dotnet build` when needed)

### 2.5 Workbench UI dispatch lifecycle (required)

To keep operator flows and tests deterministic across desktop and headless environments:

* Workbench view-model command results (status text, counters, summaries, and control state) must not depend on a running Avalonia UI event loop.
* Dispatcher wrappers must execute inline when no active application lifetime is running (for example, headless tests or startup/shutdown phases), and queue to UI thread only when the UI lifetime is active.
* Do not make correctness depend on eventual `Dispatcher.UIThread.Post(...)` queue drain.
* Workbench tests should pass regardless of whether headless Avalonia was initialized earlier in the same process.

---
