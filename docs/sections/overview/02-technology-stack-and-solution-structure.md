## 2. Technology stack and solution structure

### 2.1 Locked stack (v2)

* **Language:** C#
* **Runtime:** .NET (current LTS recommended)
* **Actor runtime:** Proto.Actor (.NET)
* **Remoting transport:** Proto.Remote over gRPC
* **Wire/message schema:** Protobuf (`.proto`)
* **Cross-platform GUI:** Avalonia UI (single “Workbench” app)
* **Settings/metadata store:** SQLite + Dapper
* **Telemetry:** OpenTelemetry (metrics/traces/logs)
* **GPU compute (optional):** ILGPU (CUDA first; OpenCL if feasible), CPU fallback always
* **No native CUDA/HIP / no C++ requirement**

### 2.2 Solution layout (high-level)

* `Nbn.Shared`: shared contracts/helpers (addressing, quantization, generated proto code, validation).
* Runtime service roots: `Nbn.Runtime.SettingsMonitor`, `Nbn.Runtime.HiveMind`, `Nbn.Runtime.Reproduction`, `Nbn.Runtime.IO`, `Nbn.Runtime.Observability`, `Nbn.Runtime.Artifacts`.
* Runtime execution: `Nbn.Runtime.Brain` (BrainRoot/BrainSignalRouter) and `Nbn.Runtime.RegionHost` (RegionShard workers, optional debug mirrors).
* Tools/UI: `Nbn.Tools.Workbench` (operator desktop over runtime APIs and local launch helpers), `Nbn.Tools.EvolutionSim` (standalone artifact-first evolution/speciation stress simulator), `Nbn.Tools.PerfProbe` (placement/runtime profiling and report generation).
* Tests: `Nbn.Tests` (format, simulation, parity, reproduction).
