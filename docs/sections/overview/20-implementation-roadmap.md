## 20. Implementation roadmap (tentative)

1. Define `.proto` and generate C# types
2. Implement `.nbn` reader/writer, validator, and quantization helpers
3. Implement RegionShard CPU backend and BrainSignalRouter delivery
4. Implement HiveMind global tick (compute+deliver), timeout accounting, and metrics
5. Implement SettingsMonitor registry + capability heartbeats
6. Implement IO Gateway + per-brain coordinators + Workbench IO/Energy panel
7. Implement `.nbs` snapshotting and full-brain recovery
8. Implement plasticity overlay tracking and optional rebasing
9. Implement reproduction manager (packed-domain transformations)
10. Implement artifact store with chunked dedup + local cache
11. Implement GPU backend with ILGPU kernel-per-function (CUDA first), parity tests, and placement heuristics
12. Expand Workbench Visualizer and Debug viewer; add orchestration conveniences

### 20.x Local demo script

For a minimal end-to-end smoke test, use the demo scripts:
`tools/demo/run_local_hivemind_demo.ps1` (Windows PowerShell) or
`tools/demo/run_local_hivemind_demo.sh` (Ubuntu/Linux).

`run_local_hivemind_demo.ps1` provides the full local stack flow:

* Creates a tiny `.nbn` (regions 0, 1, and 31 with 1 neuron each) with a single self-loop axon in region 1 to exercise SignalBatch delivery, and stores it in a local artifact store
* Starts SettingsMonitor first, then worker nodes (worker inventory/bootstrap), then central services (HiveMind, IO Gateway, Reproduction, and Observability)
* Spawns the demo brain through IO (`SpawnBrainViaIO`), with HiveMind-managed worker placement for BrainRoot/SignalRouter/coordinators/RegionShards
* Logs output to `tools/demo/local-demo/<timestamp>/logs`
* Workbench local-launch logs are written to `%LOCALAPPDATA%\Nbn.Workbench\logs` (for example `HiveMind.out.log`, `IoGateway.out.log`, `WorkerNode.out.log`, `Reproduction.out.log`, `Observability.out.log`, `SettingsMonitor.out.log`, and `workbench.log`)
* Includes an energy/plasticity scenario step via `Nbn.Tools.DemoHost io-scenario` that applies credit/rate/cost-energy/plasticity commands and emits JSON acks
* Includes a deterministic reproduction scenario via `Nbn.Tools.DemoHost repro-scenario` (default `spawn-policy=never`) that emits JSON with compatibility, abort code, mutation summary, and child artifact metadata
* Includes a deterministic reproduction verification suite via `Nbn.Tools.DemoHost repro-suite` that runs multiple behavior checks (success path, invalid input/media/reference paths, span-mismatch gate, strength-live fallback, and spawn-attempt path) and emits per-case pass/fail JSON

The demo uses default ports (SettingsMonitor 12010, WorkerNode base 11940, HiveMind 12020, IO 12050, Reproduction 12070, Observability 12060) and can be edited in the script
parameters if needed.

`run_local_hivemind_demo.sh` mirrors the worker-node-first startup flow on Linux and can run the same `io-scenario`/`repro-scenario` commands directly against the local demo services it launches.

Troubleshooting:

* If `io-scenario` returns `brain_not_found`, inspect `spawn.log` first, then verify worker health/inventory and placement completion before retrying scenario commands.
* If command requests timeout, verify `--io-address`/`--io-id` and ensure IO Gateway is running before scenario execution.
* If `repro-scenario` returns `repro_unavailable`, ensure IO launched with `--repro-address`/`--repro-name` and Reproduction is online.
* If `repro-scenario` returns `repro_parent_*_artifact_not_found`, verify `--store-uri` and parent artifact sha/size values.
* If `repro-suite` reports `all_passed=false`, inspect `repro-suite.log` case `failures` entries to identify the failing behavior contract.
* For abort-code triage procedures, use `tools/demo/reproduction_operator_runbook.md`.
* For observability checks, verify metrics/traces for:
  `nbn.hivemind.brain.tick_cost.total`,
  `nbn.hivemind.brain.energy.depleted`,
  `nbn.regionhost.plasticity.strength_code.changed`,
  `nbn.hivemind.snapshot.overlay.records`,
  `nbn.hivemind.rebase.overlay.records`.

---
