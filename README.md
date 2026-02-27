# NothingButNeurons v2 (NBNv2)

Distributed neural simulation framework based on the NBNv2 design spec in
`NBNv2_Design.md`. This repo includes:

- `src/` runtime services and shared libraries
- `tools/` Workbench (Avalonia UI)
- `tests/` format and simulation tests

The solution file is `NBNv2.sln`.

## Worker-first local bring-up (recommended)

Use this startup order for operator flows and local demos:

1. `SettingsMonitor` (registry + settings)
2. One or more `WorkerNode` processes (heartbeat/capability inventory)
3. `HiveMind` (placement + tick control)
4. `Observability` (single shared `DebugHub` + `VisualizationHub` for the deployment)
5. `IO Gateway` (and optional `Reproduction`)
6. Spawn brains through IO (`SpawnBrainViaIO`) and let HiveMind place work on workers

## SettingsMonitor quickstart (CLI)

```bash
dotnet run --project src/Nbn.Runtime.SettingsMonitor -- \
  --db settingsmonitor.db \
  --bind-host 127.0.0.1 --port 12010
```

## HiveMind quickstart (CLI)

```bash
dotnet run --project src/Nbn.Runtime.HiveMind -- \
  --bind-host 127.0.0.1 --port 12020 \
  --tick-hz 30 --min-tick-hz 5 \
  --compute-timeout-ms 200 --deliver-timeout-ms 200 \
  --enable-otel --otel-metrics --otel-traces \
  --otel-endpoint http://localhost:4317 \
  --otel-service-name nbn.hivemind
```

OpenTelemetry env vars (alternatives to CLI flags):
- `NBN_HIVE_OTEL_ENABLED`
- `NBN_HIVE_OTEL_METRICS_ENABLED`
- `NBN_HIVE_OTEL_TRACES_ENABLED`
- `NBN_HIVE_OTEL_CONSOLE`
- `NBN_HIVE_OTEL_ENDPOINT` (falls back to `OTEL_EXPORTER_OTLP_ENDPOINT`)
- `NBN_HIVE_OTEL_SERVICE_NAME` (falls back to `OTEL_SERVICE_NAME`)

## RegionHost advanced manual quickstart (debug-only)

Example invocation (replace IDs/ports/sha/size with real values):

```bash
dotnet run --project src/Nbn.Runtime.RegionHost -- \
  --bind-host 127.0.0.1 --port 12040 \
  --brain-id 11111111-2222-3333-4444-555555555555 \
  --region 9 --neuron-start 0 --neuron-count 0 --shard-index 0 \
  --router-address 127.0.0.1:12010 --router-id brain-router \
  --tick-address 127.0.0.1:12000 --tick-id HiveMind \
  --output-address 127.0.0.1:12020 --output-id output-coordinator \
  --nbn-sha256 <nbn_sha256_hex> --nbn-size <nbn_size_bytes> \
  --nbs-sha256 <nbs_sha256_hex> --nbs-size <nbs_size_bytes> \
  --artifact-root <artifact_store_path>
```

Notes:
- `--nbs-*` flags are optional (omit for no snapshot overlays).
- Output region shards (`region 31`) require a valid `--output-*` PID.
- RegionHost registers/unregisters its shard with the HiveMind PID provided via `--tick-*`.
- This path is for manual shard debugging only; the normal runtime flow is `WorkerNode` placement via HiveMind.

## WorkerNode quickstart (CLI)

Example invocation:

```bash
dotnet run --project src/Nbn.Runtime.WorkerNode -- \
  --bind-host 127.0.0.1 --port 12041 \
  --logical-name nbn.worker --root-name worker-node \
  --settings-host 127.0.0.1 --settings-port 12010 --settings-name SettingsMonitor \
  --service-roles all
```

Service-role controls:
- `--service-roles <list>` sets enabled worker roles.
- `--service-role <role>` enables role(s) incrementally.
- `--disable-service-role <role>` disables role(s) incrementally.
- Role tokens: `all`, `none`, `brain-root`, `signal-router`, `input-coordinator`, `output-coordinator`, `region-shard`.
- Environment override: `NBN_WORKER_SERVICE_ROLES`.

Worker distributable publish scripts (single-file, self-contained by default):

```powershell
powershell -ExecutionPolicy Bypass -File tools/dist/publish_worker_node.ps1
```

```bash
bash tools/dist/publish_worker_node.sh linux-x64 win-x64
```

## Observability Event Routing

Runtime services now emit `VisualizationEvent` and `DebugOutbound` messages (for
example: brain spawn/active/pause/terminate, shard spawned, tick, neuron-fired,
axon-sent, and timeout anomalies). Emission target can be configured with:

Observability is a core runtime service, not a Workbench-owned service. A deployment
uses one shared `VisualizationHub` and one shared `DebugHub`; multiple Workbench
instances connect as clients. Visualization subscriptions are tracked per subscriber,
so one Workbench changing panes/brain focus does not disable visualization for other
active subscribers.

- `NBN_OBS_ADDRESS` (full `host:port`, takes precedence)
- `NBN_OBS_HOST` + `NBN_OBS_PORT` (default port `12060`)
- `NBN_OBS_DEBUG_HUB` (default `DebugHub`)
- `NBN_OBS_VIZ_HUB` (default `VisualizationHub`)
- `NBN_OBS_DISABLED=true` to disable emission

Debug delivery is subscriber opt-in. `DebugSubscribe` supports scoped filters:
- `min_severity`
- `context_regex`
- `include_context_prefixes` / `exclude_context_prefixes` (location-style scopes, e.g. `hivemind.` or `region.`)
- `include_summary_prefixes` / `exclude_summary_prefixes` (log-type/category scopes, e.g. `brain.` or `tick.`)

Workbench can mirror these via SettingsMonitor keys:
- `debug.stream.enabled`
- `debug.stream.min_severity`
- `debug.stream.context_regex`
- `debug.stream.include_context_prefixes`
- `debug.stream.exclude_context_prefixes`
- `debug.stream.include_summary_prefixes`
- `debug.stream.exclude_summary_prefixes`

Emitter-side debug flow is also opt-in:
- HiveMind emits debug only when `debug.stream.enabled=true`, and only at/above `debug.stream.min_severity`.
- HiveMind propagates those runtime debug settings to RegionShards via `UpdateShardRuntimeConfig`.
- RegionHost/WorkerNode startup defaults debug emission off, with env fallbacks:
  - `NBN_DEBUG_STREAM_ENABLED` (`true|false`)
  - `NBN_DEBUG_STREAM_MIN_SEVERITY` (`SevTrace|SevDebug|SevInfo|SevWarn|SevError|SevFatal`)

## Energy + Plasticity Demo Scenario

The local PowerShell demo script now includes an energy/plasticity scenario step
by default:

```powershell
tools/demo/run_local_hivemind_demo.ps1
```

The script now follows worker-node-first bootstrap:
- starts `SettingsMonitor`, worker nodes, then central services (`HiveMind`, `IO`, `Reproduction`, `Observability`)
- spawns the demo brain through `SpawnBrainViaIO` (no manual BrainHost/RegionHost wiring)

The scenario uses `Nbn.Tools.DemoHost io-scenario` to apply:
- `EnergyCredit`
- `EnergyRate`
- `SetCostEnergyEnabled`
- `SetPlasticityEnabled`

and prints JSON `IoCommandAck` results plus final `BrainInfo`.

You can also run it directly:

```bash
dotnet run --project tools/Nbn.Tools.DemoHost -c Release --no-build -- \
  io-scenario \
  --io-address 127.0.0.1:12050 --io-id io-gateway \
  --brain-id <brain_guid> \
  --credit 500 --rate 3 \
  --cost-enabled true --energy-enabled true \
  --plasticity-enabled true --plasticity-rate 0.05 --probabilistic true --json
```

Useful telemetry names for this workflow:
- `nbn.hivemind.brain.tick_cost.total`
- `nbn.hivemind.brain.energy.depleted`
- `nbn.regionhost.plasticity.strength_code.changed`
- `nbn.hivemind.snapshot.overlay.records`
- `nbn.hivemind.rebase.overlay.records`

Quick troubleshooting:
- `brain_not_found` in ack: in demo-script runs inspect `tools/demo/local-demo/<timestamp>/logs/spawn.log` for spawn/registration status, then verify worker logs and placement availability.
- request timeout: verify `--io-address`/`--io-id` and that IO Gateway is running.
- spawn failures with worker errors: verify worker nodes are online in SettingsMonitor and advertising required service roles.

Workbench launch log location:
- Workbench-driven local launches write logs to `%LOCALAPPDATA%\Nbn.Workbench\logs` (for example `HiveMind.out.log`, `IoGateway.out.log`, `WorkerNode.out.log`, `Reproduction.out.log`, `Observability.out.log`, `SettingsMonitor.out.log`, and `workbench.log`).
- Demo scripts write logs to `tools/demo/local-demo/<timestamp>/logs`.

## Reproduction Runtime + Operator Runbook

Runtime reproduction is exposed through IO messages:
- `nbn.io.ReproduceByBrainIds`
- `nbn.io.ReproduceByArtifacts`

Result semantics (from current implementation):
- Gate or runtime failures: `result.report.compatible=false` with `result.report.abort_reason=<code>`.
- Successful synthesis: `result.child_def` populated with child `.nbn` artifact ref and `result.summary` populated.
- Spawn behavior follows `config.spawn_child`:
  - `SPAWN_CHILD_DEFAULT_ON` tries to spawn.
  - `SPAWN_CHILD_NEVER` returns child artifact without spawning.
  - `SPAWN_CHILD_ALWAYS` forces spawn attempt.
- If spawn fails after successful synthesis, response keeps `child_def` and sets abort reason to spawn failure codes (for example `repro_spawn_unavailable`, `repro_spawn_failed`, `repro_spawn_request_failed`).

Operator runbook:
- `tools/demo/reproduction_operator_runbook.md`

Local deterministic repro demo flow:
- `tools/demo/run_local_hivemind_demo.ps1` starts worker nodes first, then central services, spawns a brain through IO/HiveMind placement, and runs `Nbn.Tools.DemoHost repro-scenario`.
- The scenario log is written to `tools/demo/local-demo/<timestamp>/logs/repro-scenario.log`.
- The script also runs `Nbn.Tools.DemoHost repro-suite` and writes `tools/demo/local-demo/<timestamp>/logs/repro-suite.log` with per-case pass/fail checks.
- Note: these are demo-script logs; Workbench local-launch logs are under `%LOCALAPPDATA%\Nbn.Workbench\logs`.
- Default expected fields are:
  - `result.compatible == true`
  - `result.abort_reason == ""`
  - `result.child_def.sha256` present
  - `result.spawned == false` (default `spawn-policy=never`)

Suite expected summary:
- `all_passed == true`
- `failed_cases == 0`
- `total_cases` covers success, gate aborts, invalid references/media, and spawn-attempt behavior.

Direct command example:

```bash
dotnet run --project tools/Nbn.Tools.DemoHost -c Release --no-build -- \
  repro-scenario \
  --io-address 127.0.0.1:12050 --io-id io-gateway \
  --parent-a-sha256 <hex> --parent-a-size <bytes> \
  --parent-b-sha256 <hex> --parent-b-size <bytes> \
  --store-uri <artifact_root_or_file_uri> \
  --seed 12345 --spawn-policy never --strength-source base --json
```

```bash
dotnet run --project tools/Nbn.Tools.DemoHost -c Release --no-build -- \
  repro-suite \
  --io-address 127.0.0.1:12050 --io-id io-gateway \
  --parent-a-sha256 <hex> --parent-a-size <bytes> \
  --store-uri <artifact_root_or_file_uri> \
  --seed 12345 --json
```

## Workbench Visualizer (Neural Activity View)

The Workbench Visualizer projects runtime visualization events into a canvas with
two modes:

- Full-brain mode (default): region nodes and inter-region routes.
- Focus-region mode: neuron and gateway nodes for one region plus focused routes.

`Region focus` is blank by default, which means full-brain mode. Enter a region id
and use `Zoom` to switch to focus mode, or use `Full brain` to clear focus.

### Core controls

- Brain selection: choose a known brain or paste/add a `BrainId`.
- Projection options: tick window, event type filter, region filter, search, and
  low-signal inclusion.
- Tick override: apply/clear temporary HiveMind target tick rate override from the
  Visualizer panel.
- Color mode: switch between state-value, activity, and topology emphasis.
- Snapshot/stream cards: both `Projection Snapshot` and `Visualization Stream` are
  toggleable and default-collapsed.

### Canvas interaction model

- Hover: tooltip card follows current hover target and clears when off-target.
- Select: single-click node/edge selects it; right-click toggles pin state.
- Navigate: `Alt+Left` / `Alt+Right` cycles nodes, `Alt+Enter` navigates selection.
- Pin/Clear: `Alt+P` toggles pin for selection, `Esc` clears interaction state.
- Double-click behavior: empty canvas fits view, region/gateway node zooms to that
  region, neuron node keeps normal selection behavior.

### Zoom and pan controls

- On-canvas controls: `-`, `+`, `Fit view`, and pan buttons `L/R/U/D`.
- Mouse/trackpad: `Shift+Wheel` zooms, `Shift+drag` (left button) or middle-drag
  pans, and wheel without `Shift` continues normal page scrolling.
- Touch: two-finger gesture supports pinch-zoom and drag-pan.
- Zoom range is clamped to `35%` to `400%` (`0.35` to `4.0` scale).

### Diagnostics and operator visibility

- `Ctrl+Shift+C` copies a structured diagnostics report (filters/options, projection
  summary, canvas nodes/edges, recent events, and topology hydration status).
- Selected event payload is visible in the stream panel and supports export/clear.
- Legend, interaction summary, and pin summary update with current selection/hover
  state.

### Operational limits and safeguards

- Rendered stream list is capped to the newest `400` events.
- Pending queue is capped at `1600`; oldest pending items are dropped above cap.
- Streaming refresh cadence is throttled to about `180 ms`.
- Default tick window is `64` with max `4096`.
- Snapshot side panel is intentionally trimmed to top `10` regions and top `14`
  edges for readability. Canvas topology itself is not trimmed to those row caps.

### Test surface

Visualizer behavior is covered primarily by:

- `tests/Nbn.Tests/Workbench/VizActivityCanvasLayoutBuilderTests.cs`
- `tests/Nbn.Tests/Workbench/VizPanelViewModelInteractionTests.cs`
- `tests/Nbn.Tests/Workbench/VizActivityProjectionBuilderTests.cs`
