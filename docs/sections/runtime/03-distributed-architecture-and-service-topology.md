## 3. Distributed architecture and service topology

### 3.1 Processes (“nodes”) and actor placement

Each running process hosts a Proto.Actor **ActorSystem**. Actors can be spawned on any process. A process may:

* host one or more “service root” actors (HiveMind, SettingsMonitor, IO Gateway, etc.), or
* be a **worker-only** process that hosts only worker actors (RegionShards and/or IO coordinators), or
* host everything on a single machine for local development (not a special case, still uses Proto.Actor for communications, etc.).

Runtime service roots bind all interfaces by default unless the operator explicitly pins `--bind-host` to a narrower interface such as `127.0.0.1`. When a service binds all interfaces and no explicit advertise host is supplied, NBN resolves a non-loopback local advertised address so peer discovery does not publish `0.0.0.0`.

NBN treats placement as a runtime concern:

* RegionShards are expected to be distributed across worker processes.
* Per-brain IO coordinators (InputCoordinator/OutputCoordinator) may be placed on worker processes for scale or locality.
* BrainRoot and BrainSignalRouter may be placed on any process; default placement is co-located with HiveMind unless policy says otherwise.

### 3.2 Root services and logical roles

**SettingsMonitor**

* Registry: nodes, addresses, root actor names, leases/heartbeats
* Settings store: global configuration and mutable runtime settings
* Canonical service endpoint keys for Workbench/service discovery: `service.endpoint.hivemind`, `service.endpoint.io_gateway`, `service.endpoint.reproduction_manager`, `service.endpoint.speciation_manager`, `service.endpoint.worker_node`, and `service.endpoint.observability` (encoded as `host:port/actor`)
* Multi-endpoint discovery groundwork also publishes adjacent `service.endpoint_set.*` values containing repeated endpoint candidates for the same logical service; legacy single-endpoint keys remain the compatibility fallback until callers migrate to caller-side route selection.
* Capability store: node CPU/GPU characteristics and benchmark scores
  * Worker nodes publish real probed CPU cores, raw free/total RAM and storage, GPU/VRAM visibility, ILGPU accelerator availability, explicit CPU/GPU scores, explicit NBN limit percentages, and current load/pressure snapshots when the host/runtime can resolve them.
  * SettingsMonitor is the canonical persisted snapshot for those worker capability rows; HiveMind still owns freshness filtering, rerun requests, and placement/rebalance policy on top of the stored snapshot.
  * CPU/GPU scores are explicit measured/configured values used as placement inputs; placeholder zero scores are not the intended steady-state contract for workers.
* All other services report via SettingsMonitor proto messages (no direct DB access); HiveMind publishes brain lifecycle/tick/controller updates

**HiveMind**

* Owns the **global tick counter** and all tick pacing
* Coordinates brain spawning/unloading
* Coordinates placement, rescheduling, and recovery
* Owns the settings-backed worker capability rerun cadence and pressure-triggered rebalance decisions
* Aggregates cost and enforces energy policies

**IO Gateway**

* Single well-known gateway for External World
* Spawns per-brain input/output coordinators and routes external commands
* External World never needs to know RegionShard placement or actor PIDs

**Observability hubs** (core service)

* DebugHub: human-readable debug Stream with filtering/throttling
* VisualizationHub: stable structured event Stream for Visualizer
* One shared hub instance of each type per deployment/cluster
* Multiple Workbench clients subscribe as peers; one client changing focus must not disable another client's active visualization
* Streams can be disabled entirely at runtime

**Artifact Store** (optional but recommended/default)

* Content-addressed, deduplicating store for `.nbn` and `.nbs` artifacts
* Supports partial fetch and local caching

### 3.3 Brain actor topology

For each brain:

* `BrainRoot` (control/routing/metadata)
* `BrainSignalRouter` (tick-phase signal delivery aggregator, track RegionShards for routing of signals)
* `RegionShards` (compute units, distributed)
* `InputCoordinator` and `OutputCoordinator` (distributed, controlled via IO Gateway)

---
