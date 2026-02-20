# NothingButNeurons v2

## Design, Architecture, Protocols, File Formats, and Implementation Specification

**Stack:** C#/.NET • Proto.Actor (Proto.Remote over gRPC) • Protobuf • Avalonia Workbench • SQLite+Dapper • OpenTelemetry • ILGPU (CUDA-first)

---

## Table of contents

1. Purpose and scope
2. Technology stack and solution structure
3. Distributed architecture and service topology
4. Identifiers, addressing, and IDs
5. Simulation model and tick semantics
6. Global tick engine, backpressure, and scheduling
7. Cost and energy model
8. Distance model (region + neuron)
9. Sharding and placement
10. RegionShard compute backends
11. Plasticity (axon strength adaptation)
12. Brain lifecycle, failure recovery, and snapshots
13. I/O architecture and External World interface
14. Reproduction and evolution
15. Observability: debug, visualization, metrics, tracing
16. Artifact storage and deduplication
17. File formats: `.nbn` (definition) and `.nbs` (state)
18. Database schemas (SQLite)
19. Protocol schemas (`.proto`)
20. Implementation roadmap (tentative)
    Appendix A: Defaults and constants
    Appendix B: Function catalog (IDs, formulas, tiers, costs)
    Appendix C: Region axial map (3D-inspired) and distance examples
21. Aleph MCP workflow for NBNv2

---

## 1. Purpose and scope

NothingButNeurons (NBN) is a distributed neural simulation framework. It models “brains” composed of regions, and each region contains neurons connected by directed **axons**. NBN is designed for:

* **Cross-platform execution:** Windows and Ubuntu (headless services and GUIs).
* **Distributed execution:** compute and coordination can be spread across multiple processes and machines.
* **Tick-based pacing:** a global tick constrains timing differences caused by deployment topology and load.
* **Reproduction/evolution:** creation of new brain definitions from existing running brains, with configurable mutation and structural changes.
* **Observability:** debugging, visualization, and infrastructure-level telemetry are first-class and can be disabled.

NBN is not an ML training framework (no backpropagation/gradient descent).

---

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

### 2.2 Solution layout (recommended)

* `Nbn.Shared`

  * Addressing, quantization, bit packing helpers
  * `.proto` generated code
  * common constants and validation logic
  * Base actor class, if common overrides/additions to Proto.Actor base used
* `Nbn.Runtime.SettingsMonitor` (node)

  * registry + settings service
  * node heartbeat/capabilities store
  * optional artifact metadata index (not chunk payload)
* `Nbn.Runtime.HiveMind` (node)

  * global tick engine
  * placement/rescheduling coordinator
  * brain lifecycle coordinator, including spawning/loading
* `Nbn.Runtime.Reproduction` (node or hosted with HiveMind)

  * ReproductionManager actor
* `Nbn.Runtime.Brain` (library + actors hosted on any node)

  * BrainRoot actor
  * BrainSignalRouter actor
* `Nbn.Runtime.RegionHost` (worker node)

  * RegionShard actors (CPU/GPU compute)
  * optional NeuronDebug actors (mirrors/probes)
* `Nbn.Runtime.IO` (node)

  * IO Gateway actor (well-known endpoint for External World)
  * per-brain InputCoordinator and OutputCoordinator actors (placeable on any node(s))
* `Nbn.Runtime.Observability` (node, optional)

  * DebugHub and VisualizationHub (can be disabled)
* `Nbn.Runtime.Artifacts` (service/library; can run as node or embedded)

  * artifact store client/server, cache, dedup logic
* `Nbn.Tools.Workbench` (Avalonia app)

  * Orchestrator (service discovery/health/launch)
  * Designer (brain creation/import/export)
  * Visualizer (graph/activity)
  * Debug viewer
  * Energy/IO console
  * Reproduction console
* `Nbn.Tests`

  * format tests, simulation tests, parity tests, reproduction tests

### 2.3 Project tooling (Beads)

NBNv2 uses Beads (`bd`) for task tracking and Beads Viewer (`bv`) for graph views.

Initialization:
* Run `bd init` in the repo root once (creates `.beads/`).
* Also initialize in each project folder that owns a `.csproj` (use `bd init --skip-hooks`).
* Do not create tasks or issues as part of initialization.

Usage notes:
* Use `bd` for task lifecycle updates; keep `.beads/` under version control unless you explicitly use `--stealth`.
* For automation, use `bv --robot-*` only. The interactive TUI blocks the session.
* NEVER use git worktrees (they don't work well with Beads).

### 2.4 Build/test defaults (required)

Always use these flags for local and CI builds/tests:

* **Configuration:** `-c Release`
* **Disable build servers:** `--disable-build-servers` for both `dotnet build` and `dotnet test`

Preferred commands:

* `dotnet build -c Release --disable-build-servers`
* `dotnet test -c Release --disable-build-servers` (add `--no-build` only if you already built with the same config)

---

## 3. Distributed architecture and service topology

### 3.1 Processes (“nodes”) and actor placement

Each running process hosts a Proto.Actor **ActorSystem**. Actors can be spawned on any process. A process may:

* host one or more “service root” actors (HiveMind, SettingsMonitor, IO Gateway, etc.), or
* be a **worker-only** process that hosts only worker actors (RegionShards and/or IO coordinators), or
* host everything on a single machine for local development (not a special case, still uses Proto.Actor for communications, etc.).

NBN treats placement as a runtime concern:

* RegionShards are expected to be distributed across worker processes.
* Per-brain IO coordinators (InputCoordinator/OutputCoordinator) may be placed on worker processes for scale or locality.
* BrainRoot and BrainSignalRouter may be placed on any process; default placement is co-located with HiveMind unless policy says otherwise.

### 3.2 Root services and logical roles

**SettingsMonitor**

* Registry: nodes, addresses, root actor names, leases/heartbeats
* Settings store: global configuration and mutable runtime settings
* Capability store: node CPU/GPU characteristics and benchmark scores
* All other services report via SettingsMonitor proto messages (no direct DB access); HiveMind publishes brain lifecycle/tick/controller updates

**HiveMind**

* Owns the **global tick counter** and all tick pacing
* Coordinates brain spawning/unloading
* Coordinates placement, rescheduling, and recovery
* Aggregates cost and enforces energy policies

**IO Gateway**

* Single well-known gateway for External World
* Spawns per-brain input/output coordinators and routes external commands
* External World never needs to know RegionShard placement or actor PIDs

**Observability hubs** (optional)

* DebugHub: human-readable debug Stream with filtering/throttling
* VisualizationHub: stable structured event Stream for Visualizer
* Both can be disabled entirely

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

## 4. Identifiers, addressing, and IDs

### 4.1 Stable IDs

* `BrainId`: UUID (16 bytes)
* `NodeId`: UUID (16 bytes)
* UUID byte order (on disk / on wire): RFC 4122 network order (big-endian; matches canonical hex string)
* `ArtifactId`: SHA-256 (32 bytes), computed on canonical bytes of artifact payload
* `SnapshotId`: SHA-256 of `.nbs` payload or UUID (implementation choice; prefer SHA-256)

### 4.2 RegionShardId

A RegionShard is uniquely identified within a brain by:

* `region_id` (0..31)
* `shard_index` (0..N-1 for that region, contiguous and stable for the brain’s current placement epoch)

For convenience, also define a packed `ShardId32`:

* bits 0..15: shard_index
* bits 16..20: region_id (5 bits)
* bits 21..31: reserved (0)

### 4.3 Runtime neuron address: Address32

Runtime routing uses a compact 32-bit address:

* `region_id`: 5 bits (0..31)
* `neuron_id`: 27 bits (0..134,217,727)

Packed as:

* bits 0..26  = neuron_id
* bits 27..31 = region_id

Helpers:

* `region_id = addr >> 27`
* `neuron_id = addr & ((1<<27)-1)`

Note: Address32 is runtime-only. The `.nbn` axon record stores `target_neuron_id` with 22 bits, so `.nbn` region spans MUST be <= `2^22 - 1` (4194303) and axon targets MUST fit that range.

### 4.4 Input and Output regions (fixed)

* **Input region:** `region_id = 0`
* **Output region:** `region_id = 31`

These are not configurable.

---

## 5. Simulation model and tick semantics

### 5.1 Neuron state (runtime)

Each neuron has:

* Persistent buffer `B` (float)
* Inbox accumulator `I` for contributions arriving for the next merge (float + flags for product accumulation)
* Enabled flag (runtime control; separate from “exists in definition”; potentially different Proto.Actor inbox mode/function)
* Function selectors and quantized parameters (from `.nbn` neuron record)
* Outgoing axon list (targets + strength codes, from `.nbn`, with optional overlay from `.nbs`)

Neurons “exist” if their neuron record exists in `.nbn` (Input/Output always exist). Other regions may be absent entirely (no region section).

### 5.2 Double-buffered accumulation with persistent buffer

Signals delivered during tick `N` must not affect neuron activation within tick `N`. They affect tick `N+1`.

Mechanism:

* During delivery phase of tick `N`, incoming contributions accumulate into `I`.
* At compute phase start of tick `N+1`, `I` is merged into `B` according to the neuron’s accumulation function, then `I` is cleared.

This avoids losing accumulation when a neuron does not activate in a tick, because `B` persists across ticks.

### 5.3 Accumulation functions

Accumulation functions are selected per neuron (2-bit ID). Defined in Appendix B and the `.proto` enums.

Required semantics:

* **SUM:** `B ← B + I`
* **PRODUCT (first-input special):**

  * `I` is tracked as `(hasInput, value)`
  * If no input, merge does nothing
  * If there is input, `B ← B * I.value`
* **MAX:** `B ← max(B, I)`
* **NONE:** merge does nothing

### 5.4 Activation and reset gates

Each tick compute phase:

1. If neuron is disabled (runtime), it does not compute activation; however it may still accumulate inbox depending on policy (default: inbox still accumulates, merge still occurs, activation suppressed).
2. Pre-activation gate: activate only if `B > PreActivationThreshold` (threshold may be negative).
3. Activation function computes `potential`.
4. Reset function updates `B` based on `(B, potential, activation_threshold, out_degree)`.
5. Fire if `abs(potential) > ActivationThreshold`, producing outgoing axon contributions.

### 5.5 I/O connectivity rules (invariants)

These apply to all brain creation paths: random generation, manual editing, reproduction, and imports.

* **Input region (0):**

  * No axon may target any neuron in region 0 (no `ToAddress.region_id == 0`).
* **Output region (31):**

  * Output region neurons may have outgoing axons.
  * No axon from output region may target any neuron in region 31 (no `From.region==31 AND To.region==31`).
* **Self-loop axons:**

  * Allowed for all regions except output region (disallowed by the rule above) and input region (disallowed because nothing may target input).
* **Duplicate axons:**

  * For any source neuron, at most one axon may target a given `(target_region_id, target_neuron_id)`.

---

## 6. Global tick engine, backpressure, and scheduling

### 6.1 Global tick counter

HiveMind maintains a single global `tick_id` shared by all brains and all RegionShards.

### 6.2 Two-phase tick model

Each tick consists of two global phases:

**Phase A: Compute phase (TickCompute)**

* HiveMind sends `TickCompute(tick_id)` to all active RegionShards of all active brains (via BrainRoot -> BrainSignalRouter ->RegionHost, which forward).
* Each RegionShard:

  * merges inbox into persistent buffers
  * computes activations/resets
  * prepares outgoing contributions grouped by destination RegionShard
  * emits output events for output region
  * reports `TickComputeDone`

**Phase B: Delivery phase (TickDeliver)**

* HiveMind instructs each brain’s BrainRoot (->BrainSignalRouter) to flush all prepared outgoing contributions for tick `tick_id`.
* BrainSignalRouter delivers aggregated `SignalBatch` messages to destination RegionShards.
* Delivery should be sent as a request (sender populated) so RegionShards can reply with `SignalBatchAck` to the router; avoid empty sender PIDs in remoting.
* Destination RegionShards acknowledge receipt for that tick.
* BrainSignalRouter reports `TickDeliverDone`

**Tick progression rule:**
HiveMind does not start `TickCompute(tick_id+1)` until:

* all brains have completed `TickDeliverDone(tick_id)`, or
* a timeout policy triggers, followed by backpressure action.

This ensures that all cross-shard signals produced in tick `N` are delivered and available for tick `N+1` compute, independent of network latency.

### 6.3 Timeouts and late arrivals

For each tick and each phase:

* HiveMind tracks missing `Done` messages.
* A timeout is recorded when a required `Done`/ack is not received by the deadline.
* Late arrivals (messages received after the timeout or after tick advancement) are recorded as late telemetry.
* Late arrivals do not retroactively change tick completion decisions.

Both timeouts and late arrivals are surfaced as metrics and can drive rescheduling/pause actions.

### 6.4 Tick pacing parameters

Define tick pacing primarily in Hz and derive periods:

* `target_tick_hz` (dynamic)
* `min_tick_hz` (floor; lowest acceptable rate before pausing brains)
* `max_tick_period_ms = 1000 / min_tick_hz`
* `target_tick_period_ms = 1000 / target_tick_hz`

The tick engine adjusts `target_tick_hz` downward (slower) under backpressure, never below `min_tick_hz`.

### 6.5 Backpressure policy

When timeouts or sustained lateness occur:

1. Degrade tick rate (reduce `target_tick_hz` down to `min_tick_hz`)
2. Reschedule RegionShards (move/split/merge on worker nodes)
3. Pause one or more Brains according to a configured priority strategy

Pause priority strategies include:

* oldest-first (by spawn time)
* newest-first (by spawn time)
* lowest energy remaining
* lowest configured priority value
* external-world-specified ordering (list of IDs)

### 6.6 Rescheduling rate limits and tick pausing

Rescheduling and full recovery are disruptive. To prevent thrashing:

* Rescheduling is rate-limited:

  * at most once every `reschedule_min_ticks`, and/or
  * at most once every `reschedule_min_minutes`
  * if a reschedule/recovery is triggered too soon, it is queued

When a reschedule/recovery is initiated:

* HiveMind completes the current tick (compute+deliver) or times out that tick.
* HiveMind then **pauses new tick dispatch**.
* HiveMind waits a small stabilization interval (`reschedule_quiet_ms`) after the tick completion decision.
* HiveMind performs rescheduling and/or recovery.
* HiveMind resumes tick dispatch.

---

## 7. Cost and energy model

### 7.1 Overview

NBN can compute a per-brain per-tick **cost** and deduct it from an **energy account** supplied by the External World (or the Workbench). Cost/energy can be disabled.

* `cost_enabled` (per brain)
* `energy_enabled` (per brain)

If `energy_enabled` is true and a brain’s energy balance would drop below zero due to tick cost, the brain terminates (unload and potentially trigger a reschedule).

### 7.2 Cost units

Cost is counted in integer micro-units (`int64`) for stable accounting.

Cost components:

* Accumulation cost (per neuron merged)
* Activation cost (per neuron activated)
* Reset cost (per neuron activated)
* Axon distance cost (per fired axon contribution)
* Optional remote transport costs (default OFF)

Remote transport costs:

* `remote_cost_per_batch`
* `remote_cost_per_contribution`

Default behavior:

* Both are set to zero and disabled unless explicitly enabled. Placement/distribution should not affect cost by default.

### 7.3 Per-shard cost reporting

Each RegionShard reports per tick:

* `tick_cost_total`
* optional breakdowns:

  * `cost_accum`
  * `cost_activation`
  * `cost_reset`
  * `cost_distance`
  * `cost_remote` (0 unless enabled)

HiveMind aggregates per brain and globally.

### 7.4 Energy model

Energy is supplied via IO commands:

* one-time credits and/or
* continuous rates (units per second)

Energy is tracked per brain.

Brain termination on insufficient energy:

* HiveMind terminates the brain’s actors and unloads placement state.
* HiveMind notifies IO Gateway with:

  * BrainId
  * termination reason
  * base `.nbn` ArtifactRef
  * last `.nbs` snapshot ArtifactRef (if available)
  * last known energy balance and last tick cost

The External World may decide to respawn/restart the brain using the reported artifact(s).

---

## 8. Distance model (region + neuron)

### 8.1 Region distance (3D-inspired axial layering)

Regions are modeled as slices along an input-to-output axis with “planes” of regions per slice. This induces locality preference without complex geometry.

Define a per-region axial coordinate `z(region_id)`:

* `z(0)  = -3`  (Input end)
* `z(1)  = -2`, `z(2)  = -2`, `z(3)  = -2`
* `z(4..8)  = -1`
* `z(9..22) =  0`  (Center mass; includes 9..15 and 16..22)
* `z(23..27)= +1`
* `z(28..30)= +2`
* `z(31) = +3`  (Output end)

Region distance units:

* If `region_a == region_b`: `region_dist = 0`
* Else if `z(a) == z(b)`: `region_dist = region_intraslice_unit` (default 3 -- note that **same** region does not incur this)
* Else: `region_dist = region_axial_unit * abs(z(a) - z(b))` (default axial unit 5)

This yields:

* Input and Output far apart
* Regions within the same slice equidistant
* Adjacent slices uniformly close

### 8.2 Neuron distance within a region (ring metric with wrap)

Within a region of span `S` (neuron IDs `0..S-1`):

* `d = abs(i - j)`
* `wrap_d = min(d, S - d)`
* `neuron_dist_units = wrap_d`

Optionally scale to reduce magnitude (default):

* `neuron_dist_units = wrap_d >> neuron_dist_shift` (default shift 10; 1024 neurons = 1 unit)

### 8.3 Combined distance and cost

For an axon from `(rA, nA)` to `(rB, nB)` in the same brain:

* `dist_units = region_weight * region_dist(rA, rB) + neuron_dist_units(nA, nB, span_of_rB)`
* `distance_cost = axon_base_cost + axon_unit_cost * dist_units`

Region span for neuron distance uses the destination region span. If span is unknown at compute time (should not happen), use a fallback shift-only metric.

---

## 9. Sharding and placement

### 9.1 Region sections and stride

The `.nbn` file stores regions as independent sections. Each region section includes an **axon offset checkpoint table** with a fixed **stride** (default 1024).

Shard alignment rules:

* RegionShards cover contiguous neuron ID ranges within one region.
* Shard boundaries align to the stride:

  * `neuron_start % stride == 0`
  * `neuron_count % stride == 0`, except the final shard in a region may be shorter to cover `[last_stride_boundary, neuron_span)`

### 9.2 Node capabilities reporting

Worker processes report capabilities periodically to SettingsMonitor:

* CPU core count
* free RAM
* GPU presence and VRAM (if any)
* microbenchmark scores (CPU and GPU)
* ILGPU accelerator availability (CUDA/OpenCL/CPU)

HiveMind uses this for placement decisions.

### 9.3 Placement and rescheduling coordinator

HiveMind runs a `ShardPlacementManager` Actor which:

* decides initial placement of RegionShards and per-brain IO coordinators
* migrates shards on repeated lateness/timeouts
* splits/merges shards as needed
* updates all routing tables (BrainSignalRouter and coordinators)
* respects rescheduling rate limits (Section 6.6)

Placement heuristics:

* co-locate shards with heavy mutual traffic
* prefer GPU-capable nodes for Tier A/B-heavy function mixes
* avoid shard sizes that exceed memory limits

---

## 10. RegionShard compute backends

### 10.1 CPU backend (baseline)

The CPU backend is the reference implementation. It uses:

* contiguous arrays for neuron parameters and state
* grouped execution by function IDs
* `MathF` implementations for float32 functions
* parallelism (`Parallel.For`) per group where beneficial

### 10.2 GPU backend (optional, ILGPU)

GPU compute uses ILGPU and the kernel-per-function model:

* no switch-based “mega kernel”
* one kernel per activation function ID used in the shard
* one kernel per reset function ID used in the shard
* kernels for accumulation merges if beneficial

Kernel compilation:

* kernels are compiled lazily per function ID used in active shards
* kernel caching is per accelerator instance

### 10.3 Function tiers

Each function ID has:

* a tier (A/B/C)
* a relative compute cost weight

Tier guidance:

* Tier A: cheap, GPU-friendly (linear, clamp, relu, etc.)
* Tier B: moderate (tanh, sigmoid, exp/log variants)
* Tier C: expensive or numerically tricky (pow, gauss, quad)

Placement may weight shards toward GPU nodes if Tier A/B dominates and GPU is available.

---

## 11. Plasticity (axon strength adaptation)

### 11.1 Overview

Plasticity changes axon strengths slowly based on usage and signal scale.

Plasticity is configurable per brain:

* `plasticity_enabled`
* `plasticity_rate` (small)
* `plasticity_probabilistic_updates` (on/off)
* `plasticity_delta` (small)
* `plasticity_rebase_threshold` / `plasticity_rebase_threshold_pct` (optional; number of changed-axon codes or percent)

Plasticity changes are applied at runtime in float space and only persisted to `.nbs` when the quantized **strength code** differs from the base `.nbn` strength code.

Sub-quantum changes are allowed and are not preserved across snapshots unless they cross a quantization boundary.

### 11.2 Runtime representation

For each axon, maintain:

* base strength code from `.nbn`
* optional overlay strength code (only if code differs)
* an ephemeral float `strength_value` used for runtime math

When plasticity is enabled:

* updates are applied to the runtime float value
* after update, the float is re-quantized to a strength code to determine whether an overlay update is required
* if the code differs from base, it is recorded in the overlay
* if the code equals base, overlay entry is removed

### 11.3 Update rule (default)

Let:

* `p = clamp(potential, -1, +1)`
* `u = abs(p)` (0..1)
* `lr = plasticity_rate`

Per axon fired:

* Optional probabilistic gate:

  * `prob = lr * u`
  * deterministic PRNG based on `(brain_seed, tick_id, from_addr32, to_addr32)`
  * apply update only if `rand < prob`
* Update direction:

  * if `sign(p) == sign(strength_value)`: increase magnitude by `plasticity_delta * u`
  * else: decrease magnitude by `plasticity_delta * u`
* Clamp to [-1, 1] and re-quantize

### 11.4 Rebasing (optional)

Rebasing creates a new `.nbn` where base strength codes incorporate current overlay codes, then clears overlay. Trigger may be:

* external-world request
* threshold-based automatic policy (configurable)

---

## 12. Brain lifecycle, failure recovery, and snapshots

### 12.1 Spawn

Brains are spawned by providing a `.nbn` artifact reference. HiveMind coordinates:

* loading region directories
* allocating RegionShards and IO coordinators
* establishing routing tables
* initializing energy accounts and settings

### 12.2 Snapshot (`.nbs`)

Snapshots store:

* persistent neuron buffers
* enabled bitsets (optional)
* energy balance
* axon strength overlay codes (only where code differs from base)

Snapshots are taken at tick boundaries to avoid needing to store inbox state.

### 12.3 Failure recovery

If any RegionShard for a brain is lost due to process/node failure:

* HiveMind pauses tick dispatch (Section 6.6)
* HiveMind unloads the brain’s current runtime actors
* HiveMind restores the **entire brain** from the last `.nbn` + `.nbs` snapshot
* RegionShards are respawned and re-placed as needed
* Tick dispatch resumes

Partial shard-only restoration is not used.

### 12.4 Brain termination notifications and rebalancing

When a brain terminates (energy exhaustion, explicit kill from External World or Workbench, unrecoverable error):

* IO Gateway receives a `BrainTerminated` notification with artifact references
* IO Gateway notifies External World with the BrainId and artifact reference
* HiveMind removes the brain from tick barrier participation
* HiveMind may trigger a placement rebalance if the cluster becomes imbalanced (immediate or queued check)

---

## 13. I/O architecture and External World interface

### 13.1 External World interface principles

* External World interacts with NBN via **Proto.Actor remoting** using NBN `.proto` messages.
* External World does not need to know tick IDs to write inputs or receive outputs.
* Inputs are applied on the next tick automatically by the IO coordinators and the tick delivery phase.
* Outputs are delivered with tick correlation, but External World is not required to use it.

### 13.2 IO Gateway and per-brain coordinators

**IO Gateway** is a well-known actor name registered in SettingsMonitor. It:

* accepts client connections, subscriptions, and control messages
* forwards brain-level requests to HiveMind and/or ReproductionManager
* spawns per-brain `InputCoordinator` and `OutputCoordinator` actors

**Placement:** Coordinators may be placed on any worker process(es). IO Gateway maintains their location and routes to them transparently.

### 13.3 Input mapping

External World writes to `input_index i`, mapped to:

* `region_id = 0`
* `neuron_id = i`

Input width equals the number of neurons in region 0.

InputCoordinator buffers writes between ticks and emits them as contributions during the tick delivery phase so they are visible on the next tick compute phase.

External World may also send a vector of values for all inputs (length must == input width).

### 13.4 Output mapping

Outputs come from neurons in region 31.
`output_index i` maps to:

* `region_id = 31`
* `neuron_id = i`

Output width equals the number of neurons in region 31.

Output events are emitted per tick and delivered to subscribed clients.

* **OutputEvent (single):** emitted when an output neuron fires (abs(potential) > ActivationThreshold).
* **OutputVectorEvent (vector):** emitted every tick for output region shards; values are activation potentials for each output neuron (0 if gated by pre-activation/disabled).

External World may subscribe, per Brain, to individual and/or vector outputs.

### 13.5 Energy controls

IO supports:

* one-time energy credit
* energy rate
* enable/disable cost and energy

### 13.6 Brain death notifications

When a brain terminates, IO publishes:

* BrainId
* reason
* base `.nbn` artifact ref
* last `.nbs` snapshot artifact ref (if present)
* last energy balance and last tick cost totals

---

## 14. Reproduction and evolution

### 14.1 Overview

Reproduction creates a new `.nbn` brain definition from one or more running brains (by
 BrainId) or from artifact references. It is performed by a `ReproductionManager` 
service. Reproduction *uses* artifacts regardless.

Reproduction operates primarily on packed/quantized representations:

* neuron records and axon records
* function IDs and parameter codes
* strength codes (*optionally* included in transformation)

Axon strength values are not used for gating/similarity checks, including live strength overlays.

### 14.2 Reproduction inputs

Reproduction requests may specify parents by:

* BrainId (preferred): NBN resolves to the latest base `.nbn` plus latest `.nbs` overlay if configured
* ArtifactRef: `.nbn` (and optionally `.nbs`)

Strength source options:

* base-only
* live (base + overlay codes)

### 14.3 Similarity gating cascade

Reproduction aborts early if any stage fails:

1. **Format compatibility:** both parents are NBN2 and share compatible quantization schemas and function ID contracts
2. **IO region invariants:** parents obey input/output connectivity rules; child must obey them too
3. **Region presence similarity:** region presence means “region has at least one neuron in it” (region section exists); by default the number of present regions must match exactly
4. **Per-region neuron span similarity:** how many neurons within the region, within tolerances (absolute and/or percentage; smallest limit wins)
5. **Function distribution similarity:** activation/reset/accum histograms within thresholds, for each region
6. **Connectivity distribution similarity:** out-degree distribution and target-region distribution within thresholds
7. **Spot-check overlap:** sample loci and compare connectivity patterns (ignoring strength)

### 14.4 Alignment and locus

Neuron alignment is by locus:

* `(region_id, neuron_id)` is the canonical locus.
* Regions are independently aligned by locus.

Regions may be absent. Adding a neuron to an empty region creates a new region section; removing the last neuron removes the region section.

### 14.5 Structural mutations

Reproduction can:

* add/remove neurons (except in regions 0 and 31)
* add/remove axons (following the rules for regions 0 and 31)
* reroute axons (following the rules for regions 0 and 31)
* adjust axon strengths (optional)
* mutate function IDs and parameter codes

Constraints:

* Regions 0 and 31: neurons cannot be added or removed during reproduction.
* Axon invariants for input/output regions always apply.
* Duplicate axons from a given source neuron to the same target are not allowed; mutation operations must avoid creating them.

Separate structural probabilities include:

* probability to add a neuron to an empty region (if any empty regions)
* probability to remove the only/last neuron from a region (if any such regions)
* probability to disable a neuron within a non-empty region
* probability to reactivate a disabled locus (within an existing region)

Limits are configurable by:

* absolute counts and/or (average) percentage of the parent’s size
* if both are provided, the smallest effective limit applies

### 14.6 Deleting neurons and inbound axons

When a neuron is deleted:

* All axons that target it must be handled:

  * removed, or
  * rerouted within the same target region to a nearby neuron ID (ring distance metric), with configurable probability and max distance (use a distribution so closer within the distance limit is more likely)

Nearby target selection:

* choose a candidate neuron ID in the same region based on ring distance with wrap
* candidates must exist and be valid targets under IO invariants

### 14.7 Axon strength transformation (optional)

Strength transformation modes:

* copy parent A
* copy parent B
* average (decoded float average then re-encode)
* weighted average (weights configurable)
* mutate (small random perturbation in float then re-encode)
* mixed strategies with probabilities

Strength is not used as a reproduction gate.

### 14.8 Out-degree control (average-based)

Hard per-neuron axon count is limited by file format (0..511). Reproduction additionally enforces average out-degree constraints:

* `max_avg_out_degree_brain`
* optionally per-region max average

Pruning policies:

* remove lowest `abs(strength)` first (default)
* remove newly added connections first (option)
* random removal (option)

### 14.9 Outputs

Reproduction returns:

* child `.nbn` artifact ref
* similarity report and mutation summary

Optionally (default on):

* spawn the child brain immediately and return its BrainId

---

## 15. Observability: debug, visualization, metrics, tracing

### 15.1 Debug and visualization streams (optional)

Debug and visualization can be disabled entirely:

* if disabled, hubs may not run, and actors drop debug/viz events at source.

Debug level is configurable in Workbench and/or settings:

* severity threshold
* context filters
* throttling rates

### 15.2 OpenTelemetry (NBN-managed)

When enabled, NBN configures and runs OpenTelemetry exporters. Configuration can be:

* file-based settings (SQLite settings)
* environment variables
* Workbench controls (dev mode)

Metrics include:

* tick durations (compute phase, deliver phase)
* timeout counts
* late arrival counts
* shard compute ms, deliver ms
* reschedule events
* per-brain cost and energy
* signal batch sizes and counts

Traces include:

* tick phase spans
* reschedule/recovery spans
* artifact fetch spans

### 15.3 Lateness accounting

Both are recorded:

* timeouts (deadline exceeded)
* late arrivals (received after timeout or after tick advancement)

---

## 16. Artifact storage and deduplication

### 16.1 Goals

* Avoid storing multiple full copies when artifacts are mostly identical (common for repeated spawns and reproduction).
* Support very large artifacts.
* Support partial fetch (region-only and snapshot-only usage).
* Use permissive/free licensing (custom implementation permitted).

### 16.2 Content-addressed chunk store (recommended)

Implement an Artifact Store as a content-addressed storage (CAS) with chunk-level deduplication:

* Each artifact has an `ArtifactId = SHA-256(canonical_bytes)`.
* Artifact payload bytes are split into chunks using **content-defined chunking** (CDC) to improve dedup when insertions shift offsets.

  * Suggested CDC parameters: min 512 KiB, avg 2 MiB, max 8 MiB
* Each chunk stored by `chunk_hash = SHA-256(chunk_bytes)`.

Metadata is stored in SQLite (manifest tables). Chunk payloads stored on disk:

* `chunks/aa/<hash>` where `aa` is first byte of hash in hex.

Compression:

* chunks may (should) be compressed (e.g., zstd) before storage
* compression is chunk-local; hash is computed on uncompressed chunk bytes for correctness

### 16.3 Manifest structure

An artifact manifest stores:

* artifact_id
* media_type (`application/x-nbn`, `application/x-nbs`)
* byte_length
* ordered list of chunk hashes with uncompressed sizes
* optional region-section index to support partial fetch

Clients:

* download manifest
* download missing chunks
* reconstruct canonical bytes on demand

### 16.4 Dedup interactions with plasticity and reproduction

Plasticity:

* `.nbs` stores only buffer state and strength-code overlays, typically much smaller than `.nbn` (at least if re-basing enabled).
* Dedup naturally handles repeated snapshots and similar overlays.

Reproduction:

* regions and axon arrays often share large common chunks
* CDC improves dedup even when new regions/axons shift offsets

### 16.5 Optional region-section indexing (default / if feasible)

The store may additionally index `.nbn` region sections:

* per region: offset and length in canonical bytes
* enables efficient partial fetch of required regions for worker nodes

---

## 17. File formats: `.nbn` and `.nbs`

### 17.1 Common conventions

* Endianness: little-endian for all multibyte integers
* UUID bytes: RFC 4122 network order (big-endian; matches canonical hex string)
* All bitfields are defined with bit 0 as LSB of their integer container
* File extensions:

  * `.nbn` = brain definition
  * `.nbs` = brain state snapshot

---

### 17.2 `.nbn` format (NBN2)

#### 17.2.1 Top-level layout

1. Fixed header (`NbnHeaderV2`, 1024 bytes)
2. Region sections (0..31), each at offsets specified by header directory

Regions 0 and 31 must be present. Regions without neurons are not included.

#### 17.2.2 `NbnHeaderV2` (1024 bytes)

Offsets and sizes:

**0x000–0x003 (4 bytes)**

* `magic` = ASCII `"NBN2"`

**0x004–0x005 (2 bytes)**

* `version_u16` = 2

**0x006 (1 byte)**

* `endianness_u8` = 1 (little)

**0x007 (1 byte)**

* `header_bytes_pow2_u8` = 10 (1024 bytes)

**0x008–0x00F (8 bytes)**

* `brain_seed_u64`

**0x010–0x013 (4 bytes)**

* `axon_stride_u32` (default 1024)

**0x014–0x017 (4 bytes)**

* `flags_u32`

  * bit 0: reserved (must be 0)
  * bit 1: reserved
  * others reserved

**0x018–0x01F (8 bytes)**

* `reserved_u64`

**0x020–0x0FF (224 bytes)** Quantization schema block (fixed fields)
This block defines the decode/encode mapping parameters for each quantized field in neuron/axon records. Bit widths are fixed by record layout; this block defines ranges and mapping types.

Each quant field uses the structure:

* `map_type_u8` (0x00)
* `reserved_u8` (0x01)
* `reserved_u16` (0x02–0x03)
* `min_f32` (0x04–0x07)
* `max_f32` (0x08–0x0B)
* `gamma_f32` (0x0C–0x0F)

Total: 16 bytes per field.

Fields in order (16 bytes each):

1. Strength (axon strength code, 5 bits)
2. PreActivationThreshold (6 bits)
3. ActivationThreshold (6 bits)
4. ParamA (6 bits)
5. ParamB (6 bits)

Quant block size: 5 * 16 = 80 bytes, stored starting at 0x020. Remaining bytes up to 0x0FF reserved and must be zero.

**Quant map types:**

* `0 = LINEAR_SIGNED_CENTERED`
  codes map to symmetric range with two mid codes mapping to 0 when code count is even
* `1 = LINEAR_UNSIGNED`
  codes map to [min, max] with min>=0 typically
* `2 = GAMMA_SIGNED_CENTERED`
  like signed centered but with gamma companding toward 0
* `3 = GAMMA_UNSIGNED`
  like unsigned with gamma companding toward min

Default recommended mappings:

* Strength: GAMMA_SIGNED_CENTERED, min=-1, max=+1, gamma=2.0
* PreActivationThreshold: GAMMA_SIGNED_CENTERED, min=-1, max=+1, gamma=2.0
* ActivationThreshold: GAMMA_UNSIGNED, min=0, max=1, gamma=2.0
* ParamA/B: GAMMA_SIGNED_CENTERED, min=-3, max=+3, gamma=2.0

**0x100–0x3FF (768 bytes)** Region directory: 32 entries × 24 bytes
Entry `i` corresponds to `region_id = i`.

Each entry layout (24 bytes):

* `neuron_span_u32` (4 bytes)
* `total_axons_u64` (8 bytes)
* `region_offset_u64` (8 bytes)
* `region_flags_u32` (4 bytes; reserved, must be 0)

Rules:

* `neuron_span_u32` MUST be in [0..4194303] (22-bit) because axon records store `target_neuron_id` in 22 bits. `axon_stride_u32` defines the checkpoint spacing and shard-alignment unit: RegionShards MUST start on stride boundaries and (except for the final tail shard) cover a stride-multiple neuron count. The final shard in a region MAY be shorter to cover `[last_stride_boundary, neuron_span)` when `neuron_span` is not a multiple of `axon_stride_u32`.
* region_offset is 0 if region absent.
* region 0 and 31 must have neuron_span>0 and region_offset>0.

**0x400–0x3FF (end)** does not exist; header ends at 0x3FF (1024 bytes)

#### 17.2.3 Quantization mapping formulas

Let `bits` be the bit-width, `max_code = (1<<bits)-1`, and `code ∈ [0..max_code]`.

**Signed centered mapping (even code count)**
For even `max_code+1`, two center codes map to 0:

* `center_lo = (max_code+1)/2 - 1`
* `center_hi = (max_code+1)/2`

Define a signed normalized `t`:

* if `code == center_lo or code == center_hi`: `t = 0`
* else:

  * map codes below center to negative, above to positive
  * `k = code - center_hi` for above; `k = code - center_lo` for below
  * `t = k / (center_lo)` (approx symmetric)

Then:

* LINEAR_SIGNED_CENTERED: `value = t * max_abs` where `max_abs = max(abs(min), abs(max))`, then clamp to [min,max]
* GAMMA_SIGNED_CENTERED: `value = sign(t) * (abs(t)^gamma) * max_abs`, then clamp

**Unsigned mapping**
Let normalized `u = code / max_code`.

* LINEAR_UNSIGNED: `value = min + u * (max - min)`
* GAMMA_UNSIGNED: `value = min + (u^gamma) * (max - min)`

Encoding is the inverse mapping with rounding to nearest code and clamping.

#### 17.2.4 Region section layout

At `region_offset`:

**RegionSectionHeader**

* `region_id_u8` (1)
* `reserved_u8` (1)
* `reserved_u16` (2)
* `neuron_span_u32` (4)
* `total_axons_u64` (8)
* `stride_u32` (4) must equal header stride
* `checkpoint_count_u32` (4) = `ceil(neuron_span/stride) + 1` (integer math: `(neuron_span + stride - 1) / stride + 1`)

Then:

* `axon_checkpoints_u64[checkpoint_count]` (8 * checkpoint_count)
* `neuron_records` (6 * neuron_span bytes)
* `axon_records` (4 * total_axons bytes)

Checkpoint semantics:

* `axon_checkpoints[k]` is the cumulative axon count for all neurons with `neuron_id < k*stride`.
* `axon_checkpoints[0] = 0`
* `axon_checkpoints[last] = total_axons`, where `last = ceil(neuron_span/stride)`. The final stride boundary before `neuron_span` may be `< neuron_span` when the span is not a multiple of `stride`.

To find axon start offset for a neuron `i`:

* `k = i / stride`
* `base = axon_checkpoints[k]`
* scan neuron records from `k*stride` to `i-1` summing `axon_count`
* `start = base + sum`
* `count = axon_count(i)`
  Runtime implementations typically precompute per-neuron offsets on load for O(1) access (perhaps depending on brain/region size).

#### 17.2.5 Neuron record (6 bytes, 48 bits)

NeuronId is implicit as record index within region.

Bit layout (LSB=bit0):

* bits 0..8   : `axon_count` (9 bits, 0..511)
* bits 9..14  : `paramB_code` (6)
* bits 15..20 : `paramA_code` (6)
* bits 21..26 : `activation_threshold_code` (6)
* bits 27..32 : `preactivation_threshold_code` (6)
* bits 33..38 : `reset_function_id` (6)
* bits 39..44 : `activation_function_id` (6)
* bits 45..46 : `accumulation_function_id` (2)
* bit 47      : `exists` (1)

Rules:

* If `exists==0` (when deleted/disable by reproduction), `axon_count` must be 0.
* Regions 0 and 31:

  * all neuron records must have `exists==1` (cannot delete/disable definition loci)

#### 17.2.6 Axon record (4 bytes, 32 bits)

Bit layout:

* bits 0..4   : `strength_code` (5)
* bits 5..26  : `target_neuron_id` (22)
* bits 27..31 : `target_region_id` (5)

Rules:

* Axons for each neuron are stored contiguously in the region’s axon record array.
* Within a neuron’s axon list, records must be sorted by `(target_region_id, target_neuron_id)` ascending.
* `target_neuron_id` MUST be < the target region's `neuron_span` and <= 4194303.
* Duplicate axons from a given neuron to the same `(target_region_id, target_neuron_id)` are not allowed.
* Validation invariants:

  * no axon targets region 0
  * no axon from region 31 targets region 31

---

### 17.3 `.nbs` format (NBS2 state snapshot)

#### 17.3.1 Snapshot goals

* Full-brain restore after failures
* Store persistent buffers and optional enabled mask
* Store axon strength overlay codes (only where quantized code differs from base `.nbn`)
* Store energy balance and settings flags

#### 17.3.2 Snapshot layout

1. Fixed header (`NbsHeaderV2`, 512 bytes)
2. Region state sections for all regions present in base `.nbn`
3. Optional axon overlay section
4. Optional metadata (reserved)

#### 17.3.3 `NbsHeaderV2` (512 bytes)

Offsets and sizes:

**0x000–0x003**

* `magic` = ASCII `"NBS2"`

**0x004–0x005**

* `version_u16` = 2

**0x006**

* `endianness_u8` = 1

**0x007**

* `header_bytes_pow2_u8` = 9 (512 bytes)

**0x008–0x017 (16 bytes)**

* `brain_id_uuid` (RFC 4122 byte order)

**0x018–0x01F (8 bytes)**

* `snapshot_tick_id_u64`

**0x020–0x027 (8 bytes)**

* `timestamp_ms_u64`

**0x028–0x02F (8 bytes)**

* `energy_remaining_i64`

**0x030–0x04F (32 bytes)**

* `base_nbn_sha256`

**0x050–0x053 (4 bytes)**

* `flags_u32`

  * bit 0: enabled_bitset_included
  * bit 1: axon_overlay_included
  * bit 2: cost_enabled
  * bit 3: energy_enabled
  * bit 4: plasticity_enabled
  * others reserved

**0x054–0x07F (44 bytes)** Buffer quantization schema (fixed)

* `buffer_map_type_u8`
* `reserved_u8`
* `reserved_u16`
* `buffer_min_f32`
* `buffer_max_f32`
* `buffer_gamma_f32`
* remaining reserved to 44 bytes (must be zero)

Default buffer mapping:

* GAMMA_SIGNED_CENTERED, min=-4, max=+4, gamma=2.0
  Buffer is stored as `int16` code over this range.

**0x080–0x1FF**

* reserved, must be zero

#### 17.3.4 Region state section

For each region present in the base `.nbn` (region directory neuron_span>0), in region_id ascending:

Section header:

* `region_id_u8`
* `reserved_u8`
* `reserved_u16`
* `neuron_span_u32` (must match `.nbn`)
* `buffer_codes_i16[neuron_span]` (persistent B)
* optional `enabled_bitset` if header flag includes it:

  * length = ceil(neuron_span / 8) bytes

Snapshots are taken at tick boundaries; inbox accumulators are not stored.

#### 17.3.5 Axon overlay section

If `axon_overlay_included`:

* `overlay_count_u32`
* then `overlay_count` overlay records:

Overlay record (12 bytes):

* `from_addr32_u32`
* `to_addr32_u32`
* `strength_code_u8` (0..31)
* `reserved_u8`
* `reserved_u16`

Overlay semantics:

* If an overlay record exists for `(from,to)`, that strength code replaces the base strength code from `.nbn`.
* Only store overlay records where the overlay code differs from base `.nbn` code.

---

## 18. Database schemas (SQLite)

### 18.1 SettingsMonitor database

Tables (recommended, values not exhaustive):

**nodes**

* node_id TEXT PRIMARY KEY (UUID)
* logical_name TEXT
* address TEXT (host:port)
* root_actor_name TEXT
* last_seen_ms INTEGER
* is_alive INTEGER

**node_capabilities**

* node_id TEXT
* time_ms INTEGER
* cpu_cores INTEGER
* ram_free_bytes INTEGER
* has_gpu INTEGER
* gpu_name TEXT
* vram_free_bytes INTEGER
* cpu_score REAL
* gpu_score REAL
* PRIMARY KEY (node_id, time_ms)

**settings**

* key TEXT PRIMARY KEY
* value TEXT
* updated_ms INTEGER

**brains**

* brain_id TEXT PRIMARY KEY
* base_nbn_sha256 BLOB (32)
* last_snapshot_sha256 BLOB (32) NULL
* spawned_ms INTEGER
* last_tick_id INTEGER
* state TEXT (Active/Paused/Recovering/Dead)
* notes TEXT NULL

### 18.2 Artifact store metadata database (if hosted with SQLite)

Tables (recommended):

**artifacts**

* artifact_sha256 BLOB(32) PRIMARY KEY
* media_type TEXT
* byte_length INTEGER
* created_ms INTEGER
* manifest_sha256 BLOB(32)
* ref_count INTEGER

**chunks**

* chunk_sha256 BLOB(32) PRIMARY KEY
* byte_length INTEGER
* stored_length INTEGER
* compression TEXT
* ref_count INTEGER

**artifact_chunks**

* artifact_sha256 BLOB(32)
* seq INTEGER
* chunk_sha256 BLOB(32)
* chunk_uncompressed_length INTEGER
* PRIMARY KEY (artifact_sha256, seq)

**artifact_region_index** (optional)

* artifact_sha256 BLOB(32)
* region_id INTEGER
* offset INTEGER
* length INTEGER
* PRIMARY KEY (artifact_sha256, region_id)

---

## 19. Protocol schemas (`.proto`)

The following `.proto` files define the canonical NBN wire schema. They are intended for Proto.Actor remote messaging and for External World integration.

### 19.1 `nbn_common.proto`

```proto
syntax = "proto3";
package nbn;

message Uuid {
  bytes value = 1; // 16 bytes, RFC 4122 byte order
}

message Sha256 {
  bytes value = 1; // 32 bytes
}

message Address32 {
  fixed32 value = 1;
}

message ShardId32 {
  fixed32 value = 1; // region_id in bits 16..20, shard_index in bits 0..15
}

message ArtifactRef {
  Sha256 sha256 = 1;
  string media_type = 2;   // "application/x-nbn", "application/x-nbs"
  fixed64 size_bytes = 3;
  string store_uri = 4;    // optional: artifact store base URI or logical name
}

enum Severity {
  SEV_TRACE = 0;
  SEV_DEBUG = 1;
  SEV_INFO  = 2;
  SEV_WARN  = 3;
  SEV_ERROR = 4;
  SEV_FATAL = 5;
}
```

### 19.2 `nbn_functions.proto`

```proto
syntax = "proto3";
package nbn;

enum AccumulationFunction {
  ACCUM_SUM     = 0;
  ACCUM_PRODUCT = 1;
  ACCUM_MAX     = 2;
  ACCUM_NONE    = 3;
}

enum ActivationFunction {
  ACT_NONE      = 0;
  ACT_IDENTITY  = 1;
  ACT_STEP_UP   = 2;
  ACT_STEP_MID  = 3;
  ACT_STEP_DOWN = 4;
  ACT_ABS       = 5;
  ACT_CLAMP     = 6;
  ACT_RELU      = 7;
  ACT_NRELU     = 8;
  ACT_SIN       = 9;
  ACT_TAN       = 10;
  ACT_TANH      = 11;
  ACT_ELU       = 12;
  ACT_EXP       = 13;
  ACT_PRELU     = 14;
  ACT_LOG       = 15;
  ACT_MULT      = 16;
  ACT_ADD       = 17;
  ACT_SIG       = 18;
  ACT_SILU      = 19;
  ACT_PCLAMP    = 20;
  ACT_MODL      = 21;
  ACT_MODR      = 22;
  ACT_SOFTP     = 23;
  ACT_SELU      = 24;
  ACT_LIN       = 25;
  ACT_LOGB      = 26;
  ACT_POW       = 27;
  ACT_GAUSS     = 28;
  ACT_QUAD      = 29;

  // 30..63 reserved
}

enum ResetFunction {
  RESET_ZERO = 0;
  RESET_HOLD = 1;
  RESET_CLAMP_POTENTIAL = 2;
  RESET_CLAMP1 = 3;
  RESET_POTENTIAL_CLAMP_BUFFER = 4;
  RESET_NEG_POTENTIAL_CLAMP_BUFFER = 5;
  RESET_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER = 6;
  RESET_TENTH_POTENTIAL_CLAMP_BUFFER = 7;
  RESET_HALF_POTENTIAL_CLAMP_BUFFER = 8;
  RESET_DOUBLE_POTENTIAL_CLAMP_BUFFER = 9;
  RESET_FIVEX_POTENTIAL_CLAMP_BUFFER = 10;
  RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER = 11;
  RESET_NEG_TENTH_POTENTIAL_CLAMP_BUFFER = 12;
  RESET_NEG_HALF_POTENTIAL_CLAMP_BUFFER = 13;
  RESET_NEG_DOUBLE_POTENTIAL_CLAMP_BUFFER = 14;
  RESET_NEG_FIVEX_POTENTIAL_CLAMP_BUFFER = 15;
  RESET_INVERSE_POTENTIAL_CLAMP_BUFFER = 16;

  RESET_POTENTIAL_CLAMP1 = 17;
  RESET_NEG_POTENTIAL_CLAMP1 = 18;
  RESET_HUNDREDTHS_POTENTIAL_CLAMP1 = 19;
  RESET_TENTH_POTENTIAL_CLAMP1 = 20;
  RESET_HALF_POTENTIAL_CLAMP1 = 21;
  RESET_DOUBLE_POTENTIAL_CLAMP1 = 22;
  RESET_FIVEX_POTENTIAL_CLAMP1 = 23;
  RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP1 = 24;
  RESET_NEG_TENTH_POTENTIAL_CLAMP1 = 25;
  RESET_NEG_HALF_POTENTIAL_CLAMP1 = 26;
  RESET_NEG_DOUBLE_POTENTIAL_CLAMP1 = 27;
  RESET_NEG_FIVEX_POTENTIAL_CLAMP1 = 28;
  RESET_INVERSE_POTENTIAL_CLAMP1 = 29;

  RESET_POTENTIAL = 30;
  RESET_NEG_POTENTIAL = 31;
  RESET_HUNDREDTHS_POTENTIAL = 32;
  RESET_TENTH_POTENTIAL = 33;
  RESET_HALF_POTENTIAL = 34;
  RESET_DOUBLE_POTENTIAL = 35;
  RESET_FIVEX_POTENTIAL = 36;
  RESET_NEG_HUNDREDTHS_POTENTIAL = 37;
  RESET_NEG_TENTH_POTENTIAL = 38;
  RESET_NEG_HALF_POTENTIAL = 39;
  RESET_NEG_DOUBLE_POTENTIAL = 40;
  RESET_NEG_FIVEX_POTENTIAL = 41;
  RESET_INVERSE_POTENTIAL = 42;

  RESET_HALF = 43;
  RESET_TENTH = 44;
  RESET_HUNDREDTH = 45;
  RESET_NEGATIVE = 46;
  RESET_NEG_HALF = 47;
  RESET_NEG_TENTH = 48;
  RESET_NEG_HUNDREDTH = 49;

  RESET_DOUBLE_CLAMP1 = 50;
  RESET_FIVEX_CLAMP1 = 51;
  RESET_NEG_DOUBLE_CLAMP1 = 52;
  RESET_NEG_FIVEX_CLAMP1 = 53;

  RESET_DOUBLE = 54;
  RESET_FIVEX = 55;
  RESET_NEG_DOUBLE = 56;
  RESET_NEG_FIVEX = 57;

  RESET_DIVIDE_AXON_CT = 58;
  RESET_INVERSE_CLAMP1 = 59;
  RESET_INVERSE = 60;

  // 61..63 reserved
}
```

### 19.3 `nbn_settings.proto`

```proto
syntax = "proto3";
package nbn.settings;

option csharp_namespace = "Nbn.Proto.Settings";

import "nbn_common.proto";

message NodeOnline {
  nbn.Uuid node_id = 1;
  string logical_name = 2;
  string address = 3;          // host:port
  string root_actor_name = 4;  // stable
}

message NodeOffline {
  nbn.Uuid node_id = 1;
  string logical_name = 2;
}

message NodeHeartbeat {
  nbn.Uuid node_id = 1;
  fixed64 time_ms = 2;
  NodeCapabilities caps = 3;
}

message NodeCapabilities {
  uint32 cpu_cores = 1;
  fixed64 ram_free_bytes = 2;

  bool has_gpu = 3;
  string gpu_name = 4;
  fixed64 vram_free_bytes = 5;

  float cpu_score = 6;
  float gpu_score = 7;

  bool ilgpu_cuda_available = 8;
  bool ilgpu_opencl_available = 9;
}

message NodeStatus {
  nbn.Uuid node_id = 1;
  string logical_name = 2;
  string address = 3;
  string root_actor_name = 4;
  fixed64 last_seen_ms = 5;
  bool is_alive = 6;
}

message BrainStatus {
  nbn.Uuid brain_id = 1;
  fixed64 spawned_ms = 2;
  fixed64 last_tick_id = 3;
  string state = 4;
}

message BrainControllerStatus {
  nbn.Uuid brain_id = 1;
  nbn.Uuid node_id = 2;
  string actor_name = 3;
  fixed64 last_seen_ms = 4;
  bool is_alive = 5;
}

message NodeListRequest { }

message NodeListResponse {
  repeated NodeStatus nodes = 1;
}

message BrainListRequest { }

message BrainListResponse {
  repeated BrainStatus brains = 1;
  repeated BrainControllerStatus controllers = 2;
}

message SettingListRequest { }

message SettingListResponse {
  repeated SettingValue settings = 1;
}

message SettingGet {
  string key = 1;
}

message SettingValue {
  string key = 1;
  string value = 2;
  fixed64 updated_ms = 3;
}

message SettingSet {
  string key = 1;
  string value = 2;
}

message SettingChanged {
  string key = 1;
  string value = 2;
  fixed64 updated_ms = 3;
}

message SettingSubscribe {
  string subscriber_actor = 1; // actor name/path
}

message SettingUnsubscribe {
  string subscriber_actor = 1;
}

message BrainRegistered {
  nbn.Uuid brain_id = 1;
  fixed64 spawned_ms = 2;
  fixed64 last_tick_id = 3;
  string state = 4;

  nbn.Uuid controller_node_id = 5;
  string controller_node_address = 6;
  string controller_node_logical_name = 7;
  string controller_root_actor_name = 8;
  string controller_actor_name = 9;
}

message BrainStateChanged {
  nbn.Uuid brain_id = 1;
  string state = 2;
  string notes = 3;
}

message BrainTick {
  nbn.Uuid brain_id = 1;
  fixed64 last_tick_id = 2;
}

message BrainControllerHeartbeat {
  nbn.Uuid brain_id = 1;
  fixed64 time_ms = 2;
}

message BrainUnregistered {
  nbn.Uuid brain_id = 1;
  fixed64 time_ms = 2;
}
```

### 19.4 `nbn_control.proto`

```proto
syntax = "proto3";
package nbn.control;

import "nbn_common.proto";

message SpawnBrain {
  nbn.ArtifactRef brain_def = 1; // .nbn
}

message SpawnBrainAck {
  nbn.Uuid brain_id = 1;
}

message PauseBrain {
  nbn.Uuid brain_id = 1;
  string reason = 2;
}

message ResumeBrain {
  nbn.Uuid brain_id = 1;
}

message KillBrain {
  nbn.Uuid brain_id = 1;
  string reason = 2;
}

message BrainTerminated {
  nbn.Uuid brain_id = 1;
  string reason = 2;
  nbn.ArtifactRef base_def = 3;       // .nbn
  nbn.ArtifactRef last_snapshot = 4;  // .nbs (optional; may be empty sha)
  sint64 last_energy_remaining = 5;
  sint64 last_tick_cost = 6;
  fixed64 time_ms = 7;
}

message TickCompute {
  fixed64 tick_id = 1;
  float target_tick_hz = 2;
}

message TickComputeDone {
  fixed64 tick_id = 1;
  nbn.Uuid brain_id = 2;
  uint32 region_id = 3;
  nbn.ShardId32 shard_id = 4;

  fixed64 compute_ms = 5;

  sint64 tick_cost_total = 6;
  sint64 cost_accum = 7;
  sint64 cost_activation = 8;
  sint64 cost_reset = 9;
  sint64 cost_distance = 10;
  sint64 cost_remote = 11;

  uint32 fired_count = 12;
  uint32 out_batches = 13;
  uint32 out_contribs = 14;
}

message TickDeliver {
  fixed64 tick_id = 1;
}

message TickDeliverDone {
  fixed64 tick_id = 1;
  nbn.Uuid brain_id = 2;
  fixed64 deliver_ms = 3;
  uint32 delivered_batches = 4;
  uint32 delivered_contribs = 5;
}

message GetHiveMindStatus { }

message HiveMindStatus {
  fixed64 last_completed_tick_id = 1;
  bool tick_loop_enabled = 2;
  float target_tick_hz = 3;
  uint32 pending_compute = 4;
  uint32 pending_deliver = 5;
  bool reschedule_in_progress = 6;
  uint32 registered_brains = 7;
  uint32 registered_shards = 8;
}

message GetBrainRouting {
  nbn.Uuid brain_id = 1;
}

message BrainRoutingInfo {
  nbn.Uuid brain_id = 1;
  string brain_root_pid = 2;
  string signal_router_pid = 3;
  uint32 shard_count = 4;
  uint32 routing_count = 5;
}

message RegisterShard {
  nbn.Uuid brain_id = 1;
  uint32 region_id = 2;
  uint32 shard_index = 3;
  string shard_pid = 4;
  uint32 neuron_start = 5;
  uint32 neuron_count = 6;
}

message RegisterOutputSink {
  nbn.Uuid brain_id = 1;
  string output_pid = 2;
}

message UpdateShardOutputSink {
  nbn.Uuid brain_id = 1;
  uint32 region_id = 2;
  uint32 shard_index = 3;
  string output_pid = 4; // empty clears
}

message GetBrainIoInfo {
  nbn.Uuid brain_id = 1;
}

message BrainIoInfo {
  nbn.Uuid brain_id = 1;
  uint32 input_width = 2;
  uint32 output_width = 3;
}
```

### 19.5 `nbn_signals.proto`

```proto
syntax = "proto3";
package nbn.signal;

import "nbn_common.proto";

message Contribution {
  uint32 target_neuron_id = 1; // local to destination region
  float value = 2;
}

message SignalBatch {
  nbn.Uuid brain_id = 1;
  uint32 region_id = 2;
  nbn.ShardId32 shard_id = 3;
  fixed64 tick_id = 4; // tick_id whose delivery phase is delivering this batch
  repeated Contribution contribs = 5;
}

message SignalBatchAck {
  nbn.Uuid brain_id = 1;
  uint32 region_id = 2;
  nbn.ShardId32 shard_id = 3;
  fixed64 tick_id = 4;
}

// Produced by RegionShard during compute phase, sent to BrainSignalRouter:
message OutboxBatch {
  nbn.Uuid brain_id = 1;
  fixed64 tick_id = 2; // compute tick that produced the outbox
  uint32 dest_region_id = 3;
  nbn.ShardId32 dest_shard_id = 4;
  repeated Contribution contribs = 5;
}
```

### 19.6 `nbn_io.proto`

```proto
syntax = "proto3";
package nbn.io;

import "nbn_common.proto";
import "nbn_control.proto";
import "nbn_repro.proto";
import "nbn_signals.proto";

message Connect {
  string client_name = 1;
}

message ConnectAck {
  string server_name = 1;
  fixed64 server_time_ms = 2;
}

message BrainInfoRequest {
  nbn.Uuid brain_id = 1;
}

message BrainInfo {
  nbn.Uuid brain_id = 1;
  uint32 input_width = 2;
  uint32 output_width = 3;

  bool cost_enabled = 4;
  bool energy_enabled = 5;
  sint64 energy_remaining = 6;

  bool plasticity_enabled = 7;
}

message BrainEnergyState {
  sint64 energy_remaining = 1;
  sint64 energy_rate_units_per_second = 2;
  bool cost_enabled = 3;
  bool energy_enabled = 4;
  bool plasticity_enabled = 5;
  float plasticity_rate = 6;
  bool plasticity_probabilistic_updates = 7;
  sint64 last_tick_cost = 8;
}

message RegisterBrain {
  nbn.Uuid brain_id = 1;
  uint32 input_width = 2;
  uint32 output_width = 3;
  nbn.ArtifactRef base_definition = 4;
  nbn.ArtifactRef last_snapshot = 5;
  BrainEnergyState energy_state = 6;
}

message UnregisterBrain {
  nbn.Uuid brain_id = 1;
  string reason = 2;
}

message RegisterIoGateway {
  nbn.Uuid brain_id = 1;
  string io_gateway_pid = 2; // "address/id" or "id" if local
}

message SpawnBrainViaIO {
  nbn.control.SpawnBrain request = 1;
}

message SpawnBrainViaIOAck {
  nbn.control.SpawnBrainAck ack = 1;
}

message InputWrite {
  nbn.Uuid brain_id = 1;
  uint32 input_index = 2;
  float value = 3;
}

message InputVector {
  nbn.Uuid brain_id = 1;
  repeated float values = 2;
}

message DrainInputs {
  nbn.Uuid brain_id = 1;
  fixed64 tick_id = 2;
}

message InputDrain {
  nbn.Uuid brain_id = 1;
  fixed64 tick_id = 2;
  repeated nbn.signal.Contribution contribs = 3;
}

message SubscribeOutputs {
  nbn.Uuid brain_id = 1;
}

message UnsubscribeOutputs {
  nbn.Uuid brain_id = 1;
}

message OutputEvent {
  nbn.Uuid brain_id = 1;
  uint32 output_index = 2;
  float value = 3;
  fixed64 tick_id = 4;
}

message SubscribeOutputsVector {
  nbn.Uuid brain_id = 1;
}

message UnsubscribeOutputsVector {
  nbn.Uuid brain_id = 1;
}

message OutputVectorEvent {
  nbn.Uuid brain_id = 1;
  fixed64 tick_id = 2;
  repeated float values = 3;
}

message EnergyCredit {
  nbn.Uuid brain_id = 1;
  sint64 amount = 2;
}

message EnergyRate {
  nbn.Uuid brain_id = 1;
  sint64 units_per_second = 2;
}

message SetCostEnergyEnabled {
  nbn.Uuid brain_id = 1;
  bool cost_enabled = 2;
  bool energy_enabled = 3;
}

message SetPlasticityEnabled {
  nbn.Uuid brain_id = 1;
  bool plasticity_enabled = 2;
  float plasticity_rate = 3;
  bool probabilistic_updates = 4;
}

message RequestSnapshot {
  nbn.Uuid brain_id = 1;
}

message SnapshotReady {
  nbn.Uuid brain_id = 1;
  nbn.ArtifactRef snapshot = 2; // .nbs
}

message ExportBrainDefinition {
  nbn.Uuid brain_id = 1;
  bool rebase_overlays = 2; // if true, incorporate overlays into base .nbn
}

message BrainDefinitionReady {
  nbn.Uuid brain_id = 1;
  nbn.ArtifactRef brain_def = 2; // .nbn
}

message ReproduceByBrainIds {
  nbn.repro.ReproduceByBrainIdsRequest request = 1;
}

message ReproduceByArtifacts {
  nbn.repro.ReproduceByArtifactsRequest request = 1;
}

message ReproduceResult {
  nbn.repro.ReproduceResult result = 1;
}
```

### 19.7 `nbn_debug.proto`

```proto
syntax = "proto3";
package nbn.debug;

import "nbn_common.proto";

message DebugOutbound {
  nbn.Severity severity = 1;
  string context = 2;
  string summary = 3;
  string message = 4;

  string sender_actor = 5; // string path/id
  string sender_node = 6;  // logical name or node id string
  fixed64 time_ms = 7;
}

message DebugInbound {
  DebugOutbound outbound = 1;
  fixed64 server_received_ms = 2;
}

message DebugSubscribe {
  string subscriber_actor = 1; // actor name/path
  nbn.Severity min_severity = 2;
  string context_regex = 3;
}

message DebugUnsubscribe {
  string subscriber_actor = 1;
}

message DebugFlushAll { }
```

### 19.8 `nbn_viz.proto`

```proto
syntax = "proto3";
package nbn.viz;

import "nbn_common.proto";

enum VizEventType {
  VIZ_UNKNOWN = 0;

  VIZ_BRAIN_SPAWNED = 1;
  VIZ_BRAIN_ACTIVE = 2;
  VIZ_BRAIN_PAUSED = 3;
  VIZ_BRAIN_TERMINATED = 4;

  VIZ_SHARD_SPAWNED = 10;
  VIZ_SHARD_MOVED = 11;

  VIZ_TICK = 20;

  VIZ_NEURON_BUFFER = 30;
  VIZ_NEURON_FIRED = 31;

  VIZ_AXON_SENT = 40;
}

message VisualizationEvent {
  string event_id = 1;     // uuid string or monotonic id
  fixed64 time_ms = 2;
  VizEventType type = 3;

  nbn.Uuid brain_id = 4;
  fixed64 tick_id = 5;

  uint32 region_id = 6;
  nbn.ShardId32 shard_id = 7;

  nbn.Address32 source = 8;
  nbn.Address32 target = 9;

  float value = 10;        // signal or buffer/potential depending on event
  float strength = 11;     // axon strength if relevant
}
```

### 19.9 `nbn_repro.proto`

```proto
syntax = "proto3";
package nbn.repro;

import "nbn_common.proto";

enum StrengthSource {
  STRENGTH_BASE_ONLY = 0; // use .nbn only
  STRENGTH_LIVE_CODES = 1; // base + overlay codes from latest .nbs
}

enum SpawnChildPolicy {
  SPAWN_CHILD_DEFAULT_ON = 0; // spawn unless explicitly disabled
  SPAWN_CHILD_NEVER = 1;
  SPAWN_CHILD_ALWAYS = 2;
}

enum PrunePolicy {
  PRUNE_LOWEST_ABS_STRENGTH_FIRST = 0;
  PRUNE_NEW_CONNECTIONS_FIRST = 1;
  PRUNE_RANDOM = 2;
}

message ReproduceLimits {
  // Limits can be specified as absolute and/or percentage.
  // If both are present for the same dimension, the smallest effective limit applies.

  uint32 max_neurons_added_abs = 1;
  float  max_neurons_added_pct = 2;

  uint32 max_neurons_removed_abs = 3;
  float  max_neurons_removed_pct = 4;

  uint32 max_axons_added_abs = 5;
  float  max_axons_added_pct = 6;

  uint32 max_axons_removed_abs = 7;
  float  max_axons_removed_pct = 8;

  uint32 max_regions_added_abs = 9;
  uint32 max_regions_removed_abs = 10;
}

message ReproduceConfig {
  // Similarity thresholds
  float max_region_span_diff_ratio = 1;
  float max_function_hist_distance = 2;
  float max_connectivity_hist_distance = 3;

  // Region presence handling
  float prob_add_neuron_to_empty_region = 10;
  float prob_remove_last_neuron_from_region = 11;

  // Neuron enable/disable/reactivation within existing regions
  float prob_disable_neuron = 12;
  float prob_reactivate_neuron = 13;

  // Axon handling
  float prob_add_axon = 20;
  float prob_remove_axon = 21;
  float prob_reroute_axon = 22; // reroute an axon (where inbound and outbound neurons both exist), instead of choosing one parents' route
  float prob_reroute_inbound_axon_on_delete = 23; // inbound to deleted neuron
  //float prob_delete_inbound_axon_on_delete = 24; // implicit (1-prob_reroute_inbound_axon_on_delete)

  // Value selection for neuron parameter codes
  float prob_choose_parentA = 30;
  float prob_choose_parentB = 31;
  float prob_average = 32;
  float prob_mutate = 33;

  // Function selection/mutation
  float prob_choose_funcA = 40;
  //float prob_choose_funcB = 41; // implicit
  float prob_mutate_func = 42;

  // Strength handling (optional)
  bool strength_transform_enabled = 50;
  float prob_strength_choose_A = 51;
  float prob_strength_choose_B = 52;
  float prob_strength_average = 53;
  float prob_strength_weighted_average = 54;
  float strength_weight_A = 55;
  float strength_weight_B = 56;
  float prob_strength_mutate = 57;

  // Out-degree control
  float max_avg_out_degree_brain = 60;
  PrunePolicy prune_policy = 61;

  // Limits
  ReproduceLimits limits = 70;

  // Child spawn
  SpawnChildPolicy spawn_child = 80;
}

message ReproduceByBrainIdsRequest {
  nbn.Uuid parentA = 1;
  nbn.Uuid parentB = 2;
  StrengthSource strength_source = 3;
  ReproduceConfig config = 4;
  fixed64 seed = 5;
}

message ReproduceByArtifactsRequest {
  nbn.ArtifactRef parentA_def = 1; // .nbn
  nbn.ArtifactRef parentA_state = 2; // optional .nbs
  nbn.ArtifactRef parentB_def = 3; // .nbn
  nbn.ArtifactRef parentB_state = 4; // optional .nbs
  StrengthSource strength_source = 5;
  ReproduceConfig config = 6;
  fixed64 seed = 7;
}

message SimilarityReport {
  bool compatible = 1;
  string abort_reason = 2;

  float region_span_score = 10;
  float function_score = 11;
  float connectivity_score = 12;

  uint32 regions_present_A = 20;
  uint32 regions_present_B = 21;
  uint32 regions_present_child = 22;
}

message MutationSummary {
  uint32 neurons_added = 1;
  uint32 neurons_removed = 2;
  uint32 axons_added = 3;
  uint32 axons_removed = 4;
  uint32 axons_rerouted = 5;
  uint32 functions_mutated = 6;
  uint32 strength_codes_changed = 7;
}

message ReproduceResult {
  SimilarityReport report = 1;
  MutationSummary summary = 2;

  nbn.ArtifactRef child_def = 10; // .nbn
  bool spawned = 11;
  nbn.Uuid child_brain_id = 12; // valid if spawned==true
}
```

---

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
`tools/demo/run_local_hivemind_demo.sh` (Ubuntu/Linux). The scripts:

* Creates a tiny `.nbn` (regions 0, 1, and 31 with 1 neuron each) with a single self-loop axon in region 1 to exercise SignalBatch delivery, and stores it in a local artifact store
* Starts SettingsMonitor, HiveMind, a DemoBrainHost (BrainRoot + named BrainSignalRouter), a RegionHost shard for region 1, IO Gateway, and Observability
* Logs output to `tools/demo/local-demo/logs`

The demo uses default ports (SettingsMonitor 12010, HiveMind 12020, BrainHost 12011, RegionHost 12040, IO 12050, Observability 12060) and can be edited in the script
parameters if needed.

---

# Appendix A: Defaults and constants

* Regions: 0..31
* Input region: 0
* Output region: 31
* Default `.nbn` stride: 1024
* Shard alignment: multiples of stride
* Neuron record size: 6 bytes
* Axon record size: 4 bytes
* Max axons per neuron: 511 (format cap)
* Default max average out-degree for reproduction and random brain generation: 100
* Default region_weight: 1
* Default axon_base_cost: 1 (micro-units)
* Default axon_unit_cost: 1 (micro-units per distance unit)
* Default cost/energy disabled unless enabled per brain
* Default remote transport cost disabled (0)

---

# Appendix B: Function catalog (IDs, formulas, tiers, costs)

All functions operate on float32 values (`MathF` semantics). The “cost weight” is an abstract multiplier used by cost accounting; it is not wall-clock time.

## B.1 AccumulationFunction (2-bit)

* `ACCUM_SUM (0)`

  * Merge: `B = B + I`
  * Tier: A, Cost weight: 1.0
* `ACCUM_PRODUCT (1)`

  * Inbox tracked as `(hasInput, value)`
  * Merge if `hasInput`: `B = B * value`
  * Tier: A, Cost weight: 1.2
* `ACCUM_MAX (2)`

  * Merge: `B = max(B, I)`
  * Tier: A, Cost weight: 1.0
* `ACCUM_NONE (3)`

  * Merge does nothing
  * Tier: A, Cost weight: 0.1

## B.2 ActivationFunction (6-bit)

`Activate(buffer B, paramA A, paramB Bp) -> potential`

* `ACT_NONE (0)`
  potential = 0
  Tier A, cost 0.0

* `ACT_IDENTITY (1)`
  potential = B
  Tier A, cost 1.0

* `ACT_STEP_UP (2)`
  potential = (B <= 0) ? 0 : 1
  Tier A, cost 1.0

* `ACT_STEP_MID (3)`
  potential = (B < 0) ? -1 : (B == 0 ? 0 : 1)
  Tier A, cost 1.0

* `ACT_STEP_DOWN (4)`
  potential = (B < 0) ? -1 : 0
  Tier A, cost 1.0

* `ACT_ABS (5)`
  potential = abs(B)
  Tier A, cost 1.1

* `ACT_CLAMP (6)`
  potential = clamp(B, -1, +1)
  Tier A, cost 1.1

* `ACT_RELU (7)`
  potential = max(0, B)
  Tier A, cost 1.1

* `ACT_NRELU (8)`
  potential = min(B, 0)
  Tier A, cost 1.1

* `ACT_SIN (9)`
  potential = sin(B)
  Tier B, cost 1.4

* `ACT_TAN (10)`
  potential = clamp(tan(B), -1, +1)
  Tier B, cost 1.6

* `ACT_TANH (11)`
  potential = tanh(B)
  Tier B, cost 1.6

* `ACT_ELU (12)` uses A
  potential = (B > 0) ? B : A * (exp(B) - 1)
  Tier B, cost 1.8

* `ACT_EXP (13)`
  potential = exp(B)
  Tier B, cost 1.8

* `ACT_PRELU (14)` uses A
  potential = (B >= 0) ? B : A * B
  Tier A/B, cost 1.4

* `ACT_LOG (15)`
  potential = (B == 0) ? 0 : log(B)
  Tier B, cost 1.9

* `ACT_MULT (16)` uses A
  potential = B * A
  Tier A, cost 1.2

* `ACT_ADD (17)` uses A
  potential = B + A
  Tier A, cost 1.2

* `ACT_SIG (18)`
  potential = 1 / (1 + exp(-B))
  Tier B, cost 2.0

* `ACT_SILU (19)`
  potential = B / (1 + exp(-B))
  Tier B, cost 2.0

* `ACT_PCLAMP (20)` uses A and Bp
  potential = (Bp <= A) ? 0 : clamp(B, A, Bp)
  Tier A, cost 1.3

* `ACT_MODL (21)` uses A
  potential = B % A
  Tier C, cost 2.6

* `ACT_MODR (22)` uses A
  potential = A % B
  Tier C, cost 2.6

* `ACT_SOFTP (23)`
  potential = log(1 + exp(B))
  Tier C, cost 2.8

* `ACT_SELU (24)` uses A and Bp
  potential = Bp * (B >= 0 ? B : A*(exp(B)-1))
  Tier C, cost 2.8

* `ACT_LIN (25)` uses A and Bp
  potential = A * B + Bp
  Tier A, cost 1.4

* `ACT_LOGB (26)` uses A
  potential = (A == 0) ? 0 : log(B, A)
  Tier C, cost 3.0

* `ACT_POW (27)` uses A
  potential = pow(B, A)
  Tier C, cost 3.5

* `ACT_GAUSS (28)`
  potential = exp((-B)^2)
  Tier C, cost 5.0

* `ACT_QUAD (29)` uses A and Bp
  potential = A*(B^2) + Bp*B
  Tier C, cost 6.0

## B.3 ResetFunction (6-bit)

`Reset(buffer B, potential P, activation_threshold T, out_degree K) -> new_buffer`

Below, `clamp(x, lo, hi)` clamps x.

* `RESET_ZERO (0)`
  new = 0
  Tier A, cost 0.2

* `RESET_HOLD (1)`
  new = clamp(B, -T, +T)
  Tier A, cost 1.0

* `RESET_CLAMP_POTENTIAL (2)`
  new = clamp(B, -abs(P), +abs(P))
  Tier A, cost 1.0

* `RESET_CLAMP1 (3)`
  new = clamp(B, -1, +1)
  Tier A, cost 1.0

* `RESET_POTENTIAL_CLAMP_BUFFER (4)`
  new = clamp(P, -abs(B), +abs(B))
  Tier A, cost 1.0

* `RESET_NEG_POTENTIAL_CLAMP_BUFFER (5)`
  new = clamp(-P, -abs(B), +abs(B))
  Tier A, cost 1.0

* `RESET_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER (6)`
  new = clamp(0.01*P, -abs(B), +abs(B))
  Tier A, cost 1.0

* `RESET_TENTH_POTENTIAL_CLAMP_BUFFER (7)`
  new = clamp(0.1*P, -abs(B), +abs(B))
  Tier A, cost 1.0

* `RESET_HALF_POTENTIAL_CLAMP_BUFFER (8)`
  new = clamp(0.5*P, -abs(B), +abs(B))
  Tier A, cost 1.0

* `RESET_DOUBLE_POTENTIAL_CLAMP_BUFFER (9)`
  new = clamp(2*P, -abs(B), +abs(B))
  Tier B, cost 1.2

* `RESET_FIVEX_POTENTIAL_CLAMP_BUFFER (10)`
  new = clamp(5*P, -abs(B), +abs(B))
  Tier B, cost 1.3

* `RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER (11)`
  new = clamp(-0.01*P, -abs(B), +abs(B))
  Tier A, cost 1.0

* `RESET_NEG_TENTH_POTENTIAL_CLAMP_BUFFER (12)`
  new = clamp(-0.1*P, -abs(B), +abs(B))
  Tier A, cost 1.0

* `RESET_NEG_HALF_POTENTIAL_CLAMP_BUFFER (13)`
  new = clamp(-0.5*P, -abs(B), +abs(B))
  Tier A, cost 1.0

* `RESET_NEG_DOUBLE_POTENTIAL_CLAMP_BUFFER (14)`
  new = clamp(-2*P, -abs(B), +abs(B))
  Tier B, cost 1.2

* `RESET_NEG_FIVEX_POTENTIAL_CLAMP_BUFFER (15)`
  new = clamp(-5*P, -abs(B), +abs(B))
  Tier B, cost 1.3

* `RESET_INVERSE_POTENTIAL_CLAMP_BUFFER (16)`
  new = clamp(1/P, -abs(B), +abs(B))
  Tier C, cost 1.8

* `RESET_POTENTIAL_CLAMP1 (17)`
  new = clamp(P, -1, +1)
  Tier A, cost 1.0

* `RESET_NEG_POTENTIAL_CLAMP1 (18)`
  new = clamp(-P, -1, +1)
  Tier A, cost 1.0

* `RESET_HUNDREDTHS_POTENTIAL_CLAMP1 (19)`
  new = clamp(0.01*P, -1, +1)
  Tier A, cost 1.0

* `RESET_TENTH_POTENTIAL_CLAMP1 (20)`
  new = clamp(0.1*P, -1, +1)
  Tier A, cost 1.0

* `RESET_HALF_POTENTIAL_CLAMP1 (21)`
  new = clamp(0.5*P, -1, +1)
  Tier A, cost 1.0

* `RESET_DOUBLE_POTENTIAL_CLAMP1 (22)`
  new = clamp(2*P, -1, +1)
  Tier B, cost 1.2

* `RESET_FIVEX_POTENTIAL_CLAMP1 (23)`
  new = clamp(5*P, -1, +1)
  Tier B, cost 1.3

* `RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP1 (24)`
  new = clamp(-0.01*P, -1, +1)
  Tier A, cost 1.0

* `RESET_NEG_TENTH_POTENTIAL_CLAMP1 (25)`
  new = clamp(-0.1*P, -1, +1)
  Tier A, cost 1.0

* `RESET_NEG_HALF_POTENTIAL_CLAMP1 (26)`
  new = clamp(-0.5*P, -1, +1)
  Tier A, cost 1.0

* `RESET_NEG_DOUBLE_POTENTIAL_CLAMP1 (27)`
  new = clamp(-2*P, -1, +1)
  Tier B, cost 1.2

* `RESET_NEG_FIVEX_POTENTIAL_CLAMP1 (28)`
  new = clamp(-5*P, -1, +1)
  Tier B, cost 1.3

* `RESET_INVERSE_POTENTIAL_CLAMP1 (29)`
  new = clamp(1/P, -1, +1)
  Tier C, cost 1.8

* `RESET_POTENTIAL (30)`
  new = clamp(P, -T, +T)
  Tier A, cost 1.0

* `RESET_NEG_POTENTIAL (31)`
  new = clamp(-P, -T, +T)
  Tier A, cost 1.0

* `RESET_HUNDREDTHS_POTENTIAL (32)`
  new = clamp(0.01*P, -T, +T)
  Tier A, cost 1.0

* `RESET_TENTH_POTENTIAL (33)`
  new = clamp(0.1*P, -T, +T)
  Tier A, cost 1.0

* `RESET_HALF_POTENTIAL (34)`
  new = clamp(0.5*P, -T, +T)
  Tier A, cost 1.0

* `RESET_DOUBLE_POTENTIAL (35)`
  new = clamp(2*P, -T, +T)
  Tier B, cost 1.2

* `RESET_FIVEX_POTENTIAL (36)`
  new = clamp(5*P, -T, +T)
  Tier B, cost 1.3

* `RESET_NEG_HUNDREDTHS_POTENTIAL (37)`
  new = clamp(-0.01*P, -T, +T)
  Tier A, cost 1.0

* `RESET_NEG_TENTH_POTENTIAL (38)`
  new = clamp(-0.1*P, -T, +T)
  Tier A, cost 1.0

* `RESET_NEG_HALF_POTENTIAL (39)`
  new = clamp(-0.5*P, -T, +T)
  Tier A, cost 1.0

* `RESET_NEG_DOUBLE_POTENTIAL (40)`
  new = clamp(-2*P, -T, +T)
  Tier B, cost 1.2

* `RESET_NEG_FIVEX_POTENTIAL (41)`
  new = clamp(-5*P, -T, +T)
  Tier B, cost 1.3

* `RESET_INVERSE_POTENTIAL (42)`
  new = clamp(1/P, -T, +T)
  Tier C, cost 1.8

* `RESET_HALF (43)`
  new = clamp(0.5*B, -T, +T)
  Tier A, cost 1.0

* `RESET_TENTH (44)`
  new = clamp(0.1*B, -T, +T)
  Tier A, cost 1.0

* `RESET_HUNDREDTH (45)`
  new = clamp(0.01*B, -T, +T)
  Tier A, cost 1.0

* `RESET_NEGATIVE (46)`
  new = clamp(-B, -T, +T)
  Tier A, cost 1.0

* `RESET_NEG_HALF (47)`
  new = clamp(-0.5*B, -T, +T)
  Tier A, cost 1.0

* `RESET_NEG_TENTH (48)`
  new = clamp(-0.1*B, -T, +T)
  Tier A, cost 1.0

* `RESET_NEG_HUNDREDTH (49)`
  new = clamp(-0.01*B, -T, +T)
  Tier A, cost 1.0

* `RESET_DOUBLE_CLAMP1 (50)`
  new = clamp(2*B, -1, +1)
  Tier B, cost 1.2

* `RESET_FIVEX_CLAMP1 (51)`
  new = clamp(5*B, -1, +1)
  Tier B, cost 1.3

* `RESET_NEG_DOUBLE_CLAMP1 (52)`
  new = clamp(-2*B, -1, +1)
  Tier B, cost 1.2

* `RESET_NEG_FIVEX_CLAMP1 (53)`
  new = clamp(-5*B, -1, +1)
  Tier B, cost 1.3

* `RESET_DOUBLE (54)`
  new = clamp(2*B, -T, +T)
  Tier B, cost 1.2

* `RESET_FIVEX (55)`
  new = clamp(5*B, -T, +T)
  Tier B, cost 1.3

* `RESET_NEG_DOUBLE (56)`
  new = clamp(-2*B, -T, +T)
  Tier B, cost 1.2

* `RESET_NEG_FIVEX (57)`
  new = clamp(-5*B, -T, +T)
  Tier B, cost 1.3

* `RESET_DIVIDE_AXON_CT (58)`
  new = clamp(B / max(1, K), -T, +T)
  Tier A, cost 1.1

* `RESET_INVERSE_CLAMP1 (59)`
  new = clamp(-1/B, -1, +1)
  Tier C, cost 1.8

* `RESET_INVERSE (60)`
  new = clamp(-1/B, -T, +T)
  Tier C, cost 1.8

---

# Appendix C: Region axial map and examples

## C.1 Axial coordinate table

* z=-3: region 0
* z=-2: regions 1,2,3
* z=-1: regions 4,5,6,7,8
* z=0 : regions 9..22
* z=+1: regions 23..27
* z=+2: regions 28..30
* z=+3: region 31

## C.2 Region distance examples (default params)

With `region_intraslice_unit=3` and `region_axial_unit=5`:

* dist(0,31) = 5 * |(-3) - (+3)| = 30
* dist(1,2)  = 3 (same slice)  // note that if they were **same** region this cost would not be incurred
* dist(3,4)  = 5 * |(-2) - (-1)| = 5
* dist(8,23) = 5 * |(-1) - (+1)| = 10

---

## 21. Aleph MCP workflow for NBNv2

### 21.1 Purpose

Aleph is the default workflow for deep codebase analysis in this repo:

* file-first investigation
* scoped sub-queries for discovery and synthesis
* compact context handoff into final edits

Use Aleph to reduce context bloat while still collecting concrete evidence (paths, symbols, message contracts, and behavior edges).

For non-trivial tasks, Aleph-first is the expected posture: do minimal shell scouting, then move quickly into Aleph contexts and delegated sub-query packets for analysis.

### 21.2 When Aleph use is expected (strong default)

Use Aleph before substantial edits when **any** of the following is true:

* task touches 3+ files, or likely spans UI + ViewModel + service/protocol layers
* any target file is large (roughly 500+ LOC) or dense/low-cohesion
* request explicitly asks for a careful sweep, architecture review, or “fine-tooth comb” pass
* ownership/call path/invariant location is not obvious from a quick file scan
* you need to synthesize behavior across modules before changing code

If you skip Aleph despite these triggers, provide a one-line reason in your progress update.

### 21.3 When direct shell-only analysis is acceptable

Aleph is optional for narrowly scoped work such as:

* single-file, clearly located fixes with low ambiguity
* mechanical edits with exact known paths/symbols
* pure build/test/run/format loops after edits are complete

If scope expands mid-task, switch to Aleph immediately.

### 21.4 Model defaults and upgrade policy

* Default behavior: sub-queries inherit the currently selected Codex/main conversation model.
* Current baseline expectation is GPT-5.3 class quality; Spark baseline is GPT-5.3-Spark class speed.
* Do not hard-pin version numbers when an alias/channel is available.
* For explicit model routing, prefer family aliases so upgrades happen automatically as new versions release:
  * stable/primary family alias (GPT-5)
  * fast scouting family alias (GPT-5-Spark)
* Never reduce reasoning effort below **medium** for either primary or Spark family calls.

### 21.5 Reasoning-effort policy

* **Medium (default):**
  * repository scouting
  * finding relevant files/types/messages
  * assembling focused snippets
  * simple, low-risk refactors
* **High:**
  * cross-module behavior changes
  * protocol/schema or lifecycle changes
  * recovery, ordering, concurrency, or correctness-critical logic

### 21.6 Sub-query routing guidance

Use Spark-family sub-queries at **medium** for bounded discovery tasks:

* locate ownership of a workflow
* map call paths and message types
* collect candidate edit files and tests

Use primary-model sub-queries (medium/high as needed) for:

* ambiguous architectural reasoning
* synthesis across many findings
* risky refactors where subtle regressions are likely

For cross-file implementation work, sub-queries are expected (not optional): run a small pack of focused sub-queries before first major edit.

### 21.7 Minimum Aleph workflow (required when section 21.2 triggers)

1. Load each primary file into its own Aleph context (avoid mixing unrelated files into one context).
2. Run 3+ focused sub-queries (ownership map, invariants map, test/verification map). Delegate bounded discovery packets via sub-query (or sub-agent via sub-query) instead of long manual command loops in the main thread.
3. `peek_context` only the specific ranges needed to confirm semantics and side effects.
4. Post a brief evidence map before editing:
   * candidate files
   * key methods/symbols
   * risk points/invariants
5. After edits, re-run targeted Aleph searches to verify constraints and hotspot behavior remain coherent.

### 21.8 Practical workflow

1. Scout structure first (roots, key projects, protocols, tests).
2. Run narrow sub-queries with explicit scope and expected output.
3. Validate findings in the main thread before editing.
4. Implement changes locally.
5. Run quality gates and update docs for changed behavior/workflows.

### 21.9 Tooling knobs

Aleph supports explicit sub-query controls when needed:

* `--sub-query-model`
* `--sub-query-reasoning-effort`

If omitted, defaults should inherit from the main session model/settings.

### 21.10 Guardrails (quality + anti-overuse)

* Treat sub-query output as evidence, not authority.
* Prefer multiple small scoped queries over one broad query.
* Require concrete references in sub-query outputs (paths, symbols, contracts).
* Keep final merge decisions in the primary thread.
* Do not bulk-load the entire repository when a bounded file set is enough.
* Prefer 2-6 focused queries over recursive/deep pipelines unless complexity clearly demands it.
* Keep Aleph contexts task-scoped; close/ignore stale contexts to avoid contaminated reasoning.
* Do not skip sub-queries for multi-file/cross-layer code-search-and-edit tasks.

### 21.11 Under-use prevention checklist

Before making substantial edits, run this quick gate:

1. Will this likely touch UI + VM + backend/validation/protocol code?
2. Are there invariants/constraints that must hold across multiple creation paths (random/manual/import/repro)?
3. Would a mistake likely create broad regressions or expensive rework?

If any answer is "yes", Aleph should be used first (per 21.2 + 21.7).

Minimum sub-query pack for these cases:

1. file/symbol ownership map
2. invariant/constraint enforcement map across creation paths
3. test surface + regression risk map

Typical NBN examples where Aleph should be considered mandatory:

* Workbench Designer flow changes (layout + graph + editor semantics)
* random brain generation plus manual-editor validation/invariant parity
* protocol or lifecycle changes that span runtime + IO + tooling

When Aleph is used for one of the above, include a short evidence map in progress updates before major edits.

### 21.12 Sub-query starter pack for code-search/edit tasks

Use these prompts as a default pattern (adapt scope/path names per task):

1. "Within `[scope]`, list exact files/symbols owning `[behavior]`; output file list + symbol list + why relevant."
2. "Find where `[invariant/constraint]` is enforced today; output code paths, gaps, and likely missed paths."
3. "Map call path for `[user flow]` from UI to runtime/validation; output ordered path and breakpoints."
4. "List tests touching `[behavior]` and missing cases likely to regress after `[change]`."

Keep each sub-query narrow and evidence-backed; merge conclusions in the main thread before editing.

### 21.13 Sub-query packet pattern (default for scouting)

Use short delegated packets for discovery-heavy work so main-thread context stays small:

1. Issue packet: Beads issue detail + dependency/related issue scan + acceptance clues.
2. Repo scout packet: `rg` ownership sweep + recent `git log/show` around candidate files.
3. Slice packet: `search_context` + `peek_context` over only the symbols/ranges needed.

When these packets are available, prefer them over repeated ad-hoc `rg`/`git`/`Get-Content` loops in the primary thread.

### 21.14 Aleph CLI/MCP reliability notes (Windows + syntax)

Use these defaults to avoid recurring Aleph friction:

1. Increase sub-query timeout for real scouting packets (5 minutes recommended):
   * `configure(sub_query_timeout="300")`
   * Verify with `exec_python("get_config()")` and confirm `sub_query_timeout_seconds` is `300.0`.
2. Aleph MCP tools that accept `output` require one of:
   * `markdown`
   * `json`
   * `object`
   `text` is invalid for these tools.
3. Codex sub-query backend on Windows:
   * Aleph sub-query currently invokes `codex` (not `codex.cmd`).
   * Validate subprocess resolution with:
     * `python -c "import subprocess; print(subprocess.run(['codex','--version']).returncode)"`
   * If that fails while `codex.cmd --version` works, ensure a runnable `codex` executable is on `PATH` (for example a `codex.exe` shim that delegates to `codex.cmd`).

When sub-query packets are required by policy, resolve the above first rather than falling back to long manual command loops.

## Multi-agent coordination and workspace boundaries

To prevent cross-agent collisions in multi-project work:

* When you start work in a specific project or directory under the NBN repo root, create a sentinel file named `.working` in that directory (include your name and start time if helpful).
* Remove the `.working` file when you are finished in that directory.
* You may edit files in another project/directory ONLY if there is no `.working` file present there.
* Include creation/removal of `.working` files in the same commit/push as your related changes. Do not touch other agents' changes in other directories.
* NEVER edit or change files outside the NBN repo root (and its subdirectories). If you believe an external change is required, stop and ask for confirmation first.

---

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create Beads issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase  # If possible / note other gents may be working in other Projects / directories. Consider using auto-stash.
   bd sync
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes YOU created, prune remote branches (other agents may be working in other Projects / directories)
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
- NEVER use git worktrees
