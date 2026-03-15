## 9. Sharding and placement

### 9.1 Region sections and stride

The `.nbn` file stores regions as independent sections. Each region section includes an **axon offset checkpoint table** with a fixed **stride** (default 1024).

Shard alignment rules:

* RegionShards cover contiguous neuron ID ranges within one region.
* Shard boundaries align to the stride:

  * `neuron_start % stride == 0`
  * `neuron_count % stride == 0`, except the final shard in a region may be shorter to cover `[last_stride_boundary, neuron_span)`

RegionHost may materialize a shard from a selective `.nbn` region fetch when the artifact manifest exposes region-index metadata, but placement ownership and epoch activation still remain with HiveMind.

### 9.2 Node capabilities and placement telemetry

Worker processes report capabilities periodically to SettingsMonitor:

* CPU core count
* free and total RAM
* free and total storage at the worker artifact/runtime root
* GPU presence plus free and total VRAM (if any)
* microbenchmark scores (CPU and GPU)
* ILGPU accelerator availability (CUDA/OpenCL/CPU)
* explicit worker limit percentages for CPU, RAM, storage, GPU compute, and GPU VRAM
* current worker load/pressure snapshots used for placement filtering and pressure-triggered rebalance (`process_cpu_load_percent`, process RAM use, and derived storage/VRAM pressure from free vs total bytes)

For workers, these values are expected to come from real host probing or explicit operator configuration, not placeholder defaults. CPU scores can be derived from a representative RegionShard CPU microbenchmark; GPU score/report rows may exist before runtime GPU execution is enabled, but GPU runtime benchmark scenarios must skip cleanly until the RegionShard GPU backend is available. HiveMind owns the settings-backed worker capability rerun cadence (`worker.capability.benchmark_refresh_seconds`) by sending refresh requests to workers; workers publish the refreshed result back through the normal SettingsMonitor heartbeat path instead of bypassing the stored snapshot.

HiveMind also maintains placement telemetry in worker inventory snapshots:

* worker-to-worker average peer latency, gathered through targeted RTT probes between workers
* peer latency sample counts, so scheduling can distinguish measured locality from unknown paths

Placement decisions use inter-worker latency as the primary locality signal for shard-to-shard cost. Worker-to-HiveMind latency remains useful for liveness/health checks, but it is not sufficient on its own to score distributed shard placement.

### 9.3 Placement and rescheduling coordinator

HiveMind runs a `ShardPlacementManager` Actor which:

* decides initial placement of RegionShards and per-brain IO coordinators
* migrates shards on repeated lateness/timeouts
* splits/merges shards as needed
* updates all routing tables (BrainSignalRouter and coordinators)
* refreshes inter-worker peer latency telemetry before latency-sensitive placement/reschedule decisions
* executes real placement plan changes rather than a simulated reschedule delay
* resumes tick dispatch only after moved shards have registered for the current placement epoch and their output sinks/routing state have been refreshed
* respects rescheduling rate limits (Section 6.6)

Placement heuristics:

* prefer keeping a single brain inside the lowest-latency locality available (same machine first, then same low-latency network segment) before splitting it across slower links
* prefer distributing different brains across workers before splitting one brain across higher-latency worker boundaries
* accept temporary free-resource imbalance when needed to preserve lower-latency locality for a brain
* use average inter-worker RTT as the main placement signal; hosted-brain balancing and free-capacity spread are secondary tie-breakers
* still fall back to higher-latency or cross-device/network placement when the lower-latency worker subset cannot satisfy the required shard plan
* co-locate shards with heavy mutual traffic
* prefer GPU-capable nodes for Tier A/B-heavy function mixes, but treat GPU compute score and VRAM fit as separate constraints
* reject workers whose reported pressure repeatedly exceeds their configured CPU/RAM/storage/VRAM limits, then use the normal queued reschedule path for pressure-triggered rebalance
* avoid shard sizes that exceed memory limits

---
