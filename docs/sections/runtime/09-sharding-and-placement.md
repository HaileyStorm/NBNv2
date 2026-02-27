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
