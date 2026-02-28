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
