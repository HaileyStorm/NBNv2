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
* CUDA accelerators are preferred over OpenCL when both are available

Kernel compilation:

* kernels are compiled lazily per function ID used in active shards
* kernel caching is per accelerator instance

Runtime selection and fallback:

* `NBN_REGIONSHARD_BACKEND=auto|cpu|gpu` selects the preferred backend
* `auto` chooses the stronger backend for the shard on that worker using the scaled capability scores and shard size gate; `gpu` and `cpu` remain explicit overrides
* the current ILGPU fast path preserves parity for the supported deterministic shard shape: no visualization, no plasticity/runtime axon mutation, supported activation/reset function IDs, and host-visible state synchronized before `TickComputeDone`
* unsupported function IDs or runtime features fall back explicitly to CPU for that shard compute rather than changing simulation semantics

### 10.3 Function tiers

Each function ID has:

* a tier (A/B/C)
* a relative compute cost weight

Tier guidance:

* Tier A: cheap, GPU-friendly (linear, clamp, relu, add/mult, etc.)
* Tier B: moderate (tanh, sigmoid, exp/log variants)
* Tier C: expensive or numerically tricky (pow, gauss, quad)

Placement may weight shards toward GPU-capable nodes when GPU execution is enabled and a worker reports better effective GPU capability plus VRAM fit, but `auto` execution still remains capability-first rather than blindly GPU-first.

---
