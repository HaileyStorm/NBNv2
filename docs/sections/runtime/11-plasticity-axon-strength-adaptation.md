## 11. Plasticity (axon strength adaptation)

### 11.1 Overview

Plasticity changes axon strengths slowly based on usage, signal scale, and bounded local nudges.

This surface is intentionally approximate. It is not a full training framework.

Plasticity is configurable per brain:

* `plasticity_enabled`
* `plasticity_rate` (small)
* `plasticity_probabilistic_updates` (on/off)
* `plasticity_delta` (small)
* `plasticity_rebase_threshold` / `plasticity_rebase_threshold_pct` (optional; number of changed-axon codes or percent)

Compatibility default:

* if `plasticity_delta` is omitted or `0`, runtime uses `plasticity_rate` as the effective delta

Plasticity changes are applied at runtime in float space and only persisted to `.nbs` when the quantized **strength code** differs from the base `.nbn` strength code.

Sub-quantum changes are allowed and are not preserved across snapshots unless they cross a quantization boundary.

Non-goals:

* no backpropagation
* no layered predictive-coding training architecture
* no centralized training loop

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
* `d = plasticity_delta` (effective delta after compatibility default)

Per axon fired:

* Optional probabilistic gate:

  * `prob = lr * u`
  * deterministic PRNG based on `(brain_seed, tick_id, from_addr32, to_addr32)`
  * apply update only if `rand < prob`
* Update direction:

  * if `sign(p) == sign(strength_value)`: increase magnitude by `d * u`
  * else: decrease magnitude by `d * u`
* Approximate local nudge modulation (bounded, deterministic):

  * local-target only (`target_region == shard_region` and target neuron is local)
  * `predictive_alignment = clamp(p * target_buffer, -1, +1)`
  * `predictive_scale = 1 + 0.35 * predictive_alignment`
  * `source_memory_scale = 1 + 0.15 * abs(source_buffer)`
  * `stabilization_scale = clamp(1 - 0.25 * abs(strength_value), 0.35, 1)`
  * `nudge_scale = clamp(predictive_scale * source_memory_scale * stabilization_scale, 0.4, 1.6)`
  * `effective_delta = d * u * nudge_scale`
* For non-local targets, `nudge_scale = 1` (baseline `d * u` behavior).
* If `strength_value == 0`, update direction is seeded from `sign(p)` so dormant axons can start adapting.
* Clamp to [-1, 1] and re-quantize.

Cadence robustness notes:

* nudges use current local shard state (`source_buffer`, local `target_buffer`) and deterministic tick execution
* behavior does not require per-tick external input; periodic, bursty, and irregular inputs remain valid

### 11.4 Rebasing (optional)

Rebasing creates a new `.nbn` where base strength codes incorporate current overlay codes, then clears overlay. Trigger may be:

* external-world request
* threshold-based automatic policy (configurable)

Automatic threshold policy details:

* evaluate at the end of compute with deterministic shard-local state
* compute `changed_code_count` as the number of axons where runtime code differs from base code
* trigger rebase when either condition is met:
  * `plasticity_rebase_threshold > 0` and `changed_code_count >= plasticity_rebase_threshold`
  * `plasticity_rebase_threshold_pct > 0` and `changed_code_count / total_axons >= plasticity_rebase_threshold_pct` (fraction in `[0, 1]`)
* when triggered, runtime promotes each axon's base code to current runtime code and clears overlay markers

Note: automatic threshold rebasing updates runtime/base-code bookkeeping and overlay state. Persisting that rebased state to a stored `.nbn` artifact still uses the explicit export/rebase flow.

### 11.5 Homeostasis is separate from plasticity

Homeostasis decay is a neuron-buffer policy, not an axon-strength policy:

* homeostasis mutates neuron buffer `B` before pre-activation gating
* plasticity mutates axon strength values after firing logic
* enabling/disabling homeostasis must not implicitly enable/disable plasticity
* enabling/disabling plasticity must not implicitly enable/disable homeostasis

---
