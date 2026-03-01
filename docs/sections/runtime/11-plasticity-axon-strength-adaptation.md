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

### 11.5 Homeostasis is separate from plasticity

Homeostasis decay is a neuron-buffer policy, not an axon-strength policy:

* homeostasis mutates neuron buffer `B` before pre-activation gating
* plasticity mutates axon strength values after firing logic
* enabling/disabling homeostasis must not implicitly enable/disable plasticity
* enabling/disabling plasticity must not implicitly enable/disable homeostasis

---
