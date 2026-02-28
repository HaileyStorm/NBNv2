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
