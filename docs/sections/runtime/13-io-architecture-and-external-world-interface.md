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
For a running brain, this width is fixed from registration/definition metadata and is not mutated by observed output events.

Output events are emitted per tick and delivered to subscribed clients.

* **OutputEvent (single):** emitted when an output neuron fires (abs(potential) > ActivationThreshold).
* **OutputVectorEvent (vector):** emitted by IO as one full brain-level vector per tick with deterministic ordering by `output_index` (`0..output_width-1`). For sharded output regions, shard-local vectors are merged by absolute output-index ranges before publication. Invalid vector payloads (width/range/overlap/late-tick violations) are rejected and surfaced via deterministic IO telemetry/debug paths.

External World may subscribe, per Brain, to individual and/or vector outputs.

### 13.5 Energy controls

IO supports:

* one-time energy credit
* energy rate
* enable/disable cost and energy

Command writes can be sent as requests and return `IoCommandAck` with:

* command name
* success/failure
* reason text
* optional runtime `BrainEnergyState` snapshot for immediate operator feedback

### 13.6 Brain death notifications

When a brain terminates, IO publishes:

* BrainId
* reason
* base `.nbn` artifact ref
* last `.nbs` snapshot artifact ref (if present)
* last energy balance and last tick cost totals

---
