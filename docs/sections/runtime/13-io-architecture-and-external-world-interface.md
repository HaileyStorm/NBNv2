## 13. I/O architecture and External World interface

### 13.1 External World interface principles

* External World interacts with NBN via **Proto.Actor remoting** using NBN `.proto` messages.
* External World does not need to know tick IDs to write inputs or receive outputs.
* Inputs are applied on the next tick automatically by the IO coordinators and the tick delivery phase.
* Outputs are delivered with tick correlation, but External World is not required to use it.
* External World may query placement-ready worker capacity through IO when it needs runtime-sizing hints for environment orchestration; the returned snapshot reflects HiveMind's placement-eligible worker view rather than raw SettingsMonitor rows and includes exclusion counts/reasons for workers filtered out of spawn planning.
* External World may explicitly terminate a running brain through IO; it does not need a separate HiveMind control-plane connection for ordinary brain-lifecycle teardown.
* External World may switch the continuous vector source between `potential` and `buffer` through IO. Requests that include `brain_id` apply only to that Brain; requests without `brain_id` update the runtime default for future and non-overridden Brains. Sparse `OutputEvent` subscriptions remain a separate transport choice.

### 13.2 IO Gateway and per-brain coordinators

**IO Gateway** is a well-known actor name registered in SettingsMonitor. It:

* accepts client connections, subscriptions, and control messages
* forwards brain-level requests to HiveMind and/or ReproductionManager
* forwards Speciation and PPO manager requests through SettingsMonitor-discovered endpoints, refreshing stale service roots before returning stable unavailable/failure responses
* forwards placement-ready worker capacity queries to HiveMind and returns the placement-filtered worker inventory plus exclusion diagnostics through the IO contract
* spawns per-brain `InputCoordinator` and `OutputCoordinator` actors

Brain lifecycle control carries backpressure pause metadata:

* `SpawnBrain.pause_priority` sets the Brain's configured ranking for `lowest-priority` backpressure pausing
* direct controller `RegisterBrain.pause_priority` can override or supply the same value for already-running Brains
* when omitted, `pause_priority` defaults to `0`

**Placement:** Coordinators may be placed on any worker process(es). IO Gateway maintains their location and routes to them transparently.
Registration/refresh metadata includes the current input/output coordinator PID labels, ownership flags, and the input coordinator mode plus tick-drain armed state so IO Gateway can route directly to worker-hosted coordinators, preserve live state across coordinator moves, and let routers suppress idle `dirty_on_change` drain round-trips without changing `replay_latest_vector` behavior.

### 13.3 Input mapping

External World writes to `input_index i`, mapped to:

* `region_id = 0`
* `neuron_id = i`

Input width equals the number of neurons in region 0.

InputCoordinator supports two runtime modes:

* `dirty_on_change` (default): buffers writes between ticks and emits only changed inputs as contributions during tick delivery (visible on next tick compute).
* `replay_latest_vector`: stores the latest full input vector and emits all indices every tick during delivery, even when values did not change.

When multiple input writes arrive within one tick window, the most recent value for each index is the value emitted at the next tick boundary.

External World may also send a vector of values for all inputs (length must == input width).
Single-value and vector writes must contain finite real numbers; `NaN` and infinities are rejected by IO runtime validation.
When `InputWrite` or `InputVector` are sent as request-style actor messages through IO Gateway, the caller receives `IoCommandAck` only after the per-brain input coordinator accepts the write and the brain router has observed the input for the next tick-drain decision. Fire-and-forget sends remain valid when the caller does not need that ordering guarantee.

Direct runtime state writes are also available for tooling and deterministic evaluation scenarios:

* `RuntimeNeuronPulse` injects one contribution into a specific neuron.
* `RuntimeNeuronStateWrite` sets one neuron's current buffer and/or accumulator state.
* `ResetBrainRuntimeState` clears transient neuron buffer and/or accumulator state across the entire Brain without changing the `.nbn` definition or runtime feature toggles such as cost/energy, plasticity, or homeostasis. HiveMind queues the reset and executes it at the per-brain deliver-to-compute barrier so reset cannot interleave with in-flight delivery for that Brain. The reset also clears pending output publication state for that Brain and rejects any late single/vector outputs from ticks older than the post-reset tick floor.
* `SynchronizeBrainRuntimeConfig` forces the current per-brain runtime config snapshot to be re-applied to every registered shard and returns only after all shard actors acknowledge the update. It is intentionally scoped to paused Brains and should be used as a pre-resume barrier after config writes when deterministic evaluation requires plasticity/homeostasis/output-mode settings to be active on every shard before compute resumes.

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

Output subscriptions default to exact delivery. Callers that prefer bounded-memory pressure behavior over per-tick completeness can opt into `OUTPUT_SUBSCRIPTION_DELIVERY_MODE_LATEST_ONLY`; the output coordinator then coalesces pending delivery for that subscriber and emits only the latest pending single/vector event for each flush window. Subscriber termination also prunes coordinator fan-out state and IO Gateway replay state so disconnected clients are not replayed after coordinator moves.

Output vector source is runtime-selectable:

* `potential` (default): vector samples activation potential semantics (existing behavior).
* `buffer`: vector samples each output neuron's current persistent buffer value each tick, without requiring a fire event.

IO selection scope is explicit:

* `SetOutputVectorSource.brain_id` present: override only that Brain's vector source.
* `SetOutputVectorSource.brain_id` omitted: update the runtime default used by newly spawned Brains and any existing Brains that have not received a per-brain override.

External World may subscribe, per Brain, to individual and/or vector outputs.

### 13.5 Energy controls

IO supports:

* one-time energy credit
* energy rate
* per-brain cost+energy override (`cost_enabled` + `energy_enabled`, paired)
* system cost+energy master key (`cost_energy.system.enabled`) from SettingsMonitor, combined with per-brain runtime setting (`effective = system && brain`)
* plasticity control (`enabled`, `rate`, `probabilistic_updates`, `delta`, `rebase_threshold`, `rebase_threshold_pct`, optional energy/cost modulation bounds)
* system plasticity master key (`plasticity.system.enabled`) from SettingsMonitor, combined with per-brain `enabled` at runtime (`effective = system && brain`)
* system plasticity default mode/rate keys in SettingsMonitor (`plasticity.system.probabilistic_updates`, `plasticity.system.rate`) for operator sync surfaces (Workbench Energy + Plasticity and Orchestrator Settings)
* homeostasis control (`enabled`, target/update modes, base probability, min-step codes, optional energy coupling scales)
* PPO optional core service status/start/stop/reward-feedback forwarding; IO discovers the PPO manager through `service.endpoint.ppo_manager` and clients do not need a direct PPO endpoint

Command writes can be sent as requests and return `IoCommandAck` with:

* command name
* success/failure
* reason text
* optional runtime `BrainEnergyState` snapshot for immediate operator feedback
* for `set_plasticity`, ACK also carries:
  * configured requested enablement (`configured_plasticity_enabled`)
  * current authoritative effective enablement snapshot (`effective_plasticity_enabled`)

Authoritative semantics:

* when HiveMind is available, `set_plasticity` ACK reports accepted configured intent plus the current authoritative effective state from IO runtime snapshot
* effective state is authoritative only after HiveMind re-register/runtime-config reconciliation (for example, system plasticity master off still yields effective false)
* when HiveMind is unavailable, IO applies locally and ACK effective/configured values match local state

Homeostasis operator ranges:

* `homeostasis_base_probability`: `[0,1]`
* `homeostasis_min_step_codes`: `>= 1`
* `homeostasis_energy_target_scale`: `[0,4]`
* `homeostasis_energy_probability_scale`: `[0,4]`

Plasticity operator ranges:

* `plasticity_rate`: `>= 0`
* `plasticity_delta`: `>= 0` (if omitted/`0`, runtime uses `plasticity_rate`)
* `plasticity_rebase_threshold`: `>= 0` (0 disables count trigger)
* `plasticity_rebase_threshold_pct`: `[0,1]` (0 disables percent trigger)
* `plasticity_energy_cost_reference_tick_cost`: `> 0` (when modulation enabled)
* `plasticity_energy_cost_response_strength`: `[0,8]`
* `plasticity_energy_cost_min_scale`: `[0,1]`
* `plasticity_energy_cost_max_scale`: `[0,1]` and `>= min_scale`

### 13.6 Direct brain/runtime reward-control contract

Direct brain/runtime reward-control is the NBN-owned path for external reward feedback that modulates a live Brain. It is distinct from the optional PPO optimizer described in Section 3.2 and Section 14: reproduction-action PPO samples candidate artifacts, tunes enabled reproduction mutation probabilities, and commits lineage through Speciation; direct reward-control targets an already-running Brain through the IO runtime-control boundary.

The contract is intentionally narrower than "arbitrary training":

* reward samples must identify the target `brain_id`, controller identity, objective/reward signal, observation tick or explicit observation fence, control action id, and target control surface
* rewards and action parameters must be finite, bounded, and validated before they reach HiveMind or shard actors
* stale, duplicate, or mismatched rewards must be rejected deterministically instead of updating controller state twice
* the implemented initial surface is `plasticity_rate`, accepted only while the target Brain is paused and bounded to `[0,1]`; broader surfaces such as bounded neuron state writes/pulses, homeostasis settings, cost/energy knobs, output/source mode, and future neuromodulation controls require explicit ranges, ownership, timing, and persistence semantics before exposure
* `.nbn` structure, neuron counts, axon topology, function IDs, and artifact lineage remain outside direct reward-control; those changes go through Reproduction/import/export contracts
* IO-region invariants still apply: direct control may write runtime values for input/output neurons through supported paths, but it may not create illegal region `0` targets, output-to-output axons, duplicate axons, or hidden IO neuron count edits

Mutation timing is part of the safety model:

* controls that affect compute-visible state must be paused-only or queued to a HiveMind-owned deliver-to-compute barrier
* tick `N` reward/action feedback must not retroactively affect tick `N` compute or delivery
* output subscriptions are not sufficient observation fences for policy learning unless the subscription contract explicitly states post-deliver ordering for the sampled tick
* live snapshots used for reward observations must report the completed tick boundary they represent
* when a control enables runtime features unsupported by the active compute backend, RegionHost must use the existing explicit fallback/unsupported-feature path rather than silently diverging across CPU/GPU execution

Direct reward-control and reproduction-action PPO may run in the same deployment only when ownership is explicit. Reproduction-action PPO owns artifact candidate mutation and reward feedback about child artifacts; direct reward-control owns live runtime modulation for a specific Brain. If both are enabled for related brains, the caller must keep reward signals, controller ids, and action provenance separate so one policy cannot consume the other's feedback samples.

The current proto/API surface is `ApplyDirectRuntimeRewardControl` through IO with HiveMind as the authority. Requests carry `controller_id`, `action_id`, `objective_name`, `reward_signal`, observation/action ticks, target surface, finite reward, and finite control value. HiveMind rejects non-IO senders, active-brain actions for the initial paused-only surface, stale or duplicate provenance, unsupported surfaces, non-finite values, out-of-range controls, and action ticks other than the next visible tick using stable reason codes. Accepted actions report `applied_tick_floor`; for the paused-only surface, shard runtime config is updated immediately before resume is allowed. The reward-control ledger is bounded and ephemeral; it is not serialized into `.nbs`.

### 13.7 Brain death notifications

When a brain terminates, IO publishes:

* BrainId
* reason
* base `.nbn` artifact ref
* last `.nbs` snapshot artifact ref (if present)
* last energy balance and last tick cost totals

---
