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

Whole-brain runtime-state resets use this same barrier. HiveMind queues the request and only asks IO/Brain runtime surfaces to apply it once that Brain has completed `TickDeliverDone(tick_id)` for the current tick and before `TickCompute(tick_id+1)` is allowed to start.

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
`HiveMindStatus` reports both the configured/control target cadence and the current effective target, flags when automatic backpressure reduction is active, and exposes recent timeout/lateness window counts so operator surfaces can show current tick health without parsing logs.

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

HiveMind configures the selector with:

* `NBN_HIVE_PAUSE_STRATEGY` / `--pause-strategy`
* `NBN_HIVE_PAUSE_ORDER` / `--pause-order` when strategy = `external-order`

Current implementation detail:

* each backpressure pause decision pauses the first eligible Brain selected by the configured ordering
* repeated timeout streaks continue applying the same ordering and can pause additional Brains over time
* `lowest configured priority value` uses `pause_priority` from `SpawnBrain` or direct `RegisterBrain` control messages (default `0`)
* HiveMind status also exposes current over-quota worker count plus recent worker-pressure counts across the configured pressure-rebalance window so operator tooling can distinguish immediate worker saturation from recent-but-recovered pressure.

### 6.6 Rescheduling rate limits and tick pausing

Rescheduling and full recovery are disruptive. To prevent thrashing:

* Rescheduling is rate-limited:

  * at most once every `reschedule_min_ticks`, and/or
  * at most once every `reschedule_min_minutes`
  * if a reschedule/recovery is triggered too soon, it is queued instead of being dropped
  * queued requests are retried automatically at the earliest tick/time window allowed by the current throttles; duplicate requests may be coalesced into one pending retry

When a reschedule/recovery is initiated:

* HiveMind completes the current tick (compute+deliver) or times out that tick.
* HiveMind then **pauses new tick dispatch**.
* HiveMind waits a small stabilization interval (`reschedule_quiet_ms`) after the tick completion decision.
* HiveMind performs real rescheduling and/or recovery work: assignment/unassignment, routing snapshot refresh, and placement reconciliation.
* For shard moves, HiveMind updates shard output sinks and waits for the moved shards to re-register for the new placement epoch before considering the reschedule complete.
* HiveMind resumes tick dispatch only after the new routing/output-sink state is active for the current placement epoch.

---
