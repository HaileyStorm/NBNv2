# Nbn.Runtime.HiveMind

Owns global tick loop, scheduling barriers, timeout/lateness handling, and brain lifecycle orchestration.

## Stable behavior notes

- `HiveMindActor` shell/state, control-plane registration/authorization/brain-control, settings application/parsing/status, worker inventory/visualization/peer-latency, placement, and tick/reschedule work are intentionally split by responsibility so HiveMind refactors can stay file-bounded without changing runtime behavior.
- `HiveMindActor.ReceiveAsync` is intentionally a thin dispatcher. Message routing stays grouped by responsibility slice so control-plane, artifact, placement, settings, worker-inventory, runtime-surface, and tick/reschedule changes can be reviewed independently without changing behavior.
- Spawn admission and spawn completion are separate control-plane contracts: `SpawnBrain` returns once placement is accepted into the runtime queue with a stable `brain_id`, while `AwaitSpawnPlacement` reports the later placed/ready outcome for that same brain and preserves explicit failure reasons if placement or reconcile fails.
- Placement assignment dispatch is serialized per worker node inside HiveMind at the brain-batch level. HiveMind can dispatch a whole brain's current assignments to one worker together, but it only lets one brain at a time consume assignment-timeout budget on that worker, so bursty same-worker spawn requests queue instead of failing just because earlier brains were still ahead in the worker mailbox.
- Default spawn-placement wait budgets account for queued worker batches instead of assuming isolated placement. `AwaitSpawnPlacement` without an explicit timeout therefore scales with the current serial-placement window on the targeted workers.
- Rescheduling is a real placement transition, not a timer-only delay. HiveMind pauses new ticks, issues placement plan updates, refreshes routing/output-sink state, and resumes only after moved shards re-register for the current placement epoch.
- Actor-driven `UnregisterBrain` from a hosted brain-root/signal-router is ignored while a placement epoch is being replaced or recovered; only stable current-epoch teardown is allowed to remove the tracked brain.
- Reschedule throttles (`reschedule_min_ticks`, `reschedule_min_minutes`) may defer a request, but they do not drop it. Deferred requests are coalesced and retried automatically when the earliest legal window opens.
- Placement scoring prefers low-latency whole-brain locality using inter-worker RTT telemetry. Hosted-brain balancing and free-capacity spread are secondary tie-breakers behind locality and current-worker affinity.
- HiveMind owns the settings-backed worker capability refresh cadence and sends refresh requests to workers, but refreshed values only enter placement after SettingsMonitor persists the next heartbeat snapshot.
- HiveMind also owns the settings-backed RegionShard auto-GPU cutoff (`worker.runtime.region_shard_gpu_neuron_threshold`) and stamps the resolved per-worker threshold onto placement assignments so WorkerNode can honor the same runtime policy without its own SettingsMonitor subscription. The setting accepts `auto` or an explicit neuron count; `auto` scales the measured baseline cutoff (`262144` neurons at GPU score `550.258`) against each worker's effective GPU score.
- Worker freshness and worker-loss recovery age SettingsMonitor worker snapshots against the SettingsMonitor snapshot clock domain, with local elapsed time only extending the last received snapshot, so mixed-machine placement does not evict healthy workers just because host clocks differ.
- Placement and rebalance treat GPU compute score and VRAM fit as separate constraints, but only weight GPU placement when RegionShard GPU execution is enabled (`NBN_REGIONSHARD_BACKEND` not forced to `cpu`); `auto` backend selection still chooses the stronger scaled backend per shard on the worker rather than hard-coding GPU-first behavior. They still filter workers that are currently over their configured CPU/RAM/storage/VRAM pressure limits and route pressure-triggered rebalances through the same queued reschedule path as other reschedule causes.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
