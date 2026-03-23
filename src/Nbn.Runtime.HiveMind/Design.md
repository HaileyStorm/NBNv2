# Nbn.Runtime.HiveMind

Owns global tick loop, scheduling barriers, timeout/lateness handling, and brain lifecycle orchestration.

## Stable behavior notes

- `HiveMindActor` placement work is intentionally split by responsibility: request/ack entry flow, placement execution/reconcile orchestration, endpoint-matching helpers, and plan-building/snapshot readers live in separate partial files so placement refactors can stay file-bounded without changing runtime behavior.
- Rescheduling is a real placement transition, not a timer-only delay. HiveMind pauses new ticks, issues placement plan updates, refreshes routing/output-sink state, and resumes only after moved shards re-register for the current placement epoch.
- Actor-driven `UnregisterBrain` from a hosted brain-root/signal-router is ignored while a placement epoch is being replaced or recovered; only stable current-epoch teardown is allowed to remove the tracked brain.
- Reschedule throttles (`reschedule_min_ticks`, `reschedule_min_minutes`) may defer a request, but they do not drop it. Deferred requests are coalesced and retried automatically when the earliest legal window opens.
- Placement scoring prefers low-latency whole-brain locality using inter-worker RTT telemetry. Hosted-brain balancing and free-capacity spread are secondary tie-breakers behind locality and current-worker affinity.
- HiveMind owns the settings-backed worker capability refresh cadence and sends refresh requests to workers, but refreshed values only enter placement after SettingsMonitor persists the next heartbeat snapshot.
- Worker freshness and worker-loss recovery age SettingsMonitor worker snapshots against the SettingsMonitor snapshot clock domain, with local elapsed time only extending the last received snapshot, so mixed-machine placement does not evict healthy workers just because host clocks differ.
- Placement and rebalance treat GPU compute score and VRAM fit as separate constraints, but only weight GPU placement when RegionShard GPU execution is enabled (`NBN_REGIONSHARD_BACKEND` not forced to `cpu`); `auto` backend selection still chooses the stronger scaled backend per shard on the worker rather than hard-coding GPU-first behavior. They still filter workers that are currently over their configured CPU/RAM/storage/VRAM pressure limits and route pressure-triggered rebalances through the same queued reschedule path as other reschedule causes.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
