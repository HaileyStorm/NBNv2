# Nbn.Runtime.HiveMind

Owns global tick loop, scheduling barriers, timeout/lateness handling, and brain lifecycle orchestration.

## Stable behavior notes

- Rescheduling is a real placement transition, not a timer-only delay. HiveMind pauses new ticks, issues placement plan updates, refreshes routing/output-sink state, and resumes only after moved shards re-register for the current placement epoch.
- Reschedule throttles (`reschedule_min_ticks`, `reschedule_min_minutes`) may defer a request, but they do not drop it. Deferred requests are coalesced and retried automatically when the earliest legal window opens.
- Placement scoring prefers low-latency whole-brain locality using inter-worker RTT telemetry. Hosted-brain balancing and free-capacity spread are secondary tie-breakers behind locality and current-worker affinity.
- HiveMind owns the settings-backed worker capability refresh cadence and sends refresh requests to workers, but refreshed values only enter placement after SettingsMonitor persists the next heartbeat snapshot.
- Worker freshness and worker-loss recovery age SettingsMonitor worker snapshots against the SettingsMonitor snapshot clock domain, with local elapsed time only extending the last received snapshot, so mixed-machine placement does not evict healthy workers just because host clocks differ.
- Placement and rebalance treat GPU compute score and VRAM fit as separate constraints, filter workers that are currently over their configured CPU/RAM/storage/VRAM pressure limits, and route pressure-triggered rebalances through the same queued reschedule path as other reschedule causes.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
