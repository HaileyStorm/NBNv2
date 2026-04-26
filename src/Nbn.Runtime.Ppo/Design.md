# Nbn.Runtime.Ppo

Owns the optional core PPO optimization service. PPO depends on IO for live-generated parent snapshot artifacts, Reproduction for candidate synthesis/assessment, and Speciation for lineage tracking/admission commits.

## Stable responsibilities

- `PpoManagerActor` owns PPO run lifecycle state and dependency readiness for the IO Gateway, Reproduction manager, and Speciation manager endpoints.
- PPO is a service-level optimizer surface. It does not execute inside RegionHost shard compute, mutate live shard state, or participate in HiveMind tick barriers.
- Run admission requires IO, Reproduction, and Speciation to be discoverable, either through explicit process options or SettingsMonitor service endpoint discovery.
- PPO rollout execution observes parent brains through IO `BrainInfoRequest` plus `RequestSnapshot` responses that are marked `generated_from_live_state`, then submits `ReproduceByArtifactsRequest` to Reproduction with `SpawnChildNever` and IO-region count protection enabled.
- PPO commits candidate artifact lineage through Speciation `BatchEvaluateApply` in commit mode. PPO writes provenance metadata, but Reproduction remains authoritative for synthesis/assessment and Speciation remains authoritative for species membership.
- PPO does not use output subscriptions as post-deliver-safe trajectory observations until IO exposes an explicit post-deliver observation fence.
- Endpoint discovery watches `service.endpoint.io_gateway`, `service.endpoint.reproduction_manager`, and `service.endpoint.speciation_manager`; invalid or removed observations fall back to explicit CLI endpoint hints if present.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
