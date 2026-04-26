# Nbn.Runtime.Ppo

Owns the optional core PPO optimization service. PPO depends on Reproduction for candidate synthesis/assessment and Speciation for lineage tracking/admission context.

## Stable responsibilities

- `PpoManagerActor` owns PPO run lifecycle state and dependency readiness for the Reproduction and Speciation manager endpoints.
- PPO is a service-level optimizer surface. It does not execute inside RegionHost shard compute, mutate live shard state, or participate in HiveMind tick barriers.
- Run admission requires both Reproduction and Speciation to be discoverable, either through explicit process options or SettingsMonitor service endpoint discovery.
- The initial PPO contract is intentionally limited to status, dependency reporting, and run start/stop control with PPO hyperparameter validation. Reproduction/Speciation remain authoritative for child synthesis, compatibility, and species membership.
- Endpoint discovery watches `service.endpoint.reproduction_manager` and `service.endpoint.speciation_manager`; invalid or removed observations fall back to explicit CLI endpoint hints if present.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
