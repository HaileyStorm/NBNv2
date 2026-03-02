# Nbn.Runtime.Speciation

Owns the runtime speciation control plane: taxonomy epochs, species membership persistence, lineage edges, and startup reconciliation for known brains.

## Stable responsibilities

- Single-writer `SpeciationManagerActor` serializes membership/epoch decisions.
- SQLite persistence keeps immutable membership decisions within an epoch and retains historical epochs after reset.
- Startup loads persisted epoch state, then reconciles missing memberships for currently registered brains from SettingsMonitor.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
