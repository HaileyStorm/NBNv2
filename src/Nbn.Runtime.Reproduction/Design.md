# Nbn.Runtime.Reproduction

Owns compatibility gates, child synthesis, mutation summaries, spawn policy behavior, compatibility-only assessment requests, and fixed-count multi-run orchestration.

## Stable responsibilities

- Function mutation keeps full ID-space compatibility (no hard bans), while biasing activation/reset/accumulation mutation targets toward lower-volatility families so child brains trend toward stable buffer ranges over generations.
- Compatibility assessment requests run the same similarity gates as reproduction but do not synthesize child artifacts or attempt spawn.
- `run_count` is normalized per request (`0` => `1`, bounded max) and response `runs` are emitted in deterministic `run_index` order while top-level result fields mirror run `0` for compatibility with legacy callers.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
