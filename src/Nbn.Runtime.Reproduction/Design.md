# Nbn.Runtime.Reproduction

Owns compatibility gates, child synthesis, mutation summaries, spawn policy behavior, compatibility-only assessment requests, and fixed-count multi-run orchestration.

## Stable responsibilities

- Function mutation keeps full ID-space compatibility (no hard bans), while biasing activation/reset/accumulation mutation targets toward lower-volatility families so child brains trend toward stable buffer ranges over generations.
- Compatibility assessment requests run the same similarity gates as reproduction but do not synthesize child artifacts or attempt spawn. Runtime handles reproduction and assessment requests re-entrantly so assessment-only speciation checks do not starve behind unrelated reproduction work in the same actor mailbox, and repeated artifact-based assessments reuse a bounded parsed-parent cache keyed by artifact SHA so newborn-species exemplar checks do not keep reparsing the same artifacts.
- `run_count` is normalized per request (`0` => `1`, bounded max) and response `runs` are emitted in deterministic `run_index` order while top-level result fields mirror run `0` for compatibility with legacy callers.
- Successful reproduction reports carry both parent-pair gate similarity (`similarity_score`) and child-lineage similarity (`lineage_similarity_score` plus per-parent lineage scores) so downstream speciation can classify from child-vs-lineage evidence instead of parent-vs-parent gate similarity.
- Artifact-based parent/state loads and child exports honor non-file `store_uri` values through the shared artifact resolver, including built-in HTTP(S) artifact services. Parent definition loads still require full SHA/media-type validation, child exports preserve the resolved logical `store_uri`, remote live-state overlay reads remain best-effort, and store/load failures continue to surface as concrete `*_artifact_store_unavailable`, `*_artifact_not_found`, or `*_parse_failed` outcomes rather than compatibility-only results.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
