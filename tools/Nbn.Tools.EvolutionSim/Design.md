# Nbn.Tools.EvolutionSim

Owns standalone reproduction/speciation stress simulation runs driven from artifact parents (without introducing new core runtime services).

## Stable behavior boundaries

* Artifact-first flow by default (`spawn_children=false` unless explicitly enabled).
* Parent pool preflight requires resolvable artifact store locations; `store_uri` comes from each parent entry or falls back to `--store-uri` / `NBN_ARTIFACT_ROOT`.
* Inverse-compatibility run-count policy with deterministic seeds and hard cap compliance (`run_count` remains within runtime-supported bounds).
* Optional speciation commit after child creation (enabled by default, disable explicitly): commit accepts `brain_id`, artifact-ref, or artifact-URI candidates; artifact candidates commit using deterministic identity derivation without requiring spawn.
* Parent pool mode supports artifact refs (default, with child-artifact pool growth) and `brain_id` parents (for runs that select live parents without artifact parent lists).
* Session control surface is tool-local (`start`, `stop`, `status`) and designed for later Workbench pane integration.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
