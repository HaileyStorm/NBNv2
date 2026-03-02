# Nbn.Tools.EvolutionSim

Owns standalone reproduction/speciation stress simulation runs driven from artifact parents (without introducing new core runtime services).

## Stable behavior boundaries

* Artifact-first flow by default (`spawn_children=false` unless explicitly enabled).
* Inverse-compatibility run-count policy with deterministic seeds and hard cap compliance (`run_count` remains within runtime-supported bounds).
* Optional speciation commit after child creation (enabled by default, disable explicitly): commit uses `brain_id` when available; artifact-only candidates are treated as expected no-op because current speciation commit contract requires `brain_id`.
* Session control surface is tool-local (`start`, `stop`, `status`) and designed for later Workbench pane integration.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
