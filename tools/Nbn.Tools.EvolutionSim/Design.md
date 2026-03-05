# Nbn.Tools.EvolutionSim

Owns standalone reproduction/speciation stress simulation runs driven from artifact parents (without introducing new core runtime services).

## Stable behavior boundaries

* Artifact-first flow by default (`spawn_children=false` unless explicitly enabled).
* Parent pool preflight requires resolvable artifact store locations; `store_uri` comes from each parent entry or falls back to `--store-uri` / `NBN_ARTIFACT_ROOT`.
* Inverse-compatibility run-count policy with deterministic seeds and hard cap compliance (`run_count` remains within runtime-supported bounds).
* Optional speciation commit after child creation (enabled by default, disable explicitly): commit accepts `brain_id`, artifact-ref, or artifact-URI candidates; artifact candidates commit using deterministic identity derivation without requiring spawn.
* Speciation commit metadata prefers lineage-aware child similarity from reproduction (`lineage_similarity_score`) and keeps legacy score fallbacks so assignment decisions track child-vs-parent lineage evidence instead of only parent gate compatibility.
* When speciation commit is enabled, simulator runs seed each unique initial parent into the current epoch before iteration commits so lineage-aware assignment has parent membership evidence even when spawn is disabled.
* Parent pool mode supports artifact refs (default, with child-artifact pool growth) and `brain_id` parents (for runs that select live parents without artifact parent lists).
* Parent pool growth remains evolutionary at capacity: once `max_parent_pool_size` is reached, simulator replaces non-seed parents with new unique children instead of freezing pool composition.
* Simulator applies configurable deterministic plateau pressure via `run_pressure_mode`: `stability` (default) reduces run pressure when commit-similarity minima plateau, `divergence` applies the opposite exploratory nudge, and `neutral` disables adaptive nudging.
* Parent pair sampling supports `parent_selection_bias` (`divergence|neutral|stability`): divergence weights selection toward newer pool entries, stability weights toward older entries, and neutral keeps uniform random parent selection.
* Reproduction request config is full-profile (not spawn-only): simulator resolves `repro.config.*` settings from SettingsMonitor when `--settings-address` is provided and otherwise falls back to shared defaults aligned with Workbench Reproduction defaults.
* Status payload includes pool-growth and mutation observability counters (`parent_pool_size`, `children_added_to_pool`, run/mutation totals), plus source-separated similarity telemetry: overall observed range, compatibility-assessment range, reproduction-report range, and successful-speciation-commit range. This keeps simulator telemetry comparable with Speciation history metrics without conflating score sources.
* Session control surface is tool-local (`start`, `stop`, `status`) and is consumed by Workbench Speciation pane simulator controls.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
