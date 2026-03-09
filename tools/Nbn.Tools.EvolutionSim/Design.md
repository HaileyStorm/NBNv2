# Nbn.Tools.EvolutionSim

Owns standalone reproduction/speciation stress simulation runs driven from artifact parents (without introducing new core runtime services).

## Stable behavior boundaries

* Artifact-first flow by default (`spawn_children=false` unless explicitly enabled).
* Parent pool preflight requires resolvable artifact store locations; `store_uri` comes from each parent entry or falls back to `--store-uri` / `NBN_ARTIFACT_ROOT`.
* Inverse-compatibility run-count policy with deterministic seeds and hard cap compliance (`run_count` remains within runtime-supported bounds).
* Optional speciation commit after child creation (enabled by default, disable explicitly): commit accepts `brain_id`, artifact-ref, or artifact-URI candidates; artifact candidates commit using deterministic identity derivation without requiring spawn.
* Speciation commit metadata prefers lineage-aware child similarity from reproduction (`lineage_similarity_score`) and keeps legacy score fallbacks so assignment decisions track child-vs-parent lineage evidence instead of only parent gate compatibility.
* When speciation commit is enabled, simulator seeds each unique initial parent into the current epoch before iteration commits, using an assessment-only founder-pair similarity against another selected seed parent when available so distinct founders do not collapse into one species just because the first seed started the epoch. This preserves lineage-aware parent membership evidence even when spawn is disabled.
* Parent pool mode supports artifact refs (default, with child-artifact pool growth) and `brain_id` parents (for runs that select live parents without artifact parent lists).
* Parent pool growth remains evolutionary at capacity: once `max_parent_pool_size` is reached, simulator still turns over non-seed parents, but successful committed children now displace the most overrepresented evictable lineage family first (then the most overrepresented species within that family) and may be skipped entirely when they would only crowd out a rarer lineage family already represented in the pool.
* Simulator applies configurable deterministic plateau pressure via `run_pressure_mode`: `stability` (default) reduces run pressure when commit-similarity minima plateau, `divergence` applies the opposite exploratory nudge, and `neutral` disables adaptive nudging.
* Parent pair sampling supports `parent_selection_bias` (`divergence|neutral|stability`): divergence weights selection toward newer lineages, stability weights toward older lineages, and neutral keeps uniform random parent selection. When multiple tracked lineage families are present, the non-neutral modes bias by lineage-family age and normalize per-parent weight by current lineage-family representation so one repeatedly splitting branch does not keep resetting its own novelty. If only one lineage family is present, divergence suppresses recency weighting and spreads picks by species representation within that family to favor breadth over repeatedly selecting the newest leaf; stability still falls back to species-level age/representation inside the family.
* Reproduction request config is full-profile (not spawn-only): simulator resolves `repro.config.*` settings from SettingsMonitor when `--settings-address` is provided and otherwise falls back to shared defaults aligned with Workbench Reproduction defaults.
* Status payload includes pool-growth and mutation observability counters (`parent_pool_size`, `children_added_to_pool`, run/mutation totals), plus source-separated similarity telemetry: overall observed range, compatibility-assessment range, reproduction-report range, and successful-speciation-commit range. `sim_commit` prefers the committed source-species similarity returned by Speciation, falling back to the pre-commit lineage/request similarity only when older or incomplete runtime metadata omits the committed field. This keeps simulator telemetry comparable with Speciation history metrics without conflating score sources.
* Session control surface is tool-local (`start`, `stop`, `status`) and is consumed by Workbench Speciation pane simulator controls.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
