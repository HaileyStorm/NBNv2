# NBNv2 specification template

This template is the canonical assembly source for the full specification.

- Generated full document: `docs/NBNv2.md`
- Assembly map: `docs/manifest/NBNv2-DocumentMap.md`
- Include marker contract: `<!-- NBN:INCLUDE path="..." -->`

## Layout policy

Canonical documents live in two places:

1. Cross-cutting sections in repo-root `docs/sections/*`.
2. Project-specific design notes near code in `*/Design.md` (`src/*`, `tools/*`, `tests/*`).
3. `Docs/` or `docs/` subfolders under project roots are not canonical.

`docs/INDEX.md` remains the stable entrypoint for humans and agents.

## Table of contents

1. Purpose and scope
2. Technology stack and solution structure
3. Distributed architecture and service topology
4. Identifiers, addressing, and IDs
5. Simulation model and tick semantics
6. Global tick engine, backpressure, and scheduling
7. Cost and energy model
8. Distance model (region and neuron)
9. Sharding and placement
10. RegionShard compute backends
11. Plasticity (axon strength adaptation)
12. Brain lifecycle, failure recovery, and snapshots
13. I/O architecture and External World interface
14. Reproduction and evolution
15. Observability: debug, visualization, metrics, tracing
16. Artifact storage and deduplication
17. File formats: .nbn and .nbs
18. Database schemas (SQLite)
19. Protocol schemas (.proto)
20. Implementation roadmap
Appendix A. Defaults and constants
Appendix B. Function catalog
Appendix C. Region axial map and distance examples

---

## Front matter

<!-- NBN:INCLUDE path="docs/sections/overview/00-front-matter.md" -->

## 1. Purpose and scope

<!-- NBN:INCLUDE path="docs/sections/overview/01-purpose-and-scope.md" -->

## 2. Technology stack and solution structure

<!-- NBN:INCLUDE path="docs/sections/overview/02-technology-stack-and-solution-structure.md" -->

## 3. Distributed architecture and service topology

<!-- NBN:INCLUDE path="docs/sections/runtime/03-distributed-architecture-and-service-topology.md" -->

## 4. Identifiers, addressing, and IDs

<!-- NBN:INCLUDE path="docs/sections/runtime/04-identifiers-addressing-and-ids.md" -->

## 5. Simulation model and tick semantics

<!-- NBN:INCLUDE path="docs/sections/runtime/05-simulation-model-and-tick-semantics.md" -->

## 6. Global tick engine, backpressure, and scheduling

<!-- NBN:INCLUDE path="docs/sections/runtime/06-global-tick-engine-backpressure-and-scheduling.md" -->

## 7. Cost and energy model

<!-- NBN:INCLUDE path="docs/sections/runtime/07-cost-and-energy-model.md" -->

## 8. Distance model (region and neuron)

<!-- NBN:INCLUDE path="docs/sections/runtime/08-distance-model-region-and-neuron.md" -->

## 9. Sharding and placement

<!-- NBN:INCLUDE path="docs/sections/runtime/09-sharding-and-placement.md" -->

## 10. RegionShard compute backends

<!-- NBN:INCLUDE path="docs/sections/runtime/10-regionshard-compute-backends.md" -->

## 11. Plasticity (axon strength adaptation)

<!-- NBN:INCLUDE path="docs/sections/runtime/11-plasticity-axon-strength-adaptation.md" -->

## 12. Brain lifecycle, failure recovery, and snapshots

<!-- NBN:INCLUDE path="docs/sections/runtime/12-brain-lifecycle-failure-recovery-and-snapshots.md" -->

## 13. I/O architecture and External World interface

<!-- NBN:INCLUDE path="docs/sections/runtime/13-io-architecture-and-external-world-interface.md" -->

## 14. Reproduction and evolution

<!-- NBN:INCLUDE path="docs/sections/runtime/14-reproduction-and-evolution.md" -->

## 15. Observability: debug, visualization, metrics, tracing

<!-- NBN:INCLUDE path="docs/sections/runtime/15-observability-debug-visualization-metrics-tracing.md" -->

## 16. Artifact storage and deduplication

<!-- NBN:INCLUDE path="docs/sections/formats/16-artifact-storage-and-deduplication.md" -->

## 17. File formats: .nbn and .nbs

<!-- NBN:INCLUDE path="docs/sections/formats/17-file-formats-nbn-and-nbs.md" -->

## 18. Database schemas (SQLite)

<!-- NBN:INCLUDE path="docs/sections/runtime/18-database-schemas-sqlite.md" -->

## 19. Protocol schemas (.proto)

<!-- NBN:INCLUDE path="docs/sections/protocols/19-protocol-schemas-proto.md" -->

## 20. Implementation roadmap

<!-- NBN:INCLUDE path="docs/sections/overview/20-implementation-roadmap.md" -->

## Appendix A. Defaults and constants

<!-- NBN:INCLUDE path="docs/sections/reference/A-defaults-and-constants.md" -->

## Appendix B. Function catalog

<!-- NBN:INCLUDE path="docs/sections/reference/B-function-catalog.md" -->

## Appendix C. Region axial map and distance examples

<!-- NBN:INCLUDE path="docs/sections/reference/C-region-axial-map-and-distance-examples.md" -->

---

## Component ownership notes

These are kept adjacent to implementation code and included here for unified reading context.

<!-- NBN:INCLUDE path="src/Nbn.Shared/Design.md" -->
<!-- NBN:INCLUDE path="src/Nbn.Runtime.SettingsMonitor/Design.md" -->
<!-- NBN:INCLUDE path="src/Nbn.Runtime.HiveMind/Design.md" -->
<!-- NBN:INCLUDE path="src/Nbn.Runtime.IO/Design.md" -->
<!-- NBN:INCLUDE path="src/Nbn.Runtime.Reproduction/Design.md" -->
<!-- NBN:INCLUDE path="src/Nbn.Runtime.Observability/Design.md" -->
<!-- NBN:INCLUDE path="src/Nbn.Runtime.Artifacts/Design.md" -->
<!-- NBN:INCLUDE path="src/Nbn.Runtime.Brain/Design.md" -->
<!-- NBN:INCLUDE path="src/Nbn.Runtime.BrainHost/Design.md" -->
<!-- NBN:INCLUDE path="src/Nbn.Runtime.RegionHost/Design.md" -->
<!-- NBN:INCLUDE path="src/Nbn.Runtime.WorkerNode/Design.md" -->
<!-- NBN:INCLUDE path="tools/Nbn.Tools.Workbench/Design.md" -->
<!-- NBN:INCLUDE path="tools/Nbn.Tools.DemoHost/Design.md" -->
<!-- NBN:INCLUDE path="tests/Nbn.Tests/Design.md" -->

## Supplemental docs

- `docs/placement-lifecycle.md`
- `docs/temp/NBNv2_HumanGuide_TEMP.md` (transition source retained until split verification is complete)
