# NothingButNeurons v2

## Design, Architecture, Protocols, File Formats, and Implementation Specification

**Stack:** C#/.NET â€¢ Proto.Actor (Proto.Remote over gRPC) â€¢ Protobuf â€¢ Avalonia Workbench â€¢ SQLite+Dapper â€¢ OpenTelemetry â€¢ ILGPU (CUDA-first)

---

## Table of contents

1. Purpose and scope
2. Technology stack and solution structure
3. Distributed architecture and service topology
4. Identifiers, addressing, and IDs
5. Simulation model and tick semantics
6. Global tick engine, backpressure, and scheduling
7. Cost and energy model
8. Distance model (region + neuron)
9. Sharding and placement
10. RegionShard compute backends
11. Plasticity (axon strength adaptation)
12. Brain lifecycle, failure recovery, and snapshots
13. I/O architecture and External World interface
14. Reproduction and evolution
15. Observability: debug, visualization, metrics, tracing
16. Artifact storage and deduplication
17. File formats: `.nbn` (definition) and `.nbs` (state)
18. Database schemas (SQLite)
19. Protocol schemas (`.proto`)
20. Implementation roadmap (tentative)
    Appendix A: Defaults and constants
    Appendix B: Function catalog (IDs, formulas, tiers, costs)
    Appendix C: Region axial map (3D-inspired) and distance examples
21. Agent policy (global + local)

---
