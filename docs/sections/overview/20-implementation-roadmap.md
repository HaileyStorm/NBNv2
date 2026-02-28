## 20. Status and next steps

### 20.1 Completed baseline

- Core protobuf contracts and generated models are in place.
- `.nbn`/`.nbs` format read/write and validation pipeline is implemented.
- Tick-based runtime orchestration (compute then deliver) is implemented through HiveMind plus Brain/Region actors.
- Core runtime services are operational: SettingsMonitor, HiveMind, IO, Reproduction, Observability, and Artifacts.
- Snapshot/recovery, plasticity overlays, and reproduction workflows are implemented and exercised by tests/tools.
- Workbench orchestration, designer, debug, and visualization surfaces are implemented for operator workflows.
- Local demo scripts provide repeatable end-to-end bring-up on Windows and Linux.
- Documentation assembly pipeline is implemented (`docs/INDEX.md` -> `docs/NBNv2.md`) with CI freshness checks.

### 20.2 Current priorities

1. GPU backend parity and cross-backend correctness coverage.
2. Distributed resilience hardening (timeouts, restart behavior, and failure diagnostics under load).
3. Scale/performance validation for larger topologies and sustained multi-brain workloads.
4. Operator documentation refinement (runbooks, failure triage, and reproducible checklists).
5. Workbench UX and observability ergonomics for high-volume sessions.

### 20.3 Recommended local validation path

- Build/test baseline:
  - `dotnet build -c Release --disable-build-servers`
  - `dotnet test -c Release --disable-build-servers`
- End-to-end demo path:
  - Windows: `tools/demo/run_local_hivemind_demo.ps1`
  - Linux: `tools/demo/run_local_hivemind_demo.sh`
- Documentation freshness:
  - Windows: `pwsh -NoProfile -File tools/docs/render-nbnv2-docs.ps1 -Check`
  - Linux/macOS: `bash tools/docs/render-nbnv2-docs.sh --check`
