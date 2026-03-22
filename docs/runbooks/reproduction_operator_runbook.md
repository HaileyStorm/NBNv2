# Reproduction Operator Runbook

## Scope
This runbook covers reproduction requests sent through IO Gateway (`ReproduceByBrainIds` and `ReproduceByArtifacts`) and handled by `ReproductionManager`.

## Quick Start (Workbench Artifact Flow)
Open Workbench and use `Orchestrator` `Start All` to launch the local runtime services.
Use `Designer` `Generate Random Brain` and `Spawn Brain` if you want a live IO/HiveMind placement sanity check before running reproduction commands, or start from existing `.nbn` / `.nbs` files if you want fixed artifact inputs.

To run artifact-based reproduction in the supported operator flow:

1. Open the `Reproduction` pane.
2. Select Parent A / Parent B `.nbn` files and optional `.nbs` state files.
3. Set `Artifact store root` to the local path or `file://` store you want Workbench to publish through.
4. Set a fixed `Seed`, choose `Strength source`, and choose `Spawn policy`.
5. Click `Run` and inspect `Status`, `Similarity summary`, and `Mutation summary`.

Workbench publishes local `.nbn` / `.nbs` inputs to a reachable artifact store automatically before it calls IO Gateway, so you do not need a separate helper CLI to precompute `sha256` / `size` values first.

Default happy-path expectations when `Spawn policy = Never`:

- compatibility is `true`
- abort reason is empty
- a child definition artifact is returned
- no child brain is spawned

For a spawn attempt, set `Spawn policy` to `Default (On)` or `Always`.

## Deterministic Verification Options
There is no separate repro-suite CLI anymore. Use one of these supported paths instead:

- Live operator smoke path: use the Workbench `Reproduction` pane with fixed parent files and a fixed `Seed`.
- Automated semantic regression path:

```bash
dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers \
  --filter "FullyQualifiedName~Nbn.Tests.Reproduction.ReproductionManagerActorTests|FullyQualifiedName~Nbn.Tests.Integration.IoGatewayArtifactReferenceTests"
```

The automated path covers the fixed-case checks previously exercised by the old helper workflow, including missing parent refs, media-type failures, artifact lookup failures, region-span mismatches, live-strength fallback, and spawn behavior.

## Headless / API Clients
For non-Workbench automation, send `ReproduceByBrainIds` or `ReproduceByArtifacts` through IO Gateway from your own client code. The supported request path remains IO Gateway -> ReproductionManager; removing the helper tool does not change the runtime API contract.

## Request/Response Semantics
- Input can be by brain IDs or artifact refs.
- Similarity gate failures return `report.compatible=false` and `report.abort_reason=<code>`.
- Success returns `child_def` + `summary`; spawn is controlled by `config.spawn_child`.
- Spawn failures set `report.compatible=false` with a spawn failure reason but still keep `child_def` when child artifact generation succeeded.

## Common Abort Reasons and Actions
| Abort reason | Source | Meaning | Operator action |
| --- | --- | --- | --- |
| `repro_unavailable` | IO Gateway | IO has no repro PID configured/reachable | Start `Nbn.Runtime.Reproduction` and pass `--repro-address/--repro-name` to IO |
| `repro_request_failed` | IO Gateway | IO request to repro actor threw/timeout | Check `io.err.log` and `reproduction.err.log`; verify ports and actor names |
| `repro_missing_parent_a_def` / `repro_missing_parent_b_def` | ReproductionManager | Required parent artifact ref missing in request | Ensure request includes both parent `.nbn` refs |
| `repro_parent_a_artifact_not_found` / `repro_parent_b_artifact_not_found` | ReproductionManager | Parent hash not present in artifact store | Verify `sha256`, `size`, and `store_uri`/artifact root |
| `repro_parent_a_media_type_invalid` / `repro_parent_b_media_type_invalid` | ReproductionManager | Parent media type is not `application/x-nbn` | Send `.nbn` refs with correct media type |
| `repro_parent_a_io_invariants_invalid` / `repro_parent_b_io_invariants_invalid` | ReproductionManager | Parent violates IO or duplicate-axon invariants | Validate parent artifact with `NbnBinaryValidator`; regenerate invalid input |
| `repro_parent_a_format_invalid` / `repro_parent_b_format_invalid` | ReproductionManager | Parent parse/format validation failed | Re-export artifact from known-good source |
| `repro_format_incompatible` | ReproductionManager | Parent quantization/format contracts differ | Use parents from same schema/format contract |
| `repro_region_presence_mismatch` | ReproductionManager | Present-region count mismatch | Select more similar parents or relax strategy upstream |
| `repro_region_span_mismatch` | ReproductionManager | Region-span tolerance gate failed | Raise `max_region_span_diff_ratio` or choose closer parents |
| `repro_function_hist_mismatch` | ReproductionManager | Function-distribution distance gate failed | Raise `max_function_hist_distance` or choose closer parents |
| `repro_connectivity_hist_mismatch` | ReproductionManager | Connectivity-distance gate failed | Raise `max_connectivity_hist_distance` or choose closer parents |
| `repro_spot_check_overlap_mismatch` | ReproductionManager | Spot-check overlap below required threshold | Use closer parents and stable seed for repeatability |
| `repro_child_validation_failed` | ReproductionManager | Synthesized child failed NBN validation | Tighten mutation limits/probabilities; inspect logs and mutation summary |
| `repro_spawn_unavailable` | ReproductionManager | Spawn requested but repro actor has no IO PID | Start repro node with `--io-address/--io-name` |
| `repro_spawn_failed` | ReproductionManager | Spawn ack missing/invalid BrainId | Inspect HiveMind/IO logs plus worker logs for assignment/lifecycle failures |
| `repro_spawn_request_failed` | ReproductionManager | Spawn request threw/timeout | Verify IO/HiveMind health and worker-node reachability/heartbeat freshness |

## Operational Checks
- Confirm worker-node inventory first:
  - worker processes are online in SettingsMonitor (`is_alive=true`, recent heartbeats, expected capabilities)
  - worker roles include placement targets needed for spawn (`brain-root`, `signal-router`, coordinators, `region-shard`)
- Confirm placement path is healthy:
  - HiveMind is reachable and has eligible workers (no `worker_unavailable` placement condition)
  - no active reschedule/recovery pause is blocking placement completion
- Confirm IO has repro routing:
  - IO launched with `--repro-address <host:port> --repro-name ReproductionManager`.
- Confirm repro actor has IO routing:
  - Repro launched with `--io-address <host:port> --io-name io-gateway`.
- Confirm parent artifacts are present in the referenced store root, or let Workbench publish local files into the configured artifact store root.
- Use fixed seeds for deterministic triage (`--seed`).
