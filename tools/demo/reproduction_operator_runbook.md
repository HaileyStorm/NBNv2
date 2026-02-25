# Reproduction Operator Runbook

## Scope
This runbook covers reproduction requests sent through IO Gateway (`ReproduceByBrainIds` and `ReproduceByArtifacts`) and handled by `ReproductionManager`.

## Quick Start (Deterministic Artifact Flow)
Run the local PowerShell demo script (worker-node-first bring-up):
- starts `SettingsMonitor`
- starts worker nodes and waits for worker/service heartbeats
- starts `HiveMind`, `IO`, `Reproduction`, and `Observability`
- spawns the demo brain through IO/HiveMind placement (`SpawnBrainViaIO`)

```powershell
tools/demo/run_local_hivemind_demo.ps1
```

The script emits repro JSON to:

`tools/demo/local-demo/<timestamp>/logs/repro-scenario.log`

The script also emits a multi-case verification suite report:

`tools/demo/local-demo/<timestamp>/logs/repro-suite.log`

Default repro scenario policy is `spawn-policy=never`, so the expected happy-path shape is:

- `result.compatible == true`
- `result.abort_reason == ""`
- `result.child_def.sha256` is populated
- `result.spawned == false`
- `result.child_brain_id == ""`

For a spawn attempt, set `-ReproSpawnPolicy default` or `-ReproSpawnPolicy always`.

## Repro Suite (Multi-Behavior Verification)
`Nbn.Tools.DemoHost repro-suite` runs a deterministic set of behavior checks and emits per-case pass/fail output:

- `compatible_spawn_never`
- `missing_parent_b_def`
- `parent_b_media_type_invalid`
- `parent_a_artifact_not_found`
- `region_span_mismatch`
- `strength_live_without_state`
- `spawn_always_attempt`

Each case includes:
- `expected` assertions
- `actual` observed values
- `failures` list (empty when passed)

A passing suite has:
- `all_passed == true`
- `failed_cases == 0`

Direct command:

```bash
dotnet run --project tools/Nbn.Tools.DemoHost -c Release --no-build -- \
  repro-suite \
  --io-address 127.0.0.1:12050 --io-id io-gateway \
  --parent-a-sha256 <hex> --parent-a-size <bytes> \
  --store-uri <artifact_root_or_file_uri> \
  --seed 12345 --json
```

## Direct Command
You can invoke the scenario command directly:

```bash
dotnet run --project tools/Nbn.Tools.DemoHost -c Release --no-build -- \
  repro-scenario \
  --io-address 127.0.0.1:12050 --io-id io-gateway \
  --parent-a-sha256 <hex> --parent-a-size <bytes> \
  --parent-b-sha256 <hex> --parent-b-size <bytes> \
  --store-uri <artifact_root_or_file_uri> \
  --seed 12345 --spawn-policy never --strength-source base --json
```

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
- Confirm parent artifacts are present in the referenced store root.
- Use fixed seeds for deterministic triage (`--seed`).
