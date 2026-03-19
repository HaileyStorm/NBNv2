# Speciation Operator Runbook

## Scope
This runbook covers the dedicated Speciation service (`Nbn.Runtime.Speciation`) and the Workbench Speciation pane.

## Quick Start
Open Workbench and use `Orchestrator` `Start All` so SettingsMonitor, HiveMind, IO, Reproduction, Speciation, and Observability share the same discovery/config path. Use `Spawn Sample Brain` if you need a live brain for end-to-end placement or assignment checks.

Then use:

- `Orchestrator` to confirm `Speciation` is online and discovery published `service.endpoint.speciation_manager`
- `Speciation` pane for status, policy settings, history, `Start New Epoch`, `Reset All`, and epoch deletion controls

## Service Bring-up
Direct launch:

```powershell
dotnet run --project src/Nbn.Runtime.Speciation -c Release --no-build -- `
  --bind-host 0.0.0.0 --port 12080 `
  --settings-host 127.0.0.1 --settings-port 12010 --settings-name SettingsMonitor
```

If you use the `--no-build` command above, build the repo or the referenced project first.

Useful flags:

- `--db <path>` selects the SQLite taxonomy store
- `--manager-name <name>` and `--server-name <name>` override the remoting/service-registration names
- `--enable-otel --otel-metrics --otel-traces --otel-endpoint <uri>` enables OTEL export
- `--otel-console` prints metrics/traces to stdout for local triage

Default runtime policy comes from SettingsMonitor `workbench.speciation.*` keys, not from separate speciation-policy CLI overrides.

## Normal Operations
- `Status`: use the Workbench Speciation pane or `SpeciationStatus` through IO to confirm the current epoch id plus membership/species/lineage-edge counts.
- `Start New Epoch`: use `SpeciationSetConfig.start_new_epoch=true` when taxonomy should roll forward while preserving history.
- `Reset All`: use `SpeciationResetAll` only when the full taxonomy/history should be cleared and epoch numbering reseeded from `1`.
- `Delete Historical Epoch`: only delete non-current epochs. Current epoch deletion is intentionally rejected.
- `Startup reconcile`: on service start, speciation reloads persisted state and fills missing memberships for brains registered in SettingsMonitor using `startup_reconcile_decision_reason`.

## Telemetry
Core metrics emitted by `Nbn.Runtime.Speciation`:

- `nbn.speciation.startup.reconcile.total`
- `nbn.speciation.startup.reconcile.memberships.added`
- `nbn.speciation.startup.reconcile.memberships.existing`
- `nbn.speciation.assignment.decisions`
- `nbn.speciation.assignment.duration.ms`
- `nbn.speciation.epoch.transition.total`
- `nbn.speciation.status.membership_count`
- `nbn.speciation.status.species_count`
- `nbn.speciation.status.lineage_edge_count`

Core spans:

- `speciation.startup.reconcile`
- `speciation.assign`
- `speciation.evaluate`
- `speciation.epoch.transition`

Recommended triage filters:

- `decision_reason`
- `failure_reason`
- `apply_mode`
- `candidate_mode`
- `transition`

## Common Failure Reasons
| Failure reason | Surface | Meaning | Operator action |
| --- | --- | --- | --- |
| `service_initializing` | Speciation responses | Actor/store startup is not finished yet | Wait for service startup to complete; check speciation log for initialization failure |
| `service_unavailable` | IO Gateway response | IO does not have a reachable speciation PID | Start `Nbn.Runtime.Speciation`; confirm IO discovery or `--speciation-address/--speciation-name` wiring |
| `request_failed` | IO Gateway response | Forwarding to speciation actor timed out or threw | Check `io` and `speciation` logs; verify ports, actor names, and remoting reachability |
| `store_error` | Speciation responses | SQLite load/write failed | Inspect speciation DB path, filesystem permissions, disk space, and exception text in logs |
| `invalid_request` | Speciation responses | Bad epoch/config request (for example delete current epoch or epoch id `<= 0`) | Fix caller input; use `Start New Epoch` instead of deleting the current epoch |
| `invalid_candidate` | Speciation evaluate/assign/query | Candidate lacked usable `brain_id`, `artifact_ref`, or `artifact_uri` | Send a valid candidate reference |
| `membership_immutable` | Speciation assign commit | Candidate already has membership in the current epoch | Use a new epoch if reclassification is required |

## Diagnosis Checklist
- Confirm SettingsMonitor is reachable and publishing `service.endpoint.speciation_manager`.
- Confirm Speciation status returns the expected current epoch and non-zero counts after startup reconcile.
- When assignment behavior looks wrong, inspect `decision_metadata_json` first; it carries policy snapshot, lineage inputs, similarity evidence, compatibility-attempt provenance, and split-threshold context.
- When newborn-lineage continuity looks stalled, compare speciation decision metadata with reproduction compatibility availability/failure reasons before changing tests or thresholds.
- When Workbench charts look stale, verify `SpeciationStatus` and `SpeciationListHistory` responses first; the UI should reflect service state rather than invent it.

## Recovery Notes
- For transient IO-routing failures, fix endpoint discovery/remoting first, then retry the request; do not rewrite historical rows manually.
- For corrupted or intentionally abandoned taxonomy state, take a DB copy, use `Reset All`, and allow startup reconcile plus new commits to rebuild the current epoch.
- For membership changes that must preserve audit history, prefer `Start New Epoch` over direct DB edits or test-only expectation changes.
