# Nbn.Runtime.WorkerNode

Owns worker node inventory, placement execution endpoints, and lifecycle participation.

## Stable behavior notes

- Region-shard placement is artifact-backed. If base artifact metadata is unavailable at assignment time, WorkerNode returns a retryable `PlacementFailureWorkerUnavailable` ack instead of hosting a synthetic shard.
- Artifact-backed placement resolves non-file `store_uri` values through the shared artifact resolver, including built-in HTTP(S) artifact services and env-mapped logical store URIs. Resolver-wrapped remote stores keep node-local cache behavior under `NBN_ARTIFACT_CACHE_ROOT`; transport/configuration failures remain retryable placement failures until metadata or artifact bytes become reachable.
- WorkerNode answers targeted peer-latency probe requests for HiveMind placement decisions and reports average RTT plus sample count per requested peer.
- Peer RTT probes use request/reply echo messages against the peer worker root actor. The echo request must respond directly to the caller so remoting preserves the real sender for round-trip measurement.
- WorkerNode heartbeats publish raw capability snapshots derived from real host probing plus explicit CPU/GPU score calculation, then attach explicit NBN limit metadata (`--cpu-pct`, `--ram-pct`, `--storage-pct`, `--gpu-compute-pct`, `--gpu-vram-pct`; legacy `--gpu-pct` sets both GPU limits).
- WorkerNode and RegionHost share the same CUDA-first ILGPU device-selection rules, so worker capability publication, test skip decisions, and runtime shard execution all agree on what counts as a compatible accelerator.
- Worker CPU/GPU score microbenchmarks run on initial publication, are rerun on the next heartbeat after placement changes invalidate the score cache, and are also rerun when HiveMind sends the settings-backed capability refresh request cadence. WorkerNode never bypasses SettingsMonitor with direct capability replies.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
