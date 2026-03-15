# Nbn.Runtime.WorkerNode

Owns worker node inventory, placement execution endpoints, and lifecycle participation.

## Stable behavior notes

- Region-shard placement is artifact-backed. If base artifact metadata is unavailable at assignment time, WorkerNode returns a retryable `PlacementFailureWorkerUnavailable` ack instead of hosting a synthetic shard.
- Artifact-backed placement resolves non-file `store_uri` values through the shared artifact resolver, including built-in HTTP(S) artifact services and env-mapped logical store URIs. Resolver-wrapped remote stores keep node-local cache behavior under `NBN_ARTIFACT_CACHE_ROOT`; transport/configuration failures remain retryable placement failures until metadata or artifact bytes become reachable.
- WorkerNode answers targeted peer-latency probe requests for HiveMind placement decisions and reports average RTT plus sample count per requested peer.
- Peer RTT probes use request/reply echo messages against the peer worker root actor. The echo request must respond directly to the caller so remoting preserves the real sender for round-trip measurement.
- WorkerNode heartbeats publish capability snapshots derived from real host probing plus explicit CPU/GPU score calculation. Scaling knobs (`--cpu-pct`, `--ram-pct`, `--storage-pct`, `--gpu-pct`) apply after probing so simulated partial-capacity workers still preserve realistic relative resource ratios and accelerator flags.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
