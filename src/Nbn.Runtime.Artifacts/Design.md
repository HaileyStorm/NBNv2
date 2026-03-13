# Nbn.Runtime.Artifacts

Owns artifact fetch/store contracts, dedup manifests, artifact reference semantics, and `store_uri` resolution.

Non-file `store_uri` values resolve through registered artifact-store adapters rather than local path translation. Hosts may register adapters in-process or bootstrap exact URI mappings through the `NBN_ARTIFACT_STORE_URI_MAP` JSON environment variable. Runtime fetch/store callers that honor this resolver path include HiveMind, RegionHost, WorkerNode, and Reproduction.

`NBN_ARTIFACT_STORE_URI_MAP` format:

```json
{
  "memory+prod://artifact-store/main": "D:\\nbn\\artifact-mirror",
  "s3+cache://cluster-a/artifacts": "E:\\nbn\\s3-cache"
}
```

Keys must match the exact non-file `store_uri` carried in artifact refs. Values point to the local backing root or adapter target for that URI. Use exact keys rather than prefix matching so different logical stores cannot alias accidentally.

Remote adapters preserve content-addressed reads/writes by hash and may populate a node-local full-artifact cache after the first fetch. `IArtifactStore.TryOpenArtifactAsync(...)` continues to mean a complete artifact stream; selective reads use the separate `TryOpenArtifactRangeAsync(...)` path. `.nbn` manifests may carry region-index metadata so RegionHost and WorkerNode can fetch only the required region section when the store supports range reads. Seekable `.nbn` writes auto-populate that index unless the caller supplies explicit `ArtifactStoreWriteOptions.RegionIndex` values.

## Bookkeeping and retention

Chunk identity is always derived from the uncompressed chunk bytes. Optional chunk compression only changes how the payload is stored on disk; the SQLite metadata row records the storage-only details (`stored_length`, `compression`) that readers need to reconstruct the original bytes.

Chunk and artifact rows also carry `ref_count` bookkeeping. `chunks.ref_count` increments when a newly stored artifact references an existing chunk hash, which is how cross-artifact dedup reuse is tracked today. Duplicate stores are keyed by `artifact_sha256`: re-storing the same artifact bytes with the same effective manifest metadata reuses the existing manifest/catalog row instead of inserting a duplicate artifact row or incrementing those counters again. That duplicate path does not currently reconcile a later media-type or region-index change onto the existing row.

Lifecycle management is currently append-only. There is no public delete/release API for artifact rows, chunk rows, or node-local cache files, and there is no automatic GC/TTL eviction path. Operators should treat both the CAS store and node-local caches as manually cleaned artifacts until an explicit reclamation feature is implemented.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
