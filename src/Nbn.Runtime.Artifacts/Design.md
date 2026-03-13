# Nbn.Runtime.Artifacts

Owns artifact fetch/store contracts, dedup manifests, artifact reference semantics, and `store_uri` resolution.

Non-file `store_uri` values resolve through registered artifact-store adapters rather than local path translation. Hosts may register adapters in-process, resolve direct `http://` / `https://` artifact-service URIs through the built-in `HttpArtifactStore`, or bootstrap exact URI mappings through the `NBN_ARTIFACT_STORE_URI_MAP` JSON environment variable. Runtime fetch/store callers that honor this resolver path include HiveMind, RegionHost, WorkerNode, and Reproduction.

`NBN_ARTIFACT_STORE_URI_MAP` format:

```json
{
  "memory+prod://artifact-store/main": "D:\\nbn\\artifact-mirror",
  "s3+cache://cluster-a/artifacts": "https://artifact-gateway.cluster-a.internal/"
}
```

Keys must match the exact non-file `store_uri` carried in artifact refs. Values may point to a local backing root, `file://` URI, or HTTP(S) artifact-service base URI for that logical store. Use exact keys rather than prefix matching so different logical stores cannot alias accidentally.

Remote adapters preserve content-addressed reads/writes by hash and may populate a node-local full-artifact cache after the first fetch. `IArtifactStore.TryOpenArtifactAsync(...)` continues to mean a complete artifact stream; selective reads use the separate `TryOpenArtifactRangeAsync(...)` path. `.nbn` manifests may carry region-index metadata so RegionHost and WorkerNode can fetch only the required region section when the store supports range reads. Seekable `.nbn` writes auto-populate that index unless the caller supplies explicit `ArtifactStoreWriteOptions.RegionIndex` values. That auto-indexing path is best-effort: malformed or out-of-range `.nbn` header metadata skips persisted region-index entries instead of failing storage, and the artifact store still relies on downstream consumers for full format validation.

The built-in HTTP(S) backend expects `{base}/v1/manifests/{sha256}`, `{base}/v1/artifacts/{sha256}`, HTTP `Range` on artifact GETs, and `POST {base}/v1/artifacts` with artifact media type in `Content-Type`. Optional region-index metadata is carried in `X-Nbn-Artifact-Region-Index` as a base64-encoded JSON array. Resolver-wrapped HTTP stores keep `enableNodeLocalCache=true` by default, so the first successful remote fetch/write-through hydrates the node-local cache under `NBN_ARTIFACT_CACHE_ROOT` (or `<artifact-root>/.cache`) without changing artifact identity.

Failure semantics are explicit: unresolved non-file URIs throw instead of falling back to local paths, unsupported env-map target types throw configuration errors, remote `404` responses mean the manifest/artifact is absent in that store, `405`/`501` range responses fall back to full reads, and other non-success transport responses bubble back to the runtime caller.

## Bookkeeping and retention

Chunk identity is always derived from the uncompressed chunk bytes. Optional chunk compression only changes how the payload is stored on disk; the SQLite metadata row records the storage-only details (`stored_length`, `compression`) that readers need to reconstruct the original bytes.

Chunk and artifact rows also carry `ref_count` bookkeeping. `chunks.ref_count` increments when a newly stored artifact references an existing chunk hash, which is how cross-artifact dedup reuse is tracked today. When concurrent writers race on the same chunk hash, later writers tolerate the transient file-present/metadata-missing window by re-reading committed metadata or deriving the storage metadata from the existing chunk bytes before reusing that chunk.

Duplicate stores are keyed by `artifact_sha256`. Re-storing the same artifact bytes with the same media type reuses the existing artifact row instead of inserting a duplicate row or incrementing those counters again. Later compatible stores may enrich missing region-index metadata on that existing row, but conflicting media-type changes or conflicting region-index entries are rejected explicitly.

Lifecycle management is currently append-only. There is no public delete/release API for artifact rows, chunk rows, or node-local cache files, and there is no automatic GC/TTL eviction path. Operators should treat both the CAS store and node-local caches as manually cleaned artifacts until an explicit reclamation feature is implemented.

Shared-root contention is supported for the local CAS store on a single machine: multiple processes may point `LocalArtifactStore` at the same artifact root and rely on SQLite WAL/busy-timeout behavior plus chunk metadata recovery for duplicate chunks. The node-local `.cache` tree is still a per-process or per-node optimization, not a shared cache-coherence layer, so operators should keep cache roots distinct when multiple processes or hosts resolve the same remote store.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
