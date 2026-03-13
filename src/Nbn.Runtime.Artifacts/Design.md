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

Remote adapters preserve content-addressed reads/writes by hash and may populate a node-local full-artifact cache after the first fetch. Partial or region-indexed reads remain a separate concern; `IArtifactStore.TryOpenArtifactAsync(...)` continues to mean a complete artifact stream.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
