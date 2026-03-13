# Nbn.Runtime.Artifacts

Owns artifact fetch/store contracts, dedup manifests, artifact reference semantics, and `store_uri` resolution.

Non-file `store_uri` values resolve through registered artifact-store adapters rather than local path translation. Hosts may register adapters in-process or bootstrap exact URI mappings through the `NBN_ARTIFACT_STORE_URI_MAP` JSON environment variable. Remote adapters preserve content-addressed reads/writes by hash and may populate a node-local full-artifact cache after the first fetch. Partial or region-indexed reads remain a separate concern; `IArtifactStore.TryOpenArtifactAsync(...)` continues to mean a complete artifact stream.

## Maintenance guidance

Keep this file concise and decision-focused. Update when stable behavior, ownership boundaries, or invariants change. Prefer editing/replacing stale text over appending long history; avoid transient run logs or speculative notes.
