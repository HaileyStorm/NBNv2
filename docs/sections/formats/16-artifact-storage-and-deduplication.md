## 16. Artifact storage and deduplication

### 16.1 Goals

* Avoid storing multiple full copies when artifacts are mostly identical (common for repeated spawns and reproduction).
* Support very large artifacts.
* Support partial fetch (region-only and snapshot-only usage).
* Use permissive/free licensing (custom implementation permitted).

### 16.2 Content-addressed chunk store (recommended)

Implement an Artifact Store as a content-addressed storage (CAS) with chunk-level deduplication:

* Each artifact has an `ArtifactId = SHA-256(canonical_bytes)`.
* Artifact payload bytes are split into chunks using **content-defined chunking** (CDC) to improve dedup when insertions shift offsets.

  * Suggested CDC parameters: min 512 KiB, avg 2 MiB, max 8 MiB
* Each chunk stored by `chunk_hash = SHA-256(chunk_bytes)`.

Metadata is stored in SQLite (manifest tables). Chunk payloads stored on disk:

* `chunks/aa/<hash>` where `aa` is first byte of hash in hex.

Compression:

* chunks may (should) be compressed (e.g., zstd) before storage
* compression is chunk-local; hash is computed on uncompressed chunk bytes for correctness

### 16.3 Manifest structure

An artifact manifest stores:

* artifact_id
* media_type (`application/x-nbn`, `application/x-nbs`)
* byte_length
* ordered list of chunk hashes with uncompressed sizes
* optional region-section index to support partial fetch

Clients:

* download manifest
* download missing chunks
* reconstruct canonical bytes on demand
* resolve non-file `store_uri` values through a pluggable artifact-store adapter/client path instead of treating them as local filesystem roots
* keep a node-local cache of fetched full artifacts/chunks so repeated loads reuse local bytes after the first fetch
* fail explicitly when no adapter is registered for a non-file `store_uri`; do not silently redirect those reads/writes to a local fallback path
* bootstrap exact non-file `store_uri` mappings at process start through `NBN_ARTIFACT_STORE_URI_MAP` (JSON object of `store_uri -> backing root/adapter target`) when an in-process adapter registration path is not available

Example bootstrap mapping:

```json
{
  "memory+prod://artifact-store/main": "D:\\nbn\\artifact-mirror",
  "s3+cache://cluster-a/artifacts": "E:\\nbn\\s3-cache"
}
```

Runtime fetch/store callers that honor this resolver path include HiveMind, RegionHost, WorkerNode, and Reproduction. Use exact key matches rather than prefix matching so different logical stores do not alias accidentally.

### 16.4 Dedup interactions with plasticity and reproduction

Plasticity:

* `.nbs` stores only buffer state and strength-code overlays, typically much smaller than `.nbn` (at least if re-basing enabled).
* Dedup naturally handles repeated snapshots and similar overlays.

Reproduction:

* regions and axon arrays often share large common chunks
* CDC improves dedup even when new regions/axons shift offsets

### 16.5 Optional region-section indexing (default / if feasible)

The store may additionally index `.nbn` region sections:

* per region: offset and length in canonical bytes
* enables efficient partial fetch of required regions for worker nodes

---
