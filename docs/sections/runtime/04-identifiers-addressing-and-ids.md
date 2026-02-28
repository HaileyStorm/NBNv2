## 4. Identifiers, addressing, and IDs

### 4.1 Stable IDs

* `BrainId`: UUID (16 bytes)
* `NodeId`: UUID (16 bytes)
* UUID byte order (on disk / on wire): RFC 4122 network order (big-endian; matches canonical hex string)
* `ArtifactId`: SHA-256 (32 bytes), computed on canonical bytes of artifact payload
* `SnapshotId`: SHA-256 of `.nbs` payload or UUID (implementation choice; prefer SHA-256)

### 4.2 RegionShardId

A RegionShard is uniquely identified within a brain by:

* `region_id` (0..31)
* `shard_index` (0..N-1 for that region, contiguous and stable for the brain’s current placement epoch)

For convenience, also define a packed `ShardId32`:

* bits 0..15: shard_index
* bits 16..20: region_id (5 bits)
* bits 21..31: reserved (0)

### 4.3 Runtime neuron address: Address32

Runtime routing uses a compact 32-bit address:

* `region_id`: 5 bits (0..31)
* `neuron_id`: 27 bits (0..134,217,727)

Packed as:

* bits 0..26  = neuron_id
* bits 27..31 = region_id

Helpers:

* `region_id = addr >> 27`
* `neuron_id = addr & ((1<<27)-1)`

Note: Address32 is runtime-only. The `.nbn` axon record stores `target_neuron_id` with 22 bits, so `.nbn` region spans MUST be <= `2^22 - 1` (4194303) and axon targets MUST fit that range.

### 4.4 Input and Output regions (fixed)

* **Input region:** `region_id = 0`
* **Output region:** `region_id = 31`

These are not configurable.

---
