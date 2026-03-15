## 18. Database schemas (SQLite)

### 18.1 SettingsMonitor database

Tables (recommended, values not exhaustive):

**nodes**

* node_id TEXT PRIMARY KEY (UUID)
* logical_name TEXT
* address TEXT (host:port)
* root_actor_name TEXT
* last_seen_ms INTEGER
* is_alive INTEGER

**node_capabilities**

* node_id TEXT
* time_ms INTEGER
* cpu_cores INTEGER
* ram_free_bytes INTEGER
* storage_free_bytes INTEGER
* has_gpu INTEGER
* gpu_name TEXT
* vram_free_bytes INTEGER
* cpu_score REAL
* gpu_score REAL
* ram_total_bytes INTEGER
* storage_total_bytes INTEGER
* vram_total_bytes INTEGER
* cpu_limit_percent INTEGER
* ram_limit_percent INTEGER
* storage_limit_percent INTEGER
* gpu_compute_limit_percent INTEGER
* gpu_vram_limit_percent INTEGER
* process_cpu_load_percent REAL
* process_ram_used_bytes INTEGER
* PRIMARY KEY (node_id, time_ms)

**settings**

* key TEXT PRIMARY KEY
* value TEXT
* updated_ms INTEGER

**brains**

* brain_id TEXT PRIMARY KEY
* base_nbn_sha256 BLOB (32)
* last_snapshot_sha256 BLOB (32) NULL
* spawned_ms INTEGER
* last_tick_id INTEGER
* state TEXT (Active/Paused/Recovering/Dead)
* notes TEXT NULL

### 18.2 Artifact store metadata database (if hosted with SQLite)

Tables (recommended):

**artifacts**

* artifact_sha256 BLOB(32) PRIMARY KEY
* media_type TEXT
* byte_length INTEGER
* created_ms INTEGER
* manifest_sha256 BLOB(32)
* ref_count INTEGER

**chunks**

* chunk_sha256 BLOB(32) PRIMARY KEY
* byte_length INTEGER
* stored_length INTEGER
* compression TEXT
* ref_count INTEGER

**artifact_chunks**

* artifact_sha256 BLOB(32)
* seq INTEGER
* chunk_sha256 BLOB(32)
* chunk_uncompressed_length INTEGER
* PRIMARY KEY (artifact_sha256, seq)

**artifact_region_index** (optional)

* artifact_sha256 BLOB(32)
* region_id INTEGER
* offset INTEGER
* length INTEGER
* PRIMARY KEY (artifact_sha256, region_id)

When present, `artifact_region_index` records canonical `.nbn` region-section byte ranges that can guide selective region fetches; callers must still cross-check those ranges against the `.nbn` header directory before trusting them.

Current artifact-store implementations use `stored_length` and `compression` as chunk-storage metadata, and use `ref_count` as insert-time bookkeeping for unique artifact/chunk references. Duplicate rows remain keyed by `artifact_sha256`; later compatible stores may update `manifest_sha256` and persist missing region-index rows on that existing artifact, while conflicting media-type or region-index metadata is rejected. Concurrent writers that encounter an existing chunk file tolerate a transient missing metadata row by re-reading committed chunk metadata or deriving it from the stored chunk bytes before reusing that chunk, and SQLite connections set a busy timeout so same-machine shared-root writers wait instead of surfacing immediate `SQLITE_BUSY` failures. There is currently no release/delete API, no ref-count decrement path, and no automatic GC, so these counters do not drive reclamation yet.

---
