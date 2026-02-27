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
* has_gpu INTEGER
* gpu_name TEXT
* vram_free_bytes INTEGER
* cpu_score REAL
* gpu_score REAL
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

---
