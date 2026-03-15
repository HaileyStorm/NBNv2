## 12. Brain lifecycle, failure recovery, and snapshots

### 12.1 Spawn

Brains are spawned by providing a `.nbn` artifact reference. HiveMind coordinates:

* loading region directories
* allocating RegionShards and IO coordinators
* establishing routing tables
* initializing energy accounts and settings

### 12.2 Snapshot (`.nbs`)

Snapshots store:

* persistent neuron buffers
* enabled bitsets (optional)
* energy balance
* persisted homeostasis runtime config (when present in the snapshot header)
* axon strength overlay codes (only where code differs from base)

Snapshots are taken at tick boundaries to avoid needing to store inbox state.

Any placement or recovery flow that reloads a brain from `.nbn + .nbs` artifacts restores the persisted homeostasis settings when the snapshot header carries them.

### 12.3 Failure recovery

If any RegionShard or placement worker for a brain is lost due to process/node failure:

* HiveMind pauses tick dispatch (Section 6.6)
* HiveMind unloads the brain's current runtime actors
* HiveMind marks the brain `Recovering` before replacement work starts
* HiveMind restores the **entire brain** from the last stored `.nbn` + `.nbs` artifact refs, including persisted homeostasis settings when the snapshot carries them; it does not rebuild only the missing shard from surviving live shards
* RegionShards are respawned and re-placed as needed
* Tick dispatch resumes only after the recovery placement epoch is active
* Successful recovery emits `Recovering -> Active` (or `Recovering -> Paused` if the brain was already paused); unrecoverable artifact/placement failure emits `Recovering -> Dead`

Partial shard-only restoration is not used.

RegionShard startup may selectively fetch only the assigned `.nbn` region section when artifact manifests provide region-index metadata, but that optimization does not change recovery ownership or scope. HiveMind still restores the entire brain from durable `.nbn + .nbs` artifacts, and `.nbs` snapshot validation still uses whole-brain snapshot bytes rather than shard-only snapshot fragments.

### 12.4 Brain termination notifications and rebalancing

When a brain terminates (energy exhaustion, explicit kill from External World or Workbench, unrecoverable error):

* IO Gateway receives a `BrainTerminated` notification with artifact references
* IO Gateway notifies External World with the BrainId and artifact reference
* HiveMind removes the brain from tick barrier participation
* HiveMind may trigger a placement rebalance if the cluster becomes imbalanced (immediate or queued check)

---
