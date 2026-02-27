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
* axon strength overlay codes (only where code differs from base)

Snapshots are taken at tick boundaries to avoid needing to store inbox state.

### 12.3 Failure recovery

If any RegionShard for a brain is lost due to process/node failure:

* HiveMind pauses tick dispatch (Section 6.6)
* HiveMind unloads the brainâ€™s current runtime actors
* HiveMind restores the **entire brain** from the last `.nbn` + `.nbs` snapshot
* RegionShards are respawned and re-placed as needed
* Tick dispatch resumes

Partial shard-only restoration is not used.

### 12.4 Brain termination notifications and rebalancing

When a brain terminates (energy exhaustion, explicit kill from External World or Workbench, unrecoverable error):

* IO Gateway receives a `BrainTerminated` notification with artifact references
* IO Gateway notifies External World with the BrainId and artifact reference
* HiveMind removes the brain from tick barrier participation
* HiveMind may trigger a placement rebalance if the cluster becomes imbalanced (immediate or queued check)

---
