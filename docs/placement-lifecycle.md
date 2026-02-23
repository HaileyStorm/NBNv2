# Worker Placement Lifecycle (NBNv2-91b.1)

This document defines the initial control-plane lifecycle introduced for worker-managed placement contracts.

## Lifecycle states

- `PLACEMENT_LIFECYCLE_UNKNOWN`
- `PLACEMENT_LIFECYCLE_REQUESTED`
- `PLACEMENT_LIFECYCLE_ASSIGNING`
- `PLACEMENT_LIFECYCLE_ASSIGNED`
- `PLACEMENT_LIFECYCLE_RUNNING`
- `PLACEMENT_LIFECYCLE_RECONCILING`
- `PLACEMENT_LIFECYCLE_FAILED`
- `PLACEMENT_LIFECYCLE_TERMINATED`

## Transition intent

1. `REQUESTED`: set when `RequestPlacement` is accepted and a new `placement_epoch` is created.
2. `ASSIGNING`: set when `PlacementAssignmentAck` reports pending/accepted assignment progress.
3. `ASSIGNED`: set when controller endpoints are registered (`RegisterBrain`) or assignment reports ready.
4. `RUNNING`: set when active shard registration is observed (`RegisterShard`) or reconcile reports matched state.
5. `RECONCILING`: set when shard topology no longer matches expected running shape (for example zero registered shards) or reconcile reports action required.
6. `FAILED`: set from explicit assignment/reconcile failures with a `PlacementFailureReason`.
7. `TERMINATED`: terminal semantic for completed teardown paths.

## Epoch and request identity

- Each accepted `RequestPlacement` creates/increments `placement_epoch`.
- `request_id` is carried through `RequestPlacement`, `PlacementAck`, and `PlacementLifecycleInfo`.
- Runtime consumers should ignore stale assignment/reconcile messages with mismatched epoch.

## Contract groups introduced

- Worker inventory: `PlacementWorkerInventoryRequest`, `PlacementWorkerInventoryEntry`, `PlacementWorkerInventory`
- Assignment orchestration: `PlacementAssignment`, `PlacementAssignmentRequest`, `PlacementAssignmentAck`
- Reconciliation: `PlacementReconcileRequest`, `PlacementObservedAssignment`, `PlacementReconcileReport`
- Lifecycle query: `GetPlacementLifecycle`, `PlacementLifecycleInfo`
