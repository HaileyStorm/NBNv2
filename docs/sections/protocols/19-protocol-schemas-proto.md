## 19. Protocol schemas (`.proto`)

The following `.proto` files define the canonical NBN wire schema. The source of truth is `src/Nbn.Shared/Protos/*.proto`; the appendix below mirrors those files so operators and integrators can read the current contract in one place.

### 19.0 Control-surface notes

- `subscriber_actor` fields in debug, IO output, and visualization subscriptions let callers supply a stable subscription identity instead of relying on the transient request sender PID.
- `SetTickRateOverride` applies an operator override on top of the baseline target tick rate until explicitly cleared; `HiveMindStatus.has_tick_rate_override` and `tick_rate_override_hz` report the effective override state.
- `HiveMindStatus` also carries the configured baseline tick target, automatic-backpressure-reduction flag, recent timeout/lateness window counts, and worker-pressure summary counts so operator clients can surface current runtime load from one status request.
- `BrainInfo`, IO `RegisterBrain`, and `BrainIoInfo` expose coordinator mode, output-vector source, coordinator PID labels, and IO-gateway ownership booleans so tools can distinguish gateway-owned coordinators from worker-hosted coordinators.
- Placement lifecycle, worker inventory, peer-latency, and capability-refresh messages in `nbn_control.proto` are the canonical operator-facing control-plane telemetry for placement orchestration and worker readiness.
- Drift-check process: when a `.proto` contract changes, update the mirrored appendix content from the matching file under `src/Nbn.Shared/Protos`, re-render `docs/NBNv2.md`, run `dotnet test tests/Nbn.Tests/Nbn.Tests.csproj -c Release --disable-build-servers --filter FullyQualifiedName~Nbn.Tests.Proto.ProtoCompatibilityTests`, and run the docs freshness check (`bash tools/docs/render-nbnv2-docs.sh --check` on Linux/macOS or `powershell -NoProfile -File tools/docs/render-nbnv2-docs.ps1 -Check` on Windows).

### 19.1 `nbn_common.proto`

```proto
syntax = "proto3";
package nbn;

option csharp_namespace = "Nbn.Proto";

message Uuid {
  bytes value = 1; // 16 bytes, RFC 4122 byte order
}

message Sha256 {
  bytes value = 1; // 32 bytes
}

message Address32 {
  fixed32 value = 1;
}

message ShardId32 {
  fixed32 value = 1; // region_id in bits 16..20, shard_index in bits 0..15
}

message ArtifactRef {
  Sha256 sha256 = 1;
  string media_type = 2;   // "application/x-nbn", "application/x-nbs"
  fixed64 size_bytes = 3;
  string store_uri = 4;    // optional: artifact store base URI or logical name
}

enum Severity {
  SEV_TRACE = 0;
  SEV_DEBUG = 1;
  SEV_INFO  = 2;
  SEV_WARN  = 3;
  SEV_ERROR = 4;
  SEV_FATAL = 5;
}

```

### 19.2 `nbn_functions.proto`

```proto
syntax = "proto3";
package nbn;

option csharp_namespace = "Nbn.Proto";

enum AccumulationFunction {
  ACCUM_SUM     = 0;
  ACCUM_PRODUCT = 1;
  ACCUM_MAX     = 2;
  ACCUM_NONE    = 3;
}

enum ActivationFunction {
  ACT_NONE      = 0;
  ACT_IDENTITY  = 1;
  ACT_STEP_UP   = 2;
  ACT_STEP_MID  = 3;
  ACT_STEP_DOWN = 4;
  ACT_ABS       = 5;
  ACT_CLAMP     = 6;
  ACT_RELU      = 7;
  ACT_NRELU     = 8;
  ACT_SIN       = 9;
  ACT_TAN       = 10;
  ACT_TANH      = 11;
  ACT_ELU       = 12;
  ACT_EXP       = 13;
  ACT_PRELU     = 14;
  ACT_LOG       = 15;
  ACT_MULT      = 16;
  ACT_ADD       = 17;
  ACT_SIG       = 18;
  ACT_SILU      = 19;
  ACT_PCLAMP    = 20;
  ACT_MODL      = 21;
  ACT_MODR      = 22;
  ACT_SOFTP     = 23;
  ACT_SELU      = 24;
  ACT_LIN       = 25;
  ACT_LOGB      = 26;
  ACT_POW       = 27;
  ACT_GAUSS     = 28;
  ACT_QUAD      = 29;

  // 30..63 reserved
}

enum ResetFunction {
  RESET_ZERO = 0;
  RESET_HOLD = 1;
  RESET_CLAMP_POTENTIAL = 2;
  RESET_CLAMP1 = 3;
  RESET_POTENTIAL_CLAMP_BUFFER = 4;
  RESET_NEG_POTENTIAL_CLAMP_BUFFER = 5;
  RESET_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER = 6;
  RESET_TENTH_POTENTIAL_CLAMP_BUFFER = 7;
  RESET_HALF_POTENTIAL_CLAMP_BUFFER = 8;
  RESET_DOUBLE_POTENTIAL_CLAMP_BUFFER = 9;
  RESET_FIVEX_POTENTIAL_CLAMP_BUFFER = 10;
  RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER = 11;
  RESET_NEG_TENTH_POTENTIAL_CLAMP_BUFFER = 12;
  RESET_NEG_HALF_POTENTIAL_CLAMP_BUFFER = 13;
  RESET_NEG_DOUBLE_POTENTIAL_CLAMP_BUFFER = 14;
  RESET_NEG_FIVEX_POTENTIAL_CLAMP_BUFFER = 15;
  RESET_INVERSE_POTENTIAL_CLAMP_BUFFER = 16;

  RESET_POTENTIAL_CLAMP1 = 17;
  RESET_NEG_POTENTIAL_CLAMP1 = 18;
  RESET_HUNDREDTHS_POTENTIAL_CLAMP1 = 19;
  RESET_TENTH_POTENTIAL_CLAMP1 = 20;
  RESET_HALF_POTENTIAL_CLAMP1 = 21;
  RESET_DOUBLE_POTENTIAL_CLAMP1 = 22;
  RESET_FIVEX_POTENTIAL_CLAMP1 = 23;
  RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP1 = 24;
  RESET_NEG_TENTH_POTENTIAL_CLAMP1 = 25;
  RESET_NEG_HALF_POTENTIAL_CLAMP1 = 26;
  RESET_NEG_DOUBLE_POTENTIAL_CLAMP1 = 27;
  RESET_NEG_FIVEX_POTENTIAL_CLAMP1 = 28;
  RESET_INVERSE_POTENTIAL_CLAMP1 = 29;

  RESET_POTENTIAL = 30;
  RESET_NEG_POTENTIAL = 31;
  RESET_HUNDREDTHS_POTENTIAL = 32;
  RESET_TENTH_POTENTIAL = 33;
  RESET_HALF_POTENTIAL = 34;
  RESET_DOUBLE_POTENTIAL = 35;
  RESET_FIVEX_POTENTIAL = 36;
  RESET_NEG_HUNDREDTHS_POTENTIAL = 37;
  RESET_NEG_TENTH_POTENTIAL = 38;
  RESET_NEG_HALF_POTENTIAL = 39;
  RESET_NEG_DOUBLE_POTENTIAL = 40;
  RESET_NEG_FIVEX_POTENTIAL = 41;
  RESET_INVERSE_POTENTIAL = 42;

  RESET_HALF = 43;
  RESET_TENTH = 44;
  RESET_HUNDREDTH = 45;
  RESET_NEGATIVE = 46;
  RESET_NEG_HALF = 47;
  RESET_NEG_TENTH = 48;
  RESET_NEG_HUNDREDTH = 49;

  RESET_DOUBLE_CLAMP1 = 50;
  RESET_FIVEX_CLAMP1 = 51;
  RESET_NEG_DOUBLE_CLAMP1 = 52;
  RESET_NEG_FIVEX_CLAMP1 = 53;

  RESET_DOUBLE = 54;
  RESET_FIVEX = 55;
  RESET_NEG_DOUBLE = 56;
  RESET_NEG_FIVEX = 57;

  RESET_DIVIDE_AXON_CT = 58;
  RESET_INVERSE_CLAMP1 = 59;
  RESET_INVERSE = 60;

  // 61..63 reserved
}
```

### 19.3 `nbn_settings.proto`

```proto
syntax = "proto3";
package nbn.settings;

option csharp_namespace = "Nbn.Proto.Settings";

import "nbn_common.proto";

message NodeOnline {
  nbn.Uuid node_id = 1;
  string logical_name = 2;
  string address = 3;          // host:port
  string root_actor_name = 4;  // stable
}

message NodeOffline {
  nbn.Uuid node_id = 1;
  string logical_name = 2;
}

message NodeHeartbeat {
  nbn.Uuid node_id = 1;
  fixed64 time_ms = 2;
  NodeCapabilities caps = 3;
}

message NodeCapabilities {
  uint32 cpu_cores = 1;
  fixed64 ram_free_bytes = 2;

  bool has_gpu = 3;
  string gpu_name = 4;
  fixed64 vram_free_bytes = 5;

  float cpu_score = 6;
  float gpu_score = 7;

  bool ilgpu_cuda_available = 8;
  bool ilgpu_opencl_available = 9;
  fixed64 storage_free_bytes = 10;
  fixed64 ram_total_bytes = 11;
  fixed64 storage_total_bytes = 12;
  fixed64 vram_total_bytes = 13;
  uint32 cpu_limit_percent = 14;
  uint32 ram_limit_percent = 15;
  uint32 storage_limit_percent = 16;
  uint32 gpu_compute_limit_percent = 17;
  uint32 gpu_vram_limit_percent = 18;
  float process_cpu_load_percent = 19;
  fixed64 process_ram_used_bytes = 20;
}

message NodeStatus {
  nbn.Uuid node_id = 1;
  string logical_name = 2;
  string address = 3;
  string root_actor_name = 4;
  fixed64 last_seen_ms = 5;
  bool is_alive = 6;
}

message BrainStatus {
  nbn.Uuid brain_id = 1;
  fixed64 spawned_ms = 2;
  fixed64 last_tick_id = 3;
  string state = 4;
}

message BrainControllerStatus {
  nbn.Uuid brain_id = 1;
  nbn.Uuid node_id = 2;
  string actor_name = 3;
  fixed64 last_seen_ms = 4;
  bool is_alive = 5;
}

message NodeListRequest { }

message NodeListResponse {
  repeated NodeStatus nodes = 1;
}

message WorkerInventorySnapshotRequest { }

message WorkerReadinessCapability {
  nbn.Uuid node_id = 1;
  string logical_name = 2;
  string address = 3;
  string root_actor_name = 4;
  bool is_alive = 5;
  bool is_ready = 6;
  fixed64 last_seen_ms = 7;
  bool has_capabilities = 8;
  fixed64 capability_time_ms = 9;
  NodeCapabilities capabilities = 10;
}

message WorkerInventorySnapshotResponse {
  repeated WorkerReadinessCapability workers = 1;
  fixed64 snapshot_ms = 2;
}

message BrainListRequest { }

message BrainListResponse {
  repeated BrainStatus brains = 1;
  repeated BrainControllerStatus controllers = 2;
}

message SettingListRequest { }

message SettingListResponse {
  repeated SettingValue settings = 1;
}

message SettingGet {
  string key = 1;
}

message SettingValue {
  string key = 1;
  string value = 2;
  fixed64 updated_ms = 3;
}

message SettingSet {
  string key = 1;
  string value = 2;
}

message SettingChanged {
  string key = 1;
  string value = 2;
  fixed64 updated_ms = 3;
}

message SettingSubscribe {
  string subscriber_actor = 1; // actor name/path
}

message SettingUnsubscribe {
  string subscriber_actor = 1;
}

message BrainRegistered {
  nbn.Uuid brain_id = 1;
  fixed64 spawned_ms = 2;
  fixed64 last_tick_id = 3;
  string state = 4;

  nbn.Uuid controller_node_id = 5;
  string controller_node_address = 6;
  string controller_node_logical_name = 7;
  string controller_root_actor_name = 8;
  string controller_actor_name = 9;
}

message BrainStateChanged {
  nbn.Uuid brain_id = 1;
  string state = 2;
  string notes = 3;
}

message BrainTick {
  nbn.Uuid brain_id = 1;
  fixed64 last_tick_id = 2;
}

message BrainControllerHeartbeat {
  nbn.Uuid brain_id = 1;
  fixed64 time_ms = 2;
}

message BrainUnregistered {
  nbn.Uuid brain_id = 1;
  fixed64 time_ms = 2;
}

```

### 19.4 `nbn_control.proto`

```proto
syntax = "proto3";
package nbn.control;

option csharp_namespace = "Nbn.Proto.Control";

import "nbn_common.proto";

message SpawnBrain {
  nbn.ArtifactRef brain_def = 1; // .nbn
  optional sint32 pause_priority = 2;
}

message SpawnBrainAck {
  nbn.Uuid brain_id = 1;
  string failure_reason_code = 2;
  string failure_message = 3;
}

message PauseBrain {
  nbn.Uuid brain_id = 1;
  string reason = 2;
}

message ResumeBrain {
  nbn.Uuid brain_id = 1;
}

message RegisterBrain {
  nbn.Uuid brain_id = 1;
  string brain_root_pid = 2;
  string signal_router_pid = 3;
  optional sint32 pause_priority = 4;
}

message UpdateBrainSignalRouter {
  nbn.Uuid brain_id = 1;
  string signal_router_pid = 2;
}

message UnregisterBrain {
  nbn.Uuid brain_id = 1;
}

message RegisterShard {
  nbn.Uuid brain_id = 1;
  uint32 region_id = 2;
  uint32 shard_index = 3;
  string shard_pid = 4;
  uint32 neuron_start = 5;
  uint32 neuron_count = 6;
}

message UnregisterShard {
  nbn.Uuid brain_id = 1;
  uint32 region_id = 2;
  uint32 shard_index = 3;
}

message RegisterOutputSink {
  nbn.Uuid brain_id = 1;
  string output_pid = 2;
}

message SetBrainVisualization {
  nbn.Uuid brain_id = 1;
  bool enabled = 2;
  bool has_focus_region = 3;
  uint32 focus_region_id = 4;
  string subscriber_actor = 5; // optional stable subscriber identity (actor path/id)
}

message SetBrainCostEnergy {
  nbn.Uuid brain_id = 1;
  bool cost_enabled = 2;
  bool energy_enabled = 3;
}

message SetBrainPlasticity {
  nbn.Uuid brain_id = 1;
  bool plasticity_enabled = 2;
  float plasticity_rate = 3;
  bool probabilistic_updates = 4;
  float plasticity_delta = 5;
  uint32 plasticity_rebase_threshold = 6;
  float plasticity_rebase_threshold_pct = 7;
  bool plasticity_energy_cost_modulation_enabled = 8;
  sint64 plasticity_energy_cost_reference_tick_cost = 9;
  float plasticity_energy_cost_response_strength = 10;
  float plasticity_energy_cost_min_scale = 11;
  float plasticity_energy_cost_max_scale = 12;
}

enum HomeostasisTargetMode {
  HOMEOSTASIS_TARGET_ZERO = 0;
  HOMEOSTASIS_TARGET_FIXED = 1;
}

enum HomeostasisUpdateMode {
  HOMEOSTASIS_UPDATE_PROBABILISTIC_QUANTIZED_STEP = 0;
}

message SetBrainHomeostasis {
  nbn.Uuid brain_id = 1;
  bool homeostasis_enabled = 2;
  HomeostasisTargetMode homeostasis_target_mode = 3;
  HomeostasisUpdateMode homeostasis_update_mode = 4;
  float homeostasis_base_probability = 5;
  uint32 homeostasis_min_step_codes = 6;
  bool homeostasis_energy_coupling_enabled = 7;
  float homeostasis_energy_target_scale = 8;
  float homeostasis_energy_probability_scale = 9;
}

enum InputCoordinatorMode {
  INPUT_COORDINATOR_MODE_DIRTY_ON_CHANGE = 0;
  INPUT_COORDINATOR_MODE_REPLAY_LATEST_VECTOR = 1;
}

enum OutputVectorSource {
  OUTPUT_VECTOR_SOURCE_POTENTIAL = 0;
  OUTPUT_VECTOR_SOURCE_BUFFER = 1;
}

message UpdateShardOutputSink {
  nbn.Uuid brain_id = 1;
  uint32 region_id = 2;
  uint32 shard_index = 3;
  string output_pid = 4; // empty clears
}

message UpdateShardVisualization {
  nbn.Uuid brain_id = 1;
  uint32 region_id = 2;
  uint32 shard_index = 3;
  bool enabled = 4;
  bool has_focus_region = 5;
  uint32 focus_region_id = 6;
  uint32 viz_stream_min_interval_ms = 7;
}

message UpdateShardRuntimeConfig {
  nbn.Uuid brain_id = 1;
  uint32 region_id = 2;
  uint32 shard_index = 3;
  bool cost_enabled = 4;
  bool energy_enabled = 5;
  bool plasticity_enabled = 6;
  float plasticity_rate = 7;
  bool probabilistic_updates = 8;
  bool debug_enabled = 9;
  nbn.Severity debug_min_severity = 10;
  bool homeostasis_enabled = 11;
  HomeostasisTargetMode homeostasis_target_mode = 12;
  HomeostasisUpdateMode homeostasis_update_mode = 13;
  float homeostasis_base_probability = 14;
  uint32 homeostasis_min_step_codes = 15;
  bool homeostasis_energy_coupling_enabled = 16;
  float homeostasis_energy_target_scale = 17;
  float homeostasis_energy_probability_scale = 18;
  float plasticity_delta = 19;
  uint32 plasticity_rebase_threshold = 20;
  float plasticity_rebase_threshold_pct = 21;
  bool remote_cost_enabled = 22;
  sint64 remote_cost_per_batch = 23;
  sint64 remote_cost_per_contribution = 24;
  float cost_tier_a_multiplier = 25;
  float cost_tier_b_multiplier = 26;
  float cost_tier_c_multiplier = 27;
  bool plasticity_energy_cost_modulation_enabled = 28;
  sint64 plasticity_energy_cost_reference_tick_cost = 29;
  float plasticity_energy_cost_response_strength = 30;
  float plasticity_energy_cost_min_scale = 31;
  float plasticity_energy_cost_max_scale = 32;
  OutputVectorSource output_vector_source = 33;
}

message SnapshotOverlayRecord {
  fixed32 from_address = 1;
  fixed32 to_address = 2;
  uint32 strength_code = 3;
}

message CaptureShardSnapshot {
  nbn.Uuid brain_id = 1;
  uint32 region_id = 2;
  uint32 shard_index = 3;
  fixed64 tick_id = 4;
}

message CaptureShardSnapshotAck {
  nbn.Uuid brain_id = 1;
  uint32 region_id = 2;
  uint32 shard_index = 3;
  uint32 neuron_start = 4;
  uint32 neuron_count = 5;
  repeated sint32 buffer_codes = 6;
  bytes enabled_bitset = 7;
  repeated SnapshotOverlayRecord overlays = 8;
  bool success = 9;
  string error = 10;
}

message GetBrainIoInfo {
  nbn.Uuid brain_id = 1;
}

message BrainIoInfo {
  nbn.Uuid brain_id = 1;
  uint32 input_width = 2;
  uint32 output_width = 3;
  InputCoordinatorMode input_coordinator_mode = 4;
  OutputVectorSource output_vector_source = 5;
  string input_coordinator_pid = 6; // "address/id" or "id" if local
  string output_coordinator_pid = 7; // "address/id" or "id" if local
  bool io_gateway_owns_input_coordinator = 8;
  bool io_gateway_owns_output_coordinator = 9;
}

enum ShardPlanMode {
  SHARD_PLAN_SINGLE = 0;
  SHARD_PLAN_FIXED = 1;
  SHARD_PLAN_MAX_NEURONS = 2;
}

message ShardPlan {
  ShardPlanMode mode = 1;
  uint32 shard_count = 2;
  uint32 max_neurons_per_shard = 3;
}

// Requested -> Assigning -> Assigned -> Running.
// Reconcile paths temporarily use Reconciling; any hard failure enters Failed.
enum PlacementLifecycleState {
  PLACEMENT_LIFECYCLE_UNKNOWN = 0;
  PLACEMENT_LIFECYCLE_REQUESTED = 1;
  PLACEMENT_LIFECYCLE_ASSIGNING = 2;
  PLACEMENT_LIFECYCLE_ASSIGNED = 3;
  PLACEMENT_LIFECYCLE_RUNNING = 4;
  PLACEMENT_LIFECYCLE_RECONCILING = 5;
  PLACEMENT_LIFECYCLE_FAILED = 6;
  PLACEMENT_LIFECYCLE_TERMINATED = 7;
}

enum PlacementFailureReason {
  PLACEMENT_FAILURE_NONE = 0;
  PLACEMENT_FAILURE_INVALID_BRAIN = 1;
  PLACEMENT_FAILURE_WORKER_UNAVAILABLE = 2;
  PLACEMENT_FAILURE_ASSIGNMENT_REJECTED = 3;
  PLACEMENT_FAILURE_ASSIGNMENT_TIMEOUT = 4;
  PLACEMENT_FAILURE_RECONCILE_MISMATCH = 5;
  PLACEMENT_FAILURE_INTERNAL_ERROR = 6;
}

enum PlacementAssignmentTarget {
  PLACEMENT_TARGET_UNKNOWN = 0;
  PLACEMENT_TARGET_BRAIN_ROOT = 1;
  PLACEMENT_TARGET_SIGNAL_ROUTER = 2;
  PLACEMENT_TARGET_REGION_SHARD = 3;
  PLACEMENT_TARGET_INPUT_COORDINATOR = 4;
  PLACEMENT_TARGET_OUTPUT_COORDINATOR = 5;
}

enum PlacementAssignmentState {
  PLACEMENT_ASSIGNMENT_UNKNOWN = 0;
  PLACEMENT_ASSIGNMENT_PENDING = 1;
  PLACEMENT_ASSIGNMENT_ACCEPTED = 2;
  PLACEMENT_ASSIGNMENT_READY = 3;
  PLACEMENT_ASSIGNMENT_FAILED = 4;
  PLACEMENT_ASSIGNMENT_DRAINING = 5;
}

enum PlacementReconcileState {
  PLACEMENT_RECONCILE_UNKNOWN = 0;
  PLACEMENT_RECONCILE_MATCHED = 1;
  PLACEMENT_RECONCILE_REQUIRES_ACTION = 2;
  PLACEMENT_RECONCILE_FAILED = 3;
}

message PlacementWorkerInventoryRequest { }

message PlacementWorkerInventoryEntry {
  nbn.Uuid worker_node_id = 1;
  string worker_address = 2;
  string worker_root_actor_name = 3;
  bool is_alive = 4;
  fixed64 last_seen_ms = 5;
  uint32 cpu_cores = 6;
  fixed64 ram_free_bytes = 7;
  bool has_gpu = 8;
  fixed64 vram_free_bytes = 9;
  float cpu_score = 10;
  float gpu_score = 11;
  fixed64 capability_epoch = 12;
  fixed64 storage_free_bytes = 13;
  float average_peer_latency_ms = 14;
  uint32 peer_latency_sample_count = 15;
  fixed64 ram_total_bytes = 16;
  fixed64 storage_total_bytes = 17;
  fixed64 vram_total_bytes = 18;
  uint32 cpu_limit_percent = 19;
  uint32 ram_limit_percent = 20;
  uint32 storage_limit_percent = 21;
  uint32 gpu_compute_limit_percent = 22;
  uint32 gpu_vram_limit_percent = 23;
  float process_cpu_load_percent = 24;
  fixed64 process_ram_used_bytes = 25;
}

message PlacementWorkerInventory {
  repeated PlacementWorkerInventoryEntry workers = 1;
  fixed64 snapshot_ms = 2;
}

message PlacementPeerTarget {
  nbn.Uuid worker_node_id = 1;
  string worker_address = 2;
  string worker_root_actor_name = 3;
}

message PlacementPeerLatencyRequest {
  repeated PlacementPeerTarget peers = 1;
  uint32 timeout_ms = 2;
}

message PlacementPeerLatencyResponse {
  nbn.Uuid worker_node_id = 1;
  float average_peer_latency_ms = 2;
  uint32 sample_count = 3;
}

message PlacementLatencyEchoRequest { }

message PlacementLatencyEchoAck { }

message WorkerCapabilityRefreshRequest {
  fixed64 requested_ms = 1;
  string reason = 2;
}

message WorkerCapabilityRefreshAck {
  bool accepted = 1;
  fixed64 requested_ms = 2;
  string message = 3;
}

message PlacementAssignment {
  string assignment_id = 1;
  nbn.Uuid brain_id = 2;
  fixed64 placement_epoch = 3;
  PlacementAssignmentTarget target = 4;
  nbn.Uuid worker_node_id = 5;
  uint32 region_id = 6;
  uint32 shard_index = 7;
  uint32 neuron_start = 8;
  uint32 neuron_count = 9;
  string actor_name = 10;
}

message PlacementAssignmentRequest {
  PlacementAssignment assignment = 1;
}

message PlacementAssignmentAck {
  string assignment_id = 1;
  nbn.Uuid brain_id = 2;
  fixed64 placement_epoch = 3;
  PlacementAssignmentState state = 4;
  bool accepted = 5;
  bool retryable = 6;
  PlacementFailureReason failure_reason = 7;
  string message = 8;
  fixed64 retry_after_ms = 9;
}

message PlacementUnassignmentRequest {
  PlacementAssignment assignment = 1;
}

message PlacementUnassignmentAck {
  string assignment_id = 1;
  nbn.Uuid brain_id = 2;
  fixed64 placement_epoch = 3;
  bool accepted = 4;
  bool retryable = 5;
  PlacementFailureReason failure_reason = 6;
  string message = 7;
  fixed64 retry_after_ms = 8;
}

message PlacementReconcileRequest {
  nbn.Uuid brain_id = 1;
  fixed64 placement_epoch = 2;
}

message PlacementObservedAssignment {
  string assignment_id = 1;
  PlacementAssignmentTarget target = 2;
  nbn.Uuid worker_node_id = 3;
  uint32 region_id = 4;
  uint32 shard_index = 5;
  string actor_pid = 6;
}

message PlacementReconcileReport {
  nbn.Uuid brain_id = 1;
  fixed64 placement_epoch = 2;
  PlacementReconcileState reconcile_state = 3;
  repeated PlacementObservedAssignment assignments = 4;
  PlacementFailureReason failure_reason = 5;
  string message = 6;
}

message RequestPlacement {
  nbn.Uuid brain_id = 1;
  nbn.ArtifactRef base_def = 2;
  nbn.ArtifactRef last_snapshot = 3;
  ShardPlan shard_plan = 4;
  uint32 input_width = 5;
  uint32 output_width = 6;
  string request_id = 7;
  fixed64 requested_ms = 8;
  bool is_recovery = 9;
}

message PlacementAck {
  bool accepted = 1;
  string message = 2;
  fixed64 placement_epoch = 3;
  PlacementLifecycleState lifecycle_state = 4;
  PlacementFailureReason failure_reason = 5;
  fixed64 accepted_ms = 6;
  string request_id = 7;
}

message GetPlacementLifecycle {
  nbn.Uuid brain_id = 1;
}

message PlacementLifecycleInfo {
  nbn.Uuid brain_id = 1;
  fixed64 placement_epoch = 2;
  PlacementLifecycleState lifecycle_state = 3;
  PlacementFailureReason failure_reason = 4;
  PlacementReconcileState reconcile_state = 5;
  fixed64 requested_ms = 6;
  fixed64 updated_ms = 7;
  string request_id = 8;
  ShardPlan shard_plan = 9;
  uint32 registered_shards = 10;
}

message KillBrain {
  nbn.Uuid brain_id = 1;
  string reason = 2;
}

message BrainTerminated {
  nbn.Uuid brain_id = 1;
  string reason = 2;
  nbn.ArtifactRef base_def = 3;       // .nbn
  nbn.ArtifactRef last_snapshot = 4;  // .nbs (optional; may be empty sha)
  sint64 last_energy_remaining = 5;
  sint64 last_tick_cost = 6;
  fixed64 time_ms = 7;
}

message TickCompute {
  fixed64 tick_id = 1;
  float target_tick_hz = 2;
}

message TickComputeDone {
  fixed64 tick_id = 1;
  nbn.Uuid brain_id = 2;
  uint32 region_id = 3;
  nbn.ShardId32 shard_id = 4;

  fixed64 compute_ms = 5;

  sint64 tick_cost_total = 6;
  sint64 cost_accum = 7;
  sint64 cost_activation = 8;
  sint64 cost_reset = 9;
  sint64 cost_distance = 10;
  sint64 cost_remote = 11;

  uint32 fired_count = 12;
  uint32 out_batches = 13;
  uint32 out_contribs = 14;
}

message TickDeliver {
  fixed64 tick_id = 1;
}

message TickDeliverDone {
  fixed64 tick_id = 1;
  nbn.Uuid brain_id = 2;
  fixed64 deliver_ms = 3;
  uint32 delivered_batches = 4;
  uint32 delivered_contribs = 5;
}

message GetHiveMindStatus { }

message SetTickRateOverride {
  bool clear_override = 1;
  float target_tick_hz = 2;
}

message SetTickRateOverrideAck {
  bool accepted = 1;
  string message = 2;
  float target_tick_hz = 3;
  bool has_override = 4;
  float override_tick_hz = 5;
}

message HiveMindStatus {
  fixed64 last_completed_tick_id = 1;
  bool tick_loop_enabled = 2;
  float target_tick_hz = 3;
  uint32 pending_compute = 4;
  uint32 pending_deliver = 5;
  bool reschedule_in_progress = 6;
  uint32 registered_brains = 7;
  uint32 registered_shards = 8;
  bool has_tick_rate_override = 9;
  float tick_rate_override_hz = 10;
  float configured_target_tick_hz = 11;
  bool automatic_backpressure_active = 12;
  uint32 recent_tick_sample_count = 13;
  uint32 recent_timeout_tick_count = 14;
  uint32 recent_late_tick_count = 15;
  uint32 worker_pressure_window = 16;
  uint32 current_pressure_worker_count = 17;
  uint32 recent_pressure_worker_count = 18;
}

message GetBrainRouting {
  nbn.Uuid brain_id = 1;
}

message BrainRoutingInfo {
  nbn.Uuid brain_id = 1;
  string brain_root_pid = 2;
  string signal_router_pid = 3;
  uint32 shard_count = 4;
  uint32 routing_count = 5;
}

```

### 19.5 `nbn_signals.proto`

```proto
syntax = "proto3";
package nbn.signal;

option csharp_namespace = "Nbn.Proto.Signal";

import "nbn_common.proto";

message Contribution {
  uint32 target_neuron_id = 1; // local to destination region
  float value = 2;
}

message SignalBatch {
  nbn.Uuid brain_id = 1;
  uint32 region_id = 2;
  nbn.ShardId32 shard_id = 3;
  fixed64 tick_id = 4; // tick_id whose delivery phase is delivering this batch
  repeated Contribution contribs = 5;
}

message SignalBatchAck {
  nbn.Uuid brain_id = 1;
  uint32 region_id = 2;
  nbn.ShardId32 shard_id = 3;
  fixed64 tick_id = 4;
}

// Produced by RegionShard during compute phase, sent to BrainSignalRouter:
message OutboxBatch {
  nbn.Uuid brain_id = 1;
  fixed64 tick_id = 2; // compute tick that produced the outbox
  uint32 dest_region_id = 3;
  nbn.ShardId32 dest_shard_id = 4;
  repeated Contribution contribs = 5;
}
```

### 19.6 `nbn_io.proto`

```proto
syntax = "proto3";
package nbn.io;

option csharp_namespace = "Nbn.Proto.Io";

import "nbn_common.proto";
import "nbn_control.proto";
import "nbn_repro.proto";
import "nbn_speciation.proto";
import "nbn_signals.proto";

message Connect {
  string client_name = 1;
}

message ConnectAck {
  string server_name = 1;
  fixed64 server_time_ms = 2;
}

message GetPlacementWorkerInventory { }

message PlacementWorkerInventoryResult {
  bool success = 1;
  string failure_reason_code = 2;
  string failure_message = 3;
  nbn.control.PlacementWorkerInventory inventory = 4;
}

message BrainInfoRequest {
  nbn.Uuid brain_id = 1;
}

message BrainInfo {
  nbn.Uuid brain_id = 1;
  uint32 input_width = 2;
  uint32 output_width = 3;

  bool cost_enabled = 4;
  bool energy_enabled = 5;
  sint64 energy_remaining = 6;

  bool plasticity_enabled = 7;
  nbn.ArtifactRef base_definition = 8;
  nbn.ArtifactRef last_snapshot = 9;
  sint64 energy_rate_units_per_second = 10;
  float plasticity_rate = 11;
  bool plasticity_probabilistic_updates = 12;
  sint64 last_tick_cost = 13;
  bool homeostasis_enabled = 14;
  nbn.control.HomeostasisTargetMode homeostasis_target_mode = 15;
  nbn.control.HomeostasisUpdateMode homeostasis_update_mode = 16;
  float homeostasis_base_probability = 17;
  uint32 homeostasis_min_step_codes = 18;
  bool homeostasis_energy_coupling_enabled = 19;
  float homeostasis_energy_target_scale = 20;
  float homeostasis_energy_probability_scale = 21;
  float plasticity_delta = 22;
  uint32 plasticity_rebase_threshold = 23;
  float plasticity_rebase_threshold_pct = 24;
  bool plasticity_energy_cost_modulation_enabled = 25;
  sint64 plasticity_energy_cost_reference_tick_cost = 26;
  float plasticity_energy_cost_response_strength = 27;
  float plasticity_energy_cost_min_scale = 28;
  float plasticity_energy_cost_max_scale = 29;
  nbn.control.InputCoordinatorMode input_coordinator_mode = 30;
  nbn.control.OutputVectorSource output_vector_source = 31;
}

message BrainEnergyState {
  sint64 energy_remaining = 1;
  sint64 energy_rate_units_per_second = 2;
  bool cost_enabled = 3;
  bool energy_enabled = 4;
  bool plasticity_enabled = 5;
  float plasticity_rate = 6;
  bool plasticity_probabilistic_updates = 7;
  sint64 last_tick_cost = 8;
  bool homeostasis_enabled = 9;
  nbn.control.HomeostasisTargetMode homeostasis_target_mode = 10;
  nbn.control.HomeostasisUpdateMode homeostasis_update_mode = 11;
  float homeostasis_base_probability = 12;
  uint32 homeostasis_min_step_codes = 13;
  bool homeostasis_energy_coupling_enabled = 14;
  float homeostasis_energy_target_scale = 15;
  float homeostasis_energy_probability_scale = 16;
  float plasticity_delta = 17;
  uint32 plasticity_rebase_threshold = 18;
  float plasticity_rebase_threshold_pct = 19;
  bool plasticity_energy_cost_modulation_enabled = 20;
  sint64 plasticity_energy_cost_reference_tick_cost = 21;
  float plasticity_energy_cost_response_strength = 22;
  float plasticity_energy_cost_min_scale = 23;
  float plasticity_energy_cost_max_scale = 24;
}

message RegisterBrain {
  nbn.Uuid brain_id = 1;
  uint32 input_width = 2;
  uint32 output_width = 3;
  nbn.ArtifactRef base_definition = 4;
  nbn.ArtifactRef last_snapshot = 5;
  BrainEnergyState energy_state = 6;
  bool has_runtime_config = 7;
  bool cost_enabled = 8;
  bool energy_enabled = 9;
  bool plasticity_enabled = 10;
  float plasticity_rate = 11;
  bool plasticity_probabilistic_updates = 12;
  sint64 last_tick_cost = 13;
  bool homeostasis_enabled = 14;
  nbn.control.HomeostasisTargetMode homeostasis_target_mode = 15;
  nbn.control.HomeostasisUpdateMode homeostasis_update_mode = 16;
  float homeostasis_base_probability = 17;
  uint32 homeostasis_min_step_codes = 18;
  bool homeostasis_energy_coupling_enabled = 19;
  float homeostasis_energy_target_scale = 20;
  float homeostasis_energy_probability_scale = 21;
  float plasticity_delta = 22;
  uint32 plasticity_rebase_threshold = 23;
  float plasticity_rebase_threshold_pct = 24;
  bool plasticity_energy_cost_modulation_enabled = 25;
  sint64 plasticity_energy_cost_reference_tick_cost = 26;
  float plasticity_energy_cost_response_strength = 27;
  float plasticity_energy_cost_min_scale = 28;
  float plasticity_energy_cost_max_scale = 29;
  nbn.control.InputCoordinatorMode input_coordinator_mode = 30;
  nbn.control.OutputVectorSource output_vector_source = 31;
  string input_coordinator_pid = 32; // "address/id" or "id" if local
  string output_coordinator_pid = 33; // "address/id" or "id" if local
  bool io_gateway_owns_input_coordinator = 34;
  bool io_gateway_owns_output_coordinator = 35;
}

message UnregisterBrain {
  nbn.Uuid brain_id = 1;
  string reason = 2;
}

message RegisterIoGateway {
  nbn.Uuid brain_id = 1;
  string io_gateway_pid = 2; // "address/id" or "id" if local
}

message SpawnBrainViaIO {
  nbn.control.SpawnBrain request = 1;
}

message SpawnBrainViaIOAck {
  nbn.control.SpawnBrainAck ack = 1;
  string failure_reason_code = 2;
  string failure_message = 3;
}

message KillBrainViaIO {
  nbn.control.KillBrain request = 1;
}

message KillBrainViaIOAck {
  bool accepted = 1;
  string failure_reason_code = 2;
  string failure_message = 3;
}

message SetOutputVectorSource {
  nbn.control.OutputVectorSource output_vector_source = 1;
  nbn.Uuid brain_id = 2;
}

message SetOutputVectorSourceAck {
  bool success = 1;
  string failure_reason_code = 2;
  string failure_message = 3;
  nbn.control.OutputVectorSource output_vector_source = 4;
  nbn.Uuid brain_id = 5;
}

message InputWrite {
  nbn.Uuid brain_id = 1;
  uint32 input_index = 2;
  float value = 3;
}

message InputVector {
  nbn.Uuid brain_id = 1;
  repeated float values = 2;
}

message RuntimeNeuronPulse {
  nbn.Uuid brain_id = 1;
  uint32 target_region_id = 2;
  uint32 target_neuron_id = 3;
  float value = 4;
}

message RuntimeNeuronStateWrite {
  nbn.Uuid brain_id = 1;
  uint32 target_region_id = 2;
  uint32 target_neuron_id = 3;
  bool set_buffer = 4;
  float buffer_value = 5;
  bool set_accumulator = 6;
  float accumulator_value = 7;
}

message DrainInputs {
  nbn.Uuid brain_id = 1;
  fixed64 tick_id = 2;
}

message InputDrain {
  nbn.Uuid brain_id = 1;
  fixed64 tick_id = 2;
  repeated nbn.signal.Contribution contribs = 3;
}

message SubscribeOutputs {
  nbn.Uuid brain_id = 1;
  string subscriber_actor = 2; // optional explicit subscriber pid label ("address/id" or "id")
}

message UnsubscribeOutputs {
  nbn.Uuid brain_id = 1;
  string subscriber_actor = 2; // optional explicit subscriber pid label ("address/id" or "id")
}

message OutputEvent {
  nbn.Uuid brain_id = 1;
  uint32 output_index = 2;
  float value = 3;
  fixed64 tick_id = 4;
}

message SubscribeOutputsVector {
  nbn.Uuid brain_id = 1;
  string subscriber_actor = 2; // optional explicit subscriber pid label ("address/id" or "id")
}

message UnsubscribeOutputsVector {
  nbn.Uuid brain_id = 1;
  string subscriber_actor = 2; // optional explicit subscriber pid label ("address/id" or "id")
}

message OutputVectorEvent {
  nbn.Uuid brain_id = 1;
  fixed64 tick_id = 2;
  // Published by IO as a full brain-level vector ordered by output_index (0..output_width-1).
  // For sharded output regions, IO assembles shard-local segments before publication.
  repeated float values = 3;
}

message OutputVectorSegment {
  nbn.Uuid brain_id = 1;
  fixed64 tick_id = 2;
  uint32 output_index_start = 3;
  repeated float values = 4;
}

message EnergyCredit {
  nbn.Uuid brain_id = 1;
  sint64 amount = 2;
}

message EnergyRate {
  nbn.Uuid brain_id = 1;
  sint64 units_per_second = 2;
}

message SetCostEnergyEnabled {
  nbn.Uuid brain_id = 1;
  bool cost_enabled = 2;
  bool energy_enabled = 3;
}

message SetPlasticityEnabled {
  nbn.Uuid brain_id = 1;
  bool plasticity_enabled = 2;
  float plasticity_rate = 3;
  bool probabilistic_updates = 4;
  float plasticity_delta = 5;
  uint32 plasticity_rebase_threshold = 6;
  float plasticity_rebase_threshold_pct = 7;
  bool plasticity_energy_cost_modulation_enabled = 8;
  sint64 plasticity_energy_cost_reference_tick_cost = 9;
  float plasticity_energy_cost_response_strength = 10;
  float plasticity_energy_cost_min_scale = 11;
  float plasticity_energy_cost_max_scale = 12;
}

message SetHomeostasisEnabled {
  nbn.Uuid brain_id = 1;
  bool homeostasis_enabled = 2;
  nbn.control.HomeostasisTargetMode homeostasis_target_mode = 3;
  nbn.control.HomeostasisUpdateMode homeostasis_update_mode = 4;
  float homeostasis_base_probability = 5;
  uint32 homeostasis_min_step_codes = 6;
  bool homeostasis_energy_coupling_enabled = 7;
  float homeostasis_energy_target_scale = 8;
  float homeostasis_energy_probability_scale = 9;
}

message IoCommandAck {
  nbn.Uuid brain_id = 1;
  string command = 2;
  bool success = 3;
  string message = 4;
  bool has_energy_state = 5;
  BrainEnergyState energy_state = 6;
  bool has_configured_plasticity_enabled = 7;
  bool configured_plasticity_enabled = 8;
  bool has_effective_plasticity_enabled = 9;
  bool effective_plasticity_enabled = 10;
}

message RequestSnapshot {
  nbn.Uuid brain_id = 1;
  bool has_runtime_state = 2;
  sint64 energy_remaining = 3;
  bool cost_enabled = 4;
  bool energy_enabled = 5;
  bool plasticity_enabled = 6;
}

message SnapshotReady {
  nbn.Uuid brain_id = 1;
  nbn.ArtifactRef snapshot = 2; // .nbs
}

message ExportBrainDefinition {
  nbn.Uuid brain_id = 1;
  bool rebase_overlays = 2; // if true, incorporate overlays into base .nbn
}

message BrainDefinitionReady {
  nbn.Uuid brain_id = 1;
  nbn.ArtifactRef brain_def = 2; // .nbn
}

message ReproduceByBrainIds {
  nbn.repro.ReproduceByBrainIdsRequest request = 1;
}

message ReproduceByArtifacts {
  nbn.repro.ReproduceByArtifactsRequest request = 1;
}

message AssessCompatibilityByBrainIds {
  nbn.repro.AssessCompatibilityByBrainIdsRequest request = 1;
}

message AssessCompatibilityByArtifacts {
  nbn.repro.AssessCompatibilityByArtifactsRequest request = 1;
}

message ReproduceResult {
  nbn.repro.ReproduceResult result = 1;
}

message AssessCompatibilityResult {
  nbn.repro.ReproduceResult result = 1;
}

message SpeciationStatus {
  nbn.speciation.SpeciationStatusRequest request = 1;
}

message SpeciationStatusResult {
  nbn.speciation.SpeciationStatusResponse response = 1;
}

message SpeciationGetConfig {
  nbn.speciation.SpeciationGetConfigRequest request = 1;
}

message SpeciationGetConfigResult {
  nbn.speciation.SpeciationGetConfigResponse response = 1;
}

message SpeciationSetConfig {
  nbn.speciation.SpeciationSetConfigRequest request = 1;
}

message SpeciationSetConfigResult {
  nbn.speciation.SpeciationSetConfigResponse response = 1;
}

message SpeciationResetAll {
  nbn.speciation.SpeciationResetAllRequest request = 1;
}

message SpeciationResetAllResult {
  nbn.speciation.SpeciationResetAllResponse response = 1;
}

message SpeciationDeleteEpoch {
  nbn.speciation.SpeciationDeleteEpochRequest request = 1;
}

message SpeciationDeleteEpochResult {
  nbn.speciation.SpeciationDeleteEpochResponse response = 1;
}

message SpeciationEvaluate {
  nbn.speciation.SpeciationEvaluateRequest request = 1;
}

message SpeciationEvaluateResult {
  nbn.speciation.SpeciationEvaluateResponse response = 1;
}

message SpeciationAssign {
  nbn.speciation.SpeciationAssignRequest request = 1;
}

message SpeciationAssignResult {
  nbn.speciation.SpeciationAssignResponse response = 1;
}

message SpeciationBatchEvaluateApply {
  nbn.speciation.SpeciationBatchEvaluateApplyRequest request = 1;
}

message SpeciationBatchEvaluateApplyResult {
  nbn.speciation.SpeciationBatchEvaluateApplyResponse response = 1;
}

message SpeciationListMemberships {
  nbn.speciation.SpeciationListMembershipsRequest request = 1;
}

message SpeciationListMembershipsResult {
  nbn.speciation.SpeciationListMembershipsResponse response = 1;
}

message SpeciationQueryMembership {
  nbn.speciation.SpeciationQueryMembershipRequest request = 1;
}

message SpeciationQueryMembershipResult {
  nbn.speciation.SpeciationQueryMembershipResponse response = 1;
}

message SpeciationListHistory {
  nbn.speciation.SpeciationListHistoryRequest request = 1;
}

message SpeciationListHistoryResult {
  nbn.speciation.SpeciationListHistoryResponse response = 1;
}

```

### 19.7 `nbn_debug.proto`

```proto
syntax = "proto3";
package nbn.debug;

option csharp_namespace = "Nbn.Proto.Debug";

import "nbn_common.proto";

message DebugOutbound {
  nbn.Severity severity = 1;
  string context = 2;
  string summary = 3;
  string message = 4;

  string sender_actor = 5; // string path/id
  string sender_node = 6;  // logical name or node id string
  fixed64 time_ms = 7;
}

message DebugInbound {
  DebugOutbound outbound = 1;
  fixed64 server_received_ms = 2;
}

message DebugSubscribe {
  string subscriber_actor = 1; // actor name/path
  nbn.Severity min_severity = 2;
  string context_regex = 3;
  repeated string include_context_prefixes = 4;
  repeated string exclude_context_prefixes = 5;
  repeated string include_summary_prefixes = 6;
  repeated string exclude_summary_prefixes = 7;
}

message DebugUnsubscribe {
  string subscriber_actor = 1;
}

message DebugFlushAll { }

```

### 19.8 `nbn_viz.proto`

```proto
syntax = "proto3";
package nbn.viz;

option csharp_namespace = "Nbn.Proto.Viz";

import "nbn_common.proto";

enum VizEventType {
  VIZ_UNKNOWN = 0;

  VIZ_BRAIN_SPAWNED = 1;
  VIZ_BRAIN_ACTIVE = 2;
  VIZ_BRAIN_PAUSED = 3;
  VIZ_BRAIN_TERMINATED = 4;

  VIZ_SHARD_SPAWNED = 10;
  VIZ_SHARD_MOVED = 11;

  VIZ_TICK = 20;

  VIZ_NEURON_BUFFER = 30;
  VIZ_NEURON_FIRED = 31;

  VIZ_AXON_SENT = 40;
}

message VizSubscribe {
  string subscriber_actor = 1; // actor name/path
}

message VizUnsubscribe {
  string subscriber_actor = 1;
}

message VizFlushAll { }

message VisualizationEvent {
  string event_id = 1;     // uuid string or monotonic id
  fixed64 time_ms = 2;
  VizEventType type = 3;

  nbn.Uuid brain_id = 4;
  fixed64 tick_id = 5;

  uint32 region_id = 6;
  nbn.ShardId32 shard_id = 7;

  nbn.Address32 source = 8;
  nbn.Address32 target = 9;

  float value = 10;        // signal or buffer/potential depending on event
  float strength = 11;     // axon strength if relevant
}

```

### 19.9 `nbn_repro.proto`

```proto
syntax = "proto3";
package nbn.repro;

option csharp_namespace = "Nbn.Proto.Repro";

import "nbn_common.proto";

enum StrengthSource {
  STRENGTH_BASE_ONLY = 0; // use .nbn only
  STRENGTH_LIVE_CODES = 1; // base + overlay codes from latest .nbs
}

enum SpawnChildPolicy {
  SPAWN_CHILD_DEFAULT_ON = 0; // spawn unless explicitly disabled
  SPAWN_CHILD_NEVER = 1;
  SPAWN_CHILD_ALWAYS = 2;
}

enum PrunePolicy {
  PRUNE_LOWEST_ABS_STRENGTH_FIRST = 0;
  PRUNE_NEW_CONNECTIONS_FIRST = 1;
  PRUNE_RANDOM = 2;
}

message RegionOutDegreeCap {
  uint32 region_id = 1;
  float max_avg_out_degree = 2;
}

message ReproduceLimits {
  // Limits can be specified as absolute and/or percentage.
  // If both are present for the same dimension, the smallest effective limit applies.

  uint32 max_neurons_added_abs = 1;
  float  max_neurons_added_pct = 2;

  uint32 max_neurons_removed_abs = 3;
  float  max_neurons_removed_pct = 4;

  uint32 max_axons_added_abs = 5;
  float  max_axons_added_pct = 6;

  uint32 max_axons_removed_abs = 7;
  float  max_axons_removed_pct = 8;

  uint32 max_regions_added_abs = 9;   // 0 means no region additions
  uint32 max_regions_removed_abs = 10; // 0 means no region removals
}

message ReproduceConfig {
  // Similarity thresholds
  float max_region_span_diff_ratio = 1;
  float max_function_hist_distance = 2;
  float max_connectivity_hist_distance = 3;

  // Region presence handling
  float prob_add_neuron_to_empty_region = 10;
  float prob_remove_last_neuron_from_region = 11;

  // Neuron enable/disable/reactivation within existing regions
  float prob_disable_neuron = 12;
  float prob_reactivate_neuron = 13;

  // Axon handling
  float prob_add_axon = 20;
  float prob_remove_axon = 21;
  float prob_reroute_axon = 22; // reroute an axon (where inbound and outbound neurons both exist), instead of choosing one parents' route
  float prob_reroute_inbound_axon_on_delete = 23; // inbound to deleted neuron
  uint32 inbound_reroute_max_ring_distance = 24; // 0 means unlimited ring distance

  // Value selection for neuron parameter codes
  float prob_choose_parentA = 30;
  float prob_choose_parentB = 31;
  float prob_average = 32;
  float prob_mutate = 33;

  // Function selection/mutation
  float prob_choose_funcA = 40;
  //float prob_choose_funcB = 41; // implicit
  float prob_mutate_func = 42;

  // Strength handling (optional)
  bool strength_transform_enabled = 50;
  float prob_strength_choose_A = 51;
  float prob_strength_choose_B = 52;
  float prob_strength_average = 53;
  float prob_strength_weighted_average = 54;
  float strength_weight_A = 55;
  float strength_weight_B = 56;
  float prob_strength_mutate = 57;

  // Out-degree control
  float max_avg_out_degree_brain = 60;
  PrunePolicy prune_policy = 61;
  repeated RegionOutDegreeCap per_region_out_degree_caps = 62;

  // Limits
  ReproduceLimits limits = 70;

  // Child spawn
  SpawnChildPolicy spawn_child = 80;

  // IO-region neuron-count policy (defaults to true when unset)
  optional bool protect_io_region_neuron_counts = 81;
}

message ManualIoNeuronEdit {
  uint32 region_id = 1; // only input (0) or output (31)
  uint32 neuron_id = 2; // neuron locus within the selected region
}

message ReproduceByBrainIdsRequest {
  nbn.Uuid parentA = 1;
  nbn.Uuid parentB = 2;
  StrengthSource strength_source = 3;
  ReproduceConfig config = 4;
  fixed64 seed = 5;
  repeated ManualIoNeuronEdit manual_io_neuron_adds = 6;
  repeated ManualIoNeuronEdit manual_io_neuron_removes = 7;
  uint32 run_count = 10; // defaults to 1 when unset or 0
}

message ReproduceByArtifactsRequest {
  nbn.ArtifactRef parentA_def = 1; // .nbn
  nbn.ArtifactRef parentA_state = 2; // optional .nbs
  nbn.ArtifactRef parentB_def = 3; // .nbn
  nbn.ArtifactRef parentB_state = 4; // optional .nbs
  StrengthSource strength_source = 5;
  ReproduceConfig config = 6;
  fixed64 seed = 7;
  repeated ManualIoNeuronEdit manual_io_neuron_adds = 8;
  repeated ManualIoNeuronEdit manual_io_neuron_removes = 9;
  uint32 run_count = 10; // defaults to 1 when unset or 0
}

message AssessCompatibilityByBrainIdsRequest {
  nbn.Uuid parentA = 1;
  nbn.Uuid parentB = 2;
  StrengthSource strength_source = 3;
  ReproduceConfig config = 4;
  fixed64 seed = 5;
  repeated ManualIoNeuronEdit manual_io_neuron_adds = 6;
  repeated ManualIoNeuronEdit manual_io_neuron_removes = 7;
  uint32 run_count = 10; // defaults to 1 when unset or 0
}

message AssessCompatibilityByArtifactsRequest {
  nbn.ArtifactRef parentA_def = 1; // .nbn
  nbn.ArtifactRef parentA_state = 2; // optional .nbs
  nbn.ArtifactRef parentB_def = 3; // .nbn
  nbn.ArtifactRef parentB_state = 4; // optional .nbs
  StrengthSource strength_source = 5;
  ReproduceConfig config = 6;
  fixed64 seed = 7;
  repeated ManualIoNeuronEdit manual_io_neuron_adds = 8;
  repeated ManualIoNeuronEdit manual_io_neuron_removes = 9;
  uint32 run_count = 10; // defaults to 1 when unset or 0
}

message SimilarityReport {
  bool compatible = 1;
  string abort_reason = 2;

  float region_span_score = 10;
  float function_score = 11;
  float connectivity_score = 12;
  float similarity_score = 13;
  float lineage_similarity_score = 14;
  float lineage_parent_a_similarity_score = 15;
  float lineage_parent_b_similarity_score = 16;

  uint32 regions_present_A = 20;
  uint32 regions_present_B = 21;
  uint32 regions_present_child = 22;
}

message MutationSummary {
  uint32 neurons_added = 1;
  uint32 neurons_removed = 2;
  uint32 axons_added = 3;
  uint32 axons_removed = 4;
  uint32 axons_rerouted = 5;
  uint32 functions_mutated = 6;
  uint32 strength_codes_changed = 7;
}

message ReproduceRunOutcome {
  uint32 run_index = 1;
  fixed64 seed = 2;
  SimilarityReport report = 3;
  MutationSummary summary = 4;

  nbn.ArtifactRef child_def = 10; // .nbn when synthesized
  bool spawned = 11;
  nbn.Uuid child_brain_id = 12; // valid if spawned==true
}

message ReproduceResult {
  SimilarityReport report = 1;
  MutationSummary summary = 2;

  nbn.ArtifactRef child_def = 10; // .nbn
  bool spawned = 11;
  nbn.Uuid child_brain_id = 12; // valid if spawned==true
  repeated ReproduceRunOutcome runs = 20; // deterministic order by run_index
  uint32 requested_run_count = 21; // normalized effective run count
}

```

### 19.10 `nbn_speciation.proto`

Implementation semantics used by current runtime assignment engine:

- For `brain_id`, `artifact_ref`, or `artifact_uri` candidates without explicit `species_id`, runtime assignment derives species deterministically from parent membership evidence (`parents`) plus optional similarity metrics in `decision_metadata_json`. Runtime keeps source-species similarity and assigned-species similarity distinct: best-fit parent-species pairwise evidence drives split/hindsight logic, runtime only falls back to a lineage source when parent lineage is otherwise unambiguous, and assigned-species pairwise evidence drives intra-species cohesion.
- `SpeciationRuntimeConfig.config_snapshot_json` may include an `assignment_policy` object (`lineage_match_threshold`, `lineage_split_threshold`, `parent_consensus_threshold`, `lineage_hysteresis_margin`, `lineage_split_guard_margin`, `lineage_min_parent_memberships_before_split`, `lineage_realign_parent_membership_window`, `lineage_realign_match_margin`, `lineage_hindsight_reassign_commit_window`, `lineage_hindsight_similarity_margin`, `create_derived_species_on_divergence`, `derived_species_prefix`) controlling threshold, anti-fragmentation, bounded realignment, bounded founder-similarity clustering around recent splits, and dynamic floor relaxation.
- Assignment provenance is recorded in `SpeciationDecision.decision_metadata_json` (strategy, policy snapshot, lineage inputs, parsed scores, assigned-species similarity, source/dominant-species similarity, optional `lineage.intra_species_similarity_sample`, optional machine-usable candidate artifact refs, optional `brain_id` candidate base/snapshot artifact refs for later offline reassessment, compatibility-assessment provenance for external derived-species admission, dynamic split-threshold source/values, and split-proximity deltas). Split founders still record `lineage.intra_species_similarity_sample = 1.0` as singleton cohesion metadata, but that synthetic founder score does not seed the dynamic species floor or satisfy the newborn re-split guard. A newborn derived species remains in bootstrap mode until it records three non-founder compatibility-backed in-species samples; until then, runtime emits assigned-species thresholds from policy-only state and suppresses heuristic/source carryover from seeding the derived floor. If the actor has lost its in-memory recent-split hint, it rebuilds bootstrap source lineage from the persisted founder row for that target species before allowing same-species continuity. When speciation evaluates bootstrap admission and then falls back, metadata still preserves the attempted compatibility assessment (`assigned_species_compatibility_attempted`, `assigned_species_compatibility_admitted`, `assigned_species_compatibility_failure_reason`, `assigned_species_compatibility_elapsed_ms`, score when available, and exemplar IDs) so timeout/unavailable paths remain distinguishable from score-based rejection.
- Membership remains immutable for external callers within an epoch; changing species still requires new-epoch flow (`SpeciationSetConfig.start_new_epoch=true` or reset). Runtime may still apply bounded recent-split realignment, hindsight reassignment, and recent-derived reuse around a split, but only after speciation can issue an assessment-only compatibility request to reproduction and confirm the candidate is more compatible with the target derived species than with the source species. The same assessment-only path is now required for any pre-bootstrap admission into a newborn derived species, including same-species continuity inside that newborn lineage; bootstrap admissions must clear the assigned species' current dynamic split threshold, and bootstrap source-species comparison only demands a strict win once the gap exceeds the configured split-guard margin. During that newborn bootstrap window, speciation evaluates against one fixed earliest exemplar from the target species instead of taking a rolling minimum across every early member, so the bootstrap gate/workload stays stable while the species grows from its founder toward the first three non-founder compatibility-backed samples. Speciation owns the final admit/reject decision from those checks: if reproduction returns a finite manual similarity score, speciation may use that score even when `SimilarityReport.compatible=false` so long as the abort reason reflects a real similarity mismatch rather than lookup/load/runtime failure. When a committed `brain_id` exemplar or candidate is no longer discoverable through live IO/reproduction lookups, speciation prefers the persisted artifact refs from decision metadata and re-runs the same admission check in artifact mode; only rows without durable artifact provenance fall back to live `brain_id` assessment. Hindsight reassignment evaluates each founder batch against a fixed snapshot of the newborn species' earliest exemplars so the batch cannot recursively expand its own compatibility fan-out, and founder commits enqueue that hindsight work after the founder assignment instead of blocking the originating commit response. If that assessment is unavailable, times out, or is insufficient, runtime falls back to the pre-split source species instead of seeding the newborn floor from heuristics, but the fallback row still records whether speciation actually reached reproduction and whether a compatibility score was obtained before rejection.
- `decision_metadata_json.lineage.source_species_*` is canonical for current runtime decisions; `lineage.dominant_species_*` remains as a compatibility alias carrying the same resolved source species when present so older tools/history readers continue to parse newer rows.

```proto
syntax = "proto3";
package nbn.speciation;

option csharp_namespace = "Nbn.Proto.Speciation";

import "nbn_common.proto";

enum SpeciationFailureReason {
  SPECIATION_FAILURE_NONE = 0;
  SPECIATION_FAILURE_SERVICE_INITIALIZING = 1;
  SPECIATION_FAILURE_SERVICE_UNAVAILABLE = 2;
  SPECIATION_FAILURE_INVALID_REQUEST = 3;
  SPECIATION_FAILURE_INVALID_CANDIDATE = 4;
  SPECIATION_FAILURE_UNSUPPORTED_CANDIDATE = 5;
  SPECIATION_FAILURE_STORE_ERROR = 6;
  SPECIATION_FAILURE_MEMBERSHIP_IMMUTABLE = 7;
  SPECIATION_FAILURE_EMPTY_RESPONSE = 8;
  SPECIATION_FAILURE_REQUEST_FAILED = 9;
}

enum SpeciationApplyMode {
  SPECIATION_APPLY_MODE_DRY_RUN = 0;
  SPECIATION_APPLY_MODE_COMMIT = 1;
}

enum SpeciationCandidateMode {
  SPECIATION_CANDIDATE_MODE_UNKNOWN = 0;
  SPECIATION_CANDIDATE_MODE_BRAIN_ID = 1;
  SPECIATION_CANDIDATE_MODE_ARTIFACT_REF = 2;
  SPECIATION_CANDIDATE_MODE_ARTIFACT_URI = 3;
}

message SpeciationRuntimeConfig {
  string policy_version = 1;
  string config_snapshot_json = 2;
  string default_species_id = 3;
  string default_species_display_name = 4;
  string startup_reconcile_decision_reason = 5;
}

message SpeciationEpochInfo {
  fixed64 epoch_id = 1;
  fixed64 created_ms = 2;
  string policy_version = 3;
  string config_snapshot_json = 4;
}

message SpeciationStatusSnapshot {
  fixed64 epoch_id = 1;
  uint32 membership_count = 2;
  uint32 species_count = 3;
  uint32 lineage_edge_count = 4;
}

message SpeciationMembershipRecord {
  fixed64 epoch_id = 1;
  nbn.Uuid brain_id = 2;
  string species_id = 3;
  string species_display_name = 4;
  fixed64 assigned_ms = 5;
  string policy_version = 6;
  string decision_reason = 7;
  string decision_metadata_json = 8;
  bool has_source_brain_id = 9;
  nbn.Uuid source_brain_id = 10;
  string source_artifact_ref = 11;
  fixed64 decision_id = 12;
}

message SpeciationCandidateRef {
  oneof candidate {
    nbn.Uuid brain_id = 1;
    nbn.ArtifactRef artifact_ref = 2;
    string artifact_uri = 3;
  }
}

message SpeciationParentRef {
  oneof parent {
    nbn.Uuid brain_id = 1;
    nbn.ArtifactRef artifact_ref = 2;
    string artifact_uri = 3;
  }
}

message SpeciationDecision {
  SpeciationApplyMode apply_mode = 1;
  SpeciationCandidateMode candidate_mode = 2;
  bool success = 3;
  bool created = 4;
  bool immutable_conflict = 5;
  SpeciationFailureReason failure_reason = 6;
  string failure_detail = 7;
  string species_id = 8;
  string species_display_name = 9;
  string decision_reason = 10;
  string decision_metadata_json = 11;
  bool committed = 12;
  SpeciationMembershipRecord membership = 13;
}

message SpeciationStatusRequest {}

message SpeciationStatusResponse {
  SpeciationFailureReason failure_reason = 1;
  string failure_detail = 2;
  SpeciationStatusSnapshot status = 3;
  SpeciationEpochInfo current_epoch = 4;
  SpeciationRuntimeConfig config = 5;
}

message SpeciationGetConfigRequest {}

message SpeciationGetConfigResponse {
  SpeciationFailureReason failure_reason = 1;
  string failure_detail = 2;
  SpeciationRuntimeConfig config = 3;
  SpeciationEpochInfo current_epoch = 4;
}

message SpeciationSetConfigRequest {
  SpeciationRuntimeConfig config = 1;
  bool start_new_epoch = 2;
  fixed64 apply_time_ms = 3;
  bool has_apply_time_ms = 4;
}

message SpeciationSetConfigResponse {
  SpeciationFailureReason failure_reason = 1;
  string failure_detail = 2;
  SpeciationEpochInfo previous_epoch = 3;
  SpeciationEpochInfo current_epoch = 4;
  SpeciationRuntimeConfig config = 5;
}

message SpeciationResetAllRequest {
  fixed64 apply_time_ms = 1;
  bool has_apply_time_ms = 2;
}

message SpeciationResetAllResponse {
  SpeciationFailureReason failure_reason = 1;
  string failure_detail = 2;
  SpeciationEpochInfo previous_epoch = 3;
  SpeciationEpochInfo current_epoch = 4;
  SpeciationRuntimeConfig config = 5;
  uint32 deleted_epoch_count = 6;
  uint32 deleted_membership_count = 7;
  uint32 deleted_species_count = 8;
  uint32 deleted_decision_count = 9;
  uint32 deleted_lineage_edge_count = 10;
}

message SpeciationDeleteEpochRequest {
  fixed64 epoch_id = 1;
}

message SpeciationDeleteEpochResponse {
  SpeciationFailureReason failure_reason = 1;
  string failure_detail = 2;
  fixed64 epoch_id = 3;
  bool deleted = 4;
  uint32 deleted_membership_count = 5;
  uint32 deleted_species_count = 6;
  uint32 deleted_decision_count = 7;
  uint32 deleted_lineage_edge_count = 8;
  SpeciationEpochInfo current_epoch = 9;
}

message SpeciationEvaluateRequest {
  SpeciationCandidateRef candidate = 1;
  repeated SpeciationParentRef parents = 2;
  string species_id = 3;
  string species_display_name = 4;
  string policy_version = 5;
  string decision_reason = 6;
  string decision_metadata_json = 7;
  fixed64 decision_time_ms = 8;
  bool has_decision_time_ms = 9;
}

message SpeciationEvaluateResponse {
  SpeciationDecision decision = 1;
}

message SpeciationAssignRequest {
  SpeciationApplyMode apply_mode = 1;
  SpeciationCandidateRef candidate = 2;
  repeated SpeciationParentRef parents = 3;
  string species_id = 4;
  string species_display_name = 5;
  string policy_version = 6;
  string decision_reason = 7;
  string decision_metadata_json = 8;
  fixed64 decision_time_ms = 9;
  bool has_decision_time_ms = 10;
}

message SpeciationAssignResponse {
  SpeciationDecision decision = 1;
}

message SpeciationBatchItem {
  string item_id = 1;
  SpeciationCandidateRef candidate = 2;
  repeated SpeciationParentRef parents = 3;
  string species_id = 4;
  string species_display_name = 5;
  string policy_version = 6;
  string decision_reason = 7;
  string decision_metadata_json = 8;
  fixed64 decision_time_ms = 9;
  bool has_decision_time_ms = 10;
  SpeciationApplyMode apply_mode_override = 11;
  bool has_apply_mode_override = 12;
}

message SpeciationBatchEvaluateApplyRequest {
  SpeciationApplyMode apply_mode = 1;
  repeated SpeciationBatchItem items = 2;
}

message SpeciationBatchItemResult {
  string item_id = 1;
  SpeciationDecision decision = 2;
}

message SpeciationBatchEvaluateApplyResponse {
  SpeciationFailureReason failure_reason = 1;
  string failure_detail = 2;
  SpeciationApplyMode apply_mode = 3;
  uint32 requested_count = 4;
  uint32 processed_count = 5;
  uint32 committed_count = 6;
  repeated SpeciationBatchItemResult results = 7;
}

message SpeciationListMembershipsRequest {
  bool has_epoch_id = 1;
  fixed64 epoch_id = 2;
}

message SpeciationListMembershipsResponse {
  SpeciationFailureReason failure_reason = 1;
  string failure_detail = 2;
  repeated SpeciationMembershipRecord memberships = 3;
}

message SpeciationQueryMembershipRequest {
  nbn.Uuid brain_id = 1;
  bool has_epoch_id = 2;
  fixed64 epoch_id = 3;
}

message SpeciationQueryMembershipResponse {
  SpeciationFailureReason failure_reason = 1;
  string failure_detail = 2;
  bool found = 3;
  SpeciationMembershipRecord membership = 4;
}

message SpeciationListHistoryRequest {
  bool has_epoch_id = 1;
  fixed64 epoch_id = 2;
  bool has_brain_id = 3;
  nbn.Uuid brain_id = 4;
  uint32 limit = 5;
  uint32 offset = 6;
}

message SpeciationListHistoryResponse {
  SpeciationFailureReason failure_reason = 1;
  string failure_detail = 2;
  repeated SpeciationMembershipRecord history = 3;
  uint32 total_records = 4;
}

```
