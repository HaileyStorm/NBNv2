## 19. Protocol schemas (`.proto`)

The following `.proto` files define the canonical NBN wire schema. They are intended for Proto.Actor remote messaging and for External World integration.

### 19.1 `nbn_common.proto`

```proto
syntax = "proto3";
package nbn;

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

import "nbn_common.proto";

message SpawnBrain {
  nbn.ArtifactRef brain_def = 1; // .nbn
}

message SpawnBrainAck {
  nbn.Uuid brain_id = 1;
}

message PauseBrain {
  nbn.Uuid brain_id = 1;
  string reason = 2;
}

message ResumeBrain {
  nbn.Uuid brain_id = 1;
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

message HiveMindStatus {
  fixed64 last_completed_tick_id = 1;
  bool tick_loop_enabled = 2;
  float target_tick_hz = 3;
  uint32 pending_compute = 4;
  uint32 pending_deliver = 5;
  bool reschedule_in_progress = 6;
  uint32 registered_brains = 7;
  uint32 registered_shards = 8;
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

message RegisterShard {
  nbn.Uuid brain_id = 1;
  uint32 region_id = 2;
  uint32 shard_index = 3;
  string shard_pid = 4;
  uint32 neuron_start = 5;
  uint32 neuron_count = 6;
}

message RegisterOutputSink {
  nbn.Uuid brain_id = 1;
  string output_pid = 2;
}

message UpdateShardOutputSink {
  nbn.Uuid brain_id = 1;
  uint32 region_id = 2;
  uint32 shard_index = 3;
  string output_pid = 4; // empty clears
}

message GetBrainIoInfo {
  nbn.Uuid brain_id = 1;
}

message BrainIoInfo {
  nbn.Uuid brain_id = 1;
  uint32 input_width = 2;
  uint32 output_width = 3;
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
}
```

### 19.5 `nbn_signals.proto`

```proto
syntax = "proto3";
package nbn.signal;

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

import "nbn_common.proto";
import "nbn_control.proto";
import "nbn_repro.proto";
import "nbn_signals.proto";

message Connect {
  string client_name = 1;
}

message ConnectAck {
  string server_name = 1;
  fixed64 server_time_ms = 2;
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
}

message UnsubscribeOutputs {
  nbn.Uuid brain_id = 1;
}

message OutputEvent {
  nbn.Uuid brain_id = 1;
  uint32 output_index = 2;
  float value = 3;
  fixed64 tick_id = 4;
}

message SubscribeOutputsVector {
  nbn.Uuid brain_id = 1;
}

message UnsubscribeOutputsVector {
  nbn.Uuid brain_id = 1;
}

message OutputVectorEvent {
  nbn.Uuid brain_id = 1;
  fixed64 tick_id = 2;
  repeated float values = 3;
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
  BrainEnergyState energy_state = 6; // set when has_energy_state=true
}

message RequestSnapshot {
  nbn.Uuid brain_id = 1;
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

message ReproduceResult {
  nbn.repro.ReproduceResult result = 1;
}
```

### 19.7 `nbn_debug.proto`

```proto
syntax = "proto3";
package nbn.debug;

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

  uint32 max_regions_added_abs = 9;
  uint32 max_regions_removed_abs = 10;
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
}

message ReproduceByBrainIdsRequest {
  nbn.Uuid parentA = 1;
  nbn.Uuid parentB = 2;
  StrengthSource strength_source = 3;
  ReproduceConfig config = 4;
  fixed64 seed = 5;
}

message ReproduceByArtifactsRequest {
  nbn.ArtifactRef parentA_def = 1; // .nbn
  nbn.ArtifactRef parentA_state = 2; // optional .nbs
  nbn.ArtifactRef parentB_def = 3; // .nbn
  nbn.ArtifactRef parentB_state = 4; // optional .nbs
  StrengthSource strength_source = 5;
  ReproduceConfig config = 6;
  fixed64 seed = 7;
}

message SimilarityReport {
  bool compatible = 1;
  string abort_reason = 2;

  float region_span_score = 10;
  float function_score = 11;
  float connectivity_score = 12;

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

message ReproduceResult {
  SimilarityReport report = 1;
  MutationSummary summary = 2;

  nbn.ArtifactRef child_def = 10; // .nbn
  bool spawned = 11;
  nbn.Uuid child_brain_id = 12; // valid if spawned==true
}
```

---
