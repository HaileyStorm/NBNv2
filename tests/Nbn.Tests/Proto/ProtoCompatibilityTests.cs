using System.Linq;
using Google.Protobuf.Reflection;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Nbn.Proto.Settings;
using Nbn.Proto.Signal;
using Nbn.Proto.Viz;
using Xunit;

namespace Nbn.Tests.Proto;

public class ProtoCompatibilityTests
{
    [Fact]
    public void ProtoDescriptors_PackagesMatchSpec()
    {
        Assert.Equal("nbn", NbnCommonReflection.Descriptor.Package);
        Assert.Equal("nbn", NbnFunctionsReflection.Descriptor.Package);
        Assert.Equal("nbn.control", NbnControlReflection.Descriptor.Package);
        Assert.Equal("nbn.io", NbnIoReflection.Descriptor.Package);
        Assert.Equal("nbn.repro", NbnReproReflection.Descriptor.Package);
        Assert.Equal("nbn.settings", NbnSettingsReflection.Descriptor.Package);
        Assert.Equal("nbn.signal", NbnSignalsReflection.Descriptor.Package);
        Assert.Equal("nbn.debug", NbnDebugReflection.Descriptor.Package);
        Assert.Equal("nbn.viz", NbnVizReflection.Descriptor.Package);
    }

    [Fact]
    public void ProtoCommon_ArtifactRefFields_AreStable()
    {
        var descriptor = NbnCommonReflection.Descriptor;
        var artifactRef = descriptor.MessageTypes.Single(message => message.Name == "ArtifactRef");

        AssertField(artifactRef, "sha256", 1, FieldType.Message, "nbn.Sha256");
        AssertField(artifactRef, "media_type", 2, FieldType.String);
        AssertField(artifactRef, "size_bytes", 3, FieldType.Fixed64);
        AssertField(artifactRef, "store_uri", 4, FieldType.String);

        var uuid = descriptor.MessageTypes.Single(message => message.Name == "Uuid");
        AssertField(uuid, "value", 1, FieldType.Bytes);
    }

    [Fact]
    public void ProtoCommon_AddressAndShardFields_AreStable()
    {
        var descriptor = NbnCommonReflection.Descriptor;

        var address32 = descriptor.MessageTypes.Single(message => message.Name == "Address32");
        AssertField(address32, "value", 1, FieldType.Fixed32);

        var shardId32 = descriptor.MessageTypes.Single(message => message.Name == "ShardId32");
        AssertField(shardId32, "value", 1, FieldType.Fixed32);
    }

    [Fact]
    public void ProtoControl_SpawnBrainAck_Fields_AreStable()
    {
        var descriptor = NbnControlReflection.Descriptor;
        var spawnAck = descriptor.MessageTypes.Single(message => message.Name == "SpawnBrainAck");

        AssertField(spawnAck, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(spawnAck, "failure_reason_code", 2, FieldType.String);
        AssertField(spawnAck, "failure_message", 3, FieldType.String);
    }

    [Fact]
    public void ProtoControl_TickComputeFields_AreStable()
    {
        var descriptor = NbnControlReflection.Descriptor;
        var tickCompute = descriptor.MessageTypes.Single(message => message.Name == "TickCompute");

        AssertField(tickCompute, "tick_id", 1, FieldType.Fixed64);
        AssertField(tickCompute, "target_tick_hz", 2, FieldType.Float);
    }

    [Fact]
    public void ProtoControl_TickRateOverrideFields_AreStable()
    {
        var descriptor = NbnControlReflection.Descriptor;

        var setOverride = descriptor.MessageTypes.Single(message => message.Name == "SetTickRateOverride");
        AssertField(setOverride, "clear_override", 1, FieldType.Bool);
        AssertField(setOverride, "target_tick_hz", 2, FieldType.Float);

        var setOverrideAck = descriptor.MessageTypes.Single(message => message.Name == "SetTickRateOverrideAck");
        AssertField(setOverrideAck, "accepted", 1, FieldType.Bool);
        AssertField(setOverrideAck, "message", 2, FieldType.String);
        AssertField(setOverrideAck, "target_tick_hz", 3, FieldType.Float);
        AssertField(setOverrideAck, "has_override", 4, FieldType.Bool);
        AssertField(setOverrideAck, "override_tick_hz", 5, FieldType.Float);

        var status = descriptor.MessageTypes.Single(message => message.Name == "HiveMindStatus");
        AssertField(status, "has_tick_rate_override", 9, FieldType.Bool);
        AssertField(status, "tick_rate_override_hz", 10, FieldType.Float);
    }

    [Fact]
    public void ProtoControl_RuntimeConfigFields_AreStable()
    {
        var descriptor = NbnControlReflection.Descriptor;

        var costEnergy = descriptor.MessageTypes.Single(message => message.Name == "SetBrainCostEnergy");
        AssertField(costEnergy, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(costEnergy, "cost_enabled", 2, FieldType.Bool);
        AssertField(costEnergy, "energy_enabled", 3, FieldType.Bool);

        var plasticity = descriptor.MessageTypes.Single(message => message.Name == "SetBrainPlasticity");
        AssertField(plasticity, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(plasticity, "plasticity_enabled", 2, FieldType.Bool);
        AssertField(plasticity, "plasticity_rate", 3, FieldType.Float);
        AssertField(plasticity, "probabilistic_updates", 4, FieldType.Bool);
        AssertField(plasticity, "plasticity_delta", 5, FieldType.Float);
        AssertField(plasticity, "plasticity_rebase_threshold", 6, FieldType.UInt32);
        AssertField(plasticity, "plasticity_rebase_threshold_pct", 7, FieldType.Float);
        AssertField(plasticity, "plasticity_energy_cost_modulation_enabled", 8, FieldType.Bool);
        AssertField(plasticity, "plasticity_energy_cost_reference_tick_cost", 9, FieldType.SInt64);
        AssertField(plasticity, "plasticity_energy_cost_response_strength", 10, FieldType.Float);
        AssertField(plasticity, "plasticity_energy_cost_min_scale", 11, FieldType.Float);
        AssertField(plasticity, "plasticity_energy_cost_max_scale", 12, FieldType.Float);

        var homeostasis = descriptor.MessageTypes.Single(message => message.Name == "SetBrainHomeostasis");
        AssertField(homeostasis, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(homeostasis, "homeostasis_enabled", 2, FieldType.Bool);
        AssertField(homeostasis, "homeostasis_target_mode", 3, FieldType.Enum, "nbn.control.HomeostasisTargetMode");
        AssertField(homeostasis, "homeostasis_update_mode", 4, FieldType.Enum, "nbn.control.HomeostasisUpdateMode");
        AssertField(homeostasis, "homeostasis_base_probability", 5, FieldType.Float);
        AssertField(homeostasis, "homeostasis_min_step_codes", 6, FieldType.UInt32);
        AssertField(homeostasis, "homeostasis_energy_coupling_enabled", 7, FieldType.Bool);
        AssertField(homeostasis, "homeostasis_energy_target_scale", 8, FieldType.Float);
        AssertField(homeostasis, "homeostasis_energy_probability_scale", 9, FieldType.Float);

        var inputCoordinatorMode = descriptor.EnumTypes.Single(@enum => @enum.Name == "InputCoordinatorMode");
        AssertEnumValue(inputCoordinatorMode, "INPUT_COORDINATOR_MODE_DIRTY_ON_CHANGE", 0);
        AssertEnumValue(inputCoordinatorMode, "INPUT_COORDINATOR_MODE_REPLAY_LATEST_VECTOR", 1);

        var outputVectorSource = descriptor.EnumTypes.Single(@enum => @enum.Name == "OutputVectorSource");
        AssertEnumValue(outputVectorSource, "OUTPUT_VECTOR_SOURCE_POTENTIAL", 0);
        AssertEnumValue(outputVectorSource, "OUTPUT_VECTOR_SOURCE_BUFFER", 1);

        var shardVisualization = descriptor.MessageTypes.Single(message => message.Name == "UpdateShardVisualization");
        AssertField(shardVisualization, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(shardVisualization, "region_id", 2, FieldType.UInt32);
        AssertField(shardVisualization, "shard_index", 3, FieldType.UInt32);
        AssertField(shardVisualization, "enabled", 4, FieldType.Bool);
        AssertField(shardVisualization, "has_focus_region", 5, FieldType.Bool);
        AssertField(shardVisualization, "focus_region_id", 6, FieldType.UInt32);
        AssertField(shardVisualization, "viz_stream_min_interval_ms", 7, FieldType.UInt32);

        var shardRuntime = descriptor.MessageTypes.Single(message => message.Name == "UpdateShardRuntimeConfig");
        AssertField(shardRuntime, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(shardRuntime, "region_id", 2, FieldType.UInt32);
        AssertField(shardRuntime, "shard_index", 3, FieldType.UInt32);
        AssertField(shardRuntime, "cost_enabled", 4, FieldType.Bool);
        AssertField(shardRuntime, "energy_enabled", 5, FieldType.Bool);
        AssertField(shardRuntime, "plasticity_enabled", 6, FieldType.Bool);
        AssertField(shardRuntime, "plasticity_rate", 7, FieldType.Float);
        AssertField(shardRuntime, "probabilistic_updates", 8, FieldType.Bool);
        AssertField(shardRuntime, "debug_enabled", 9, FieldType.Bool);
        AssertField(shardRuntime, "debug_min_severity", 10, FieldType.Enum, "nbn.Severity");
        AssertField(shardRuntime, "homeostasis_enabled", 11, FieldType.Bool);
        AssertField(shardRuntime, "homeostasis_target_mode", 12, FieldType.Enum, "nbn.control.HomeostasisTargetMode");
        AssertField(shardRuntime, "homeostasis_update_mode", 13, FieldType.Enum, "nbn.control.HomeostasisUpdateMode");
        AssertField(shardRuntime, "homeostasis_base_probability", 14, FieldType.Float);
        AssertField(shardRuntime, "homeostasis_min_step_codes", 15, FieldType.UInt32);
        AssertField(shardRuntime, "homeostasis_energy_coupling_enabled", 16, FieldType.Bool);
        AssertField(shardRuntime, "homeostasis_energy_target_scale", 17, FieldType.Float);
        AssertField(shardRuntime, "homeostasis_energy_probability_scale", 18, FieldType.Float);
        AssertField(shardRuntime, "plasticity_delta", 19, FieldType.Float);
        AssertField(shardRuntime, "plasticity_rebase_threshold", 20, FieldType.UInt32);
        AssertField(shardRuntime, "plasticity_rebase_threshold_pct", 21, FieldType.Float);
        AssertField(shardRuntime, "remote_cost_enabled", 22, FieldType.Bool);
        AssertField(shardRuntime, "remote_cost_per_batch", 23, FieldType.SInt64);
        AssertField(shardRuntime, "remote_cost_per_contribution", 24, FieldType.SInt64);
        AssertField(shardRuntime, "cost_tier_a_multiplier", 25, FieldType.Float);
        AssertField(shardRuntime, "cost_tier_b_multiplier", 26, FieldType.Float);
        AssertField(shardRuntime, "cost_tier_c_multiplier", 27, FieldType.Float);
        AssertField(shardRuntime, "plasticity_energy_cost_modulation_enabled", 28, FieldType.Bool);
        AssertField(shardRuntime, "plasticity_energy_cost_reference_tick_cost", 29, FieldType.SInt64);
        AssertField(shardRuntime, "plasticity_energy_cost_response_strength", 30, FieldType.Float);
        AssertField(shardRuntime, "plasticity_energy_cost_min_scale", 31, FieldType.Float);
        AssertField(shardRuntime, "plasticity_energy_cost_max_scale", 32, FieldType.Float);
        AssertField(shardRuntime, "output_vector_source", 33, FieldType.Enum, "nbn.control.OutputVectorSource");

        var brainIoInfo = descriptor.MessageTypes.Single(message => message.Name == "BrainIoInfo");
        AssertField(brainIoInfo, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(brainIoInfo, "input_width", 2, FieldType.UInt32);
        AssertField(brainIoInfo, "output_width", 3, FieldType.UInt32);
        AssertField(brainIoInfo, "input_coordinator_mode", 4, FieldType.Enum, "nbn.control.InputCoordinatorMode");
        AssertField(brainIoInfo, "output_vector_source", 5, FieldType.Enum, "nbn.control.OutputVectorSource");

        var snapshotOverlay = descriptor.MessageTypes.Single(message => message.Name == "SnapshotOverlayRecord");
        AssertField(snapshotOverlay, "from_address", 1, FieldType.Fixed32);
        AssertField(snapshotOverlay, "to_address", 2, FieldType.Fixed32);
        AssertField(snapshotOverlay, "strength_code", 3, FieldType.UInt32);

        var captureSnapshot = descriptor.MessageTypes.Single(message => message.Name == "CaptureShardSnapshot");
        AssertField(captureSnapshot, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(captureSnapshot, "region_id", 2, FieldType.UInt32);
        AssertField(captureSnapshot, "shard_index", 3, FieldType.UInt32);
        AssertField(captureSnapshot, "tick_id", 4, FieldType.Fixed64);

        var captureSnapshotAck = descriptor.MessageTypes.Single(message => message.Name == "CaptureShardSnapshotAck");
        AssertField(captureSnapshotAck, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(captureSnapshotAck, "region_id", 2, FieldType.UInt32);
        AssertField(captureSnapshotAck, "shard_index", 3, FieldType.UInt32);
        AssertField(captureSnapshotAck, "neuron_start", 4, FieldType.UInt32);
        AssertField(captureSnapshotAck, "neuron_count", 5, FieldType.UInt32);
        AssertRepeatedField(captureSnapshotAck, "buffer_codes", 6, FieldType.SInt32);
        AssertField(captureSnapshotAck, "enabled_bitset", 7, FieldType.Bytes);
        AssertRepeatedField(captureSnapshotAck, "overlays", 8, FieldType.Message, "nbn.control.SnapshotOverlayRecord");
        AssertField(captureSnapshotAck, "success", 9, FieldType.Bool);
        AssertField(captureSnapshotAck, "error", 10, FieldType.String);
    }

    [Fact]
    public void ProtoControl_PlacementLifecycleContracts_AreStable()
    {
        var descriptor = NbnControlReflection.Descriptor;

        var requestPlacement = descriptor.MessageTypes.Single(message => message.Name == "RequestPlacement");
        AssertField(requestPlacement, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(requestPlacement, "base_def", 2, FieldType.Message, "nbn.ArtifactRef");
        AssertField(requestPlacement, "last_snapshot", 3, FieldType.Message, "nbn.ArtifactRef");
        AssertField(requestPlacement, "shard_plan", 4, FieldType.Message, "nbn.control.ShardPlan");
        AssertField(requestPlacement, "input_width", 5, FieldType.UInt32);
        AssertField(requestPlacement, "output_width", 6, FieldType.UInt32);
        AssertField(requestPlacement, "request_id", 7, FieldType.String);
        AssertField(requestPlacement, "requested_ms", 8, FieldType.Fixed64);
        AssertField(requestPlacement, "is_recovery", 9, FieldType.Bool);

        var placementAck = descriptor.MessageTypes.Single(message => message.Name == "PlacementAck");
        AssertField(placementAck, "accepted", 1, FieldType.Bool);
        AssertField(placementAck, "message", 2, FieldType.String);
        AssertField(placementAck, "placement_epoch", 3, FieldType.Fixed64);
        AssertField(placementAck, "lifecycle_state", 4, FieldType.Enum, "nbn.control.PlacementLifecycleState");
        AssertField(placementAck, "failure_reason", 5, FieldType.Enum, "nbn.control.PlacementFailureReason");
        AssertField(placementAck, "accepted_ms", 6, FieldType.Fixed64);
        AssertField(placementAck, "request_id", 7, FieldType.String);

        var getLifecycle = descriptor.MessageTypes.Single(message => message.Name == "GetPlacementLifecycle");
        AssertField(getLifecycle, "brain_id", 1, FieldType.Message, "nbn.Uuid");

        var lifecycleInfo = descriptor.MessageTypes.Single(message => message.Name == "PlacementLifecycleInfo");
        AssertField(lifecycleInfo, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(lifecycleInfo, "placement_epoch", 2, FieldType.Fixed64);
        AssertField(lifecycleInfo, "lifecycle_state", 3, FieldType.Enum, "nbn.control.PlacementLifecycleState");
        AssertField(lifecycleInfo, "failure_reason", 4, FieldType.Enum, "nbn.control.PlacementFailureReason");
        AssertField(lifecycleInfo, "reconcile_state", 5, FieldType.Enum, "nbn.control.PlacementReconcileState");
        AssertField(lifecycleInfo, "requested_ms", 6, FieldType.Fixed64);
        AssertField(lifecycleInfo, "updated_ms", 7, FieldType.Fixed64);
        AssertField(lifecycleInfo, "request_id", 8, FieldType.String);
        AssertField(lifecycleInfo, "shard_plan", 9, FieldType.Message, "nbn.control.ShardPlan");
        AssertField(lifecycleInfo, "registered_shards", 10, FieldType.UInt32);

        var workerInventoryEntry = descriptor.MessageTypes.Single(message => message.Name == "PlacementWorkerInventoryEntry");
        AssertField(workerInventoryEntry, "worker_node_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(workerInventoryEntry, "worker_address", 2, FieldType.String);
        AssertField(workerInventoryEntry, "worker_root_actor_name", 3, FieldType.String);
        AssertField(workerInventoryEntry, "is_alive", 4, FieldType.Bool);
        AssertField(workerInventoryEntry, "last_seen_ms", 5, FieldType.Fixed64);
        AssertField(workerInventoryEntry, "cpu_cores", 6, FieldType.UInt32);
        AssertField(workerInventoryEntry, "ram_free_bytes", 7, FieldType.Fixed64);
        AssertField(workerInventoryEntry, "has_gpu", 8, FieldType.Bool);
        AssertField(workerInventoryEntry, "vram_free_bytes", 9, FieldType.Fixed64);
        AssertField(workerInventoryEntry, "cpu_score", 10, FieldType.Float);
        AssertField(workerInventoryEntry, "gpu_score", 11, FieldType.Float);
        AssertField(workerInventoryEntry, "capability_epoch", 12, FieldType.Fixed64);
        AssertField(workerInventoryEntry, "storage_free_bytes", 13, FieldType.Fixed64);

        var workerInventory = descriptor.MessageTypes.Single(message => message.Name == "PlacementWorkerInventory");
        AssertRepeatedField(workerInventory, "workers", 1, FieldType.Message, "nbn.control.PlacementWorkerInventoryEntry");
        AssertField(workerInventory, "snapshot_ms", 2, FieldType.Fixed64);

        var assignment = descriptor.MessageTypes.Single(message => message.Name == "PlacementAssignment");
        AssertField(assignment, "assignment_id", 1, FieldType.String);
        AssertField(assignment, "brain_id", 2, FieldType.Message, "nbn.Uuid");
        AssertField(assignment, "placement_epoch", 3, FieldType.Fixed64);
        AssertField(assignment, "target", 4, FieldType.Enum, "nbn.control.PlacementAssignmentTarget");
        AssertField(assignment, "worker_node_id", 5, FieldType.Message, "nbn.Uuid");
        AssertField(assignment, "region_id", 6, FieldType.UInt32);
        AssertField(assignment, "shard_index", 7, FieldType.UInt32);
        AssertField(assignment, "neuron_start", 8, FieldType.UInt32);
        AssertField(assignment, "neuron_count", 9, FieldType.UInt32);
        AssertField(assignment, "actor_name", 10, FieldType.String);

        var assignmentAck = descriptor.MessageTypes.Single(message => message.Name == "PlacementAssignmentAck");
        AssertField(assignmentAck, "assignment_id", 1, FieldType.String);
        AssertField(assignmentAck, "brain_id", 2, FieldType.Message, "nbn.Uuid");
        AssertField(assignmentAck, "placement_epoch", 3, FieldType.Fixed64);
        AssertField(assignmentAck, "state", 4, FieldType.Enum, "nbn.control.PlacementAssignmentState");
        AssertField(assignmentAck, "accepted", 5, FieldType.Bool);
        AssertField(assignmentAck, "retryable", 6, FieldType.Bool);
        AssertField(assignmentAck, "failure_reason", 7, FieldType.Enum, "nbn.control.PlacementFailureReason");
        AssertField(assignmentAck, "message", 8, FieldType.String);
        AssertField(assignmentAck, "retry_after_ms", 9, FieldType.Fixed64);

        var reconcileReport = descriptor.MessageTypes.Single(message => message.Name == "PlacementReconcileReport");
        AssertField(reconcileReport, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(reconcileReport, "placement_epoch", 2, FieldType.Fixed64);
        AssertField(reconcileReport, "reconcile_state", 3, FieldType.Enum, "nbn.control.PlacementReconcileState");
        AssertRepeatedField(reconcileReport, "assignments", 4, FieldType.Message, "nbn.control.PlacementObservedAssignment");
        AssertField(reconcileReport, "failure_reason", 5, FieldType.Enum, "nbn.control.PlacementFailureReason");
        AssertField(reconcileReport, "message", 6, FieldType.String);
    }

    [Fact]
    public void ProtoControl_PlacementEnums_AreStable()
    {
        var descriptor = NbnControlReflection.Descriptor;

        var lifecycle = descriptor.EnumTypes.Single(@enum => @enum.Name == "PlacementLifecycleState");
        AssertEnumValue(lifecycle, "PLACEMENT_LIFECYCLE_UNKNOWN", 0);
        AssertEnumValue(lifecycle, "PLACEMENT_LIFECYCLE_REQUESTED", 1);
        AssertEnumValue(lifecycle, "PLACEMENT_LIFECYCLE_ASSIGNING", 2);
        AssertEnumValue(lifecycle, "PLACEMENT_LIFECYCLE_ASSIGNED", 3);
        AssertEnumValue(lifecycle, "PLACEMENT_LIFECYCLE_RUNNING", 4);
        AssertEnumValue(lifecycle, "PLACEMENT_LIFECYCLE_RECONCILING", 5);
        AssertEnumValue(lifecycle, "PLACEMENT_LIFECYCLE_FAILED", 6);
        AssertEnumValue(lifecycle, "PLACEMENT_LIFECYCLE_TERMINATED", 7);

        var failure = descriptor.EnumTypes.Single(@enum => @enum.Name == "PlacementFailureReason");
        AssertEnumValue(failure, "PLACEMENT_FAILURE_NONE", 0);
        AssertEnumValue(failure, "PLACEMENT_FAILURE_INVALID_BRAIN", 1);
        AssertEnumValue(failure, "PLACEMENT_FAILURE_WORKER_UNAVAILABLE", 2);
        AssertEnumValue(failure, "PLACEMENT_FAILURE_ASSIGNMENT_REJECTED", 3);
        AssertEnumValue(failure, "PLACEMENT_FAILURE_ASSIGNMENT_TIMEOUT", 4);
        AssertEnumValue(failure, "PLACEMENT_FAILURE_RECONCILE_MISMATCH", 5);
        AssertEnumValue(failure, "PLACEMENT_FAILURE_INTERNAL_ERROR", 6);

        var assignmentTarget = descriptor.EnumTypes.Single(@enum => @enum.Name == "PlacementAssignmentTarget");
        AssertEnumValue(assignmentTarget, "PLACEMENT_TARGET_UNKNOWN", 0);
        AssertEnumValue(assignmentTarget, "PLACEMENT_TARGET_BRAIN_ROOT", 1);
        AssertEnumValue(assignmentTarget, "PLACEMENT_TARGET_SIGNAL_ROUTER", 2);
        AssertEnumValue(assignmentTarget, "PLACEMENT_TARGET_REGION_SHARD", 3);
        AssertEnumValue(assignmentTarget, "PLACEMENT_TARGET_INPUT_COORDINATOR", 4);
        AssertEnumValue(assignmentTarget, "PLACEMENT_TARGET_OUTPUT_COORDINATOR", 5);

        var assignmentState = descriptor.EnumTypes.Single(@enum => @enum.Name == "PlacementAssignmentState");
        AssertEnumValue(assignmentState, "PLACEMENT_ASSIGNMENT_UNKNOWN", 0);
        AssertEnumValue(assignmentState, "PLACEMENT_ASSIGNMENT_PENDING", 1);
        AssertEnumValue(assignmentState, "PLACEMENT_ASSIGNMENT_ACCEPTED", 2);
        AssertEnumValue(assignmentState, "PLACEMENT_ASSIGNMENT_READY", 3);
        AssertEnumValue(assignmentState, "PLACEMENT_ASSIGNMENT_FAILED", 4);
        AssertEnumValue(assignmentState, "PLACEMENT_ASSIGNMENT_DRAINING", 5);

        var reconcileState = descriptor.EnumTypes.Single(@enum => @enum.Name == "PlacementReconcileState");
        AssertEnumValue(reconcileState, "PLACEMENT_RECONCILE_UNKNOWN", 0);
        AssertEnumValue(reconcileState, "PLACEMENT_RECONCILE_MATCHED", 1);
        AssertEnumValue(reconcileState, "PLACEMENT_RECONCILE_REQUIRES_ACTION", 2);
        AssertEnumValue(reconcileState, "PLACEMENT_RECONCILE_FAILED", 3);
    }

    [Fact]
    public void ProtoSignals_BatchFields_AreStable()
    {
        var descriptor = NbnSignalsReflection.Descriptor;

        var contribution = descriptor.MessageTypes.Single(message => message.Name == "Contribution");
        AssertField(contribution, "target_neuron_id", 1, FieldType.UInt32);
        AssertField(contribution, "value", 2, FieldType.Float);

        var signalBatch = descriptor.MessageTypes.Single(message => message.Name == "SignalBatch");
        AssertField(signalBatch, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(signalBatch, "region_id", 2, FieldType.UInt32);
        AssertField(signalBatch, "shard_id", 3, FieldType.Message, "nbn.ShardId32");
        AssertField(signalBatch, "tick_id", 4, FieldType.Fixed64);
        AssertRepeatedField(signalBatch, "contribs", 5, FieldType.Message, "nbn.signal.Contribution");

        var signalBatchAck = descriptor.MessageTypes.Single(message => message.Name == "SignalBatchAck");
        AssertField(signalBatchAck, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(signalBatchAck, "region_id", 2, FieldType.UInt32);
        AssertField(signalBatchAck, "shard_id", 3, FieldType.Message, "nbn.ShardId32");
        AssertField(signalBatchAck, "tick_id", 4, FieldType.Fixed64);

        var outboxBatch = descriptor.MessageTypes.Single(message => message.Name == "OutboxBatch");
        AssertField(outboxBatch, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(outboxBatch, "tick_id", 2, FieldType.Fixed64);
        AssertField(outboxBatch, "dest_region_id", 3, FieldType.UInt32);
        AssertField(outboxBatch, "dest_shard_id", 4, FieldType.Message, "nbn.ShardId32");
        AssertRepeatedField(outboxBatch, "contribs", 5, FieldType.Message, "nbn.signal.Contribution");
    }

    [Fact]
    public void ProtoIo_SpawnBrainViaIOAck_Fields_AreStable()
    {
        var descriptor = NbnIoReflection.Descriptor;
        var spawnAck = descriptor.MessageTypes.Single(message => message.Name == "SpawnBrainViaIOAck");

        AssertField(spawnAck, "ack", 1, FieldType.Message, "nbn.control.SpawnBrainAck");
        AssertField(spawnAck, "failure_reason_code", 2, FieldType.String);
        AssertField(spawnAck, "failure_message", 3, FieldType.String);
    }

    [Fact]
    public void ProtoIo_OutputFields_AreStable()
    {
        var descriptor = NbnIoReflection.Descriptor;

        var inputWrite = descriptor.MessageTypes.Single(message => message.Name == "InputWrite");
        AssertField(inputWrite, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(inputWrite, "input_index", 2, FieldType.UInt32);
        AssertField(inputWrite, "value", 3, FieldType.Float);

        var inputVector = descriptor.MessageTypes.Single(message => message.Name == "InputVector");
        AssertField(inputVector, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertRepeatedField(inputVector, "values", 2, FieldType.Float);

        var runtimePulse = descriptor.MessageTypes.Single(message => message.Name == "RuntimeNeuronPulse");
        AssertField(runtimePulse, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(runtimePulse, "target_region_id", 2, FieldType.UInt32);
        AssertField(runtimePulse, "target_neuron_id", 3, FieldType.UInt32);
        AssertField(runtimePulse, "value", 4, FieldType.Float);

        var runtimeStateWrite = descriptor.MessageTypes.Single(message => message.Name == "RuntimeNeuronStateWrite");
        AssertField(runtimeStateWrite, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(runtimeStateWrite, "target_region_id", 2, FieldType.UInt32);
        AssertField(runtimeStateWrite, "target_neuron_id", 3, FieldType.UInt32);
        AssertField(runtimeStateWrite, "set_buffer", 4, FieldType.Bool);
        AssertField(runtimeStateWrite, "buffer_value", 5, FieldType.Float);
        AssertField(runtimeStateWrite, "set_accumulator", 6, FieldType.Bool);
        AssertField(runtimeStateWrite, "accumulator_value", 7, FieldType.Float);

        var outputEvent = descriptor.MessageTypes.Single(message => message.Name == "OutputEvent");
        AssertField(outputEvent, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(outputEvent, "output_index", 2, FieldType.UInt32);
        AssertField(outputEvent, "value", 3, FieldType.Float);
        AssertField(outputEvent, "tick_id", 4, FieldType.Fixed64);

        var outputVectorEvent = descriptor.MessageTypes.Single(message => message.Name == "OutputVectorEvent");
        AssertField(outputVectorEvent, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(outputVectorEvent, "tick_id", 2, FieldType.Fixed64);
        AssertRepeatedField(outputVectorEvent, "values", 3, FieldType.Float);

        var outputVectorSegment = descriptor.MessageTypes.Single(message => message.Name == "OutputVectorSegment");
        AssertField(outputVectorSegment, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(outputVectorSegment, "tick_id", 2, FieldType.Fixed64);
        AssertField(outputVectorSegment, "output_index_start", 3, FieldType.UInt32);
        AssertRepeatedField(outputVectorSegment, "values", 4, FieldType.Float);
    }

    [Fact]
    public void ProtoIo_SubscriptionAndDrainFields_AreStable()
    {
        var descriptor = NbnIoReflection.Descriptor;

        var subscribeOutputs = descriptor.MessageTypes.Single(message => message.Name == "SubscribeOutputs");
        AssertField(subscribeOutputs, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(subscribeOutputs, "subscriber_actor", 2, FieldType.String);

        var unsubscribeOutputs = descriptor.MessageTypes.Single(message => message.Name == "UnsubscribeOutputs");
        AssertField(unsubscribeOutputs, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(unsubscribeOutputs, "subscriber_actor", 2, FieldType.String);

        var subscribeOutputsVector = descriptor.MessageTypes.Single(message => message.Name == "SubscribeOutputsVector");
        AssertField(subscribeOutputsVector, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(subscribeOutputsVector, "subscriber_actor", 2, FieldType.String);

        var unsubscribeOutputsVector = descriptor.MessageTypes.Single(message => message.Name == "UnsubscribeOutputsVector");
        AssertField(unsubscribeOutputsVector, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(unsubscribeOutputsVector, "subscriber_actor", 2, FieldType.String);

        var drainInputs = descriptor.MessageTypes.Single(message => message.Name == "DrainInputs");
        AssertField(drainInputs, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(drainInputs, "tick_id", 2, FieldType.Fixed64);

        var inputDrain = descriptor.MessageTypes.Single(message => message.Name == "InputDrain");
        AssertField(inputDrain, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(inputDrain, "tick_id", 2, FieldType.Fixed64);
        AssertRepeatedField(inputDrain, "contribs", 3, FieldType.Message, "nbn.signal.Contribution");
    }

    [Fact]
    public void ProtoIo_BrainInfo_Fields_AreStable()
    {
        var descriptor = NbnIoReflection.Descriptor;
        var brainInfo = descriptor.MessageTypes.Single(message => message.Name == "BrainInfo");

        AssertField(brainInfo, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(brainInfo, "input_width", 2, FieldType.UInt32);
        AssertField(brainInfo, "output_width", 3, FieldType.UInt32);
        AssertField(brainInfo, "cost_enabled", 4, FieldType.Bool);
        AssertField(brainInfo, "energy_enabled", 5, FieldType.Bool);
        AssertField(brainInfo, "energy_remaining", 6, FieldType.SInt64);
        AssertField(brainInfo, "plasticity_enabled", 7, FieldType.Bool);
        AssertField(brainInfo, "base_definition", 8, FieldType.Message, "nbn.ArtifactRef");
        AssertField(brainInfo, "last_snapshot", 9, FieldType.Message, "nbn.ArtifactRef");
        AssertField(brainInfo, "energy_rate_units_per_second", 10, FieldType.SInt64);
        AssertField(brainInfo, "plasticity_rate", 11, FieldType.Float);
        AssertField(brainInfo, "plasticity_probabilistic_updates", 12, FieldType.Bool);
        AssertField(brainInfo, "last_tick_cost", 13, FieldType.SInt64);
        AssertField(brainInfo, "homeostasis_enabled", 14, FieldType.Bool);
        AssertField(brainInfo, "homeostasis_target_mode", 15, FieldType.Enum, "nbn.control.HomeostasisTargetMode");
        AssertField(brainInfo, "homeostasis_update_mode", 16, FieldType.Enum, "nbn.control.HomeostasisUpdateMode");
        AssertField(brainInfo, "homeostasis_base_probability", 17, FieldType.Float);
        AssertField(brainInfo, "homeostasis_min_step_codes", 18, FieldType.UInt32);
        AssertField(brainInfo, "homeostasis_energy_coupling_enabled", 19, FieldType.Bool);
        AssertField(brainInfo, "homeostasis_energy_target_scale", 20, FieldType.Float);
        AssertField(brainInfo, "homeostasis_energy_probability_scale", 21, FieldType.Float);
        AssertField(brainInfo, "plasticity_delta", 22, FieldType.Float);
        AssertField(brainInfo, "plasticity_rebase_threshold", 23, FieldType.UInt32);
        AssertField(brainInfo, "plasticity_rebase_threshold_pct", 24, FieldType.Float);
        AssertField(brainInfo, "plasticity_energy_cost_modulation_enabled", 25, FieldType.Bool);
        AssertField(brainInfo, "plasticity_energy_cost_reference_tick_cost", 26, FieldType.SInt64);
        AssertField(brainInfo, "plasticity_energy_cost_response_strength", 27, FieldType.Float);
        AssertField(brainInfo, "plasticity_energy_cost_min_scale", 28, FieldType.Float);
        AssertField(brainInfo, "plasticity_energy_cost_max_scale", 29, FieldType.Float);
        AssertField(brainInfo, "input_coordinator_mode", 30, FieldType.Enum, "nbn.control.InputCoordinatorMode");
        AssertField(brainInfo, "output_vector_source", 31, FieldType.Enum, "nbn.control.OutputVectorSource");
    }

    [Fact]
    public void ProtoIo_RuntimeControlFields_AreStable()
    {
        var descriptor = NbnIoReflection.Descriptor;

        var energyState = descriptor.MessageTypes.Single(message => message.Name == "BrainEnergyState");
        AssertField(energyState, "energy_remaining", 1, FieldType.SInt64);
        AssertField(energyState, "energy_rate_units_per_second", 2, FieldType.SInt64);
        AssertField(energyState, "cost_enabled", 3, FieldType.Bool);
        AssertField(energyState, "energy_enabled", 4, FieldType.Bool);
        AssertField(energyState, "plasticity_enabled", 5, FieldType.Bool);
        AssertField(energyState, "plasticity_rate", 6, FieldType.Float);
        AssertField(energyState, "plasticity_probabilistic_updates", 7, FieldType.Bool);
        AssertField(energyState, "last_tick_cost", 8, FieldType.SInt64);
        AssertField(energyState, "homeostasis_enabled", 9, FieldType.Bool);
        AssertField(energyState, "homeostasis_target_mode", 10, FieldType.Enum, "nbn.control.HomeostasisTargetMode");
        AssertField(energyState, "homeostasis_update_mode", 11, FieldType.Enum, "nbn.control.HomeostasisUpdateMode");
        AssertField(energyState, "homeostasis_base_probability", 12, FieldType.Float);
        AssertField(energyState, "homeostasis_min_step_codes", 13, FieldType.UInt32);
        AssertField(energyState, "homeostasis_energy_coupling_enabled", 14, FieldType.Bool);
        AssertField(energyState, "homeostasis_energy_target_scale", 15, FieldType.Float);
        AssertField(energyState, "homeostasis_energy_probability_scale", 16, FieldType.Float);
        AssertField(energyState, "plasticity_delta", 17, FieldType.Float);
        AssertField(energyState, "plasticity_rebase_threshold", 18, FieldType.UInt32);
        AssertField(energyState, "plasticity_rebase_threshold_pct", 19, FieldType.Float);
        AssertField(energyState, "plasticity_energy_cost_modulation_enabled", 20, FieldType.Bool);
        AssertField(energyState, "plasticity_energy_cost_reference_tick_cost", 21, FieldType.SInt64);
        AssertField(energyState, "plasticity_energy_cost_response_strength", 22, FieldType.Float);
        AssertField(energyState, "plasticity_energy_cost_min_scale", 23, FieldType.Float);
        AssertField(energyState, "plasticity_energy_cost_max_scale", 24, FieldType.Float);

        var setPlasticity = descriptor.MessageTypes.Single(message => message.Name == "SetPlasticityEnabled");
        AssertField(setPlasticity, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(setPlasticity, "plasticity_enabled", 2, FieldType.Bool);
        AssertField(setPlasticity, "plasticity_rate", 3, FieldType.Float);
        AssertField(setPlasticity, "probabilistic_updates", 4, FieldType.Bool);
        AssertField(setPlasticity, "plasticity_delta", 5, FieldType.Float);
        AssertField(setPlasticity, "plasticity_rebase_threshold", 6, FieldType.UInt32);
        AssertField(setPlasticity, "plasticity_rebase_threshold_pct", 7, FieldType.Float);
        AssertField(setPlasticity, "plasticity_energy_cost_modulation_enabled", 8, FieldType.Bool);
        AssertField(setPlasticity, "plasticity_energy_cost_reference_tick_cost", 9, FieldType.SInt64);
        AssertField(setPlasticity, "plasticity_energy_cost_response_strength", 10, FieldType.Float);
        AssertField(setPlasticity, "plasticity_energy_cost_min_scale", 11, FieldType.Float);
        AssertField(setPlasticity, "plasticity_energy_cost_max_scale", 12, FieldType.Float);

        var setHomeostasis = descriptor.MessageTypes.Single(message => message.Name == "SetHomeostasisEnabled");
        AssertField(setHomeostasis, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(setHomeostasis, "homeostasis_enabled", 2, FieldType.Bool);
        AssertField(setHomeostasis, "homeostasis_target_mode", 3, FieldType.Enum, "nbn.control.HomeostasisTargetMode");
        AssertField(setHomeostasis, "homeostasis_update_mode", 4, FieldType.Enum, "nbn.control.HomeostasisUpdateMode");
        AssertField(setHomeostasis, "homeostasis_base_probability", 5, FieldType.Float);
        AssertField(setHomeostasis, "homeostasis_min_step_codes", 6, FieldType.UInt32);
        AssertField(setHomeostasis, "homeostasis_energy_coupling_enabled", 7, FieldType.Bool);
        AssertField(setHomeostasis, "homeostasis_energy_target_scale", 8, FieldType.Float);
        AssertField(setHomeostasis, "homeostasis_energy_probability_scale", 9, FieldType.Float);
    }

    [Fact]
    public void ProtoIo_IoCommandAck_Fields_AreStable()
    {
        var descriptor = NbnIoReflection.Descriptor;
        var ack = descriptor.MessageTypes.Single(message => message.Name == "IoCommandAck");

        AssertField(ack, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(ack, "command", 2, FieldType.String);
        AssertField(ack, "success", 3, FieldType.Bool);
        AssertField(ack, "message", 4, FieldType.String);
        AssertField(ack, "has_energy_state", 5, FieldType.Bool);
        AssertField(ack, "energy_state", 6, FieldType.Message, "nbn.io.BrainEnergyState");
        AssertField(ack, "has_configured_plasticity_enabled", 7, FieldType.Bool);
        AssertField(ack, "configured_plasticity_enabled", 8, FieldType.Bool);
        AssertField(ack, "has_effective_plasticity_enabled", 9, FieldType.Bool);
        AssertField(ack, "effective_plasticity_enabled", 10, FieldType.Bool);
    }

    [Fact]
    public void ProtoIo_RequestSnapshot_Fields_AreStable()
    {
        var descriptor = NbnIoReflection.Descriptor;
        var requestSnapshot = descriptor.MessageTypes.Single(message => message.Name == "RequestSnapshot");

        AssertField(requestSnapshot, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(requestSnapshot, "has_runtime_state", 2, FieldType.Bool);
        AssertField(requestSnapshot, "energy_remaining", 3, FieldType.SInt64);
        AssertField(requestSnapshot, "cost_enabled", 4, FieldType.Bool);
        AssertField(requestSnapshot, "energy_enabled", 5, FieldType.Bool);
        AssertField(requestSnapshot, "plasticity_enabled", 6, FieldType.Bool);
    }

    [Fact]
    public void ProtoIo_RegisterBrain_RuntimeConfigFields_AreStable()
    {
        var descriptor = NbnIoReflection.Descriptor;
        var registerBrain = descriptor.MessageTypes.Single(message => message.Name == "RegisterBrain");

        AssertField(registerBrain, "brain_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(registerBrain, "input_width", 2, FieldType.UInt32);
        AssertField(registerBrain, "output_width", 3, FieldType.UInt32);
        AssertField(registerBrain, "base_definition", 4, FieldType.Message, "nbn.ArtifactRef");
        AssertField(registerBrain, "last_snapshot", 5, FieldType.Message, "nbn.ArtifactRef");
        AssertField(registerBrain, "energy_state", 6, FieldType.Message, "nbn.io.BrainEnergyState");
        AssertField(registerBrain, "has_runtime_config", 7, FieldType.Bool);
        AssertField(registerBrain, "cost_enabled", 8, FieldType.Bool);
        AssertField(registerBrain, "energy_enabled", 9, FieldType.Bool);
        AssertField(registerBrain, "plasticity_enabled", 10, FieldType.Bool);
        AssertField(registerBrain, "plasticity_rate", 11, FieldType.Float);
        AssertField(registerBrain, "plasticity_probabilistic_updates", 12, FieldType.Bool);
        AssertField(registerBrain, "last_tick_cost", 13, FieldType.SInt64);
        AssertField(registerBrain, "homeostasis_enabled", 14, FieldType.Bool);
        AssertField(registerBrain, "homeostasis_target_mode", 15, FieldType.Enum, "nbn.control.HomeostasisTargetMode");
        AssertField(registerBrain, "homeostasis_update_mode", 16, FieldType.Enum, "nbn.control.HomeostasisUpdateMode");
        AssertField(registerBrain, "homeostasis_base_probability", 17, FieldType.Float);
        AssertField(registerBrain, "homeostasis_min_step_codes", 18, FieldType.UInt32);
        AssertField(registerBrain, "homeostasis_energy_coupling_enabled", 19, FieldType.Bool);
        AssertField(registerBrain, "homeostasis_energy_target_scale", 20, FieldType.Float);
        AssertField(registerBrain, "homeostasis_energy_probability_scale", 21, FieldType.Float);
        AssertField(registerBrain, "plasticity_delta", 22, FieldType.Float);
        AssertField(registerBrain, "plasticity_rebase_threshold", 23, FieldType.UInt32);
        AssertField(registerBrain, "plasticity_rebase_threshold_pct", 24, FieldType.Float);
        AssertField(registerBrain, "plasticity_energy_cost_modulation_enabled", 25, FieldType.Bool);
        AssertField(registerBrain, "plasticity_energy_cost_reference_tick_cost", 26, FieldType.SInt64);
        AssertField(registerBrain, "plasticity_energy_cost_response_strength", 27, FieldType.Float);
        AssertField(registerBrain, "plasticity_energy_cost_min_scale", 28, FieldType.Float);
        AssertField(registerBrain, "plasticity_energy_cost_max_scale", 29, FieldType.Float);
        AssertField(registerBrain, "input_coordinator_mode", 30, FieldType.Enum, "nbn.control.InputCoordinatorMode");
        AssertField(registerBrain, "output_vector_source", 31, FieldType.Enum, "nbn.control.OutputVectorSource");
    }

    [Fact]
    public void ProtoSettings_NodeCapabilitiesFields_AreStable()
    {
        var descriptor = NbnSettingsReflection.Descriptor;
        var caps = descriptor.MessageTypes.Single(message => message.Name == "NodeCapabilities");

        AssertField(caps, "cpu_cores", 1, FieldType.UInt32);
        AssertField(caps, "ram_free_bytes", 2, FieldType.Fixed64);
        AssertField(caps, "storage_free_bytes", 10, FieldType.Fixed64);
        AssertField(caps, "has_gpu", 3, FieldType.Bool);
        AssertField(caps, "gpu_name", 4, FieldType.String);
        AssertField(caps, "vram_free_bytes", 5, FieldType.Fixed64);
        AssertField(caps, "cpu_score", 6, FieldType.Float);
        AssertField(caps, "gpu_score", 7, FieldType.Float);
        AssertField(caps, "ilgpu_cuda_available", 8, FieldType.Bool);
        AssertField(caps, "ilgpu_opencl_available", 9, FieldType.Bool);
    }

    [Fact]
    public void ProtoSettings_WorkerInventorySnapshotFields_AreStable()
    {
        var descriptor = NbnSettingsReflection.Descriptor;

        var request = descriptor.MessageTypes.Single(message => message.Name == "WorkerInventorySnapshotRequest");
        Assert.Empty(request.Fields.InFieldNumberOrder());

        var payload = descriptor.MessageTypes.Single(message => message.Name == "WorkerReadinessCapability");
        AssertField(payload, "node_id", 1, FieldType.Message, "nbn.Uuid");
        AssertField(payload, "logical_name", 2, FieldType.String);
        AssertField(payload, "address", 3, FieldType.String);
        AssertField(payload, "root_actor_name", 4, FieldType.String);
        AssertField(payload, "is_alive", 5, FieldType.Bool);
        AssertField(payload, "is_ready", 6, FieldType.Bool);
        AssertField(payload, "last_seen_ms", 7, FieldType.Fixed64);
        AssertField(payload, "has_capabilities", 8, FieldType.Bool);
        AssertField(payload, "capability_time_ms", 9, FieldType.Fixed64);
        AssertField(payload, "capabilities", 10, FieldType.Message, "nbn.settings.NodeCapabilities");

        var response = descriptor.MessageTypes.Single(message => message.Name == "WorkerInventorySnapshotResponse");
        AssertRepeatedField(response, "workers", 1, FieldType.Message, "nbn.settings.WorkerReadinessCapability");
        AssertField(response, "snapshot_ms", 2, FieldType.Fixed64);
    }

    [Fact]
    public void ProtoRepro_RequestFields_AreStable()
    {
        var descriptor = NbnReproReflection.Descriptor;

        var byBrainIds = descriptor.MessageTypes.Single(message => message.Name == "ReproduceByBrainIdsRequest");
        AssertField(byBrainIds, "parentA", 1, FieldType.Message, "nbn.Uuid");
        AssertField(byBrainIds, "parentB", 2, FieldType.Message, "nbn.Uuid");
        AssertField(byBrainIds, "strength_source", 3, FieldType.Enum, "nbn.repro.StrengthSource");
        AssertField(byBrainIds, "config", 4, FieldType.Message, "nbn.repro.ReproduceConfig");
        AssertField(byBrainIds, "seed", 5, FieldType.Fixed64);

        var config = descriptor.MessageTypes.Single(message => message.Name == "ReproduceConfig");
        AssertField(config, "max_region_span_diff_ratio", 1, FieldType.Float);
        AssertField(config, "max_function_hist_distance", 2, FieldType.Float);
        AssertField(config, "max_connectivity_hist_distance", 3, FieldType.Float);
        AssertField(config, "inbound_reroute_max_ring_distance", 24, FieldType.UInt32);
        AssertField(config, "strength_transform_enabled", 50, FieldType.Bool);
        AssertRepeatedField(config, "per_region_out_degree_caps", 62, FieldType.Message, "nbn.repro.RegionOutDegreeCap");
        AssertField(config, "limits", 70, FieldType.Message, "nbn.repro.ReproduceLimits");
        AssertField(config, "spawn_child", 80, FieldType.Enum, "nbn.repro.SpawnChildPolicy");

        var regionCap = descriptor.MessageTypes.Single(message => message.Name == "RegionOutDegreeCap");
        AssertField(regionCap, "region_id", 1, FieldType.UInt32);
        AssertField(regionCap, "max_avg_out_degree", 2, FieldType.Float);

        var report = descriptor.MessageTypes.Single(message => message.Name == "SimilarityReport");
        AssertField(report, "similarity_score", 13, FieldType.Float);
    }

    [Fact]
    public void ProtoEnums_ValuesAreStable()
    {
        Assert.Equal(3, (int)AccumulationFunction.AccumNone);
        Assert.Equal(27, (int)ActivationFunction.ActPow);
        Assert.Equal(60, (int)ResetFunction.ResetInverse);
        Assert.Equal(5, (int)Severity.SevFatal);
        Assert.Equal(1, (int)StrengthSource.StrengthLiveCodes);
        Assert.Equal(2, (int)SpawnChildPolicy.SpawnChildAlways);
    }

    private static void AssertField(MessageDescriptor message, string fieldName, int number, FieldType fieldType, string? typeName = null)
    {
        var field = message.FindFieldByName(fieldName);
        Assert.NotNull(field);
        var actual = field!;
        Assert.Equal(number, actual.FieldNumber);
        Assert.Equal(fieldType, actual.FieldType);
        Assert.False(actual.IsRepeated);

        if (typeName is null)
        {
            return;
        }

        Assert.Equal(typeName, GetTypeName(actual));
    }

    private static void AssertRepeatedField(MessageDescriptor message, string fieldName, int number, FieldType fieldType, string? typeName = null)
    {
        var field = message.FindFieldByName(fieldName);
        Assert.NotNull(field);
        var actual = field!;
        Assert.Equal(number, actual.FieldNumber);
        Assert.Equal(fieldType, actual.FieldType);
        Assert.True(actual.IsRepeated);

        if (typeName is null)
        {
            return;
        }

        Assert.Equal(typeName, GetTypeName(actual));
    }

    private static void AssertEnumValue(EnumDescriptor @enum, string valueName, int number)
    {
        var value = @enum.FindValueByName(valueName);
        Assert.NotNull(value);
        Assert.Equal(number, value!.Number);
    }

    private static string? GetTypeName(FieldDescriptor field)
    {
        return field.FieldType switch
        {
            FieldType.Message => field.MessageType.FullName,
            FieldType.Enum => field.EnumType.FullName,
            _ => null
        };
    }
}
