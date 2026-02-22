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
    }

    [Fact]
    public void ProtoSettings_NodeCapabilitiesFields_AreStable()
    {
        var descriptor = NbnSettingsReflection.Descriptor;
        var caps = descriptor.MessageTypes.Single(message => message.Name == "NodeCapabilities");

        AssertField(caps, "cpu_cores", 1, FieldType.UInt32);
        AssertField(caps, "ram_free_bytes", 2, FieldType.Fixed64);
        AssertField(caps, "has_gpu", 3, FieldType.Bool);
        AssertField(caps, "gpu_name", 4, FieldType.String);
        AssertField(caps, "vram_free_bytes", 5, FieldType.Fixed64);
        AssertField(caps, "cpu_score", 6, FieldType.Float);
        AssertField(caps, "gpu_score", 7, FieldType.Float);
        AssertField(caps, "ilgpu_cuda_available", 8, FieldType.Bool);
        AssertField(caps, "ilgpu_opencl_available", 9, FieldType.Bool);
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
        AssertField(config, "strength_transform_enabled", 50, FieldType.Bool);
        AssertField(config, "limits", 70, FieldType.Message, "nbn.repro.ReproduceLimits");
        AssertField(config, "spawn_child", 80, FieldType.Enum, "nbn.repro.SpawnChildPolicy");

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
