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
    public void ProtoControl_TickComputeFields_AreStable()
    {
        var descriptor = NbnControlReflection.Descriptor;
        var tickCompute = descriptor.MessageTypes.Single(message => message.Name == "TickCompute");

        AssertField(tickCompute, "tick_id", 1, FieldType.Fixed64);
        AssertField(tickCompute, "target_tick_hz", 2, FieldType.Float);
    }

    [Fact]
    public void ProtoEnums_ValuesAreStable()
    {
        Assert.Equal(3, (int)AccumulationFunction.AccumNone);
        Assert.Equal(27, (int)ActivationFunction.ActPow);
        Assert.Equal(60, (int)ResetFunction.ResetInverse);
        Assert.Equal(5, (int)Severity.SevFatal);
    }

    private static void AssertField(MessageDescriptor message, string fieldName, int number, FieldType fieldType, string? typeName = null)
    {
        var field = message.FindFieldByName(fieldName);
        Assert.NotNull(field);
        Assert.Equal(number, field.FieldNumber);
        Assert.Equal(fieldType, field.FieldType);

        if (typeName is null)
        {
            return;
        }

        var actualType = field.MessageType?.FullName ?? field.EnumType?.FullName;
        Assert.Equal(typeName, actualType);
    }
}
