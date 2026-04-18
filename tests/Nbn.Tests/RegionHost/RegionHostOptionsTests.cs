using Nbn.Runtime.RegionHost;

namespace Nbn.Tests.RegionHost;

[Collection("RegionHostEnvSerial")]
public sealed class RegionHostOptionsTests
{
    [Fact]
    public void FromArgs_Parses_PlacementEpoch_FromCommandLine()
    {
        var separate = RegionHostOptions.FromArgs(["--placement-epoch", "123", "--assignment-id", "assign-123"]);
        var inline = RegionHostOptions.FromArgs(["--placement-epoch=456", "--assignment-id=assign-456"]);

        Assert.Equal<ulong>(123, separate.PlacementEpoch);
        Assert.Equal("assign-123", separate.AssignmentId);
        Assert.Equal<ulong>(456, inline.PlacementEpoch);
        Assert.Equal("assign-456", inline.AssignmentId);
    }

    [Fact]
    public void FromArgs_Parses_PlacementEpoch_FromEnvironment()
    {
        using var _ = new EnvironmentVariableScope(
            ("NBN_REGIONHOST_PLACEMENT_EPOCH", "789"),
            ("NBN_REGIONHOST_ASSIGNMENT_ID", "assign-789"));

        var options = RegionHostOptions.FromArgs([]);

        Assert.Equal<ulong>(789, options.PlacementEpoch);
        Assert.Equal("assign-789", options.AssignmentId);
    }

    private sealed class EnvironmentVariableScope : IDisposable
    {
        private readonly Dictionary<string, string?> _originals = new(StringComparer.Ordinal);

        public EnvironmentVariableScope(params (string Key, string? Value)[] values)
        {
            foreach (var (key, value) in values)
            {
                _originals[key] = Environment.GetEnvironmentVariable(key);
                Environment.SetEnvironmentVariable(key, value);
            }
        }

        public void Dispose()
        {
            foreach (var (key, value) in _originals)
            {
                Environment.SetEnvironmentVariable(key, value);
            }
        }
    }
}

[CollectionDefinition("RegionHostEnvSerial", DisableParallelization = true)]
public sealed class RegionHostEnvSerialCollection
{
}
