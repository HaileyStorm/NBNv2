using Nbn.Runtime.HiveMind;

namespace Nbn.Tests.HiveMind;

[Collection("HiveMindEnvSerial")]
public sealed class HiveMindOptionsTests
{
    [Fact]
    public void FromArgs_UsesLocalDefaults_ForIoEndpoint_WhenUnspecified()
    {
        using var _ = new EnvironmentVariableScope(
            ("NBN_HIVE_IO_ADDRESS", null),
            ("NBN_HIVE_IO_NAME", null),
            ("NBN_IO_ADVERTISE_HOST", null),
            ("NBN_IO_ADVERTISE_PORT", null),
            ("NBN_IO_BIND_HOST", null),
            ("NBN_IO_PORT", null),
            ("NBN_IO_GATEWAY_NAME", null));

        var options = HiveMindOptions.FromArgs(Array.Empty<string>());

        Assert.Equal("127.0.0.1:12050", options.IoAddress);
        Assert.Equal("io-gateway", options.IoName);
    }

    [Fact]
    public void FromArgs_UsesIoEnvironmentFallback_WhenHiveSpecificValuesAreMissing()
    {
        using var _ = new EnvironmentVariableScope(
            ("NBN_HIVE_IO_ADDRESS", null),
            ("NBN_HIVE_IO_NAME", null),
            ("NBN_IO_ADVERTISE_HOST", "10.2.3.4"),
            ("NBN_IO_ADVERTISE_PORT", "12099"),
            ("NBN_IO_BIND_HOST", null),
            ("NBN_IO_PORT", null),
            ("NBN_IO_GATEWAY_NAME", "io-custom"));

        var options = HiveMindOptions.FromArgs(Array.Empty<string>());

        Assert.Equal("10.2.3.4:12099", options.IoAddress);
        Assert.Equal("io-custom", options.IoName);
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

[CollectionDefinition("HiveMindEnvSerial", DisableParallelization = true)]
public sealed class HiveMindEnvSerialCollection
{
}
