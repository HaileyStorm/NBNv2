using Nbn.Shared;

namespace Nbn.Tests.Shared;

public sealed class NetworkAddressDefaultsTests
{
    [Fact]
    public void ResolveAdvertisedHost_UsesExplicitAdvertiseHost_WhenProvided()
    {
        var host = NetworkAddressDefaults.ResolveAdvertisedHost("0.0.0.0", "10.20.30.40");

        Assert.Equal("10.20.30.40", host);
    }

    [Fact]
    public void ResolveAdvertisedHost_UsesBindHost_WhenSpecificHostIsProvided()
    {
        var host = NetworkAddressDefaults.ResolveAdvertisedHost("10.1.2.3", null);

        Assert.Equal("10.1.2.3", host);
    }

    [Fact]
    public void ResolveAdvertisedHost_AllInterfaces_UsesEnvironmentOverride()
    {
        using var _ = new EnvironmentVariableScope(("NBN_DEFAULT_ADVERTISE_HOST", "10.9.8.7"));

        var host = NetworkAddressDefaults.ResolveAdvertisedHost(NetworkAddressDefaults.DefaultBindHost, null);

        Assert.Equal("10.9.8.7", host);
    }

    [Fact]
    public void IsLocalHost_MatchesDefaultAdvertiseOverride()
    {
        using var _ = new EnvironmentVariableScope(("NBN_DEFAULT_ADVERTISE_HOST", "10.9.8.7"));

        Assert.True(NetworkAddressDefaults.IsLocalHost("10.9.8.7"));
        Assert.False(NetworkAddressDefaults.IsLocalHost("10.9.8.8"));
    }

    [Fact]
    public void BuildEndpointSet_UsesExplicitAdvertiseHost_AsDefaultCandidate()
    {
        var endpointSet = NetworkAddressDefaults.BuildEndpointSet(
            NetworkAddressDefaults.DefaultBindHost,
            advertisedHost: "100.86.45.90",
            port: 12050,
            actorName: "io-gateway");

        var preferred = endpointSet.GetPreferredCandidate();
        Assert.Equal("io-gateway", endpointSet.ActorName);
        Assert.Equal("100.86.45.90:12050", preferred.HostPort);
        Assert.True(preferred.IsDefault);
        Assert.Equal(ServiceEndpointCandidateKind.Tailnet, preferred.Kind);
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
