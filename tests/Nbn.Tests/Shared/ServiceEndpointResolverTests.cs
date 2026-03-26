using Nbn.Shared;

namespace Nbn.Tests.Shared;

public sealed class ServiceEndpointResolverTests
{
    [Fact]
    public async Task ResolveAsync_PicksFirstReachableCandidate_InPriorityOrder()
    {
        var endpointSet = new ServiceEndpointSet(
            "io-gateway",
            [
                new ServiceEndpointCandidate("198.51.100.10:12050", "io-gateway", ServiceEndpointCandidateKind.Public, Priority: 100, IsDefault: true),
                new ServiceEndpointCandidate("100.86.45.90:12050", "io-gateway", ServiceEndpointCandidateKind.Tailnet, Priority: 90),
                new ServiceEndpointCandidate("192.168.0.14:12050", "io-gateway", ServiceEndpointCandidateKind.Lan, Priority: 80)
            ]);

        var resolved = await ServiceEndpointResolver.ResolveAsync(
            endpointSet,
            (candidate, _) => Task.FromResult(candidate.HostPort.StartsWith("100.86.45.90", StringComparison.Ordinal)));

        Assert.Equal("100.86.45.90:12050", resolved.HostPort);
        Assert.Equal("io-gateway", resolved.ActorName);
    }

    [Fact]
    public async Task ResolveAsync_FallsBackToPreferredCandidate_WhenNoCandidateIsReachable()
    {
        var endpointSet = new ServiceEndpointSet(
            "HiveMind",
            [
                new ServiceEndpointCandidate("198.51.100.10:12020", "HiveMind", ServiceEndpointCandidateKind.Public, Priority: 100, IsDefault: true),
                new ServiceEndpointCandidate("100.86.45.90:12020", "HiveMind", ServiceEndpointCandidateKind.Tailnet, Priority: 90)
            ]);

        var resolved = await ServiceEndpointResolver.ResolveAsync(
            endpointSet,
            (_, _) => Task.FromResult(false));

        Assert.Equal("198.51.100.10:12020", resolved.HostPort);
        Assert.Equal("HiveMind", resolved.ActorName);
    }

    [Fact]
    public async Task ResolveAsync_PrefersLocalCandidate_OverReachablePublicDefault_WhenTargetLooksLocal()
    {
        using var _ = new EnvironmentVariableScope(("NBN_DEFAULT_ADVERTISE_HOST", "127.0.0.1"));

        var endpointSet = new ServiceEndpointSet(
            "VisualizationHub",
            [
                new ServiceEndpointCandidate("100.123.130.93:12060", "VisualizationHub", ServiceEndpointCandidateKind.Public, Priority: 1000, IsDefault: true),
                new ServiceEndpointCandidate("127.0.0.1:12060", "VisualizationHub", ServiceEndpointCandidateKind.Loopback, Priority: 600)
            ]);

        var resolved = await ServiceEndpointResolver.ResolveAsync(
            endpointSet,
            (_, _) => Task.FromResult(true));

        Assert.Equal("127.0.0.1:12060", resolved.HostPort);
        Assert.Equal("VisualizationHub", resolved.ActorName);
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
