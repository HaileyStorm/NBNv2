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
}
