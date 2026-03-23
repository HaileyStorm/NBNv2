using Nbn.Shared;
using Proto;

namespace Nbn.Tests.Shared;

public sealed class RoutablePidReferenceTests
{
    [Fact]
    public async Task ResolveAsync_PrefersReachableCandidate_FromEncodedReference()
    {
        var encoded = RoutablePidReference.Encode(
            new ServiceEndpointSet(
                "workbench-receiver",
                [
                    new ServiceEndpointCandidate("198.51.100.10:12090", "workbench-receiver", ServiceEndpointCandidateKind.Public, Priority: 100, IsDefault: true),
                    new ServiceEndpointCandidate("100.86.45.90:12090", "workbench-receiver", ServiceEndpointCandidateKind.Tailnet, Priority: 90)
                ]));

        var resolved = await RoutablePidReference.ResolveAsync(
            encoded,
            (candidate, _) => Task.FromResult(candidate.HostPort.StartsWith("100.86.45.90", StringComparison.Ordinal)));

        Assert.NotNull(resolved);
        Assert.Equal("100.86.45.90:12090", resolved!.Address);
        Assert.Equal("workbench-receiver", resolved.Id);
    }

    [Fact]
    public void TryParsePlainPid_RejectsEncodedReference()
    {
        var encoded = RoutablePidReference.Encode(
            new ServiceEndpointSet(
                "receiver",
                [new ServiceEndpointCandidate("127.0.0.1:12090", "receiver", IsDefault: true)]));

        Assert.False(RoutablePidReference.TryParsePlainPid(encoded, out _));
    }
}
