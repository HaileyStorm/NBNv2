using Nbn.Shared;

namespace Nbn.Tests.Shared;

public sealed class NodeEndpointSetSettingsTests
{
    [Fact]
    public void BuildKey_And_TryParseKey_RoundTrip_NodeId()
    {
        var nodeId = Guid.NewGuid();
        var key = NodeEndpointSetSettings.BuildKey(nodeId);

        Assert.True(NodeEndpointSetSettings.TryParseKey(key, out var parsed));
        Assert.Equal(nodeId, parsed);
    }

    [Fact]
    public void TryParseSetting_Parses_NodeEndpointSetPayload()
    {
        var nodeId = Guid.NewGuid();
        var key = NodeEndpointSetSettings.BuildKey(nodeId);
        var value = ServiceEndpointSettings.EncodeSetValue(
            "worker-node",
            [
                new ServiceEndpointCandidate("100.86.45.90:12041", "worker-node", ServiceEndpointCandidateKind.Tailnet, Priority: 100, IsDefault: true),
                new ServiceEndpointCandidate("192.168.0.14:12041", "worker-node", ServiceEndpointCandidateKind.Lan, Priority: 50)
            ]);

        var parsed = NodeEndpointSetSettings.TryParseSetting(key, value, updatedMs: 42, out var registration);

        Assert.True(parsed);
        Assert.Equal(nodeId, registration.NodeId);
        Assert.Equal("worker-node", registration.EndpointSet.ActorName);
        Assert.Equal("100.86.45.90:12041", registration.EndpointSet.GetPreferredCandidate().HostPort);
    }
}
