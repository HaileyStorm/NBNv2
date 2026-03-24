using System.Net;
using System.Net.Sockets;
using System.Reflection;
using Nbn.Runtime.Brain;
using Nbn.Shared;
using Proto;

namespace Nbn.Tests.Brain;

public sealed class BrainSignalRouterPidMatchingTests
{
    private static readonly MethodInfo SenderMatchesPidMethod = typeof(BrainSignalRouterActor).GetMethod(
        "SenderMatchesPid",
        BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new InvalidOperationException("BrainSignalRouterActor.SenderMatchesPid was not found.");
    private static readonly MethodInfo SenderMatchesActorReferenceOrPidMethod = typeof(BrainSignalRouterActor).GetMethod(
        "SenderMatchesActorReferenceOrPid",
        BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new InvalidOperationException("BrainSignalRouterActor.SenderMatchesActorReferenceOrPid was not found.");
    private static readonly MethodInfo TryBuildLocalizedLocalPidMethod = typeof(BrainSignalRouterActor).GetMethod(
        "TryBuildLocalizedLocalPid",
        BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new InvalidOperationException("BrainSignalRouterActor.TryBuildLocalizedLocalPid was not found.");

    [Theory]
    [InlineData("shard.local:12041", "shard.local:12041", true)]
    [InlineData("LOCALHOST:12041", "localhost:12041", true)]
    [InlineData("localhost:12041", "127.0.0.1:12041", true)]
    [InlineData("127.0.0.1:12041", "localhost:12041", true)]
    [InlineData("0.0.0.0:12041", "127.0.0.1:12041", true)]
    [InlineData("127.0.0.1:12041", "0.0.0.0:12041", true)]
    [InlineData("[::1]:12041", "127.0.0.1:12041", true)]
    [InlineData("::1:12041", "::1:12042", false)]
    [InlineData("0.0.0.0:12041", "192.168.1.4:12041", false)]
    [InlineData("shard.local:12041", "shard.local:12042", false)]
    public void SenderMatchesPid_Uses_Endpoint_Equivalence_For_Same_ActorId(
        string senderAddress,
        string expectedAddress,
        bool expectedMatch)
    {
        var sender = new PID(senderAddress, "region-shard");
        var expected = new PID(expectedAddress, "region-shard");

        var match = InvokeSenderMatchesPid(sender, expected);
        Assert.Equal(expectedMatch, match);
    }

    [Fact]
    public void SenderMatchesPid_Rejects_Different_ActorId_Even_With_Equivalent_Addresses()
    {
        var sender = new PID("127.0.0.1:12041", "region-shard-A");
        var expected = new PID("localhost:12041", "region-shard-B");

        var match = InvokeSenderMatchesPid(sender, expected);
        Assert.False(match);
    }

    [Fact]
    public void SenderMatchesPid_Matches_Addressless_Exact_Pid()
    {
        var sender = new PID(string.Empty, "region-shard");
        var expected = new PID(string.Empty, "region-shard");

        var match = InvokeSenderMatchesPid(sender, expected);
        Assert.True(match);
    }

    [Fact]
    public void SenderMatchesPid_Uses_Dns_Resolution_For_Hostname_And_Ip_WhenAvailable()
    {
        IPAddress? resolvedAddress = null;
        try
        {
            resolvedAddress = Dns.GetHostAddresses(Dns.GetHostName())
                .FirstOrDefault(static address =>
                    address.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6
                    && !IPAddress.IsLoopback(address));
        }
        catch
        {
        }

        if (resolvedAddress is null)
        {
            return;
        }

        var sender = new PID($"{Dns.GetHostName()}:12041", "region-shard");
        var expected = new PID($"{resolvedAddress}:12041", "region-shard");

        var match = InvokeSenderMatchesPid(sender, expected);
        Assert.True(match);
    }

    [Fact]
    public void SenderMatchesPid_Accepts_WorkerRootPrefixed_And_Unprefixed_ActorIds()
    {
        var sender = new PID("192.168.68.140:12041", "worker-node/brain-r1-s0");
        var expected = new PID("192.168.68.140:12041", "brain-r1-s0");

        var match = InvokeSenderMatchesPid(sender, expected);
        Assert.True(match);
    }

    [Fact]
    public void SenderMatchesPid_Accepts_ProcessLocal_Sender_For_Same_Local_Actor()
    {
        var sender = new PID("nonhost", "worker-node/brain-r1-s0");
        var expected = new PID("127.0.0.1:12041", "worker-node/brain-r1-s0");

        var match = InvokeSenderMatchesPid(sender, expected);
        Assert.True(match);
    }

    [Fact]
    public void SenderMatchesActorReferenceOrPid_Accepts_Shard_Sender_On_Any_Routable_Candidate()
    {
        var sender = new PID("100.123.130.93:12041", "worker-node/brain-r1-s0");
        var resolvedPid = new PID("192.168.68.140:12041", "worker-node/brain-r1-s0");
        var actorReference = RoutablePidReference.Encode(
            new[]
            {
                new ServiceEndpointCandidate("100.123.130.93:12041", "worker-node/brain-r1-s0", ServiceEndpointCandidateKind.Tailnet, 1000, "tailnet", true),
                new ServiceEndpointCandidate("192.168.68.140:12041", "worker-node/brain-r1-s0", ServiceEndpointCandidateKind.Lan, 900, "lan")
            },
            "worker-node/brain-r1-s0");

        var match = InvokeSenderMatchesActorReferenceOrPid(sender, actorReference, resolvedPid);
        Assert.True(match);
    }

    [Fact]
    public void SenderMatchesActorReferenceOrPid_Rejects_Shard_Sender_When_ActorId_Differs()
    {
        var sender = new PID("100.123.130.93:12041", "worker-node/brain-r1-s1");
        var resolvedPid = new PID("192.168.68.140:12041", "worker-node/brain-r1-s0");
        var actorReference = RoutablePidReference.Encode(
            new[]
            {
                new ServiceEndpointCandidate("100.123.130.93:12041", "worker-node/brain-r1-s0", ServiceEndpointCandidateKind.Tailnet, 1000, "tailnet", true),
                new ServiceEndpointCandidate("192.168.68.140:12041", "worker-node/brain-r1-s0", ServiceEndpointCandidateKind.Lan, 900, "lan")
            },
            "worker-node/brain-r1-s0");

        var match = InvokeSenderMatchesActorReferenceOrPid(sender, actorReference, resolvedPid);
        Assert.False(match);
    }

    [Fact]
    public void TryBuildLocalizedLocalPid_Localizes_SameMachine_RemoteRoute()
    {
        var actorReference = RoutablePidReference.Encode(
            new[]
            {
                new ServiceEndpointCandidate("127.0.0.1:12041", "worker-node/brain-r6-s0", ServiceEndpointCandidateKind.Loopback, 1000, "loopback", true),
                new ServiceEndpointCandidate("100.123.130.93:12041", "worker-node/brain-r6-s0", ServiceEndpointCandidateKind.Tailnet, 650, "tailnet")
            },
            "worker-node/brain-r6-s0");
        var route = new ShardRoute(
            ShardIdValue: 6,
            new PID("127.0.0.1:12041", "worker-node/brain-r6-s0"),
            actorReference);

        var localized = InvokeTryBuildLocalizedLocalPid(route);
        Assert.NotNull(localized);
        Assert.Equal("worker-node/brain-r6-s0", localized!.Id);
        Assert.Equal("nonhost", localized.Address);
    }

    [Fact]
    public void TryBuildLocalizedLocalPid_Returns_Null_For_NonLocal_Route()
    {
        var actorReference = RoutablePidReference.Encode(
            new[]
            {
                new ServiceEndpointCandidate("203.0.113.7:12041", "worker-node/brain-r6-s0", ServiceEndpointCandidateKind.Public, 1000, "public", true)
            },
            "worker-node/brain-r6-s0");
        var route = new ShardRoute(
            ShardIdValue: 6,
            new PID("203.0.113.7:12041", "worker-node/brain-r6-s0"),
            actorReference);

        var localized = InvokeTryBuildLocalizedLocalPid(route);
        Assert.Null(localized);
    }

    private static bool InvokeSenderMatchesPid(PID sender, PID expected)
        => (bool)(SenderMatchesPidMethod.Invoke(obj: null, new object?[] { sender, expected })
            ?? throw new InvalidOperationException("BrainSignalRouterActor.SenderMatchesPid returned null."));

    private static bool InvokeSenderMatchesActorReferenceOrPid(PID sender, string actorReference, PID expected)
        => (bool)(SenderMatchesActorReferenceOrPidMethod.Invoke(obj: null, new object?[] { sender, actorReference, expected })
            ?? throw new InvalidOperationException("BrainSignalRouterActor.SenderMatchesActorReferenceOrPid returned null."));

    private static PID? InvokeTryBuildLocalizedLocalPid(ShardRoute route)
        => (PID?)TryBuildLocalizedLocalPidMethod.Invoke(obj: null, new object?[] { route });
}
