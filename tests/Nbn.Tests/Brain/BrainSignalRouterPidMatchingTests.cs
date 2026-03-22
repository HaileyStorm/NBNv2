using System.Net;
using System.Net.Sockets;
using System.Reflection;
using Nbn.Runtime.Brain;
using Proto;

namespace Nbn.Tests.Brain;

public sealed class BrainSignalRouterPidMatchingTests
{
    private static readonly MethodInfo SenderMatchesPidMethod = typeof(BrainSignalRouterActor).GetMethod(
        "SenderMatchesPid",
        BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new InvalidOperationException("BrainSignalRouterActor.SenderMatchesPid was not found.");

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

    private static bool InvokeSenderMatchesPid(PID sender, PID expected)
        => (bool)(SenderMatchesPidMethod.Invoke(obj: null, new object?[] { sender, expected })
            ?? throw new InvalidOperationException("BrainSignalRouterActor.SenderMatchesPid returned null."));
}
