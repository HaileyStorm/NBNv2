using System.Reflection;
using System.Net;
using System.Net.Sockets;
using Nbn.Runtime.HiveMind;
using Proto;

namespace Nbn.Tests.HiveMind;

public sealed class HiveMindPidMatchingTests
{
    private static readonly MethodInfo SenderMatchesPidMethod = typeof(HiveMindActor).GetMethod(
        "SenderMatchesPid",
        BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new InvalidOperationException("HiveMindActor.SenderMatchesPid was not found.");

    [Theory]
    [InlineData("worker.local:12040", "worker.local:12040", true)]
    [InlineData("LOCALHOST:12040", "localhost:12040", true)]
    [InlineData("localhost:12040", "127.0.0.1:12040", true)]
    [InlineData("127.0.0.1:12040", "localhost:12040", true)]
    [InlineData("0.0.0.0:12040", "127.0.0.1:12040", true)]
    [InlineData("127.0.0.1:12040", "0.0.0.0:12040", true)]
    [InlineData("::1:12040", "localhost:12040", true)]
    [InlineData("[::1]:12040", "127.0.0.1:12040", true)]
    [InlineData("::1:12040", "::1:12041", false)]
    [InlineData("0.0.0.0:12040", "192.168.1.4:12040", false)]
    [InlineData("worker.local:12040", "worker.local:12041", false)]
    public void SenderMatchesPid_Uses_Endpoint_Equivalence_For_Same_ActorId(
        string senderAddress,
        string expectedAddress,
        bool expectedMatch)
    {
        var sender = new PID(senderAddress, "worker-node");
        var expected = new PID(expectedAddress, "worker-node");

        var match = InvokeSenderMatchesPid(sender, expected);
        Assert.Equal(expectedMatch, match);
    }

    [Fact]
    public void SenderMatchesPid_Rejects_Different_ActorId_Even_With_Equivalent_Addresses()
    {
        var sender = new PID("127.0.0.1:12040", "worker-node-A");
        var expected = new PID("localhost:12040", "worker-node-B");

        var match = InvokeSenderMatchesPid(sender, expected);
        Assert.False(match);
    }

    [Fact]
    public void SenderMatchesPid_Matches_Addressless_Exact_Pid()
    {
        var sender = new PID(string.Empty, "worker-node");
        var expected = new PID(string.Empty, "worker-node");

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

        var sender = new PID($"{Dns.GetHostName()}:12040", "worker-node");
        var expected = new PID($"{resolvedAddress}:12040", "worker-node");

        var match = InvokeSenderMatchesPid(sender, expected);
        Assert.True(match);
    }

    private static bool InvokeSenderMatchesPid(PID sender, PID expected)
        => (bool)(SenderMatchesPidMethod.Invoke(obj: null, new object?[] { sender, expected })
            ?? throw new InvalidOperationException("HiveMindActor.SenderMatchesPid returned null."));
}
