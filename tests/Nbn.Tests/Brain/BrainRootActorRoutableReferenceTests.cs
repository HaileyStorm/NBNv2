using Nbn.Proto.Control;
using Nbn.Runtime.Brain;
using Nbn.Shared;
using Proto;
using Xunit;

namespace Nbn.Tests.Brain;

public class BrainRootActorRoutableReferenceTests
{
    [Fact]
    public async Task NotifyHiveMind_UsesRoutableReferences_WhenLocalEndpointCandidatesAreProvided()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();
        const string brainRootName = "brain-root-test";
        const string routerName = "router-test";

        var registerTcs = new TaskCompletionSource<RegisterBrain>(TaskCreationOptions.RunContinuationsAsynchronously);
        var updateTcs = new TaskCompletionSource<UpdateBrainSignalRouter>(TaskCreationOptions.RunContinuationsAsynchronously);
        var hiveProbe = root.Spawn(Props.FromProducer(() => new HiveProbe(registerTcs, updateTcs)));
        var router = root.SpawnNamed(Props.FromProducer(static () => new IgnoreActor()), routerName);

        var candidates = new[]
        {
            new ServiceEndpointCandidate("100.64.0.10:12040", brainRootName, ServiceEndpointCandidateKind.Tailnet, 1000, "tailnet", true),
            new ServiceEndpointCandidate("192.168.1.20:12040", brainRootName, ServiceEndpointCandidateKind.Lan, 900, "lan")
        };

        var brainRoot = root.SpawnNamed(
            Props.FromProducer(() => new BrainRootActor(
                brainId,
                hiveProbe,
                autoSpawnSignalRouter: false,
                localEndpointCandidates: candidates)),
            brainRootName);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var register = await registerTcs.Task.WaitAsync(cts.Token);
        Assert.StartsWith(RoutablePidReference.Prefix, register.BrainRootPid, StringComparison.Ordinal);
        Assert.True(RoutablePidReference.TryDecode(register.BrainRootPid, out var rootSet));
        Assert.Equal(brainRootName, rootSet.ActorName);

        root.Send(brainRoot, new SetSignalRouter(router));

        var update = await updateTcs.Task.WaitAsync(cts.Token);
        Assert.StartsWith(RoutablePidReference.Prefix, update.SignalRouterPid, StringComparison.Ordinal);
        Assert.True(RoutablePidReference.TryDecode(update.SignalRouterPid, out var routerSet));
        Assert.Equal(routerName, routerSet.ActorName);

        await system.ShutdownAsync();
    }

    private sealed class HiveProbe(
        TaskCompletionSource<RegisterBrain> registerTcs,
        TaskCompletionSource<UpdateBrainSignalRouter> updateTcs) : IActor
    {
        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case RegisterBrain register:
                    registerTcs.TrySetResult(register);
                    break;
                case UpdateBrainSignalRouter update:
                    updateTcs.TrySetResult(update);
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class IgnoreActor : IActor
    {
        public Task ReceiveAsync(IContext context) => Task.CompletedTask;
    }
}
