using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Nbn.Shared.Quantization;
using Proto;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Tests.RegionHost;

public sealed class RegionShardLifecycleForwardingTests
{
    [Fact]
    public async Task ShardLifecycleMessages_Preserve_AssignmentIdentity_WhenForwarded()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var registerSeen = new TaskCompletionSource<RegisterShard>(TaskCreationOptions.RunContinuationsAsynchronously);
        var unregisterSeen = new TaskCompletionSource<UnregisterShard>(TaskCreationOptions.RunContinuationsAsynchronously);
        var tickSink = root.Spawn(Props.FromProducer(() => new LifecycleCaptureActor(registerSeen, unregisterSeen)));
        var brainId = Guid.NewGuid();
        var shardId = ShardId32.From(regionId: 5, shardIndex: 0);
        var state = CreateState(regionId: 5, neuronStart: 0, neuronCount: 2);

        var actor = root.Spawn(Props.FromProducer(() => new RegionShardActor(
            state,
            new RegionShardActorConfig(
                BrainId: brainId,
                ShardId: shardId,
                Router: null,
                OutputSink: null,
                TickSink: tickSink,
                Routing: RegionShardRoutingTable.CreateSingleShard(state.RegionId, state.NeuronCount)))));

        root.Send(actor, new RegisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = 5,
            ShardIndex = 0,
            ShardPid = "worker/region-5-shard-0",
            NeuronStart = 0,
            NeuronCount = 2,
            PlacementEpoch = 42,
            AssignmentId = "assign-42-region-5-shard-0"
        });
        root.Send(actor, new UnregisterShard
        {
            BrainId = brainId.ToProtoUuid(),
            RegionId = 5,
            ShardIndex = 0,
            PlacementEpoch = 42,
            AssignmentId = "assign-42-region-5-shard-0"
        });

        var registered = await registerSeen.Task.WaitAsync(TimeSpan.FromSeconds(2));
        var unregistered = await unregisterSeen.Task.WaitAsync(TimeSpan.FromSeconds(2));

        Assert.Equal("assign-42-region-5-shard-0", registered.AssignmentId);
        Assert.Equal<ulong>(42, registered.PlacementEpoch);
        Assert.Equal("assign-42-region-5-shard-0", unregistered.AssignmentId);
        Assert.Equal<ulong>(42, unregistered.PlacementEpoch);

        await system.ShutdownAsync();
    }

    private static RegionShardState CreateState(int regionId, int neuronStart, int neuronCount)
    {
        var regionSpans = new int[NbnConstants.RegionCount];
        regionSpans[regionId] = neuronCount;
        regionSpans[NbnConstants.OutputRegionId] = 1;

        return new RegionShardState(
            regionId: regionId,
            neuronStart: neuronStart,
            neuronCount: neuronCount,
            brainSeed: 0x0102030405060708UL,
            strengthQuantization: QuantizationSchemas.DefaultNbn.Strength,
            regionSpans: regionSpans,
            buffer: new float[neuronCount],
            enabled: Enumerable.Repeat(true, neuronCount).ToArray(),
            exists: Enumerable.Repeat(true, neuronCount).ToArray(),
            accumulationFunctions: Enumerable.Repeat((byte)AccumulationFunction.AccumSum, neuronCount).ToArray(),
            activationFunctions: Enumerable.Repeat((byte)ActivationFunction.ActNone, neuronCount).ToArray(),
            resetFunctions: Enumerable.Repeat((byte)ResetFunction.ResetHold, neuronCount).ToArray(),
            paramA: new float[neuronCount],
            paramB: new float[neuronCount],
            preActivationThreshold: Enumerable.Repeat(-1f, neuronCount).ToArray(),
            activationThreshold: Enumerable.Repeat(1f, neuronCount).ToArray(),
            axonCounts: new ushort[neuronCount],
            axonStartOffsets: new int[neuronCount],
            axons: new RegionShardAxons(
                targetRegionIds: Array.Empty<byte>(),
                targetNeuronIds: Array.Empty<int>(),
                strengths: Array.Empty<float>(),
                baseStrengthCodes: Array.Empty<byte>(),
                runtimeStrengthCodes: Array.Empty<byte>(),
                hasRuntimeOverlay: Array.Empty<bool>(),
                fromAddress32: Array.Empty<uint>(),
                toAddress32: Array.Empty<uint>()));
    }

    private sealed class LifecycleCaptureActor(
        TaskCompletionSource<RegisterShard> registerSeen,
        TaskCompletionSource<UnregisterShard> unregisterSeen) : IActor
    {
        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case RegisterShard register:
                    registerSeen.TrySetResult(register);
                    break;
                case UnregisterShard unregister:
                    unregisterSeen.TrySetResult(unregister);
                    break;
            }

            return Task.CompletedTask;
        }
    }
}
