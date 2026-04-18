using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Runtime.Brain;
using Nbn.Runtime.IO;
using Nbn.Runtime.RegionHost;
using Nbn.Shared;
using Proto;
using ProtoControl = Nbn.Proto.Control;
using SharedShardId32 = Nbn.Shared.Addressing.ShardId32;

namespace Nbn.Runtime.WorkerNode;

public sealed partial class WorkerNodeActor
{
    private void RegisterShard(
        IContext context,
        BrainHostingState brain,
        SharedShardId32 shardId,
        int neuronStart,
        int neuronCount,
        PID shardPid,
        string assignmentId)
    {
        var hiveMindPid = ResolveHiveMindPid(context);
        if (hiveMindPid is null)
        {
            return;
        }

        var remoteShardPid = ToObservedRemotePid(context, shardPid);
        TryRequest(context, hiveMindPid, new ProtoControl.RegisterShard
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            ShardPid = PidLabel(remoteShardPid),
            NeuronStart = (uint)Math.Max(0, neuronStart),
            NeuronCount = (uint)Math.Max(0, neuronCount),
            PlacementEpoch = brain.PlacementEpoch,
            AssignmentId = assignmentId ?? string.Empty
        });
    }

    private void RegisterOutputSink(IContext context, BrainHostingState brain)
        => RegisterOutputSink(context, brain, allowClear: false);

    private void UpdateInputCoordinatorWidth(IContext context, BrainHostingState brain)
    {
        if (brain.InputCoordinatorPid is null)
        {
            return;
        }

        var inputWidth = ResolveInputWidth(brain);
        context.Send(brain.InputCoordinatorPid, new UpdateInputWidth((uint)Math.Max(1, inputWidth)));
    }

    private void UpdateOutputCoordinatorWidth(IContext context, BrainHostingState brain)
    {
        if (brain.OutputCoordinatorPid is null)
        {
            return;
        }

        var outputWidth = ResolveOutputWidth(brain);
        context.Send(brain.OutputCoordinatorPid, new UpdateOutputWidth((uint)Math.Max(1, outputWidth)));
    }

    private void RegisterOutputSink(IContext context, BrainHostingState brain, bool allowClear)
    {
        if (brain.OutputCoordinatorPid is null && !allowClear)
        {
            return;
        }

        var hiveMindPid = ResolveHiveMindPid(context);
        if (hiveMindPid is null)
        {
            return;
        }

        TryRequest(context, hiveMindPid, new ProtoControl.RegisterOutputSink
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            OutputPid = brain.OutputCoordinatorPid is null
                ? string.Empty
                : PidLabel(ToRemotePid(context, brain.OutputCoordinatorPid))
        });
    }

    private void UnregisterShard(
        IContext context,
        BrainHostingState brain,
        SharedShardId32 shardId,
        string assignmentId)
    {
        var hiveMindPid = ResolveHiveMindPid(context);
        if (hiveMindPid is null)
        {
            return;
        }

        TryRequest(context, hiveMindPid, new ProtoControl.UnregisterShard
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            RegionId = (uint)shardId.RegionId,
            ShardIndex = (uint)shardId.ShardIndex,
            PlacementEpoch = brain.PlacementEpoch,
            AssignmentId = assignmentId ?? string.Empty
        });
    }

    private void PushRouting(IContext context, BrainHostingState brain)
    {
        var routes = brain.RegionShards.Values
            .OrderBy(static entry => entry.ShardId.RegionId)
            .ThenBy(static entry => entry.ShardId.ShardIndex)
            .Select(entry => new ShardRoute(entry.ShardId.Value, entry.Pid))
            .ToArray();

        var snapshot = routes.Length == 0 ? RoutingTableSnapshot.Empty : new RoutingTableSnapshot(routes);
        if (brain.BrainRootPid is not null)
        {
            context.Send(brain.BrainRootPid, new SetRoutingTable(snapshot));
        }

        if (brain.SignalRouterPid is not null)
        {
            context.Send(brain.SignalRouterPid, new SetRoutingTable(snapshot));
        }

        if (brain.RegionShards.Count == 0)
        {
            return;
        }

        var shardRouting = BuildShardRouting(brain);
        foreach (var shard in brain.RegionShards.Values)
        {
            context.Send(shard.Pid, new RegionShardUpdateRouting(shardRouting));
        }
    }

    private void PushShardEndpoints(IContext context, BrainHostingState brain)
    {
        if (brain.RegionShards.Count == 0)
        {
            return;
        }

        var tickSink = ResolveHiveMindPid(context);
        foreach (var shard in brain.RegionShards.Values)
        {
            var outputSink = shard.ShardId.RegionId == NbnConstants.OutputRegionId
                ? brain.OutputCoordinatorPid
                : null;
            context.Send(
                shard.Pid,
                new RegionShardUpdateEndpoints(brain.SignalRouterPid, outputSink, tickSink));
        }
    }

    private void PushIoGatewayRegistration(IContext context, BrainHostingState brain)
    {
        if (!TryResolveEndpointPid(ServiceEndpointSettings.IoGatewayKey, out var ioPid))
        {
            return;
        }

        var target = brain.BrainRootPid ?? brain.SignalRouterPid;
        if (target is null)
        {
            return;
        }

        var register = new RegisterIoGateway
        {
            BrainId = brain.BrainId.ToProtoUuid(),
            IoGatewayPid = PidLabel(ioPid),
            InputTickDrainArmed = false
        };
        if (brain.RuntimeInfo?.InputCoordinatorMode is { } inputCoordinatorMode)
        {
            register.InputCoordinatorMode = inputCoordinatorMode;
        }

        context.Send(target, register);
    }

    private void UpdateRuntimeWidthsFromShards(BrainHostingState brain)
    {
        brain.RuntimeInfo ??= new BrainRuntimeInfo();
        foreach (var shard in brain.RegionShards.Values)
        {
            var span = shard.NeuronStart + shard.NeuronCount;
            if (shard.ShardId.RegionId == NbnConstants.InputRegionId)
            {
                brain.RuntimeInfo.InputWidth = Math.Max(brain.RuntimeInfo.InputWidth, span);
            }

            if (shard.ShardId.RegionId == NbnConstants.OutputRegionId)
            {
                brain.RuntimeInfo.OutputWidth = Math.Max(brain.RuntimeInfo.OutputWidth, span);
            }
        }
    }

    private int ResolveInputWidth(BrainHostingState brain)
    {
        brain.RuntimeInfo ??= new BrainRuntimeInfo();
        UpdateRuntimeWidthsFromShards(brain);
        return Math.Max(1, brain.RuntimeInfo.InputWidth);
    }

    private int ResolveOutputWidth(BrainHostingState brain)
    {
        brain.RuntimeInfo ??= new BrainRuntimeInfo();
        UpdateRuntimeWidthsFromShards(brain);
        return Math.Max(1, brain.RuntimeInfo.OutputWidth);
    }

    private RegionShardRoutingTable BuildShardRouting(
        BrainHostingState brain,
        (SharedShardId32 ShardId, int NeuronStart, int NeuronCount)? includeShard = null)
    {
        var map = new Dictionary<int, List<ShardSpan>>();
        foreach (var shard in brain.RegionShards.Values)
        {
            if (!map.TryGetValue(shard.ShardId.RegionId, out var spans))
            {
                spans = new List<ShardSpan>();
                map[shard.ShardId.RegionId] = spans;
            }

            spans.Add(new ShardSpan(shard.NeuronStart, Math.Max(1, shard.NeuronCount), shard.ShardId));
        }

        if (includeShard.HasValue)
        {
            var include = includeShard.Value;
            if (!map.TryGetValue(include.ShardId.RegionId, out var spans))
            {
                spans = new List<ShardSpan>();
                map[include.ShardId.RegionId] = spans;
            }

            spans.RemoveAll(span => span.ShardId.Equals(include.ShardId));
            spans.Add(new ShardSpan(include.NeuronStart, Math.Max(1, include.NeuronCount), include.ShardId));
        }

        var compact = new Dictionary<int, ShardSpan[]>();
        foreach (var entry in map)
        {
            var spans = entry.Value
                .OrderBy(static span => span.Start)
                .ToArray();
            compact[entry.Key] = spans;
        }

        return new RegionShardRoutingTable(compact);
    }
}
