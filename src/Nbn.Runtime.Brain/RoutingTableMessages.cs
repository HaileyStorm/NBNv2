using Nbn.Shared.Addressing;
using Proto;

namespace Nbn.Runtime.Brain;

public sealed record ShardRoute(ShardId32 ShardId, PID Pid);

public sealed record RoutingTableSnapshot
{
    public static RoutingTableSnapshot Empty { get; } = new(Array.Empty<ShardRoute>());

    public RoutingTableSnapshot(IReadOnlyList<ShardRoute>? routes)
    {
        Routes = routes ?? Array.Empty<ShardRoute>();
    }

    public IReadOnlyList<ShardRoute> Routes { get; }

    public int Count => Routes.Count;
}

public sealed record SetRoutingTable(RoutingTableSnapshot Table);

public sealed record GetRoutingTable;

public sealed record SetSignalRouter(PID SignalRouter);

public sealed record GetSignalRouter;

public sealed record SignalRouterResponse(PID? SignalRouter);
