using Nbn.Shared.Addressing;
using Proto;

namespace Nbn.Runtime.Brain;

/// <summary>
/// Describes one routed shard destination for a brain.
/// </summary>
/// <param name="ShardIdValue">Packed shard identifier value used for remoting-safe serialization.</param>
/// <param name="Pid">Region shard PID that receives routed messages for the shard.</param>
public sealed record ShardRoute(uint ShardIdValue, PID Pid)
{
    /// <summary>
    /// Gets the decoded shard identifier for the route.
    /// </summary>
    public ShardId32 ShardId => new(ShardIdValue);
}

/// <summary>
/// Represents a complete, immutable routing-table snapshot for a brain.
/// </summary>
public sealed record RoutingTableSnapshot
{
    /// <summary>
    /// Gets an empty routing-table snapshot.
    /// </summary>
    public static RoutingTableSnapshot Empty { get; } = new(Array.Empty<ShardRoute>());

    /// <summary>
    /// Creates a routing-table snapshot from the supplied routes.
    /// </summary>
    /// <param name="routes">Routes keyed by shard for the current placement epoch.</param>
    public RoutingTableSnapshot(IReadOnlyList<ShardRoute>? routes)
    {
        Routes = routes ?? Array.Empty<ShardRoute>();
    }

    /// <summary>
    /// Gets the routes contained in the snapshot.
    /// </summary>
    public IReadOnlyList<ShardRoute> Routes { get; }

    /// <summary>
    /// Gets the number of routes in the snapshot.
    /// </summary>
    public int Count => Routes.Count;
}

/// <summary>
/// Replaces the signal router's routing table with the supplied snapshot.
/// </summary>
/// <param name="Table">Snapshot to apply.</param>
public sealed record SetRoutingTable(RoutingTableSnapshot Table);

/// <summary>
/// Requests the current routing-table snapshot.
/// </summary>
public sealed record GetRoutingTable;

/// <summary>
/// Attaches the supplied signal router PID to a brain root.
/// </summary>
/// <param name="SignalRouter">Signal router PID to attach.</param>
public sealed record SetSignalRouter(PID SignalRouter);

/// <summary>
/// Requests the current signal router attachment.
/// </summary>
public sealed record GetSignalRouter;

/// <summary>
/// Returns the currently attached signal router PID, if any.
/// </summary>
/// <param name="SignalRouter">Attached signal router PID, or <see langword="null"/> when none is attached.</param>
public sealed record SignalRouterResponse(PID? SignalRouter);
