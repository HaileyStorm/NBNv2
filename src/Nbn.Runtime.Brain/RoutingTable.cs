using Nbn.Shared.Addressing;
using Proto;

namespace Nbn.Runtime.Brain;

internal sealed class RoutingTable
{
    private readonly Dictionary<ShardId32, PID> _routes = new();
    private ShardRoute[] _entries = Array.Empty<ShardRoute>();

    public IReadOnlyList<ShardRoute> Entries => _entries;

    public int Count => _entries.Length;

    public void Replace(IReadOnlyList<ShardRoute>? routes)
    {
        _routes.Clear();

        if (routes is null || routes.Count == 0)
        {
            _entries = Array.Empty<ShardRoute>();
            return;
        }

        var list = new List<ShardRoute>(routes.Count);
        var index = new Dictionary<ShardId32, int>();

        for (var i = 0; i < routes.Count; i++)
        {
            var route = routes[i];
            if (route.Pid is null)
            {
                continue;
            }

            if (index.TryGetValue(route.ShardId, out var existingIndex))
            {
                list[existingIndex] = route;
            }
            else
            {
                index[route.ShardId] = list.Count;
                list.Add(route);
            }

            _routes[route.ShardId] = route.Pid;
        }

        _entries = list.ToArray();
    }

    public bool TryGetPid(ShardId32 shardId, out PID? pid)
        => _routes.TryGetValue(shardId, out pid);
}
