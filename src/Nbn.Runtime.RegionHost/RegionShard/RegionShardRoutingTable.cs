using Nbn.Shared.Addressing;
using Nbn.Shared.Format;
using Nbn.Shared.Sharding;

namespace Nbn.Runtime.RegionHost;

public readonly struct ShardSpan
{
    public ShardSpan(int start, int count, ShardId32 shardId)
    {
        if (start < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(start), start, "Start must be non-negative.");
        }

        if (count <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), count, "Count must be positive.");
        }

        Start = start;
        Count = count;
        ShardId = shardId;
    }

    public int Start { get; }
    public int Count { get; }
    public int EndExclusive => Start + Count;
    public ShardId32 ShardId { get; }
}

public sealed class RegionShardRoutingTable
{
    private readonly Dictionary<int, ShardSpan[]> _regions;

    public RegionShardRoutingTable(Dictionary<int, ShardSpan[]> regions)
    {
        _regions = regions ?? throw new ArgumentNullException(nameof(regions));
    }

    public bool TryGetShard(int regionId, int neuronId, out ShardId32 shardId)
    {
        shardId = default;
        if (!_regions.TryGetValue(regionId, out var spans) || spans.Length == 0)
        {
            return false;
        }

        var lo = 0;
        var hi = spans.Length - 1;
        while (lo <= hi)
        {
            var mid = (lo + hi) / 2;
            var span = spans[mid];
            if (neuronId < span.Start)
            {
                hi = mid - 1;
            }
            else if (neuronId >= span.EndExclusive)
            {
                lo = mid + 1;
            }
            else
            {
                shardId = span.ShardId;
                return true;
            }
        }

        return false;
    }

    public static RegionShardRoutingTable CreateSingleShard(NbnRegionDirectoryEntry[] regions)
    {
        if (regions is null)
        {
            throw new ArgumentNullException(nameof(regions));
        }

        var map = new Dictionary<int, ShardSpan[]>();
        for (var regionId = 0; regionId < regions.Length; regionId++)
        {
            var span = regions[regionId].NeuronSpan;
            if (span == 0)
            {
                continue;
            }

            map[regionId] = new[] { new ShardSpan(0, (int)span, ShardId32.From(regionId, 0)) };
        }

        return new RegionShardRoutingTable(map);
    }

    public static RegionShardRoutingTable CreateSingleShard(int regionId, int neuronSpan)
    {
        var map = new Dictionary<int, ShardSpan[]>
        {
            [regionId] = new[] { new ShardSpan(0, neuronSpan, ShardId32.From(regionId, 0)) }
        };
        return new RegionShardRoutingTable(map);
    }

    public static RegionShardRoutingTable CreateFromPlan(IReadOnlyDictionary<int, IReadOnlyList<ShardPlanSpan>> regions)
    {
        if (regions is null)
        {
            throw new ArgumentNullException(nameof(regions));
        }

        var map = new Dictionary<int, ShardSpan[]>();
        foreach (var entry in regions)
        {
            if (entry.Value.Count == 0)
            {
                continue;
            }

            var spans = new ShardSpan[entry.Value.Count];
            for (var i = 0; i < entry.Value.Count; i++)
            {
                var plan = entry.Value[i];
                var shardId = ShardId32.From(plan.RegionId, plan.ShardIndex);
                spans[i] = new ShardSpan(plan.NeuronStart, plan.NeuronCount, shardId);
            }

            Array.Sort(spans, static (a, b) => a.Start.CompareTo(b.Start));
            map[entry.Key] = spans;
        }

        return new RegionShardRoutingTable(map);
    }
}
