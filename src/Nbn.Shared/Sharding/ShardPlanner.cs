using Nbn.Shared.Addressing;
using Nbn.Shared.Format;

namespace Nbn.Shared.Sharding;

public enum ShardPlanMode
{
    SingleShardPerRegion = 0,
    FixedShardCountPerRegion = 1,
    MaxNeuronsPerShard = 2
}

public readonly record struct ShardPlanSpan(int RegionId, int ShardIndex, int NeuronStart, int NeuronCount);

public sealed class ShardPlanResult
{
    public ShardPlanResult(
        IReadOnlyDictionary<int, IReadOnlyList<ShardPlanSpan>> regions,
        IReadOnlyList<string> warnings)
    {
        Regions = regions ?? throw new ArgumentNullException(nameof(regions));
        Warnings = warnings ?? Array.Empty<string>();
    }

    public IReadOnlyDictionary<int, IReadOnlyList<ShardPlanSpan>> Regions { get; }
    public IReadOnlyList<string> Warnings { get; }

    public int TotalShards => Regions.Sum(entry => entry.Value.Count);
}

public static class ShardPlanner
{
    public static ShardPlanResult BuildPlan(
        NbnHeaderV2 header,
        ShardPlanMode mode,
        int? shardCount,
        int? maxNeuronsPerShard)
    {
        if (header is null)
        {
            throw new ArgumentNullException(nameof(header));
        }

        var warnings = new List<string>();
        var regions = new Dictionary<int, IReadOnlyList<ShardPlanSpan>>();
        var stride = (int)header.AxonStride;
        if (stride <= 0)
        {
            stride = NbnConstants.DefaultAxonStride;
        }

        if (stride <= 0)
        {
            stride = 1;
        }

        for (var regionId = 0; regionId < header.Regions.Length; regionId++)
        {
            var span = (int)header.Regions[regionId].NeuronSpan;
            if (span <= 0)
            {
                continue;
            }

            if (regionId == NbnConstants.InputRegionId || regionId == NbnConstants.OutputRegionId)
            {
                if (mode != ShardPlanMode.SingleShardPerRegion)
                {
                    warnings.Add($"Region {regionId} forced to a single shard for IO compatibility.");
                }

                regions[regionId] = new[] { new ShardPlanSpan(regionId, 0, 0, span) };
                continue;
            }

            IReadOnlyList<ShardPlanSpan> shards = mode switch
            {
                ShardPlanMode.SingleShardPerRegion => new[] { new ShardPlanSpan(regionId, 0, 0, span) },
                ShardPlanMode.FixedShardCountPerRegion => BuildFixedShardPlan(regionId, span, stride, shardCount, warnings),
                ShardPlanMode.MaxNeuronsPerShard => BuildMaxNeuronPlan(regionId, span, stride, maxNeuronsPerShard, warnings),
                _ => new[] { new ShardPlanSpan(regionId, 0, 0, span) }
            };

            regions[regionId] = shards;
        }

        return new ShardPlanResult(regions, warnings);
    }

    private static IReadOnlyList<ShardPlanSpan> BuildFixedShardPlan(
        int regionId,
        int span,
        int stride,
        int? shardCount,
        List<string> warnings)
    {
        var requested = shardCount.GetValueOrDefault(1);
        if (requested <= 0)
        {
            requested = 1;
        }

        if (requested > ShardId32.ShardIndexMask + 1)
        {
            throw new ArgumentOutOfRangeException(nameof(shardCount), requested, "Shard count exceeds the 16-bit shard index limit.");
        }

        var totalUnits = (span + stride - 1) / stride;
        var count = Math.Min(requested, Math.Max(1, totalUnits));
        if (count < requested)
        {
            warnings.Add($"Region {regionId} limited to {count} shard(s) (requested {requested}).");
        }

        var baseUnits = totalUnits / count;
        var remainder = totalUnits % count;
        var shards = new List<ShardPlanSpan>(count);
        var start = 0;

        for (var i = 0; i < count; i++)
        {
            var units = baseUnits + (i < remainder ? 1 : 0);
            var planned = units * stride;
            var remaining = span - start;
            if (remaining <= 0)
            {
                break;
            }

            var countNeurons = planned > remaining ? remaining : planned;
            shards.Add(new ShardPlanSpan(regionId, i, start, countNeurons));
            start += countNeurons;
        }

        return shards;
    }

    private static IReadOnlyList<ShardPlanSpan> BuildMaxNeuronPlan(
        int regionId,
        int span,
        int stride,
        int? maxNeuronsPerShard,
        List<string> warnings)
    {
        var target = maxNeuronsPerShard.GetValueOrDefault(span);
        if (target <= 0)
        {
            target = span;
        }

        if (target < stride)
        {
            warnings.Add($"Region {regionId} target {target} < stride {stride}; using stride-aligned shards.");
        }

        var targetUnits = Math.Max(1, target / stride);
        var shards = new List<ShardPlanSpan>();
        var start = 0;
        var shardIndex = 0;

        while (start < span)
        {
            if (shardIndex > ShardId32.ShardIndexMask)
            {
                throw new InvalidOperationException($"Region {regionId} shard count exceeds 16-bit shard index limit.");
            }

            var count = Math.Min(span - start, targetUnits * stride);
            shards.Add(new ShardPlanSpan(regionId, shardIndex, start, count));
            start += count;
            shardIndex++;
        }

        return shards;
    }
}
