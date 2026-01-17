using System;

namespace Nbn.Shared.Addressing;

public readonly struct ShardId32 : IEquatable<ShardId32>
{
    public const int ShardIndexBits = 16;
    public const int RegionBits = 5;
    public const int RegionShift = ShardIndexBits;

    public const uint ShardIndexMask = 0xFFFFu;
    public const uint RegionMask = 0x1Fu;

    public ShardId32(uint value)
    {
        Value = value;
    }

    public uint Value { get; }

    public int RegionId => (int)((Value >> RegionShift) & RegionMask);

    public int ShardIndex => (int)(Value & ShardIndexMask);

    public static ShardId32 From(int regionId, int shardIndex)
    {
        if (!IsValidRegion(regionId))
        {
            throw new ArgumentOutOfRangeException(nameof(regionId), regionId, "RegionId must be between 0 and 31.");
        }

        if (shardIndex < 0 || shardIndex > ShardIndexMask)
        {
            throw new ArgumentOutOfRangeException(nameof(shardIndex), shardIndex, "ShardIndex must fit in 16 bits.");
        }

        var value = ((uint)regionId << RegionShift) | (uint)shardIndex;
        return new ShardId32(value);
    }

    public static bool TryFrom(int regionId, int shardIndex, out ShardId32 shardId)
    {
        if (!IsValidRegion(regionId) || shardIndex < 0 || shardIndex > ShardIndexMask)
        {
            shardId = default;
            return false;
        }

        shardId = new ShardId32(((uint)regionId << RegionShift) | (uint)shardIndex);
        return true;
    }

    public bool Equals(ShardId32 other) => Value == other.Value;

    public override bool Equals(object? obj) => obj is ShardId32 other && Equals(other);

    public override int GetHashCode() => (int)Value;

    public override string ToString() => $"r{RegionId}:s{ShardIndex}";

    private static bool IsValidRegion(int regionId) =>
        regionId >= NbnConstants.RegionMinId && regionId <= NbnConstants.RegionMaxId;
}