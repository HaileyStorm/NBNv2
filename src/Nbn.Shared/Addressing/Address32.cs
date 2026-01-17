using System;

namespace Nbn.Shared.Addressing;

public readonly struct Address32 : IEquatable<Address32>
{
    public Address32(uint value)
    {
        Value = value;
    }

    public uint Value { get; }

    public int RegionId => (int)(Value >> NbnConstants.AddressNeuronBits);

    public int NeuronId => (int)(Value & NbnConstants.AddressNeuronMask);

    public static Address32 From(int regionId, int neuronId)
    {
        if (!IsValidRegion(regionId))
        {
            throw new ArgumentOutOfRangeException(nameof(regionId), regionId, "RegionId must be between 0 and 31.");
        }

        if (neuronId < 0 || neuronId > NbnConstants.MaxAddressNeuronId)
        {
            throw new ArgumentOutOfRangeException(nameof(neuronId), neuronId, "NeuronId must fit in 27 bits.");
        }

        var value = ((uint)regionId << NbnConstants.AddressNeuronBits) | (uint)neuronId;
        return new Address32(value);
    }

    public static bool TryFrom(int regionId, int neuronId, out Address32 address)
    {
        if (!IsValidRegion(regionId) || neuronId < 0 || neuronId > NbnConstants.MaxAddressNeuronId)
        {
            address = default;
            return false;
        }

        address = new Address32(((uint)regionId << NbnConstants.AddressNeuronBits) | (uint)neuronId);
        return true;
    }

    public bool Equals(Address32 other) => Value == other.Value;

    public override bool Equals(object? obj) => obj is Address32 other && Equals(other);

    public override int GetHashCode() => (int)Value;

    public override string ToString() => $"r{RegionId}:n{NeuronId}";

    private static bool IsValidRegion(int regionId) =>
        regionId >= NbnConstants.RegionMinId && regionId <= NbnConstants.RegionMaxId;
}