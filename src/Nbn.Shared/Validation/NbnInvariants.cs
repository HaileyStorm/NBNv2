using System;

namespace Nbn.Shared.Validation;

public static class NbnInvariants
{
    public static bool IsValidRegionId(int regionId) =>
        regionId >= NbnConstants.RegionMinId && regionId <= NbnConstants.RegionMaxId;

    public static bool IsValidRegionSpan(int neuronSpan) =>
        neuronSpan >= 0 && neuronSpan <= NbnConstants.MaxAxonTargetNeuronId;

    public static bool IsValidAddressNeuronId(int neuronId) =>
        neuronId >= 0 && neuronId <= NbnConstants.MaxAddressNeuronId;

    public static bool IsValidAxonTargetNeuronId(int neuronId) =>
        neuronId >= 0 && neuronId <= NbnConstants.MaxAxonTargetNeuronId;

    public static bool TryValidateNeuronRecord(Packing.NeuronRecord record, int regionId, out string? error)
    {
        if (!IsValidRegionId(regionId))
        {
            error = "RegionId must be between 0 and 31.";
            return false;
        }

        if (record.AxonCount > NbnConstants.MaxAxonsPerNeuron)
        {
            error = "AxonCount exceeds the maximum of 511.";
            return false;
        }

        if (!record.Exists && record.AxonCount != 0)
        {
            error = "Non-existent neurons must not have axons.";
            return false;
        }

        if ((regionId == NbnConstants.InputRegionId || regionId == NbnConstants.OutputRegionId) && !record.Exists)
        {
            error = "Input and output regions must not contain deleted neurons.";
            return false;
        }

        error = null;
        return true;
    }

    public static bool TryValidateAxonRecord(
        Packing.AxonRecord record,
        int sourceRegionId,
        int? targetRegionSpan,
        out string? error)
    {
        if (!IsValidRegionId(sourceRegionId))
        {
            error = "Source region id must be between 0 and 31.";
            return false;
        }

        if (!IsValidRegionId(record.TargetRegionId))
        {
            error = "Target region id must be between 0 and 31.";
            return false;
        }

        if (!IsValidAxonTargetNeuronId(record.TargetNeuronId))
        {
            error = "Target neuron id must fit in 22 bits.";
            return false;
        }

        if (record.TargetRegionId == NbnConstants.InputRegionId)
        {
            error = "Axons may not target the input region.";
            return false;
        }

        if (sourceRegionId == NbnConstants.OutputRegionId && record.TargetRegionId == NbnConstants.OutputRegionId)
        {
            error = "Output region axons may not target the output region.";
            return false;
        }

        if (targetRegionSpan.HasValue && record.TargetNeuronId >= targetRegionSpan.Value)
        {
            error = "Target neuron id exceeds the destination region span.";
            return false;
        }

        error = null;
        return true;
    }

    public static bool TryValidateAxonList(
        ReadOnlySpan<Packing.AxonRecord> axons,
        int sourceRegionId,
        int? targetRegionSpan,
        out string? error)
    {
        byte lastRegion = 0;
        int lastNeuron = 0;
        var hasLast = false;

        for (var i = 0; i < axons.Length; i++)
        {
            var record = axons[i];
            if (!TryValidateAxonRecord(record, sourceRegionId, targetRegionSpan, out error))
            {
                return false;
            }

            if (hasLast)
            {
                if (record.TargetRegionId < lastRegion
                    || (record.TargetRegionId == lastRegion && record.TargetNeuronId < lastNeuron))
                {
                    error = "Axon records must be sorted by target region and neuron id.";
                    return false;
                }

                if (record.TargetRegionId == lastRegion && record.TargetNeuronId == lastNeuron)
                {
                    error = "Duplicate axons from the same source neuron are not allowed.";
                    return false;
                }
            }

            lastRegion = record.TargetRegionId;
            lastNeuron = record.TargetNeuronId;
            hasLast = true;
        }

        error = null;
        return true;
    }
}