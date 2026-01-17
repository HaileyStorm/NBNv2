using System;
using System.Collections.Generic;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;

namespace Nbn.Shared.Validation;

public static class NbnBinaryValidator
{
    public static NbnValidationResult ValidateNbn(NbnHeaderV2 header, IReadOnlyList<NbnRegionSection> regions)
    {
        var result = new NbnValidationResult();

        if (header.Magic != "NBN2")
        {
            result.Add("NBN header magic must be NBN2.");
        }

        if (header.Version != 2)
        {
            result.Add("NBN header version must be 2.");
        }

        if (header.Endianness != 1)
        {
            result.Add("NBN header endianness must be little-endian (1).", "header");
        }

        if (header.HeaderByteCount != NbnBinary.NbnHeaderBytes)
        {
            result.Add("NBN header size must be 1024 bytes.", "header");
        }

        if (header.Regions.Length != NbnConstants.RegionCount)
        {
            result.Add("Region directory must contain 32 entries.", "header");
        }

        var regionMap = new Dictionary<byte, NbnRegionSection>();
        foreach (var region in regions)
        {
            if (regionMap.ContainsKey(region.RegionId))
            {
                result.Add("Duplicate region section encountered.", $"region {region.RegionId}");
            }
            else
            {
                regionMap.Add(region.RegionId, region);
            }
        }

        for (var regionId = 0; regionId < header.Regions.Length; regionId++)
        {
            var entry = header.Regions[regionId];
            if (entry.NeuronSpan == 0)
            {
                if (entry.Offset != 0)
                {
                    result.Add("Absent region directory entry must have offset 0.", $"region {regionId}");
                }

                continue;
            }

            if (entry.NeuronSpan > NbnConstants.MaxAxonTargetNeuronId)
            {
                result.Add("Region neuron span exceeds 22-bit limit.", $"region {regionId}");
            }

            if (entry.Offset == 0)
            {
                result.Add("Region directory entry offset must be non-zero for present regions.", $"region {regionId}");
                continue;
            }

            if (!regionMap.TryGetValue((byte)regionId, out var section))
            {
                result.Add("Missing region section for directory entry.", $"region {regionId}");
                continue;
            }

            ValidateRegionSection(result, header, section, entry, regionId);
        }

        if (header.Regions.Length == NbnConstants.RegionCount)
        {
            var input = header.Regions[NbnConstants.InputRegionId];
            var output = header.Regions[NbnConstants.OutputRegionId];
            if (input.NeuronSpan == 0 || input.Offset == 0)
            {
                result.Add("Input region must be present.");
            }

            if (output.NeuronSpan == 0 || output.Offset == 0)
            {
                result.Add("Output region must be present.");
            }
        }

        return result;
    }

    public static NbnValidationResult ValidateNbs(
        NbsHeaderV2 header,
        IReadOnlyList<NbsRegionSection> regions,
        NbsOverlaySection? overlays)
    {
        var result = new NbnValidationResult();

        if (header.Magic != "NBS2")
        {
            result.Add("NBS header magic must be NBS2.");
        }

        if (header.Version != 2)
        {
            result.Add("NBS header version must be 2.");
        }

        if (header.Endianness != 1)
        {
            result.Add("NBS header endianness must be little-endian (1).", "header");
        }

        if (header.HeaderByteCount != NbnBinary.NbsHeaderBytes)
        {
            result.Add("NBS header size must be 512 bytes.", "header");
        }

        foreach (var region in regions)
        {
            if (!NbnInvariants.IsValidRegionId(region.RegionId))
            {
                result.Add("NBS region id is out of range.", $"region {region.RegionId}");
            }

            if (region.BufferCodes.Length != region.NeuronSpan)
            {
                result.Add("NBS region buffer length does not match neuron span.", $"region {region.RegionId}");
            }

            if (header.EnabledBitsetIncluded)
            {
                if (region.EnabledBitset is null)
                {
                    result.Add("NBS enabled bitset flag set but region missing enabled bitset.", $"region {region.RegionId}");
                }
                else
                {
                    var expectedBytes = (int)((region.NeuronSpan + 7) / 8);
                    if (region.EnabledBitset.Length != expectedBytes)
                    {
                        result.Add("NBS enabled bitset length does not match neuron span.", $"region {region.RegionId}");
                    }
                }
            }
        }

        if (header.AxonOverlayIncluded)
        {
            if (overlays is null)
            {
                result.Add("NBS overlay flag set but overlay section missing.");
            }
        }
        else if (overlays is not null && overlays.Records.Length > 0)
        {
            result.Add("Overlay records present but header does not include overlay section.");
        }

        return result;
    }

    private static void ValidateRegionSection(
        NbnValidationResult result,
        NbnHeaderV2 header,
        NbnRegionSection section,
        NbnRegionDirectoryEntry entry,
        int regionId)
    {
        var context = $"region {regionId}";
        if (section.RegionId != regionId)
        {
            result.Add("Region section id does not match directory index.", context);
        }

        if (section.NeuronSpan != entry.NeuronSpan)
        {
            result.Add("Region neuron span does not match directory entry.", context);
        }

        if (section.TotalAxons != entry.TotalAxons)
        {
            result.Add("Region axon total does not match directory entry.", context);
        }

        if (section.Stride != header.AxonStride)
        {
            result.Add("Region stride does not match header stride.", context);
        }

        var expectedCheckpointCount = (uint)((section.NeuronSpan + section.Stride - 1) / section.Stride + 1);
        if (section.CheckpointCount != expectedCheckpointCount)
        {
            result.Add("Checkpoint count does not match stride rules.", context);
        }

        if ((uint)section.Checkpoints.Length != section.CheckpointCount)
        {
            result.Add("Checkpoint array length does not match checkpoint count.", context);
        }

        if ((uint)section.NeuronRecords.Length != section.NeuronSpan)
        {
            result.Add("Neuron record count does not match neuron span.", context);
        }

        if ((ulong)section.AxonRecords.Length != section.TotalAxons)
        {
            result.Add("Axon record count does not match total axons.", context);
        }

        ulong axonSum = 0;
        for (var i = 0; i < section.NeuronRecords.Length; i++)
        {
            var neuron = section.NeuronRecords[i];
            axonSum += neuron.AxonCount;

            if (!NbnInvariants.TryValidateNeuronRecord(neuron, regionId, out var error))
            {
                result.Add(error ?? "Invalid neuron record.", $"region {regionId} neuron {i}");
            }
        }

        if (axonSum != section.TotalAxons)
        {
            result.Add("Neuron axon counts do not sum to total axons.", context);
        }

        if (section.Checkpoints.Length > 0)
        {
            if (section.Checkpoints[0] != 0)
            {
                result.Add("First checkpoint must be zero.", context);
            }

            if (section.Checkpoints[^1] != section.TotalAxons)
            {
                result.Add("Last checkpoint must equal total axons.", context);
            }

            var last = 0UL;
            for (var i = 0; i < section.Checkpoints.Length; i++)
            {
                var value = section.Checkpoints[i];
                if (value < last)
                {
                    result.Add("Checkpoint values must be non-decreasing.", context);
                    break;
                }

                last = value;
            }
        }

        ValidateAxonLists(result, section, regionId);
    }

    private static void ValidateAxonLists(NbnValidationResult result, NbnRegionSection section, int regionId)
    {
        var axonIndex = 0;
        for (var neuronIndex = 0; neuronIndex < section.NeuronRecords.Length; neuronIndex++)
        {
            var axonCount = section.NeuronRecords[neuronIndex].AxonCount;
            if (axonIndex + axonCount > section.AxonRecords.Length)
            {
                result.Add("Axon records exceed total axon count.", $"region {regionId} neuron {neuronIndex}");
                return;
            }

            if (axonCount > 0)
            {
                var span = section.AxonRecords.AsSpan(axonIndex, axonCount);
                if (!NbnInvariants.TryValidateAxonList(span, regionId, null, out var error))
                {
                    result.Add(error ?? "Invalid axon list.", $"region {regionId} neuron {neuronIndex}");
                }
            }

            axonIndex += axonCount;
        }

        if (axonIndex != section.AxonRecords.Length)
        {
            result.Add("Axon records length does not match neuron axon counts.", $"region {regionId}");
        }
    }
}
