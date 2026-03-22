using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Media;
using Avalonia.Platform.Storage;
using Avalonia.Threading;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.Format;
using Nbn.Shared.Packing;
using Nbn.Shared.Quantization;
using Nbn.Shared.Sharding;
using Nbn.Shared.Validation;
using Nbn.Tools.Workbench.Services;
using ProtoControl = Nbn.Proto.Control;
using ProtoShardPlanMode = Nbn.Proto.Control.ShardPlanMode;
using SharedShardPlanMode = Nbn.Shared.Sharding.ShardPlanMode;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class DesignerPanelViewModel
{
    private bool TryBuildNbn(out NbnHeaderV2 header, out List<NbnRegionSection> sections, out string? error)
    {
        header = null!;
        sections = new List<NbnRegionSection>();
        error = null;

        if (Brain is null)
        {
            error = "No design loaded.";
            return false;
        }

        if (Brain.AxonStride == 0)
        {
            error = "Axon stride must be greater than zero.";
            return false;
        }

        foreach (var region in Brain.Regions)
        {
            region.UpdateCounts();
        }
        Brain.UpdateTotals();

        var directory = new NbnRegionDirectoryEntry[NbnConstants.RegionCount];
        var regionCounts = Brain.Regions.ToDictionary(region => region.RegionId, region => region.NeuronCount);
        var regionMap = Brain.Regions.ToDictionary(region => region.RegionId, region => region);
        ulong offset = NbnBinary.NbnHeaderBytes;

        for (var i = 0; i < Brain.Regions.Count; i++)
        {
            var region = Brain.Regions[i];
            var neuronSpan = region.NeuronCount;
            if ((region.IsInput || region.IsOutput) && neuronSpan == 0)
            {
                error = "Input and output regions must contain neurons.";
                return false;
            }

            if (neuronSpan == 0)
            {
                directory[region.RegionId] = new NbnRegionDirectoryEntry(0, 0, 0, 0);
                continue;
            }

            if (neuronSpan > NbnConstants.MaxAxonTargetNeuronId)
            {
                error = $"Region {region.RegionId} exceeds max neuron span.";
                return false;
            }

            var neuronRecords = new NeuronRecord[neuronSpan];
            var axonRecords = new List<AxonRecord>();

            for (var neuronIndex = 0; neuronIndex < neuronSpan; neuronIndex++)
            {
                var neuron = region.Neurons[neuronIndex];
                if ((region.IsInput || region.IsOutput) && !neuron.Exists)
                {
                    error = $"Neuron {neuron.NeuronId} in region {region.RegionId} must exist.";
                    return false;
                }

                if (!neuron.Exists && neuron.Axons.Count > 0)
                {
                    error = $"Neuron {neuron.NeuronId} in region {region.RegionId} is disabled but has axons.";
                    return false;
                }

                if (neuron.Axons.Count > NbnConstants.MaxAxonsPerNeuron)
                {
                    error = $"Neuron {neuron.NeuronId} in region {region.RegionId} exceeds max axons.";
                    return false;
                }

                if (!IsActivationFunctionAllowedForRegion(region.RegionId, neuron.ActivationFunctionId))
                {
                    error = region.RegionId == NbnConstants.OutputRegionId
                        ? $"Output neuron {neuron.NeuronId} uses activation {neuron.ActivationFunctionId}, which is not allowed for output region."
                        : region.RegionId == NbnConstants.InputRegionId
                            ? $"Input neuron {neuron.NeuronId} uses activation {neuron.ActivationFunctionId}, which is not allowed for input region."
                            : $"Neuron {neuron.NeuronId} in region {region.RegionId} uses unknown activation function {neuron.ActivationFunctionId}.";
                    return false;
                }

                if (!IsResetFunctionAllowedForRegion(region.RegionId, neuron.ResetFunctionId))
                {
                    error = region.RegionId == NbnConstants.InputRegionId
                        ? $"Input neuron {neuron.NeuronId} uses reset {neuron.ResetFunctionId}, which is not allowed for input region."
                        : $"Neuron {neuron.NeuronId} in region {region.RegionId} uses unknown reset function {neuron.ResetFunctionId}.";
                    return false;
                }

                var targets = new HashSet<(int regionId, int neuronId)>();
                var orderedAxons = neuron.Axons
                    .OrderBy(axon => axon.TargetRegionId)
                    .ThenBy(axon => axon.TargetNeuronId)
                    .ToList();

                foreach (var axon in orderedAxons)
                {
                    if (axon.TargetRegionId == NbnConstants.InputRegionId)
                    {
                        error = "Axons cannot target input region.";
                        return false;
                    }

                    if (region.RegionId == NbnConstants.OutputRegionId && axon.TargetRegionId == NbnConstants.OutputRegionId)
                    {
                        error = "Output region neurons cannot target output region.";
                        return false;
                    }

                    if (!regionCounts.TryGetValue(axon.TargetRegionId, out var targetSpan) || targetSpan == 0)
                    {
                        error = $"Target region {axon.TargetRegionId} is missing.";
                        return false;
                    }

                    if (axon.TargetNeuronId < 0 || axon.TargetNeuronId >= targetSpan)
                    {
                        error = $"Target neuron {axon.TargetNeuronId} is out of range for region {axon.TargetRegionId}.";
                        return false;
                    }

                    if (regionMap.TryGetValue(axon.TargetRegionId, out var targetRegion)
                        && !targetRegion.Neurons[axon.TargetNeuronId].Exists)
                    {
                        error = $"Target neuron {axon.TargetNeuronId} in region {axon.TargetRegionId} is disabled.";
                        return false;
                    }

                    if (!targets.Add((axon.TargetRegionId, axon.TargetNeuronId)))
                    {
                        error = $"Duplicate axon from neuron {neuron.NeuronId} in region {region.RegionId}.";
                        return false;
                    }

                    axonRecords.Add(new AxonRecord((byte)axon.StrengthCode, axon.TargetNeuronId, (byte)axon.TargetRegionId));
                }

                neuronRecords[neuronIndex] = new NeuronRecord(
                    (ushort)orderedAxons.Count,
                    (byte)neuron.ParamBCode,
                    (byte)neuron.ParamACode,
                    (byte)neuron.ActivationThresholdCode,
                    (byte)neuron.PreActivationThresholdCode,
                    (byte)neuron.ResetFunctionId,
                    (byte)neuron.ActivationFunctionId,
                    (byte)neuron.AccumulationFunctionId,
                    neuron.Exists);
            }

            var checkpointCount = (uint)((neuronSpan + Brain.AxonStride - 1) / Brain.AxonStride + 1);
            var checkpoints = NbnBinary.BuildCheckpoints(neuronRecords, Brain.AxonStride);
            var section = new NbnRegionSection(
                (byte)region.RegionId,
                (uint)neuronSpan,
                (ulong)axonRecords.Count,
                Brain.AxonStride,
                checkpointCount,
                checkpoints,
                neuronRecords,
                axonRecords.ToArray());

            sections.Add(section);
            directory[region.RegionId] = new NbnRegionDirectoryEntry((uint)neuronSpan, (ulong)axonRecords.Count, offset, 0);
            offset += (ulong)section.ByteLength;
        }

        header = new NbnHeaderV2(
            "NBN2",
            2,
            1,
            10,
            Brain.BrainSeed,
            Brain.AxonStride,
            0,
            QuantizationSchemas.DefaultNbn,
            directory);

        return true;
    }

    private static DesignerBrainViewModel BuildDesignerBrainFromNbn(NbnHeaderV2 header, IReadOnlyList<NbnRegionSection> regions, string? name)
    {
        var brain = new DesignerBrainViewModel(name ?? "Imported Brain", Guid.NewGuid(), header.BrainSeed, header.AxonStride);
        var regionMap = regions.ToDictionary(region => (int)region.RegionId, region => region);

        for (var i = 0; i < NbnConstants.RegionCount; i++)
        {
            var regionVm = new DesignerRegionViewModel(i);
            if (regionMap.TryGetValue(i, out var section))
            {
                for (var n = 0; n < section.NeuronRecords.Length; n++)
                {
                    var record = section.NeuronRecords[n];
                    var neuronVm = new DesignerNeuronViewModel(i, n, record.Exists, i == 0 || i == NbnConstants.OutputRegionId)
                    {
                        ActivationFunctionId = record.ActivationFunctionId,
                        ResetFunctionId = record.ResetFunctionId,
                        AccumulationFunctionId = record.AccumulationFunctionId,
                        ParamACode = record.ParamACode,
                        ParamBCode = record.ParamBCode,
                        ActivationThresholdCode = record.ActivationThresholdCode,
                        PreActivationThresholdCode = record.PreActivationThresholdCode
                    };

                    regionVm.Neurons.Add(neuronVm);
                }

                var axonIndex = 0;
                for (var n = 0; n < section.NeuronRecords.Length; n++)
                {
                    var neuronVm = regionVm.Neurons[n];
                    var axonCount = section.NeuronRecords[n].AxonCount;
                    for (var a = 0; a < axonCount; a++)
                    {
                        var axonRecord = section.AxonRecords[axonIndex++];
                        neuronVm.Axons.Add(new DesignerAxonViewModel(axonRecord.TargetRegionId, axonRecord.TargetNeuronId, axonRecord.StrengthCode));
                    }

                    neuronVm.UpdateAxonCount();
                }
            }

            regionVm.UpdateCounts();
            brain.Regions.Add(regionVm);
        }

        brain.UpdateTotals();
        return brain;
    }

    private static DesignerNeuronViewModel CreateDefaultNeuron(DesignerRegionViewModel region, int neuronId)
    {
        var isRequired = region.IsInput || region.IsOutput;
        return new DesignerNeuronViewModel(region.RegionId, neuronId, true, isRequired)
        {
            ActivationFunctionId = GetDefaultActivationFunctionIdForRegion(region.RegionId),
            ResetFunctionId = GetDefaultResetFunctionIdForRegion(region.RegionId),
            AccumulationFunctionId = 0,
            ParamACode = 0,
            ParamBCode = 0,
            ActivationThresholdCode = 0,
            PreActivationThresholdCode = 0
        };
    }

    private static void AddDefaultNeuron(DesignerRegionViewModel region)
    {
        var neuron = CreateDefaultNeuron(region, 0);
        region.Neurons.Add(neuron);
        region.UpdateCounts();
    }

    private static IReadOnlyList<NbnRegionSection> ReadNbnRegions(byte[] data, NbnHeaderV2 header)
    {
        var regions = new List<NbnRegionSection>();
        for (var i = 0; i < header.Regions.Length; i++)
        {
            var entry = header.Regions[i];
            if (entry.NeuronSpan == 0 || entry.Offset == 0)
            {
                continue;
            }

            regions.Add(NbnBinary.ReadNbnRegionSection(data, entry.Offset));
        }

        return regions;
    }

    private static void ReadNbsSections(byte[] data, NbsHeaderV2 header, out IReadOnlyList<NbsRegionSection> regions, out NbsOverlaySection? overlay)
    {
        overlay = null;
        var list = new List<NbsRegionSection>();
        var offset = NbnBinary.NbsHeaderBytes;

        while (offset < data.Length)
        {
            if (header.AxonOverlayIncluded && data.Length - offset >= 4)
            {
                var overlayCount = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(offset, 4));
                var overlaySize = NbnBinary.GetNbsOverlaySectionSize((int)overlayCount);
                if (overlaySize > 0 && offset + overlaySize == data.Length)
                {
                    overlay = NbnBinary.ReadNbsOverlaySection(data, offset);
                    offset += overlay.ByteLength;
                    break;
                }
            }

            var region = NbnBinary.ReadNbsRegionSection(data, offset, header.EnabledBitsetIncluded);
            list.Add(region);
            offset += region.ByteLength;
        }

        regions = list;
    }

    private static string BuildDesignSummary(DesignerBrainViewModel brain, string? label)
    {
        var regionCount = brain.Regions.Count(region => region.NeuronCount > 0);
        var name = string.IsNullOrWhiteSpace(label) ? brain.Name : label;
        return $"Design: {name} - regions {regionCount} - neurons {brain.TotalNeurons} - axons {brain.TotalAxons} - stride {brain.AxonStride}";
    }

    private static string BuildNbsSummary(string fileName, NbsHeaderV2 header, IReadOnlyList<NbsRegionSection> regions, NbsOverlaySection? overlay)
    {
        var overlayCount = overlay?.Records.Length ?? 0;
        return $"Loaded NBS: {fileName} - regions {regions.Count} - overlay {overlayCount} - tick {header.SnapshotTickId}";
    }

    private static IReadOnlyList<DesignerFunctionOption> BuildActivationFunctions()
    {
        var list = new List<DesignerFunctionOption>
        {
            new(0, "ACT_NONE (0)", "potential = 0"),
            new(1, "ACT_IDENTITY (1)", "potential = B"),
            new(2, "ACT_STEP_UP (2)", "potential = (B <= 0) ? 0 : 1"),
            new(3, "ACT_STEP_MID (3)", "potential = (B < 0) ? -1 : (B == 0 ? 0 : 1)"),
            new(4, "ACT_STEP_DOWN (4)", "potential = (B < 0) ? -1 : 0"),
            new(5, "ACT_ABS (5)", "potential = abs(B)"),
            new(6, "ACT_CLAMP (6)", "potential = clamp(B, -1, +1)"),
            new(7, "ACT_RELU (7)", "potential = max(0, B)"),
            new(8, "ACT_NRELU (8)", "potential = min(B, 0)"),
            new(9, "ACT_SIN (9)", "potential = sin(B)"),
            new(10, "ACT_TAN (10)", "potential = clamp(tan(B), -1, +1)"),
            new(11, "ACT_TANH (11)", "potential = tanh(B)"),
            new(12, "ACT_ELU (12)", "potential = (B > 0) ? B : A*(exp(B)-1)", usesParamA: true),
            new(13, "ACT_EXP (13)", "potential = exp(B)"),
            new(14, "ACT_PRELU (14)", "potential = (B >= 0) ? B : A*B", usesParamA: true),
            new(15, "ACT_LOG (15)", "potential = (B == 0) ? 0 : log(B)"),
            new(16, "ACT_MULT (16)", "potential = B * A", usesParamA: true),
            new(17, "ACT_ADD (17)", "potential = B + A", usesParamA: true),
            new(18, "ACT_SIG (18)", "potential = 1 / (1 + exp(-B))"),
            new(19, "ACT_SILU (19)", "potential = B / (1 + exp(-B))"),
            new(20, "ACT_PCLAMP (20)", "potential = (Bp <= A) ? 0 : clamp(B, A, Bp)", usesParamA: true, usesParamB: true),
            new(21, "ACT_MODL (21)", "potential = B % A", usesParamA: true),
            new(22, "ACT_MODR (22)", "potential = A % B", usesParamA: true),
            new(23, "ACT_SOFTP (23)", "potential = log(1 + exp(B))"),
            new(24, "ACT_SELU (24)", "potential = Bp * (B >= 0 ? B : A*(exp(B)-1))", usesParamA: true, usesParamB: true),
            new(25, "ACT_LIN (25)", "potential = A*B + Bp", usesParamA: true, usesParamB: true),
            new(26, "ACT_LOGB (26)", "potential = (A == 0) ? 0 : log(B, A)", usesParamA: true),
            new(27, "ACT_POW (27)", "potential = pow(B, A)", usesParamA: true),
            new(28, "ACT_GAUSS (28)", "potential = exp((-B)^2)"),
            new(29, "ACT_QUAD (29)", "potential = A*(B^2) + Bp*B", usesParamA: true, usesParamB: true)
        };

        return list;
    }

    private static IReadOnlyList<DesignerFunctionOption> BuildResetFunctions()
    {
        var names = new Dictionary<int, string>
        {
            { 0, "RESET_ZERO" },
            { 1, "RESET_HOLD" },
            { 2, "RESET_CLAMP_POTENTIAL" },
            { 3, "RESET_CLAMP1" },
            { 4, "RESET_POTENTIAL_CLAMP_BUFFER" },
            { 5, "RESET_NEG_POTENTIAL_CLAMP_BUFFER" },
            { 6, "RESET_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER" },
            { 7, "RESET_TENTH_POTENTIAL_CLAMP_BUFFER" },
            { 8, "RESET_HALF_POTENTIAL_CLAMP_BUFFER" },
            { 9, "RESET_DOUBLE_POTENTIAL_CLAMP_BUFFER" },
            { 10, "RESET_FIVEX_POTENTIAL_CLAMP_BUFFER" },
            { 11, "RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP_BUFFER" },
            { 12, "RESET_NEG_TENTH_POTENTIAL_CLAMP_BUFFER" },
            { 13, "RESET_NEG_HALF_POTENTIAL_CLAMP_BUFFER" },
            { 14, "RESET_NEG_DOUBLE_POTENTIAL_CLAMP_BUFFER" },
            { 15, "RESET_NEG_FIVEX_POTENTIAL_CLAMP_BUFFER" },
            { 16, "RESET_INVERSE_POTENTIAL_CLAMP_BUFFER" },
            { 17, "RESET_POTENTIAL_CLAMP1" },
            { 18, "RESET_NEG_POTENTIAL_CLAMP1" },
            { 19, "RESET_HUNDREDTHS_POTENTIAL_CLAMP1" },
            { 20, "RESET_TENTH_POTENTIAL_CLAMP1" },
            { 21, "RESET_HALF_POTENTIAL_CLAMP1" },
            { 22, "RESET_DOUBLE_POTENTIAL_CLAMP1" },
            { 23, "RESET_FIVEX_POTENTIAL_CLAMP1" },
            { 24, "RESET_NEG_HUNDREDTHS_POTENTIAL_CLAMP1" },
            { 25, "RESET_NEG_TENTH_POTENTIAL_CLAMP1" },
            { 26, "RESET_NEG_HALF_POTENTIAL_CLAMP1" },
            { 27, "RESET_NEG_DOUBLE_POTENTIAL_CLAMP1" },
            { 28, "RESET_NEG_FIVEX_POTENTIAL_CLAMP1" },
            { 29, "RESET_INVERSE_POTENTIAL_CLAMP1" },
            { 30, "RESET_POTENTIAL" },
            { 31, "RESET_NEG_POTENTIAL" },
            { 32, "RESET_HUNDREDTHS_POTENTIAL" },
            { 33, "RESET_TENTH_POTENTIAL" },
            { 34, "RESET_HALF_POTENTIAL" },
            { 35, "RESET_DOUBLE_POTENTIAL" },
            { 36, "RESET_FIVEX_POTENTIAL" },
            { 37, "RESET_NEG_HUNDREDTHS_POTENTIAL" },
            { 38, "RESET_NEG_TENTH_POTENTIAL" },
            { 39, "RESET_NEG_HALF_POTENTIAL" },
            { 40, "RESET_NEG_DOUBLE_POTENTIAL" },
            { 41, "RESET_NEG_FIVEX_POTENTIAL" },
            { 42, "RESET_INVERSE_POTENTIAL" },
            { 43, "RESET_HALF" },
            { 44, "RESET_TENTH" },
            { 45, "RESET_HUNDREDTH" },
            { 46, "RESET_NEGATIVE" },
            { 47, "RESET_NEG_HALF" },
            { 48, "RESET_NEG_TENTH" },
            { 49, "RESET_NEG_HUNDREDTH" },
            { 50, "RESET_DOUBLE_CLAMP1" },
            { 51, "RESET_FIVEX_CLAMP1" },
            { 52, "RESET_NEG_DOUBLE_CLAMP1" },
            { 53, "RESET_NEG_FIVEX_CLAMP1" },
            { 54, "RESET_DOUBLE" },
            { 55, "RESET_FIVEX" },
            { 56, "RESET_NEG_DOUBLE" },
            { 57, "RESET_NEG_FIVEX" },
            { 58, "RESET_DIVIDE_AXON_CT" },
            { 59, "RESET_INVERSE_CLAMP1" },
            { 60, "RESET_INVERSE" }
        };

        var descriptions = new Dictionary<int, string>
        {
            { 0, "new = 0" },
            { 1, "new = clamp(B, -T, +T)" },
            { 2, "new = clamp(B, -|P|, +|P|)" },
            { 3, "new = clamp(B, -1, +1)" },
            { 4, "new = clamp(P, -|B|, +|B|)" },
            { 5, "new = clamp(-P, -|B|, +|B|)" },
            { 6, "new = clamp(0.01*P, -|B|, +|B|)" },
            { 7, "new = clamp(0.1*P, -|B|, +|B|)" },
            { 8, "new = clamp(0.5*P, -|B|, +|B|)" },
            { 9, "new = clamp(2*P, -|B|, +|B|)" },
            { 10, "new = clamp(5*P, -|B|, +|B|)" },
            { 11, "new = clamp(-0.01*P, -|B|, +|B|)" },
            { 12, "new = clamp(-0.1*P, -|B|, +|B|)" },
            { 13, "new = clamp(-0.5*P, -|B|, +|B|)" },
            { 14, "new = clamp(-2*P, -|B|, +|B|)" },
            { 15, "new = clamp(-5*P, -|B|, +|B|)" },
            { 16, "new = clamp(1/P, -|B|, +|B|)" },
            { 17, "new = clamp(P, -1, +1)" },
            { 18, "new = clamp(-P, -1, +1)" },
            { 19, "new = clamp(0.01*P, -1, +1)" },
            { 20, "new = clamp(0.1*P, -1, +1)" },
            { 21, "new = clamp(0.5*P, -1, +1)" },
            { 22, "new = clamp(2*P, -1, +1)" },
            { 23, "new = clamp(5*P, -1, +1)" },
            { 24, "new = clamp(-0.01*P, -1, +1)" },
            { 25, "new = clamp(-0.1*P, -1, +1)" },
            { 26, "new = clamp(-0.5*P, -1, +1)" },
            { 27, "new = clamp(-2*P, -1, +1)" },
            { 28, "new = clamp(-5*P, -1, +1)" },
            { 29, "new = clamp(1/P, -1, +1)" },
            { 30, "new = clamp(P, -T, +T)" },
            { 31, "new = clamp(-P, -T, +T)" },
            { 32, "new = clamp(0.01*P, -T, +T)" },
            { 33, "new = clamp(0.1*P, -T, +T)" },
            { 34, "new = clamp(0.5*P, -T, +T)" },
            { 35, "new = clamp(2*P, -T, +T)" },
            { 36, "new = clamp(5*P, -T, +T)" },
            { 37, "new = clamp(-0.01*P, -T, +T)" },
            { 38, "new = clamp(-0.1*P, -T, +T)" },
            { 39, "new = clamp(-0.5*P, -T, +T)" },
            { 40, "new = clamp(-2*P, -T, +T)" },
            { 41, "new = clamp(-5*P, -T, +T)" },
            { 42, "new = clamp(1/P, -T, +T)" },
            { 43, "new = clamp(0.5*B, -T, +T)" },
            { 44, "new = clamp(0.1*B, -T, +T)" },
            { 45, "new = clamp(0.01*B, -T, +T)" },
            { 46, "new = clamp(-B, -T, +T)" },
            { 47, "new = clamp(-0.5*B, -T, +T)" },
            { 48, "new = clamp(-0.1*B, -T, +T)" },
            { 49, "new = clamp(-0.01*B, -T, +T)" },
            { 50, "new = clamp(2*B, -1, +1)" },
            { 51, "new = clamp(5*B, -1, +1)" },
            { 52, "new = clamp(-2*B, -1, +1)" },
            { 53, "new = clamp(-5*B, -1, +1)" },
            { 54, "new = clamp(2*B, -T, +T)" },
            { 55, "new = clamp(5*B, -T, +T)" },
            { 56, "new = clamp(-2*B, -T, +T)" },
            { 57, "new = clamp(-5*B, -T, +T)" },
            { 58, "new = clamp(B / max(1,K), -T, +T)" },
            { 59, "new = clamp(-1/B, -1, +1)" },
            { 60, "new = clamp(-1/B, -T, +T)" }
        };

        var list = new List<DesignerFunctionOption>();
        for (var i = 0; i <= MaxKnownResetFunctionId; i++)
        {
            if (names.TryGetValue(i, out var name))
            {
                var description = descriptions.TryGetValue(i, out var detail) ? detail : string.Empty;
                list.Add(new DesignerFunctionOption(i, $"{name} ({i})", description));
            }
            else
            {
                list.Add(new DesignerFunctionOption(i, $"UNKNOWN ({i})", "Undefined reset function ID."));
            }
        }

        return list;
    }

    private static IReadOnlyList<DesignerFunctionOption> BuildAccumulationFunctions()
    {
        return new List<DesignerFunctionOption>
        {
            new(0, "ACCUM_SUM (0)", "B = B + I"),
            new(1, "ACCUM_PRODUCT (1)", "B = B * I (if any input)"),
            new(2, "ACCUM_MAX (2)", "B = max(B, I)"),
            new(3, "ACCUM_NONE (3)", "No merge")
        };
    }

    private bool UsesParamA(int id)
        => ActivationUsesParamA(id);

    private bool UsesParamB(int id)
        => ActivationUsesParamB(id);

    private static bool ActivationUsesParamA(int id)
        => ParamAActivationFunctionIds.Contains(id);

    private static bool ActivationUsesParamB(int id)
        => ParamBActivationFunctionIds.Contains(id);

    private string DescribeActivation(int id)
        => ActivationFunctions.FirstOrDefault(entry => entry.Id == id)?.Description ?? "Reserved";

    private string DescribeReset(int id)
        => ResetFunctions.FirstOrDefault(entry => entry.Id == id)?.Description ?? "Reserved";

    private string DescribeAccumulation(int id)
        => AccumulationFunctions.FirstOrDefault(entry => entry.Id == id)?.Description ?? "Reserved";

    private string BuildNeuronConstraintHint(DesignerNeuronViewModel? neuron)
    {
        if (neuron is null)
        {
            return "Select a neuron to view region-specific function constraints.";
        }

        if (neuron.RegionId == NbnConstants.InputRegionId)
        {
            return "Input neurons use constrained activation/reset sets for stable external signal handling.";
        }

        if (neuron.RegionId == NbnConstants.OutputRegionId)
        {
            return "Output neurons use a constrained activation set for readable external outputs.";
        }

        return "Internal neurons allow all defined activation IDs 0-29 and reset IDs 0-60.";
    }

    private int NormalizeBrainFunctionConstraints(DesignerBrainViewModel brain)
    {
        var normalized = 0;
        foreach (var region in brain.Regions)
        {
            foreach (var neuron in region.Neurons)
            {
                if (NormalizeNeuronFunctionConstraints(neuron, out _, includeStatus: false))
                {
                    normalized++;
                }
            }
        }

        return normalized;
    }

    private bool NormalizeNeuronFunctionConstraints(DesignerNeuronViewModel neuron, out string? statusMessage, bool includeStatus = true)
    {
        statusMessage = null;
        var previousActivation = neuron.ActivationFunctionId;
        var previousReset = neuron.ResetFunctionId;
        var previousParamA = neuron.ParamACode;
        var previousParamB = neuron.ParamBCode;

        _suppressNeuronConstraintEnforcement = true;
        try
        {
            if (!IsActivationFunctionAllowedForRegion(neuron.RegionId, neuron.ActivationFunctionId))
            {
                neuron.ActivationFunctionId = GetDefaultActivationFunctionIdForRegion(neuron.RegionId);
            }

            if (!IsResetFunctionAllowedForRegion(neuron.RegionId, neuron.ResetFunctionId))
            {
                neuron.ResetFunctionId = GetDefaultResetFunctionIdForRegion(neuron.RegionId);
            }

            if (!UsesParamA(neuron.ActivationFunctionId))
            {
                neuron.ParamACode = 0;
            }

            if (!UsesParamB(neuron.ActivationFunctionId))
            {
                neuron.ParamBCode = 0;
            }
        }
        finally
        {
            _suppressNeuronConstraintEnforcement = false;
        }

        var changed = previousActivation != neuron.ActivationFunctionId
            || previousReset != neuron.ResetFunctionId
            || previousParamA != neuron.ParamACode
            || previousParamB != neuron.ParamBCode;

        if (changed && includeStatus)
        {
            statusMessage = $"Neuron R{neuron.RegionId} N{neuron.NeuronId} function settings were normalized for region constraints.";
        }

        return changed;
    }

}
