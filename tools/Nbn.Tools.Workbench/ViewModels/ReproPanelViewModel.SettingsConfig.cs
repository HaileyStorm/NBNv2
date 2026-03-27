using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Nbn.Proto.Repro;
using Nbn.Shared;
using Nbn.Tools.Workbench.Models;

namespace Nbn.Tools.Workbench.ViewModels;

public sealed partial class ReproPanelViewModel
{
    /// <summary>
    /// Refreshes the active-brain parent pickers while preserving any still-live selections.
    /// </summary>
    public void UpdateActiveBrains(IReadOnlyList<BrainListItem> brains)
    {
        var selectedA = SelectedParentABrain?.BrainId;
        var selectedB = SelectedParentBBrain?.BrainId;

        var active = brains
            .Where(entry => !string.Equals(entry.State, "Dead", StringComparison.OrdinalIgnoreCase))
            .OrderBy(entry => entry.BrainId)
            .Select(entry => new ReproBrainOption(entry.BrainId, entry.Display))
            .ToList();

        ActiveBrains.Clear();
        foreach (var entry in active)
        {
            ActiveBrains.Add(entry);
        }

        SelectedParentABrain = selectedA.HasValue
            ? ActiveBrains.FirstOrDefault(item => item.BrainId == selectedA.Value)
            : ActiveBrains.FirstOrDefault();

        SelectedParentBBrain = selectedB.HasValue
            ? ActiveBrains.FirstOrDefault(item => item.BrainId == selectedB.Value)
            : ActiveBrains.Skip(1).FirstOrDefault();
    }

    /// <summary>
    /// Applies SettingsMonitor-backed reproduction defaults into the panel draft state.
    /// </summary>
    public bool ApplySetting(SettingItem item)
    {
        if (item is null || string.IsNullOrWhiteSpace(item.Key))
        {
            return false;
        }

        var key = item.Key.Trim();
        var value = item.Value ?? string.Empty;
        switch (key)
        {
            case ReproductionSettingsKeys.StrengthSourceKey:
            {
                var parsed = ReproductionSettings.ParseStrengthSource(value, ReproductionSettings.DefaultStrengthSource);
                var selected = StrengthSources.FirstOrDefault(option => option.Value == parsed);
                if (selected is not null)
                {
                    SelectedStrengthSource = selected;
                }

                return true;
            }
            case ReproductionSettingsKeys.MaxRegionSpanDiffRatioKey:
                MaxRegionSpanDiffRatio = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultMaxRegionSpanDiffRatio));
                return true;
            case ReproductionSettingsKeys.MaxFunctionHistDistanceKey:
                MaxFunctionHistDistance = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultMaxFunctionHistDistance));
                return true;
            case ReproductionSettingsKeys.MaxConnectivityHistDistanceKey:
                MaxConnectivityHistDistance = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultMaxConnectivityHistDistance));
                return true;
            case ReproductionSettingsKeys.ProbAddNeuronToEmptyRegionKey:
                ProbAddNeuronToEmptyRegion = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbAddNeuronToEmptyRegion));
                return true;
            case ReproductionSettingsKeys.ProbRemoveLastNeuronFromRegionKey:
                ProbRemoveLastNeuronFromRegion = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbRemoveLastNeuronFromRegion));
                return true;
            case ReproductionSettingsKeys.ProbDisableNeuronKey:
                ProbDisableNeuron = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbDisableNeuron));
                return true;
            case ReproductionSettingsKeys.ProbReactivateNeuronKey:
                ProbReactivateNeuron = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbReactivateNeuron));
                return true;
            case ReproductionSettingsKeys.ProbAddAxonKey:
                ProbAddAxon = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbAddAxon));
                return true;
            case ReproductionSettingsKeys.ProbRemoveAxonKey:
                ProbRemoveAxon = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbRemoveAxon));
                return true;
            case ReproductionSettingsKeys.ProbRerouteAxonKey:
                ProbRerouteAxon = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbRerouteAxon));
                return true;
            case ReproductionSettingsKeys.ProbRerouteInboundAxonOnDeleteKey:
                ProbRerouteInboundAxonOnDelete = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbRerouteInboundAxonOnDelete));
                return true;
            case ReproductionSettingsKeys.InboundRerouteMaxRingDistanceKey:
                InboundRerouteMaxRingDistance = ReproductionSettings.FormatUInt(ParseUInt(value, ReproductionSettings.DefaultInboundRerouteMaxRingDistance));
                return true;
            case ReproductionSettingsKeys.ProbChooseParentAKey:
                ProbChooseParentA = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbChooseParentA));
                return true;
            case ReproductionSettingsKeys.ProbChooseParentBKey:
                ProbChooseParentB = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbChooseParentB));
                return true;
            case ReproductionSettingsKeys.ProbAverageKey:
                ProbAverage = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbAverage));
                return true;
            case ReproductionSettingsKeys.ProbMutateKey:
                ProbMutate = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbMutate));
                return true;
            case ReproductionSettingsKeys.ProbChooseFuncAKey:
                ProbChooseFuncA = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbChooseFuncA));
                return true;
            case ReproductionSettingsKeys.ProbMutateFuncKey:
                ProbMutateFunc = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbMutateFunc));
                return true;
            case ReproductionSettingsKeys.MaxAvgOutDegreeBrainKey:
                MaxAvgOutDegree = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultMaxAvgOutDegreeBrain));
                return true;
            case ReproductionSettingsKeys.PrunePolicyKey:
            {
                var parsed = ReproductionSettings.ParsePrunePolicy(value, ReproductionSettings.DefaultPrunePolicy);
                var selected = PrunePolicies.FirstOrDefault(option => option.Value == parsed);
                if (selected is not null)
                {
                    SelectedPrunePolicy = selected;
                }

                return true;
            }
            case ReproductionSettingsKeys.PerRegionOutDegreeCapsKey:
                PerRegionOutDegreeCaps = value.Trim();
                return true;
            case ReproductionSettingsKeys.StrengthTransformEnabledKey:
                StrengthTransformEnabled = ParseBool(value, ReproductionSettings.DefaultStrengthTransformEnabled);
                return true;
            case ReproductionSettingsKeys.ProbStrengthChooseAKey:
                ProbStrengthChooseA = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbStrengthChooseA));
                return true;
            case ReproductionSettingsKeys.ProbStrengthChooseBKey:
                ProbStrengthChooseB = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbStrengthChooseB));
                return true;
            case ReproductionSettingsKeys.ProbStrengthAverageKey:
                ProbStrengthAverage = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbStrengthAverage));
                return true;
            case ReproductionSettingsKeys.ProbStrengthWeightedAverageKey:
                ProbStrengthWeightedAverage = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbStrengthWeightedAverage));
                return true;
            case ReproductionSettingsKeys.StrengthWeightAKey:
                StrengthWeightA = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultStrengthWeightA));
                return true;
            case ReproductionSettingsKeys.StrengthWeightBKey:
                StrengthWeightB = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultStrengthWeightB));
                return true;
            case ReproductionSettingsKeys.ProbStrengthMutateKey:
                ProbStrengthMutate = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultProbStrengthMutate));
                return true;
            case ReproductionSettingsKeys.MaxNeuronsAddedAbsKey:
                MaxNeuronsAddedAbs = ReproductionSettings.FormatUInt(ParseUInt(value, ReproductionSettings.DefaultMaxNeuronsAddedAbs));
                return true;
            case ReproductionSettingsKeys.MaxNeuronsAddedPctKey:
                MaxNeuronsAddedPct = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultMaxNeuronsAddedPct));
                return true;
            case ReproductionSettingsKeys.MaxNeuronsRemovedAbsKey:
                MaxNeuronsRemovedAbs = ReproductionSettings.FormatUInt(ParseUInt(value, ReproductionSettings.DefaultMaxNeuronsRemovedAbs));
                return true;
            case ReproductionSettingsKeys.MaxNeuronsRemovedPctKey:
                MaxNeuronsRemovedPct = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultMaxNeuronsRemovedPct));
                return true;
            case ReproductionSettingsKeys.MaxAxonsAddedAbsKey:
                MaxAxonsAddedAbs = ReproductionSettings.FormatUInt(ParseUInt(value, ReproductionSettings.DefaultMaxAxonsAddedAbs));
                return true;
            case ReproductionSettingsKeys.MaxAxonsAddedPctKey:
                MaxAxonsAddedPct = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultMaxAxonsAddedPct));
                return true;
            case ReproductionSettingsKeys.MaxAxonsRemovedAbsKey:
                MaxAxonsRemovedAbs = ReproductionSettings.FormatUInt(ParseUInt(value, ReproductionSettings.DefaultMaxAxonsRemovedAbs));
                return true;
            case ReproductionSettingsKeys.MaxAxonsRemovedPctKey:
                MaxAxonsRemovedPct = ReproductionSettings.FormatFloat(ParseFloat(value, ReproductionSettings.DefaultMaxAxonsRemovedPct));
                return true;
            case ReproductionSettingsKeys.MaxRegionsAddedAbsKey:
                MaxRegionsAddedAbs = ReproductionSettings.FormatUInt(ParseUInt(value, ReproductionSettings.DefaultMaxRegionsAddedAbs));
                return true;
            case ReproductionSettingsKeys.MaxRegionsRemovedAbsKey:
                MaxRegionsRemovedAbs = ReproductionSettings.FormatUInt(ParseUInt(value, ReproductionSettings.DefaultMaxRegionsRemovedAbs));
                return true;
            case ReproductionSettingsKeys.SpawnChildKey:
            {
                var parsed = ReproductionSettings.ParseSpawnChildPolicy(value, ReproductionSettings.DefaultSpawnChildPolicy);
                var selected = SpawnPolicies.FirstOrDefault(option => option.Value == parsed);
                if (selected is not null)
                {
                    SelectedSpawnPolicy = selected;
                }

                return true;
            }
            default:
                return false;
        }
    }

    private ReproduceConfig BuildConfig()
    {
        return new ReproduceConfig
        {
            MaxRegionSpanDiffRatio = ParseFloat(MaxRegionSpanDiffRatio, ReproductionSettings.DefaultMaxRegionSpanDiffRatio),
            MaxFunctionHistDistance = ParseFloat(MaxFunctionHistDistance, ReproductionSettings.DefaultMaxFunctionHistDistance),
            MaxConnectivityHistDistance = ParseFloat(MaxConnectivityHistDistance, ReproductionSettings.DefaultMaxConnectivityHistDistance),
            ProbAddNeuronToEmptyRegion = ParseFloat(ProbAddNeuronToEmptyRegion, 0f),
            ProbRemoveLastNeuronFromRegion = ParseFloat(ProbRemoveLastNeuronFromRegion, 0f),
            ProbDisableNeuron = ParseFloat(ProbDisableNeuron, 0.01f),
            ProbReactivateNeuron = ParseFloat(ProbReactivateNeuron, 0.01f),
            ProbAddAxon = ParseFloat(ProbAddAxon, 0.05f),
            ProbRemoveAxon = ParseFloat(ProbRemoveAxon, 0.02f),
            ProbRerouteAxon = ParseFloat(ProbRerouteAxon, 0.02f),
            ProbRerouteInboundAxonOnDelete = ParseFloat(ProbRerouteInboundAxonOnDelete, 0.50f),
            InboundRerouteMaxRingDistance = ParseUInt(InboundRerouteMaxRingDistance, 0),
            ProbChooseParentA = ParseFloat(ProbChooseParentA, 0.45f),
            ProbChooseParentB = ParseFloat(ProbChooseParentB, 0.45f),
            ProbAverage = ParseFloat(ProbAverage, 0.05f),
            ProbMutate = ParseFloat(ProbMutate, 0.05f),
            ProbChooseFuncA = ParseFloat(ProbChooseFuncA, 0.50f),
            ProbMutateFunc = ParseFloat(ProbMutateFunc, 0.02f),
            MaxAvgOutDegreeBrain = ParseFloat(MaxAvgOutDegree, 100f),
            PrunePolicy = SelectedPrunePolicy.Value,
            PerRegionOutDegreeCaps = { ParsePerRegionOutDegreeCaps(PerRegionOutDegreeCaps) },
            StrengthTransformEnabled = StrengthTransformEnabled,
            ProbStrengthChooseA = ParseFloat(ProbStrengthChooseA, 0.35f),
            ProbStrengthChooseB = ParseFloat(ProbStrengthChooseB, 0.35f),
            ProbStrengthAverage = ParseFloat(ProbStrengthAverage, 0.20f),
            ProbStrengthWeightedAverage = ParseFloat(ProbStrengthWeightedAverage, 0.05f),
            StrengthWeightA = ParseFloat(StrengthWeightA, 0.50f),
            StrengthWeightB = ParseFloat(StrengthWeightB, 0.50f),
            ProbStrengthMutate = ParseFloat(ProbStrengthMutate, 0.05f),
            Limits = new ReproduceLimits
            {
                MaxNeuronsAddedAbs = ParseUInt(MaxNeuronsAddedAbs, 0),
                MaxNeuronsAddedPct = ParseFloat(MaxNeuronsAddedPct, 0f),
                MaxNeuronsRemovedAbs = ParseUInt(MaxNeuronsRemovedAbs, 0),
                MaxNeuronsRemovedPct = ParseFloat(MaxNeuronsRemovedPct, 0f),
                MaxAxonsAddedAbs = ParseUInt(MaxAxonsAddedAbs, 0),
                MaxAxonsAddedPct = ParseFloat(MaxAxonsAddedPct, 0f),
                MaxAxonsRemovedAbs = ParseUInt(MaxAxonsRemovedAbs, 0),
                MaxAxonsRemovedPct = ParseFloat(MaxAxonsRemovedPct, 0f),
                MaxRegionsAddedAbs = ParseUInt(MaxRegionsAddedAbs, 0),
                MaxRegionsRemovedAbs = ParseUInt(MaxRegionsRemovedAbs, 0)
            },
            SpawnChild = SelectedSpawnPolicy.Value
        };
    }

    private static float ParseFloat(string value, float fallback)
    {
        return float.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static uint ParseUInt(string value, uint fallback)
    {
        return uint.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static bool ParseBool(string? value, bool fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        if (bool.TryParse(value, out var parsed))
        {
            return parsed;
        }

        return value.Trim() switch
        {
            "1" => true,
            "0" => false,
            _ => fallback
        };
    }

    private static ulong ParseUlong(string value, ulong fallback)
    {
        return ulong.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static IEnumerable<RegionOutDegreeCap> ParsePerRegionOutDegreeCaps(string rawValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            yield break;
        }

        var parsedCaps = new Dictionary<uint, float>();
        var segments = rawValue.Split([',', ';', '\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
        foreach (var segment in segments)
        {
            var pair = segment.Split([':', '='], 2, StringSplitOptions.RemoveEmptyEntries);
            if (pair.Length != 2
                || !uint.TryParse(pair[0].Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var regionId)
                || !float.TryParse(pair[1].Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var maxAvgOutDegree)
                || regionId > NbnConstants.RegionMaxId
                || maxAvgOutDegree <= 0f
                || float.IsNaN(maxAvgOutDegree)
                || float.IsInfinity(maxAvgOutDegree))
            {
                continue;
            }

            if (parsedCaps.TryGetValue(regionId, out var existing))
            {
                parsedCaps[regionId] = Math.Min(existing, maxAvgOutDegree);
                continue;
            }

            parsedCaps.Add(regionId, maxAvgOutDegree);
        }

        foreach (var pair in parsedCaps.OrderBy(static entry => entry.Key))
        {
            yield return new RegionOutDegreeCap
            {
                RegionId = pair.Key,
                MaxAvgOutDegree = pair.Value
            };
        }
    }
}
