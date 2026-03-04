using System.Globalization;
using Repro = Nbn.Proto.Repro;

namespace Nbn.Shared;

public static class ReproductionSettings
{
    public const Repro.StrengthSource DefaultStrengthSource = Repro.StrengthSource.StrengthBaseOnly;
    public const Repro.SpawnChildPolicy DefaultSpawnChildPolicy = Repro.SpawnChildPolicy.SpawnChildDefaultOn;
    public const Repro.PrunePolicy DefaultPrunePolicy = Repro.PrunePolicy.PruneLowestAbsStrengthFirst;

    public const float DefaultMaxRegionSpanDiffRatio = 0.15f;
    public const float DefaultMaxFunctionHistDistance = 0.25f;
    public const float DefaultMaxConnectivityHistDistance = 0.25f;
    public const float DefaultProbAddNeuronToEmptyRegion = 0f;
    public const float DefaultProbRemoveLastNeuronFromRegion = 0f;
    public const float DefaultProbDisableNeuron = 0.01f;
    public const float DefaultProbReactivateNeuron = 0.01f;
    public const float DefaultProbAddAxon = 0.05f;
    public const float DefaultProbRemoveAxon = 0.02f;
    public const float DefaultProbRerouteAxon = 0.02f;
    public const float DefaultProbRerouteInboundAxonOnDelete = 0.50f;
    public const uint DefaultInboundRerouteMaxRingDistance = 0;
    public const float DefaultProbChooseParentA = 0.45f;
    public const float DefaultProbChooseParentB = 0.45f;
    public const float DefaultProbAverage = 0.05f;
    public const float DefaultProbMutate = 0.05f;
    public const float DefaultProbChooseFuncA = 0.50f;
    public const float DefaultProbMutateFunc = 0.02f;
    public const float DefaultMaxAvgOutDegreeBrain = 100f;
    public const bool DefaultStrengthTransformEnabled = false;
    public const float DefaultProbStrengthChooseA = 0.35f;
    public const float DefaultProbStrengthChooseB = 0.35f;
    public const float DefaultProbStrengthAverage = 0.20f;
    public const float DefaultProbStrengthWeightedAverage = 0.05f;
    public const float DefaultStrengthWeightA = 0.50f;
    public const float DefaultStrengthWeightB = 0.50f;
    public const float DefaultProbStrengthMutate = 0.05f;
    public const uint DefaultMaxNeuronsAddedAbs = 0;
    public const float DefaultMaxNeuronsAddedPct = 0f;
    public const uint DefaultMaxNeuronsRemovedAbs = 0;
    public const float DefaultMaxNeuronsRemovedPct = 0f;
    public const uint DefaultMaxAxonsAddedAbs = 0;
    public const float DefaultMaxAxonsAddedPct = 0f;
    public const uint DefaultMaxAxonsRemovedAbs = 0;
    public const float DefaultMaxAxonsRemovedPct = 0f;
    public const uint DefaultMaxRegionsAddedAbs = 0;
    public const uint DefaultMaxRegionsRemovedAbs = 0;
    public const string DefaultPerRegionOutDegreeCaps = "";

    public static IReadOnlyDictionary<string, string> DefaultSettingValues { get; } = BuildDefaultSettingValues();

    public static Repro.ReproduceConfig CreateDefaultConfig(Repro.SpawnChildPolicy? spawnChildOverride = null)
        => CreateConfigFromResolver(_ => null, spawnChildOverride);

    public static Repro.ReproduceConfig CreateConfigFromSettings(
        IReadOnlyDictionary<string, string?>? settings,
        Repro.SpawnChildPolicy? spawnChildOverride = null)
    {
        string? Resolve(string key)
        {
            if (settings is null)
            {
                return null;
            }

            return settings.TryGetValue(key, out var value) ? value : null;
        }

        return CreateConfigFromResolver(Resolve, spawnChildOverride);
    }

    public static Repro.ReproduceConfig CreateConfigFromResolver(
        Func<string, string?> resolveSetting,
        Repro.SpawnChildPolicy? spawnChildOverride = null)
    {
        ArgumentNullException.ThrowIfNull(resolveSetting);

        var config = new Repro.ReproduceConfig
        {
            MaxRegionSpanDiffRatio = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.MaxRegionSpanDiffRatioKey),
                DefaultMaxRegionSpanDiffRatio),
            MaxFunctionHistDistance = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.MaxFunctionHistDistanceKey),
                DefaultMaxFunctionHistDistance),
            MaxConnectivityHistDistance = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.MaxConnectivityHistDistanceKey),
                DefaultMaxConnectivityHistDistance),
            ProbAddNeuronToEmptyRegion = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbAddNeuronToEmptyRegionKey),
                DefaultProbAddNeuronToEmptyRegion),
            ProbRemoveLastNeuronFromRegion = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbRemoveLastNeuronFromRegionKey),
                DefaultProbRemoveLastNeuronFromRegion),
            ProbDisableNeuron = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbDisableNeuronKey),
                DefaultProbDisableNeuron),
            ProbReactivateNeuron = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbReactivateNeuronKey),
                DefaultProbReactivateNeuron),
            ProbAddAxon = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbAddAxonKey),
                DefaultProbAddAxon),
            ProbRemoveAxon = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbRemoveAxonKey),
                DefaultProbRemoveAxon),
            ProbRerouteAxon = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbRerouteAxonKey),
                DefaultProbRerouteAxon),
            ProbRerouteInboundAxonOnDelete = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbRerouteInboundAxonOnDeleteKey),
                DefaultProbRerouteInboundAxonOnDelete),
            InboundRerouteMaxRingDistance = ParseUInt(
                resolveSetting(ReproductionSettingsKeys.InboundRerouteMaxRingDistanceKey),
                DefaultInboundRerouteMaxRingDistance),
            ProbChooseParentA = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbChooseParentAKey),
                DefaultProbChooseParentA),
            ProbChooseParentB = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbChooseParentBKey),
                DefaultProbChooseParentB),
            ProbAverage = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbAverageKey),
                DefaultProbAverage),
            ProbMutate = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbMutateKey),
                DefaultProbMutate),
            ProbChooseFuncA = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbChooseFuncAKey),
                DefaultProbChooseFuncA),
            ProbMutateFunc = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbMutateFuncKey),
                DefaultProbMutateFunc),
            MaxAvgOutDegreeBrain = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.MaxAvgOutDegreeBrainKey),
                DefaultMaxAvgOutDegreeBrain),
            PrunePolicy = ParsePrunePolicy(
                resolveSetting(ReproductionSettingsKeys.PrunePolicyKey),
                DefaultPrunePolicy),
            StrengthTransformEnabled = ParseBool(
                resolveSetting(ReproductionSettingsKeys.StrengthTransformEnabledKey),
                DefaultStrengthTransformEnabled),
            ProbStrengthChooseA = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbStrengthChooseAKey),
                DefaultProbStrengthChooseA),
            ProbStrengthChooseB = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbStrengthChooseBKey),
                DefaultProbStrengthChooseB),
            ProbStrengthAverage = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbStrengthAverageKey),
                DefaultProbStrengthAverage),
            ProbStrengthWeightedAverage = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbStrengthWeightedAverageKey),
                DefaultProbStrengthWeightedAverage),
            StrengthWeightA = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.StrengthWeightAKey),
                DefaultStrengthWeightA),
            StrengthWeightB = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.StrengthWeightBKey),
                DefaultStrengthWeightB),
            ProbStrengthMutate = ParseFloat(
                resolveSetting(ReproductionSettingsKeys.ProbStrengthMutateKey),
                DefaultProbStrengthMutate),
            Limits = new Repro.ReproduceLimits
            {
                MaxNeuronsAddedAbs = ParseUInt(
                    resolveSetting(ReproductionSettingsKeys.MaxNeuronsAddedAbsKey),
                    DefaultMaxNeuronsAddedAbs),
                MaxNeuronsAddedPct = ParseFloat(
                    resolveSetting(ReproductionSettingsKeys.MaxNeuronsAddedPctKey),
                    DefaultMaxNeuronsAddedPct),
                MaxNeuronsRemovedAbs = ParseUInt(
                    resolveSetting(ReproductionSettingsKeys.MaxNeuronsRemovedAbsKey),
                    DefaultMaxNeuronsRemovedAbs),
                MaxNeuronsRemovedPct = ParseFloat(
                    resolveSetting(ReproductionSettingsKeys.MaxNeuronsRemovedPctKey),
                    DefaultMaxNeuronsRemovedPct),
                MaxAxonsAddedAbs = ParseUInt(
                    resolveSetting(ReproductionSettingsKeys.MaxAxonsAddedAbsKey),
                    DefaultMaxAxonsAddedAbs),
                MaxAxonsAddedPct = ParseFloat(
                    resolveSetting(ReproductionSettingsKeys.MaxAxonsAddedPctKey),
                    DefaultMaxAxonsAddedPct),
                MaxAxonsRemovedAbs = ParseUInt(
                    resolveSetting(ReproductionSettingsKeys.MaxAxonsRemovedAbsKey),
                    DefaultMaxAxonsRemovedAbs),
                MaxAxonsRemovedPct = ParseFloat(
                    resolveSetting(ReproductionSettingsKeys.MaxAxonsRemovedPctKey),
                    DefaultMaxAxonsRemovedPct),
                MaxRegionsAddedAbs = ParseUInt(
                    resolveSetting(ReproductionSettingsKeys.MaxRegionsAddedAbsKey),
                    DefaultMaxRegionsAddedAbs),
                MaxRegionsRemovedAbs = ParseUInt(
                    resolveSetting(ReproductionSettingsKeys.MaxRegionsRemovedAbsKey),
                    DefaultMaxRegionsRemovedAbs)
            }
        };

        config.PerRegionOutDegreeCaps.Add(ParsePerRegionOutDegreeCaps(
            resolveSetting(ReproductionSettingsKeys.PerRegionOutDegreeCapsKey)));
        config.SpawnChild = spawnChildOverride ?? ParseSpawnChildPolicy(
            resolveSetting(ReproductionSettingsKeys.SpawnChildKey),
            DefaultSpawnChildPolicy);
        return config;
    }

    public static Repro.StrengthSource ResolveStrengthSource(IReadOnlyDictionary<string, string?>? settings)
    {
        string? raw = null;
        if (settings is not null)
        {
            settings.TryGetValue(ReproductionSettingsKeys.StrengthSourceKey, out raw);
        }

        return ParseStrengthSource(raw, DefaultStrengthSource);
    }

    public static Repro.StrengthSource ParseStrengthSource(string? raw, Repro.StrengthSource fallback)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return fallback;
        }

        if (Enum.TryParse<Repro.StrengthSource>(raw.Trim(), ignoreCase: true, out var parsed))
        {
            return parsed;
        }

        return raw.Trim().ToLowerInvariant() switch
        {
            "0" => Repro.StrengthSource.StrengthBaseOnly,
            "1" => Repro.StrengthSource.StrengthLiveCodes,
            "base" => Repro.StrengthSource.StrengthBaseOnly,
            "strength_base_only" => Repro.StrengthSource.StrengthBaseOnly,
            "live" => Repro.StrengthSource.StrengthLiveCodes,
            "live_codes" => Repro.StrengthSource.StrengthLiveCodes,
            "strength_live_codes" => Repro.StrengthSource.StrengthLiveCodes,
            _ => fallback
        };
    }

    public static Repro.SpawnChildPolicy ParseSpawnChildPolicy(string? raw, Repro.SpawnChildPolicy fallback)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return fallback;
        }

        if (Enum.TryParse<Repro.SpawnChildPolicy>(raw.Trim(), ignoreCase: true, out var parsed))
        {
            return parsed;
        }

        return raw.Trim().ToLowerInvariant() switch
        {
            "0" => Repro.SpawnChildPolicy.SpawnChildDefaultOn,
            "1" => Repro.SpawnChildPolicy.SpawnChildNever,
            "2" => Repro.SpawnChildPolicy.SpawnChildAlways,
            "default" => Repro.SpawnChildPolicy.SpawnChildDefaultOn,
            "spawn_child_default_on" => Repro.SpawnChildPolicy.SpawnChildDefaultOn,
            "never" => Repro.SpawnChildPolicy.SpawnChildNever,
            "spawn_child_never" => Repro.SpawnChildPolicy.SpawnChildNever,
            "always" => Repro.SpawnChildPolicy.SpawnChildAlways,
            "spawn_child_always" => Repro.SpawnChildPolicy.SpawnChildAlways,
            _ => fallback
        };
    }

    public static Repro.PrunePolicy ParsePrunePolicy(string? raw, Repro.PrunePolicy fallback)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return fallback;
        }

        if (Enum.TryParse<Repro.PrunePolicy>(raw.Trim(), ignoreCase: true, out var parsed))
        {
            return parsed;
        }

        return raw.Trim().ToLowerInvariant() switch
        {
            "0" => Repro.PrunePolicy.PruneLowestAbsStrengthFirst,
            "1" => Repro.PrunePolicy.PruneNewConnectionsFirst,
            "2" => Repro.PrunePolicy.PruneRandom,
            "prune_lowest_abs_strength_first" => Repro.PrunePolicy.PruneLowestAbsStrengthFirst,
            "lowest_abs_strength" => Repro.PrunePolicy.PruneLowestAbsStrengthFirst,
            "prune_new_connections_first" => Repro.PrunePolicy.PruneNewConnectionsFirst,
            "new_connections" => Repro.PrunePolicy.PruneNewConnectionsFirst,
            "prune_random" => Repro.PrunePolicy.PruneRandom,
            "random" => Repro.PrunePolicy.PruneRandom,
            _ => fallback
        };
    }

    public static string ToSettingValue(Repro.StrengthSource value)
    {
        return value switch
        {
            Repro.StrengthSource.StrengthLiveCodes => "strength_live_codes",
            _ => "strength_base_only"
        };
    }

    public static string ToSettingValue(Repro.SpawnChildPolicy value)
    {
        return value switch
        {
            Repro.SpawnChildPolicy.SpawnChildNever => "spawn_child_never",
            Repro.SpawnChildPolicy.SpawnChildAlways => "spawn_child_always",
            _ => "spawn_child_default_on"
        };
    }

    public static string ToSettingValue(Repro.PrunePolicy value)
    {
        return value switch
        {
            Repro.PrunePolicy.PruneNewConnectionsFirst => "prune_new_connections_first",
            Repro.PrunePolicy.PruneRandom => "prune_random",
            _ => "prune_lowest_abs_strength_first"
        };
    }

    public static string FormatBool(bool value) => value ? "true" : "false";

    public static string FormatFloat(float value) => value.ToString("0.###", CultureInfo.InvariantCulture);

    public static string FormatUInt(uint value) => value.ToString(CultureInfo.InvariantCulture);

    private static IReadOnlyDictionary<string, string> BuildDefaultSettingValues()
    {
        return new Dictionary<string, string>(StringComparer.Ordinal)
        {
            { ReproductionSettingsKeys.StrengthSourceKey, ToSettingValue(DefaultStrengthSource) },
            { ReproductionSettingsKeys.MaxRegionSpanDiffRatioKey, FormatFloat(DefaultMaxRegionSpanDiffRatio) },
            { ReproductionSettingsKeys.MaxFunctionHistDistanceKey, FormatFloat(DefaultMaxFunctionHistDistance) },
            { ReproductionSettingsKeys.MaxConnectivityHistDistanceKey, FormatFloat(DefaultMaxConnectivityHistDistance) },
            { ReproductionSettingsKeys.ProbAddNeuronToEmptyRegionKey, FormatFloat(DefaultProbAddNeuronToEmptyRegion) },
            { ReproductionSettingsKeys.ProbRemoveLastNeuronFromRegionKey, FormatFloat(DefaultProbRemoveLastNeuronFromRegion) },
            { ReproductionSettingsKeys.ProbDisableNeuronKey, FormatFloat(DefaultProbDisableNeuron) },
            { ReproductionSettingsKeys.ProbReactivateNeuronKey, FormatFloat(DefaultProbReactivateNeuron) },
            { ReproductionSettingsKeys.ProbAddAxonKey, FormatFloat(DefaultProbAddAxon) },
            { ReproductionSettingsKeys.ProbRemoveAxonKey, FormatFloat(DefaultProbRemoveAxon) },
            { ReproductionSettingsKeys.ProbRerouteAxonKey, FormatFloat(DefaultProbRerouteAxon) },
            { ReproductionSettingsKeys.ProbRerouteInboundAxonOnDeleteKey, FormatFloat(DefaultProbRerouteInboundAxonOnDelete) },
            { ReproductionSettingsKeys.InboundRerouteMaxRingDistanceKey, FormatUInt(DefaultInboundRerouteMaxRingDistance) },
            { ReproductionSettingsKeys.ProbChooseParentAKey, FormatFloat(DefaultProbChooseParentA) },
            { ReproductionSettingsKeys.ProbChooseParentBKey, FormatFloat(DefaultProbChooseParentB) },
            { ReproductionSettingsKeys.ProbAverageKey, FormatFloat(DefaultProbAverage) },
            { ReproductionSettingsKeys.ProbMutateKey, FormatFloat(DefaultProbMutate) },
            { ReproductionSettingsKeys.ProbChooseFuncAKey, FormatFloat(DefaultProbChooseFuncA) },
            { ReproductionSettingsKeys.ProbMutateFuncKey, FormatFloat(DefaultProbMutateFunc) },
            { ReproductionSettingsKeys.MaxAvgOutDegreeBrainKey, FormatFloat(DefaultMaxAvgOutDegreeBrain) },
            { ReproductionSettingsKeys.PrunePolicyKey, ToSettingValue(DefaultPrunePolicy) },
            { ReproductionSettingsKeys.PerRegionOutDegreeCapsKey, DefaultPerRegionOutDegreeCaps },
            { ReproductionSettingsKeys.StrengthTransformEnabledKey, FormatBool(DefaultStrengthTransformEnabled) },
            { ReproductionSettingsKeys.ProbStrengthChooseAKey, FormatFloat(DefaultProbStrengthChooseA) },
            { ReproductionSettingsKeys.ProbStrengthChooseBKey, FormatFloat(DefaultProbStrengthChooseB) },
            { ReproductionSettingsKeys.ProbStrengthAverageKey, FormatFloat(DefaultProbStrengthAverage) },
            { ReproductionSettingsKeys.ProbStrengthWeightedAverageKey, FormatFloat(DefaultProbStrengthWeightedAverage) },
            { ReproductionSettingsKeys.StrengthWeightAKey, FormatFloat(DefaultStrengthWeightA) },
            { ReproductionSettingsKeys.StrengthWeightBKey, FormatFloat(DefaultStrengthWeightB) },
            { ReproductionSettingsKeys.ProbStrengthMutateKey, FormatFloat(DefaultProbStrengthMutate) },
            { ReproductionSettingsKeys.MaxNeuronsAddedAbsKey, FormatUInt(DefaultMaxNeuronsAddedAbs) },
            { ReproductionSettingsKeys.MaxNeuronsAddedPctKey, FormatFloat(DefaultMaxNeuronsAddedPct) },
            { ReproductionSettingsKeys.MaxNeuronsRemovedAbsKey, FormatUInt(DefaultMaxNeuronsRemovedAbs) },
            { ReproductionSettingsKeys.MaxNeuronsRemovedPctKey, FormatFloat(DefaultMaxNeuronsRemovedPct) },
            { ReproductionSettingsKeys.MaxAxonsAddedAbsKey, FormatUInt(DefaultMaxAxonsAddedAbs) },
            { ReproductionSettingsKeys.MaxAxonsAddedPctKey, FormatFloat(DefaultMaxAxonsAddedPct) },
            { ReproductionSettingsKeys.MaxAxonsRemovedAbsKey, FormatUInt(DefaultMaxAxonsRemovedAbs) },
            { ReproductionSettingsKeys.MaxAxonsRemovedPctKey, FormatFloat(DefaultMaxAxonsRemovedPct) },
            { ReproductionSettingsKeys.MaxRegionsAddedAbsKey, FormatUInt(DefaultMaxRegionsAddedAbs) },
            { ReproductionSettingsKeys.MaxRegionsRemovedAbsKey, FormatUInt(DefaultMaxRegionsRemovedAbs) },
            { ReproductionSettingsKeys.SpawnChildKey, ToSettingValue(DefaultSpawnChildPolicy) }
        };
    }

    private static float ParseFloat(string? value, float fallback)
    {
        return float.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private static uint ParseUInt(string? value, uint fallback)
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

        return value.Trim().ToLowerInvariant() switch
        {
            "1" => true,
            "yes" => true,
            "on" => true,
            "0" => false,
            "no" => false,
            "off" => false,
            _ => fallback
        };
    }

    private static IEnumerable<Repro.RegionOutDegreeCap> ParsePerRegionOutDegreeCaps(string? rawValue)
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
            yield return new Repro.RegionOutDegreeCap
            {
                RegionId = pair.Key,
                MaxAvgOutDegree = pair.Value
            };
        }
    }
}
