using Nbn.Proto;
using Nbn.Shared;
using ProtoSpec = Nbn.Proto.Speciation;
using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Nbn.Runtime.Speciation;

public sealed partial class SpeciationManagerActor
{
    private static SpeciationAssignmentPolicy BuildAssignmentPolicy(SpeciationRuntimeConfig runtimeConfig)
    {
        var policyNode = TryResolvePolicyNode(runtimeConfig.ConfigSnapshotJson);
        var matchThreshold = ClampScore(ReadPolicyDouble(
            policyNode,
            defaultValue: 0.92d,
            "lineage_match_threshold",
            "lineageMatchThreshold"));
        var hysteresisMargin = ClampScore(ReadPolicyDouble(
            policyNode,
            defaultValue: 0.04d,
            "lineage_hysteresis_margin",
            "lineageHysteresisMargin"));
        var resolvedSplitThreshold = ClampScore(ReadPolicyDouble(
            policyNode,
            defaultValue: 0.88d,
            "lineage_split_threshold",
            "lineageSplitThreshold"));
        if (resolvedSplitThreshold > matchThreshold)
        {
            resolvedSplitThreshold = matchThreshold;
        }

        var parentConsensusThreshold = ClampScore(ReadPolicyDouble(
            policyNode,
            defaultValue: 0.70d,
            "parent_consensus_threshold",
            "parentConsensusThreshold"));
        var splitGuardMargin = ClampScore(ReadPolicyDouble(
            policyNode,
            defaultValue: 0.02d,
            "lineage_split_guard_margin",
            "lineageSplitGuardMargin"));
        var minParentMembershipsBeforeSplit = ReadPolicyInt(
            policyNode,
            defaultValue: 1,
            "lineage_min_parent_memberships_before_split",
            "lineageMinParentMembershipsBeforeSplit");
        var recentSplitRealignParentMembershipWindow = ReadPolicyInt(
            policyNode,
            defaultValue: 3,
            "lineage_realign_parent_membership_window",
            "lineageRealignParentMembershipWindow");
        var recentSplitRealignMatchMargin = ClampScore(ReadPolicyDouble(
            policyNode,
            defaultValue: 0.05d,
            "lineage_realign_match_margin",
            "lineageRealignMatchMargin"));
        var hindsightReassignCommitWindow = ReadPolicyInt(
            policyNode,
            defaultValue: 6,
            "lineage_hindsight_reassign_commit_window",
            "lineageHindsightReassignCommitWindow");
        var hindsightReassignSimilarityMargin = ClampScore(ReadPolicyDouble(
            policyNode,
            defaultValue: 0.015d,
            "lineage_hindsight_similarity_margin",
            "lineageHindsightSimilarityMargin"));
        var createDerivedSpecies = ReadPolicyBool(
            policyNode,
            defaultValue: true,
            "create_derived_species_on_divergence",
            "createDerivedSpeciesOnDivergence");
        var derivedSpeciesPrefix = NormalizeToken(ReadPolicyString(
            policyNode,
            defaultValue: "branch",
            "derived_species_prefix",
            "derivedSpeciesPrefix"), "branch");

        return new SpeciationAssignmentPolicy(
            LineageMatchThreshold: matchThreshold,
            LineageSplitThreshold: resolvedSplitThreshold,
            ParentConsensusThreshold: parentConsensusThreshold,
            HysteresisMargin: hysteresisMargin,
            LineageSplitGuardMargin: splitGuardMargin,
            MinParentMembershipsBeforeSplit: Math.Max(1, minParentMembershipsBeforeSplit),
            RecentSplitRealignParentMembershipWindow: Math.Max(0, recentSplitRealignParentMembershipWindow),
            RecentSplitRealignMatchMargin: recentSplitRealignMatchMargin,
            HindsightReassignCommitWindow: Math.Max(0, hindsightReassignCommitWindow),
            HindsightReassignSimilarityMargin: hindsightReassignSimilarityMargin,
            CreateDerivedSpeciesOnDivergence: createDerivedSpecies,
            DerivedSpeciesPrefix: derivedSpeciesPrefix);
    }

    private static SpeciationRuntimeConfig BuildRuntimeConfigFromSettings(
        IReadOnlyDictionary<string, string> settings,
        SpeciationRuntimeConfig fallback)
    {
        var policyVersion = ReadSettingValue(
            settings,
            SpeciationSettingsKeys.PolicyVersionKey,
            fallback.PolicyVersion,
            SpeciationOptions.DefaultPolicyVersion);
        var defaultSpeciesId = ReadSettingValue(
            settings,
            SpeciationSettingsKeys.DefaultSpeciesIdKey,
            fallback.DefaultSpeciesId,
            SpeciationOptions.DefaultSpeciesId);
        var defaultSpeciesDisplayName = ReadSettingValue(
            settings,
            SpeciationSettingsKeys.DefaultSpeciesDisplayNameKey,
            fallback.DefaultSpeciesDisplayName,
            SpeciationOptions.DefaultSpeciesDisplayName);
        var startupReconcileReason = ReadSettingValue(
            settings,
            SpeciationSettingsKeys.StartupReconcileReasonKey,
            fallback.StartupReconcileDecisionReason,
            SpeciationOptions.DefaultStartupReconcileDecisionReason);

        var enabled = ParseBoolSetting(
            ReadSettingValue(settings, SpeciationSettingsKeys.ConfigEnabledKey, fallbackValue: "true", defaultValue: "true"),
            defaultValue: true);
        var matchThreshold = ClampScore(ParseDoubleSetting(
            ReadSettingValue(
                settings,
                SpeciationSettingsKeys.LineageMatchThresholdKey,
                fallbackValue: null,
                defaultValue: "0.92"),
            defaultValue: 0.92d));
        var splitThreshold = ClampScore(ParseDoubleSetting(
            ReadSettingValue(
                settings,
                SpeciationSettingsKeys.LineageSplitThresholdKey,
                fallbackValue: null,
                defaultValue: "0.88"),
            defaultValue: 0.88d));
        if (splitThreshold > matchThreshold)
        {
            splitThreshold = matchThreshold;
        }

        var parentConsensusThreshold = ClampScore(ParseDoubleSetting(
            ReadSettingValue(
                settings,
                SpeciationSettingsKeys.ParentConsensusThresholdKey,
                fallbackValue: null,
                defaultValue: "0.70"),
            defaultValue: 0.70d));
        var hysteresisMargin = Math.Max(
            0d,
            ParseDoubleSetting(
                ReadSettingValue(
                    settings,
                    SpeciationSettingsKeys.LineageHysteresisMarginKey,
                    fallbackValue: null,
                    defaultValue: "0.04"),
                defaultValue: 0.04d));
        var splitGuardMargin = ClampScore(ParseDoubleSetting(
            ReadSettingValue(
                settings,
                SpeciationSettingsKeys.LineageSplitGuardMarginKey,
                fallbackValue: null,
                defaultValue: "0.02"),
            defaultValue: 0.02d));
        var minParentMembershipsBeforeSplit = Math.Max(
            1,
            ParseIntSetting(
                ReadSettingValue(
                    settings,
                    SpeciationSettingsKeys.LineageMinParentMembershipsBeforeSplitKey,
                    fallbackValue: null,
                    defaultValue: "1"),
                defaultValue: 1));
        var realignParentMembershipWindow = Math.Max(
            0,
            ParseIntSetting(
                ReadSettingValue(
                    settings,
                    SpeciationSettingsKeys.LineageRealignParentMembershipWindowKey,
                    fallbackValue: null,
                    defaultValue: "3"),
                defaultValue: 3));
        var realignMatchMargin = ClampScore(ParseDoubleSetting(
            ReadSettingValue(
                settings,
                SpeciationSettingsKeys.LineageRealignMatchMarginKey,
                fallbackValue: null,
                defaultValue: "0.05"),
            defaultValue: 0.05d));
        var hindsightReassignCommitWindow = Math.Max(
            0,
            ParseIntSetting(
                ReadSettingValue(
                    settings,
                    SpeciationSettingsKeys.LineageHindsightReassignCommitWindowKey,
                    fallbackValue: null,
                    defaultValue: "6"),
                defaultValue: 6));
        var hindsightReassignSimilarityMargin = ClampScore(ParseDoubleSetting(
            ReadSettingValue(
                settings,
                SpeciationSettingsKeys.LineageHindsightSimilarityMarginKey,
                fallbackValue: null,
                defaultValue: "0.015"),
            defaultValue: 0.015d));
        var createDerivedSpecies = ParseBoolSetting(
            ReadSettingValue(
                settings,
                SpeciationSettingsKeys.CreateDerivedSpeciesOnDivergenceKey,
                fallbackValue: null,
                defaultValue: "true"),
            defaultValue: true);
        var derivedSpeciesPrefix = NormalizeToken(
            ReadSettingValue(
                settings,
                SpeciationSettingsKeys.DerivedSpeciesPrefixKey,
                fallbackValue: null,
                defaultValue: "branch"),
            "branch");

        var snapshot = new JsonObject
        {
            ["enabled"] = enabled,
            ["assignment_policy"] = new JsonObject
            {
                ["lineage_match_threshold"] = matchThreshold,
                ["lineage_split_threshold"] = splitThreshold,
                ["parent_consensus_threshold"] = parentConsensusThreshold,
                ["lineage_hysteresis_margin"] = hysteresisMargin,
                ["lineage_split_guard_margin"] = splitGuardMargin,
                ["lineage_min_parent_memberships_before_split"] = minParentMembershipsBeforeSplit,
                ["lineage_realign_parent_membership_window"] = realignParentMembershipWindow,
                ["lineage_realign_match_margin"] = realignMatchMargin,
                ["lineage_hindsight_reassign_commit_window"] = hindsightReassignCommitWindow,
                ["lineage_hindsight_similarity_margin"] = hindsightReassignSimilarityMargin,
                ["create_derived_species_on_divergence"] = createDerivedSpecies,
                ["derived_species_prefix"] = derivedSpeciesPrefix
            }
        };

        return new SpeciationRuntimeConfig(
            PolicyVersion: policyVersion,
            ConfigSnapshotJson: snapshot.ToJsonString(),
            DefaultSpeciesId: defaultSpeciesId,
            DefaultSpeciesDisplayName: defaultSpeciesDisplayName,
            StartupReconcileDecisionReason: startupReconcileReason);
    }

    private static string ReadSettingValue(
        IReadOnlyDictionary<string, string> settings,
        string key,
        string? fallbackValue,
        string defaultValue)
    {
        if (settings.TryGetValue(key, out var configured) && !string.IsNullOrWhiteSpace(configured))
        {
            return configured.Trim();
        }

        if (!string.IsNullOrWhiteSpace(fallbackValue))
        {
            return fallbackValue.Trim();
        }

        return defaultValue;
    }

    private static bool ParseBoolSetting(string? rawValue, bool defaultValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            return defaultValue;
        }

        return bool.TryParse(rawValue.Trim(), out var parsed) ? parsed : defaultValue;
    }

    private static double ParseDoubleSetting(string? rawValue, double defaultValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            return defaultValue;
        }

        return double.TryParse(rawValue.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : defaultValue;
    }

    private static int ParseIntSetting(string? rawValue, int defaultValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            return defaultValue;
        }

        return int.TryParse(rawValue.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : defaultValue;
    }

    private static JsonObject? TryResolvePolicyNode(string configSnapshotJson)
    {
        if (string.IsNullOrWhiteSpace(configSnapshotJson))
        {
            return null;
        }

        try
        {
            var node = JsonNode.Parse(configSnapshotJson);
            if (node is not JsonObject root)
            {
                return null;
            }

            if (root["assignment_policy"] is JsonObject assignmentPolicy)
            {
                return assignmentPolicy;
            }

            if (root["assignmentPolicy"] is JsonObject assignmentPolicyCamel)
            {
                return assignmentPolicyCamel;
            }

            return root;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static double? ReadOptionalPolicyDouble(JsonObject? policyNode, params string[] aliases)
    {
        return TryReadPolicyValue(policyNode, out double value, aliases) ? value : null;
    }

    private static double ReadPolicyDouble(JsonObject? policyNode, double defaultValue, params string[] aliases)
    {
        return TryReadPolicyValue(policyNode, out double value, aliases)
            ? value
            : defaultValue;
    }

    private static int ReadPolicyInt(JsonObject? policyNode, int defaultValue, params string[] aliases)
    {
        if (!TryReadPolicyValue(policyNode, out double value, aliases))
        {
            return defaultValue;
        }

        if (double.IsNaN(value) || double.IsInfinity(value))
        {
            return defaultValue;
        }

        return (int)Math.Max(0d, Math.Round(value, MidpointRounding.AwayFromZero));
    }

    private static bool ReadPolicyBool(JsonObject? policyNode, bool defaultValue, params string[] aliases)
    {
        if (!TryGetPolicyNode(policyNode, aliases, out var node) || node is null)
        {
            return defaultValue;
        }

        return node switch
        {
            JsonValue value when value.TryGetValue<bool>(out var asBool) => asBool,
            JsonValue value when value.TryGetValue<string>(out var asString)
                && bool.TryParse(asString, out var parsedBool) => parsedBool,
            _ => defaultValue
        };
    }

    private static string ReadPolicyString(JsonObject? policyNode, string defaultValue, params string[] aliases)
    {
        if (!TryGetPolicyNode(policyNode, aliases, out var node) || node is null)
        {
            return defaultValue;
        }

        return node is JsonValue value && value.TryGetValue<string>(out var asString) && !string.IsNullOrWhiteSpace(asString)
            ? asString.Trim()
            : defaultValue;
    }

    private static bool TryReadPolicyValue(
        JsonObject? policyNode,
        out double value,
        params string[] aliases)
    {
        if (!TryGetPolicyNode(policyNode, aliases, out var node) || !TryReadDouble(node, out value))
        {
            value = default;
            return false;
        }

        return true;
    }

    private static bool TryGetPolicyNode(
        JsonObject? policyNode,
        IReadOnlyList<string> aliases,
        out JsonNode? value)
    {
        if (policyNode is null || aliases.Count == 0)
        {
            value = null;
            return false;
        }

        var normalizedAliases = aliases
            .Select(NormalizeJsonKey)
            .Where(alias => alias.Length > 0)
            .ToHashSet(StringComparer.Ordinal);
        if (normalizedAliases.Count == 0)
        {
            value = null;
            return false;
        }

        foreach (var property in policyNode)
        {
            if (normalizedAliases.Contains(NormalizeJsonKey(property.Key)))
            {
                value = property.Value;
                return true;
            }
        }

        value = null;
        return false;
    }

    private static bool TryReadDouble(JsonNode? node, out double value)
    {
        switch (node)
        {
            case JsonValue valueNode when valueNode.TryGetValue<double>(out value):
                return true;
            case JsonValue valueNode when valueNode.TryGetValue<float>(out var asFloat):
                value = asFloat;
                return true;
            case JsonValue valueNode when valueNode.TryGetValue<decimal>(out var asDecimal):
                value = (double)asDecimal;
                return true;
            case JsonValue valueNode when valueNode.TryGetValue<long>(out var asLong):
                value = asLong;
                return true;
            case JsonValue valueNode when valueNode.TryGetValue<int>(out var asInt):
                value = asInt;
                return true;
            case JsonValue valueNode when valueNode.TryGetValue<string>(out var asString)
                && double.TryParse(asString, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed):
                value = parsed;
                return true;
            default:
                value = default;
                return false;
        }
    }

    private static double ClampScore(double value)
    {
        if (double.IsNaN(value) || double.IsInfinity(value))
        {
            return 0d;
        }

        if (value < 0d)
        {
            return 0d;
        }

        return value > 1d ? 1d : value;
    }

    private static string BuildLineageKey(IReadOnlyList<Guid> orderedParentBrainIds)
    {
        if (orderedParentBrainIds.Count == 0)
        {
            return string.Empty;
        }

        return string.Join("|", orderedParentBrainIds.Select(parentBrainId => parentBrainId.ToString("D")));
    }

    private static string BuildDerivedSpeciesId(
        string dominantSpeciesId,
        string derivedSpeciesPrefix,
        string lineageKey,
        long epochId)
    {
        var normalizedDominant = NormalizeToken(dominantSpeciesId, "species");
        var normalizedPrefix = NormalizeToken(derivedSpeciesPrefix, "branch");
        var hashInput = $"{epochId}:{lineageKey}:{normalizedDominant}:{normalizedPrefix}";
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(hashInput));
        var suffix = Convert.ToHexString(hash.AsSpan(0, 6)).ToLowerInvariant();
        const int maxSpeciesIdLength = 96;
        var prefixBudget = Math.Max(1, maxSpeciesIdLength - suffix.Length - 2);
        var truncatedPrefix = normalizedPrefix.Length <= prefixBudget
            ? normalizedPrefix
            : normalizedPrefix[..prefixBudget];
        var dominantBudget = Math.Max(1, maxSpeciesIdLength - truncatedPrefix.Length - suffix.Length - 2);
        var truncatedDominant = normalizedDominant.Length <= dominantBudget
            ? normalizedDominant
            : normalizedDominant[..dominantBudget];
        return $"{truncatedDominant}-{truncatedPrefix}-{suffix}";
    }

    private static string BuildFounderRootSpeciesId(
        string baselineSpeciesId,
        string lineageKey,
        long epochId)
    {
        var normalizedBaseline = NormalizeToken(baselineSpeciesId, "species");
        const string founderPrefix = "founder";
        var hashInput = $"{epochId}:{lineageKey}:{normalizedBaseline}:{founderPrefix}";
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(hashInput));
        var suffix = Convert.ToHexString(hash.AsSpan(0, 6)).ToLowerInvariant();
        const int maxSpeciesIdLength = 96;
        var prefixBudget = Math.Max(1, maxSpeciesIdLength - suffix.Length - 2);
        var truncatedPrefix = founderPrefix.Length <= prefixBudget
            ? founderPrefix
            : founderPrefix[..prefixBudget];
        var baselineBudget = Math.Max(1, maxSpeciesIdLength - truncatedPrefix.Length - suffix.Length - 2);
        var truncatedBaseline = normalizedBaseline.Length <= baselineBudget
            ? normalizedBaseline
            : normalizedBaseline[..baselineBudget];
        return $"{truncatedBaseline}-{truncatedPrefix}-{suffix}";
    }

    private static string BuildDerivedSpeciesDisplayName(
        string? dominantSpeciesDisplayName,
        string dominantSpeciesId,
        string derivedSpeciesId)
    {
        var parentDisplayName = ResolveSpeciesDisplayName(dominantSpeciesDisplayName, dominantSpeciesId);
        var (stem, lineageCode) = ParseLineageDisplayName(parentDisplayName);
        if (lineageCode.Length == 0
            && TryParseNumberedRootSpeciesDisplayName(stem, out _, out var rootOrdinal))
        {
            lineageCode = BuildRootSpeciesLineagePrefix(rootOrdinal);
        }

        var nextLetter = ComputeLineageLetter(derivedSpeciesId);
        var nextCode = lineageCode + nextLetter;
        return $"{stem} [{nextCode}]";
    }

    private static string ResolveSpeciesDisplayName(string? preferredDisplayName, string speciesId)
    {
        if (!string.IsNullOrWhiteSpace(preferredDisplayName))
        {
            return preferredDisplayName.Trim();
        }

        return BuildDisplayNameFromSpeciesId(speciesId);
    }

    private static (string Stem, string LineageCode) ParseLineageDisplayName(string displayName)
    {
        if (string.IsNullOrWhiteSpace(displayName))
        {
            return ("Species", string.Empty);
        }

        var trimmed = displayName.Trim();
        var openIndex = trimmed.LastIndexOf('[');
        if (openIndex >= 0 && trimmed.EndsWith(']') && openIndex < trimmed.Length - 2)
        {
            var candidateCode = trimmed[(openIndex + 1)..^1].Trim();
            if (candidateCode.Length > 0 && candidateCode.All(ch => ch is >= 'A' and <= 'Z'))
            {
                var stem = trimmed[..openIndex].TrimEnd();
                return (stem.Length == 0 ? "Species" : stem, candidateCode);
            }
        }

        return (trimmed, string.Empty);
    }

    private static string ComputeLineageLetter(string seed)
    {
        if (string.IsNullOrWhiteSpace(seed))
        {
            return "A";
        }

        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(seed.Trim()));
        var letter = (char)('A' + (hash[0] % 26));
        return letter.ToString(CultureInfo.InvariantCulture);
    }

    private static bool TryParseNumberedRootSpeciesDisplayName(
        string? speciesDisplayStem,
        out string rootStem,
        out int rootOrdinal)
    {
        rootStem = string.IsNullOrWhiteSpace(speciesDisplayStem)
            ? "Species"
            : speciesDisplayStem.Trim();
        rootOrdinal = 0;

        var separatorIndex = rootStem.LastIndexOf('-');
        if (separatorIndex <= 0 || separatorIndex >= rootStem.Length - 1)
        {
            return false;
        }

        var ordinalToken = rootStem[(separatorIndex + 1)..].Trim();
        if (!int.TryParse(
                ordinalToken,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out rootOrdinal)
            || rootOrdinal <= 0)
        {
            rootOrdinal = 0;
            return false;
        }

        rootStem = rootStem[..separatorIndex].TrimEnd();
        if (rootStem.Length == 0)
        {
            rootStem = "Species";
        }

        return true;
    }

    private static string BuildNumberedRootSpeciesDisplayName(string rootStem, int rootOrdinal)
    {
        var normalizedStem = string.IsNullOrWhiteSpace(rootStem)
            ? "Species"
            : rootStem.Trim();
        return rootOrdinal > 0
            ? $"{normalizedStem}-{rootOrdinal.ToString(CultureInfo.InvariantCulture)}"
            : normalizedStem;
    }

    private static string BuildRootSpeciesLineagePrefix(int rootOrdinal)
    {
        var normalizedOrdinal = Math.Max(1, rootOrdinal);
        var builder = new StringBuilder();
        while (normalizedOrdinal > 0)
        {
            normalizedOrdinal--;
            builder.Insert(0, (char)('A' + (normalizedOrdinal % 26)));
            normalizedOrdinal /= 26;
        }

        return builder.ToString();
    }

    private static bool IsSeedFounderCandidate(
        Guid candidateBrainId,
        IReadOnlyList<Guid> inputOrderedParentBrainIds)
    {
        if (candidateBrainId == Guid.Empty || inputOrderedParentBrainIds.Count == 0)
        {
            return false;
        }

        var containsSelf = false;
        var containsDistinctPeer = false;
        foreach (var parentBrainId in inputOrderedParentBrainIds)
        {
            if (parentBrainId == Guid.Empty)
            {
                continue;
            }

            if (parentBrainId == candidateBrainId)
            {
                containsSelf = true;
                continue;
            }

            containsDistinctPeer = true;
        }

        return containsSelf && containsDistinctPeer;
    }

    private static string BuildDisplayNameFromSpeciesId(string speciesId)
    {
        if (string.IsNullOrWhiteSpace(speciesId))
        {
            return "Species";
        }

        var tokens = speciesId
            .Trim()
            .Split(['.', '-', '_', '/', ':'], StringSplitOptions.RemoveEmptyEntries);
        if (tokens.Length == 0)
        {
            return speciesId.Trim();
        }

        var parts = tokens
            .Select(FormatDisplayToken)
            .Where(part => part.Length > 0)
            .ToArray();
        if (parts.Length == 0)
        {
            return speciesId.Trim();
        }

        return string.Join(' ', parts);
    }

    private static string FormatDisplayToken(string token)
    {
        if (string.IsNullOrWhiteSpace(token))
        {
            return string.Empty;
        }

        var trimmed = token.Trim();
        if (trimmed.Length == 0)
        {
            return string.Empty;
        }

        if (trimmed.Length == 1)
        {
            return char.ToUpperInvariant(trimmed[0]).ToString(CultureInfo.InvariantCulture);
        }

        return char.ToUpperInvariant(trimmed[0]) + trimmed[1..].ToLowerInvariant();
    }

    private static string NormalizeToken(string? value, string fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        var builder = new StringBuilder(value.Length);
        foreach (var ch in value.Trim().ToLowerInvariant())
        {
            if ((ch >= 'a' && ch <= 'z')
                || (ch >= '0' && ch <= '9')
                || ch == '-'
                || ch == '_')
            {
                builder.Append(ch);
            }
            else if (builder.Length == 0 || builder[^1] != '-')
            {
                builder.Append('-');
            }
        }

        var normalized = builder.ToString().Trim('-');
        return normalized.Length == 0 ? fallback : normalized;
    }

    private static string NormalizeJsonKey(string key)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return string.Empty;
        }

        var builder = new StringBuilder(key.Length);
        foreach (var ch in key)
        {
            if (char.IsLetterOrDigit(ch))
            {
                builder.Append(char.ToLowerInvariant(ch));
            }
        }

        return builder.ToString();
    }

    private static ProtoSpec.SpeciationApplyMode NormalizeApplyMode(ProtoSpec.SpeciationApplyMode applyMode)
    {
        return applyMode == ProtoSpec.SpeciationApplyMode.Commit
            ? ProtoSpec.SpeciationApplyMode.Commit
            : ProtoSpec.SpeciationApplyMode.DryRun;
    }

    private static ProtoSpec.SpeciationSetConfigResponse CreateProtoSetConfigResponse(
        ProtoSpec.SpeciationFailureReason failureReason,
        string failureDetail,
        SpeciationEpochInfo previousEpoch,
        SpeciationEpochInfo currentEpoch,
        SpeciationRuntimeConfig runtimeConfig)
    {
        return new ProtoSpec.SpeciationSetConfigResponse
        {
            FailureReason = failureReason,
            FailureDetail = failureDetail,
            PreviousEpoch = ToProtoEpochInfo(previousEpoch),
            CurrentEpoch = ToProtoEpochInfo(currentEpoch),
            Config = ToProtoRuntimeConfig(runtimeConfig)
        };
    }

    private static ProtoSpec.SpeciationResetAllResponse CreateProtoResetAllResponse(
        ProtoSpec.SpeciationFailureReason failureReason,
        string failureDetail,
        SpeciationEpochInfo previousEpoch,
        SpeciationEpochInfo currentEpoch,
        SpeciationRuntimeConfig runtimeConfig,
        int deletedEpochCount,
        int deletedMembershipCount,
        int deletedSpeciesCount,
        int deletedDecisionCount,
        int deletedLineageEdgeCount)
    {
        return new ProtoSpec.SpeciationResetAllResponse
        {
            FailureReason = failureReason,
            FailureDetail = failureDetail,
            PreviousEpoch = ToProtoEpochInfo(previousEpoch),
            CurrentEpoch = ToProtoEpochInfo(currentEpoch),
            Config = ToProtoRuntimeConfig(runtimeConfig),
            DeletedEpochCount = (uint)Math.Max(0, deletedEpochCount),
            DeletedMembershipCount = (uint)Math.Max(0, deletedMembershipCount),
            DeletedSpeciesCount = (uint)Math.Max(0, deletedSpeciesCount),
            DeletedDecisionCount = (uint)Math.Max(0, deletedDecisionCount),
            DeletedLineageEdgeCount = (uint)Math.Max(0, deletedLineageEdgeCount)
        };
    }

    private static ProtoSpec.SpeciationDeleteEpochResponse CreateProtoDeleteEpochResponse(
        ProtoSpec.SpeciationFailureReason failureReason,
        string failureDetail,
        long epochId,
        bool deleted,
        int deletedMembershipCount,
        int deletedSpeciesCount,
        int deletedDecisionCount,
        int deletedLineageEdgeCount,
        SpeciationEpochInfo currentEpoch)
    {
        return new ProtoSpec.SpeciationDeleteEpochResponse
        {
            FailureReason = failureReason,
            FailureDetail = failureDetail,
            EpochId = (ulong)Math.Max(0, epochId),
            Deleted = deleted,
            DeletedMembershipCount = (uint)Math.Max(0, deletedMembershipCount),
            DeletedSpeciesCount = (uint)Math.Max(0, deletedSpeciesCount),
            DeletedDecisionCount = (uint)Math.Max(0, deletedDecisionCount),
            DeletedLineageEdgeCount = (uint)Math.Max(0, deletedLineageEdgeCount),
            CurrentEpoch = ToProtoEpochInfo(currentEpoch)
        };
    }

    private static ProtoSpec.SpeciationDecision CreateDecisionFailure(
        ProtoSpec.SpeciationApplyMode applyMode,
        ProtoSpec.SpeciationFailureReason failureReason,
        string failureDetail)
    {
        return new ProtoSpec.SpeciationDecision
        {
            ApplyMode = applyMode,
            CandidateMode = ProtoSpec.SpeciationCandidateMode.Unknown,
            Success = false,
            Created = false,
            ImmutableConflict = false,
            FailureReason = failureReason,
            FailureDetail = failureDetail,
            SpeciesId = string.Empty,
            SpeciesDisplayName = string.Empty,
            DecisionReason = string.Empty,
            DecisionMetadataJson = "{}",
            Committed = false
        };
    }

    private static void RecordDecisionTelemetry(
        string operation,
        long epochId,
        double durationMs,
        Activity? activity,
        ProtoSpec.SpeciationDecision decision)
    {
        SpeciationTelemetry.RecordAssignmentDecision(operation, decision, durationMs);
        SpeciationTelemetry.CompleteAssignmentActivity(activity, epochId, decision, durationMs);
    }

    private static void RecordStartupReconcileTelemetry(
        Activity? activity,
        long epochId,
        int knownBrains,
        SpeciationReconcileResult? result,
        string outcome,
        string failureReason)
    {
        SpeciationTelemetry.RecordStartupReconcile(knownBrains, result, outcome, failureReason);
        SpeciationTelemetry.CompleteStartupReconcileActivity(
            activity,
            epochId,
            knownBrains,
            result,
            outcome,
            failureReason);
    }

    private static void RecordEpochTransitionTelemetry(
        Activity? activity,
        string transition,
        string outcome,
        string failureReason,
        long previousEpochId,
        long currentEpochId,
        int deletedMembershipCount = 0,
        int deletedSpeciesCount = 0,
        int deletedDecisionCount = 0,
        int deletedLineageEdgeCount = 0,
        int deletedEpochCount = 0)
    {
        SpeciationTelemetry.RecordEpochTransition(transition, outcome, failureReason);
        SpeciationTelemetry.CompleteEpochTransitionActivity(
            activity,
            transition,
            outcome,
            failureReason,
            previousEpochId,
            currentEpochId,
            deletedMembershipCount,
            deletedSpeciesCount,
            deletedDecisionCount,
            deletedLineageEdgeCount,
            deletedEpochCount);
    }

    private static ProtoSpec.SpeciationDecision CreateDecisionFromMembership(
        ProtoSpec.SpeciationApplyMode applyMode,
        ProtoSpec.SpeciationCandidateMode candidateMode,
        SpeciationMembershipRecord membership,
        bool created,
        bool immutableConflict,
        bool committed,
        ProtoSpec.SpeciationFailureReason failureReason,
        string failureDetail,
        bool? successOverride = null)
    {
        var success = successOverride ?? (failureReason == ProtoSpec.SpeciationFailureReason.SpeciationFailureNone);
        return new ProtoSpec.SpeciationDecision
        {
            ApplyMode = applyMode,
            CandidateMode = candidateMode,
            Success = success,
            Created = created,
            ImmutableConflict = immutableConflict,
            FailureReason = failureReason,
            FailureDetail = failureDetail,
            SpeciesId = membership.SpeciesId,
            SpeciesDisplayName = membership.SpeciesDisplayName,
            DecisionReason = membership.DecisionReason,
            DecisionMetadataJson = membership.DecisionMetadataJson,
            Committed = committed,
            Membership = ToProtoMembershipRecord(membership)
        };
    }

    private static ProtoSpec.SpeciationRuntimeConfig ToProtoRuntimeConfig(SpeciationRuntimeConfig config)
    {
        return new ProtoSpec.SpeciationRuntimeConfig
        {
            PolicyVersion = config.PolicyVersion,
            ConfigSnapshotJson = config.ConfigSnapshotJson,
            DefaultSpeciesId = config.DefaultSpeciesId,
            DefaultSpeciesDisplayName = config.DefaultSpeciesDisplayName,
            StartupReconcileDecisionReason = config.StartupReconcileDecisionReason
        };
    }

    private static ProtoSpec.SpeciationEpochInfo ToProtoEpochInfo(SpeciationEpochInfo epoch)
    {
        return new ProtoSpec.SpeciationEpochInfo
        {
            EpochId = (ulong)Math.Max(0, epoch.EpochId),
            CreatedMs = (ulong)Math.Max(0, epoch.CreatedMs),
            PolicyVersion = epoch.PolicyVersion ?? "unknown",
            ConfigSnapshotJson = epoch.ConfigSnapshotJson ?? "{}"
        };
    }

    private static ProtoSpec.SpeciationStatusSnapshot ToProtoStatusSnapshot(SpeciationStatusSnapshot status)
    {
        return new ProtoSpec.SpeciationStatusSnapshot
        {
            EpochId = (ulong)Math.Max(0, status.EpochId),
            MembershipCount = (uint)Math.Max(0, status.MembershipCount),
            SpeciesCount = (uint)Math.Max(0, status.SpeciesCount),
            LineageEdgeCount = (uint)Math.Max(0, status.LineageEdgeCount)
        };
    }

    private static ProtoSpec.SpeciationMembershipRecord ToProtoMembershipRecord(SpeciationMembershipRecord membership)
    {
        var proto = new ProtoSpec.SpeciationMembershipRecord
        {
            EpochId = (ulong)Math.Max(0, membership.EpochId),
            BrainId = membership.BrainId.ToProtoUuid(),
            SpeciesId = membership.SpeciesId,
            SpeciesDisplayName = membership.SpeciesDisplayName,
            AssignedMs = (ulong)Math.Max(0, membership.AssignedMs),
            PolicyVersion = membership.PolicyVersion,
            DecisionReason = membership.DecisionReason,
            DecisionMetadataJson = membership.DecisionMetadataJson,
            SourceArtifactRef = membership.SourceArtifactRef ?? string.Empty,
            DecisionId = (ulong)Math.Max(0, membership.DecisionId),
            HasSourceBrainId = membership.SourceBrainId.HasValue
        };
        if (membership.SourceBrainId.HasValue)
        {
            proto.SourceBrainId = membership.SourceBrainId.Value.ToProtoUuid();
        }

        return proto;
    }

    private static SpeciationRuntimeConfig FromProtoRuntimeConfig(
        ProtoSpec.SpeciationRuntimeConfig? request,
        SpeciationRuntimeConfig fallback)
    {
        if (request is null)
        {
            return fallback;
        }

        return new SpeciationRuntimeConfig(
            PolicyVersion: NormalizeOrFallback(request.PolicyVersion, fallback.PolicyVersion),
            ConfigSnapshotJson: NormalizeJsonOrFallback(request.ConfigSnapshotJson, fallback.ConfigSnapshotJson),
            DefaultSpeciesId: NormalizeOrFallback(request.DefaultSpeciesId, fallback.DefaultSpeciesId),
            DefaultSpeciesDisplayName: NormalizeOrFallback(request.DefaultSpeciesDisplayName, fallback.DefaultSpeciesDisplayName),
            StartupReconcileDecisionReason: NormalizeOrFallback(request.StartupReconcileDecisionReason, fallback.StartupReconcileDecisionReason));
    }

    private readonly record struct SpeciationAssignmentPolicy(
        double LineageMatchThreshold,
        double LineageSplitThreshold,
        double ParentConsensusThreshold,
        double HysteresisMargin,
        double LineageSplitGuardMargin,
        int MinParentMembershipsBeforeSplit,
        int RecentSplitRealignParentMembershipWindow,
        double RecentSplitRealignMatchMargin,
        int HindsightReassignCommitWindow,
        double HindsightReassignSimilarityMargin,
        bool CreateDerivedSpeciesOnDivergence,
        string DerivedSpeciesPrefix);

    private readonly record struct SimilarityEvidence(
        double? SimilarityScore,
        double? DominantSpeciesSimilarityScore,
        double? ParentASimilarityScore,
        double? ParentBSimilarityScore,
        double? FunctionScore,
        double? ConnectivityScore,
        double? RegionSpanScore);

    private readonly record struct LineageEvidence(
        IReadOnlyList<Guid> ParentBrainIds,
        IReadOnlyList<string> ParentArtifactRefs,
        int ParentMembershipCount,
        string? DominantSpeciesId,
        string? DominantSpeciesDisplayName,
        double DominantShare,
        string LineageKey,
        string? HysteresisSpeciesId,
        string? HysteresisSpeciesDisplayName,
        string? HysteresisDecisionReason);

    private readonly record struct AssignmentResolution(
        string SpeciesId,
        string SpeciesDisplayName,
        string DecisionReason,
        string Strategy,
        string StrategyDetail,
        bool ForceDecisionReason,
        double PolicyEffectiveSplitThreshold = 0d,
        double EffectiveSplitThreshold = 0d,
        bool SplitTriggeredBySpeciesFloor = false,
        string? SourceSpeciesId = null,
        string? SourceSpeciesDisplayName = null,
        double? SourceSpeciesSimilarityScore = null,
        double SourceConsensusShare = 0d,
        double? SpeciesFloorSimilarityScore = null,
        int SpeciesFloorSampleCount = 0,
        int SpeciesFloorMembershipCount = 0,
        string? RecentDerivedSourceSpeciesId = null,
        string? RecentDerivedSourceSpeciesDisplayName = null,
        double? RecentDerivedFounderSimilarityScore = null,
        string? DisplayNameRewriteSpeciesId = null,
        string? DisplayNameRewriteSpeciesDisplayName = null);

    private readonly record struct FounderRootSpeciesNamingPlan(
        string FounderSpeciesDisplayName,
        string? SourceSpeciesDisplayNameRewrite,
        string? SourceSpeciesIdToRewrite);

    private readonly record struct SpeciesSimilarityFloorState(
        int MembershipCount,
        int SimilaritySampleCount,
        int ActualSimilaritySampleCount,
        double? MinSimilarityScore);

    private readonly record struct SplitThresholdState(
        double PolicyEffectiveSplitThreshold,
        double DynamicSplitThreshold,
        bool UsesSpeciesFloor,
        double? SpeciesFloorSimilarityScore,
        int SpeciesFloorSampleCount,
        int SpeciesFloorMembershipCount);

    private readonly record struct ParentSpeciesPairwiseFit(
        string SpeciesId,
        string SpeciesDisplayName,
        double PairwiseSimilarity,
        int SupportingParentCount,
        long LatestAssignedMs);

    private readonly record struct RecentDerivedSpeciesHint(
        string SourceSpeciesId,
        string SourceSpeciesDisplayName,
        string TargetSpeciesId,
        string TargetSpeciesDisplayName,
        double FounderSimilarityScore,
        long AssignedMs);

    private readonly record struct BootstrapAssignedSpeciesAdmissionRequirement(
        string TargetSpeciesId,
        string TargetSpeciesDisplayName,
        string SourceSpeciesId,
        string SourceSpeciesDisplayName,
        int MembershipCount,
        int ActualSimilaritySampleCount);

    private readonly record struct AssignedSpeciesAdmissionAssessment(
        bool AssessmentAttempted,
        bool Admitted,
        double? SimilarityScore,
        string AssessmentMode,
        string[] ExemplarBrainIds,
        bool Compatible,
        string AbortReason,
        string FailureReason,
        long ElapsedMs);

    private readonly record struct CompatibilitySimilarityAssessment(
        bool RequestAttempted,
        double? SimilarityScore,
        bool Compatible,
        string AbortReason,
        string FailureReason,
        string AssessmentMode);

    private enum CompatibilitySubjectKind
    {
        None = 0,
        BrainId = 1,
        ArtifactRef = 2
    }

    private readonly record struct CompatibilitySubject(
        CompatibilitySubjectKind Kind,
        Guid BrainId,
        ArtifactRef? ArtifactDefRef,
        ArtifactRef? ArtifactStateRef);

    private readonly record struct ResolvedCandidate(
        ProtoSpec.SpeciationCandidateMode CandidateMode,
        Guid BrainId,
        string? SourceArtifactRef,
        ArtifactRef? CandidateArtifactRef,
        string? CandidateArtifactUri,
        ArtifactRef? CandidateBrainBaseArtifactRef = null,
        ArtifactRef? CandidateBrainSnapshotArtifactRef = null);

    private readonly record struct BrainArtifactProvenance(
        ArtifactRef? BaseArtifactRef,
        ArtifactRef? SnapshotArtifactRef);

    private bool TryGetCurrentEpoch(out SpeciationEpochInfo epoch)
    {
        if (_initialized && _currentEpoch is not null)
        {
            epoch = _currentEpoch;
            return true;
        }

        epoch = CreateFallbackEpoch();
        return false;
    }

    private SpeciationEpochInfo CreateFallbackEpoch()
    {
        return new SpeciationEpochInfo(
            EpochId: 0,
            CreatedMs: 0,
            PolicyVersion: _runtimeConfig.PolicyVersion,
            ConfigSnapshotJson: _runtimeConfig.ConfigSnapshotJson);
    }

    private SpeciationRuntimeConfig BuildResetRuntimeConfig(SpeciationResetEpochRequest request)
    {
        return new SpeciationRuntimeConfig(
            PolicyVersion: NormalizeOrFallback(request.PolicyVersion, _runtimeConfig.PolicyVersion),
            ConfigSnapshotJson: NormalizeJsonOrFallback(request.ConfigSnapshotJson, _runtimeConfig.ConfigSnapshotJson),
            DefaultSpeciesId: _runtimeConfig.DefaultSpeciesId,
            DefaultSpeciesDisplayName: _runtimeConfig.DefaultSpeciesDisplayName,
            StartupReconcileDecisionReason: _runtimeConfig.StartupReconcileDecisionReason);
    }

    private SpeciationRuntimeConfig BuildReconcileRuntimeConfig(SpeciationReconcileKnownBrainsRequest request)
    {
        return new SpeciationRuntimeConfig(
            PolicyVersion: NormalizeOrFallback(request.PolicyVersion, _runtimeConfig.PolicyVersion),
            ConfigSnapshotJson: _runtimeConfig.ConfigSnapshotJson,
            DefaultSpeciesId: NormalizeOrFallback(request.SpeciesId, _runtimeConfig.DefaultSpeciesId),
            DefaultSpeciesDisplayName: NormalizeOrFallback(request.SpeciesDisplayName, _runtimeConfig.DefaultSpeciesDisplayName),
            StartupReconcileDecisionReason: NormalizeOrFallback(request.DecisionReason, _runtimeConfig.StartupReconcileDecisionReason));
    }

    private static string NormalizeOrFallback(string? value, string fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        return value.Trim();
    }

    private static string NormalizeJsonOrFallback(string? value, string fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        var trimmed = value.Trim();
        return trimmed.Length == 0 ? "{}" : trimmed;
    }

    private static void LogError(string message)
    {
        Console.WriteLine($"[SpeciationManager] {message}");
    }
}
