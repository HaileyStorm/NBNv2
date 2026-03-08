using System.Globalization;
using Nbn.Shared;

namespace Nbn.Runtime.SettingsMonitor;

public static class SettingsMonitorDefaults
{
    public const string ArtifactChunkCompressionKindKey = "artifact.chunk.compression.kind";
    public const string ArtifactChunkCompressionLevelKey = "artifact.chunk.compression.level";
    public const string ArtifactChunkCompressionMinBytesKey = "artifact.chunk.compression.min_bytes";
    public const string ArtifactChunkCompressionOnlyIfSmallerKey = "artifact.chunk.compression.only_if_smaller";

    public static readonly ArtifactCompressionSettings ArtifactCompressionDefaults =
        new("zstd", 3, 64 * 1024, true);

    public static readonly IReadOnlyDictionary<string, string> DefaultSettings = BuildDefaultSettings();

    private static IReadOnlyDictionary<string, string> BuildDefaultSettings()
    {
        var defaults = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            { ArtifactChunkCompressionKindKey, ArtifactCompressionDefaults.Kind },
            { ArtifactChunkCompressionLevelKey, ArtifactCompressionDefaults.Level.ToString(CultureInfo.InvariantCulture) },
            { ArtifactChunkCompressionMinBytesKey, ArtifactCompressionDefaults.MinBytes.ToString(CultureInfo.InvariantCulture) },
            { ArtifactChunkCompressionOnlyIfSmallerKey, ArtifactCompressionDefaults.OnlyIfSmaller ? "true" : "false" },
            { DebugSettingsKeys.EnabledKey, "false" },
            { DebugSettingsKeys.MinSeverityKey, "SevInfo" },
            { DebugSettingsKeys.ContextRegexKey, string.Empty },
            { DebugSettingsKeys.IncludeContextPrefixesKey, string.Empty },
            { DebugSettingsKeys.ExcludeContextPrefixesKey, string.Empty },
            { DebugSettingsKeys.IncludeSummaryPrefixesKey, string.Empty },
            { DebugSettingsKeys.ExcludeSummaryPrefixesKey, string.Empty },
            { CostEnergySettingsKeys.SystemEnabledKey, "false" },
            { CostEnergySettingsKeys.RemoteCostEnabledKey, "false" },
            { CostEnergySettingsKeys.RemoteCostPerBatchKey, "0" },
            { CostEnergySettingsKeys.RemoteCostPerContributionKey, "0" },
            { CostEnergySettingsKeys.TierAMultiplierKey, "1" },
            { CostEnergySettingsKeys.TierBMultiplierKey, "1" },
            { CostEnergySettingsKeys.TierCMultiplierKey, "1" },
            { PlasticitySettingsKeys.SystemEnabledKey, "true" },
            { PlasticitySettingsKeys.SystemRateKey, "0.001" },
            { PlasticitySettingsKeys.SystemProbabilisticUpdatesKey, "true" },
            { IoCoordinatorSettingsKeys.InputCoordinatorModeKey, "dirty_on_change" },
            { IoCoordinatorSettingsKeys.OutputVectorSourceKey, "potential" },
            { TickSettingsKeys.CadenceHzKey, string.Empty },
            { VisualizationSettingsKeys.TickMinIntervalMsKey, "250" },
            { VisualizationSettingsKeys.StreamMinIntervalMsKey, "250" },
            { SpeciationSettingsKeys.ConfigEnabledKey, "true" },
            { SpeciationSettingsKeys.PolicyVersionKey, "default" },
            { SpeciationSettingsKeys.DefaultSpeciesIdKey, "species.default" },
            { SpeciationSettingsKeys.DefaultSpeciesDisplayNameKey, "Default species" },
            { SpeciationSettingsKeys.StartupReconcileReasonKey, "startup_reconcile" },
            { SpeciationSettingsKeys.LineageMatchThresholdKey, "0.92" },
            { SpeciationSettingsKeys.LineageSplitThresholdKey, "0.88" },
            { SpeciationSettingsKeys.ParentConsensusThresholdKey, "0.70" },
            { SpeciationSettingsKeys.LineageHysteresisMarginKey, "0.04" },
            { SpeciationSettingsKeys.LineageSplitGuardMarginKey, "0.02" },
            { SpeciationSettingsKeys.LineageMinParentMembershipsBeforeSplitKey, "1" },
            { SpeciationSettingsKeys.LineageRealignParentMembershipWindowKey, "3" },
            { SpeciationSettingsKeys.LineageRealignMatchMarginKey, "0.05" },
            { SpeciationSettingsKeys.LineageHindsightReassignCommitWindowKey, "6" },
            { SpeciationSettingsKeys.LineageHindsightSimilarityMarginKey, "0.015" },
            { SpeciationSettingsKeys.CreateDerivedSpeciesOnDivergenceKey, "true" },
            { SpeciationSettingsKeys.DerivedSpeciesPrefixKey, "branch" },
            { SpeciationSettingsKeys.HistoryLimitKey, "100" }
        };

        foreach (var pair in ReproductionSettings.DefaultSettingValues)
        {
            defaults[pair.Key] = pair.Value;
        }

        return defaults;
    }
}
