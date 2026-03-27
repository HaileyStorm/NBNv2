using System.Globalization;
using Nbn.Shared;

namespace Nbn.Runtime.SettingsMonitor;

/// <summary>
/// Defines canonical SettingsMonitor default keys and seed values.
/// </summary>
public static class SettingsMonitorDefaults
{
    /// <summary>
    /// Settings key for artifact chunk compression kind.
    /// </summary>
    public const string ArtifactChunkCompressionKindKey = "artifact.chunk.compression.kind";

    /// <summary>
    /// Settings key for artifact chunk compression level.
    /// </summary>
    public const string ArtifactChunkCompressionLevelKey = "artifact.chunk.compression.level";

    /// <summary>
    /// Settings key for the minimum chunk size eligible for compression.
    /// </summary>
    public const string ArtifactChunkCompressionMinBytesKey = "artifact.chunk.compression.min_bytes";

    /// <summary>
    /// Settings key controlling whether compressed chunks must be smaller than the original payload.
    /// </summary>
    public const string ArtifactChunkCompressionOnlyIfSmallerKey = "artifact.chunk.compression.only_if_smaller";

    /// <summary>
    /// Canonical fallback artifact compression settings used when the store is empty or invalid.
    /// </summary>
    public static readonly ArtifactCompressionSettings ArtifactCompressionDefaults =
        new("zstd", 3, 64 * 1024, true);

    /// <summary>
    /// Canonical default setting rows seeded into a fresh SettingsMonitor store.
    /// </summary>
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
            { WorkerCapabilitySettingsKeys.BenchmarkRefreshSecondsKey, WorkerCapabilitySettingsKeys.DefaultBenchmarkRefreshSeconds.ToString(CultureInfo.InvariantCulture) },
            { WorkerCapabilitySettingsKeys.PressureRebalanceWindowKey, WorkerCapabilitySettingsKeys.DefaultPressureRebalanceWindow.ToString(CultureInfo.InvariantCulture) },
            { WorkerCapabilitySettingsKeys.PressureViolationRatioKey, WorkerCapabilityMath.FormatRatio(WorkerCapabilitySettingsKeys.DefaultPressureViolationRatio) },
            { WorkerCapabilitySettingsKeys.PressureLimitTolerancePercentKey, WorkerCapabilityMath.FormatRatio(WorkerCapabilitySettingsKeys.DefaultPressureLimitTolerancePercent) },
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
