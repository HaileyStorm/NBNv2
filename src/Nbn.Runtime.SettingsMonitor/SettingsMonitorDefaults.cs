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
        new("none", 3, 64 * 1024, true);

    public static readonly IReadOnlyDictionary<string, string> DefaultSettings =
        new Dictionary<string, string>(StringComparer.Ordinal)
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
            { PlasticitySettingsKeys.SystemProbabilisticUpdatesKey, "true" }
        };
}
