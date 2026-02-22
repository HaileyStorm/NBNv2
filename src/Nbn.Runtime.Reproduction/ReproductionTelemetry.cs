using System.Collections.Generic;
using System.Diagnostics.Metrics;

namespace Nbn.Runtime.Reproduction;

public static class ReproductionTelemetry
{
    private const string MeterName = "Nbn.Runtime.Reproduction";
    private static readonly Meter Meter = new(MeterName);

    private static readonly Counter<long> StrengthOverlayApplied =
        Meter.CreateCounter<long>("nbn.reproduction.strength.overlay.applied");

    private static readonly Counter<long> StrengthOverlayFallback =
        Meter.CreateCounter<long>("nbn.reproduction.strength.overlay.fallback");

    public static void RecordStrengthOverlayApplied(string parentLabel, int count = 1)
    {
        if (count <= 0)
        {
            return;
        }

        StrengthOverlayApplied.Add(
            count,
            new KeyValuePair<string, object?>("parent_label", parentLabel));
    }

    public static void RecordStrengthOverlayFallback(string parentLabel, string reason, int count = 1)
    {
        if (count <= 0)
        {
            return;
        }

        StrengthOverlayFallback.Add(
            count,
            new KeyValuePair<string, object?>("parent_label", parentLabel),
            new KeyValuePair<string, object?>("reason", reason));
    }
}
