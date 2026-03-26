using System.Globalization;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private static bool ParseDebugEnabledSetting(string? value, bool fallback)
        => ParseBooleanSetting(value, fallback);

    private static ProtoControl.InputCoordinatorMode ParseInputCoordinatorModeSetting(
        string? value,
        ProtoControl.InputCoordinatorMode fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        var normalized = value.Trim().ToLowerInvariant().Replace('-', '_');
        return normalized switch
        {
            "0" or "dirty" or "dirty_on_change" => ProtoControl.InputCoordinatorMode.DirtyOnChange,
            "1" or "replay" or "replay_latest_vector" => ProtoControl.InputCoordinatorMode.ReplayLatestVector,
            _ => fallback
        };
    }

    private static ProtoControl.OutputVectorSource ParseOutputVectorSourceSetting(
        string? value,
        ProtoControl.OutputVectorSource fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        var normalized = value.Trim().ToLowerInvariant().Replace('-', '_');
        return normalized switch
        {
            "0" or "potential" => ProtoControl.OutputVectorSource.Potential,
            "1" or "buffer" => ProtoControl.OutputVectorSource.Buffer,
            _ => fallback
        };
    }

    private static bool TryParseTickRateOverrideSetting(string? value, out float? overrideHz)
    {
        overrideHz = null;
        if (string.IsNullOrWhiteSpace(value))
        {
            return true;
        }

        var trimmed = value.Trim();
        var normalized = trimmed.ToLowerInvariant();
        if (normalized is "0" or "off" or "none" or "clear" or "default")
        {
            return true;
        }

        if (normalized.EndsWith("ms", StringComparison.Ordinal))
        {
            var numeric = trimmed[..^2].Trim();
            if (!float.TryParse(numeric, NumberStyles.Float, CultureInfo.InvariantCulture, out var ms)
                || !float.IsFinite(ms)
                || ms <= 0f)
            {
                return false;
            }

            overrideHz = 1000f / ms;
            return float.IsFinite(overrideHz.Value) && overrideHz.Value > 0f;
        }

        if (normalized.EndsWith("hz", StringComparison.Ordinal))
        {
            trimmed = trimmed[..^2].Trim();
        }

        if (!float.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out var hz)
            || !float.IsFinite(hz)
            || hz <= 0f)
        {
            return false;
        }

        overrideHz = hz;
        return true;
    }

    private static uint ParseVisualizationMinIntervalSetting(string? value, uint fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !uint.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return fallback;
        }

        return Math.Min(parsed, 60_000u);
    }

    private static uint ComputeVizStride(float targetTickHz, uint minIntervalMs)
    {
        if (minIntervalMs == 0u || !float.IsFinite(targetTickHz) || targetTickHz <= 0f)
        {
            return 1u;
        }

        var tickMs = 1000f / targetTickHz;
        if (!float.IsFinite(tickMs) || tickMs <= 0f || tickMs >= minIntervalMs)
        {
            return 1u;
        }

        var stride = (uint)Math.Ceiling(minIntervalMs / tickMs);
        return Math.Max(1u, stride);
    }

    private static bool ParseBooleanSetting(string? value, bool fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "1" or "true" or "yes" or "on" => true,
            "0" or "false" or "no" or "off" => false,
            _ => fallback
        };
    }

    private static long ParseNonNegativeInt64Setting(string? value, long fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !long.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return fallback;
        }

        return Math.Max(0L, parsed);
    }

    private static int ParseWorkerCapabilityRefreshSeconds(string? value, int fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !int.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return fallback;
        }

        return Math.Max(0, parsed);
    }

    private static int ParseWorkerPressureWindow(string? value, int fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !int.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return fallback;
        }

        return Math.Max(1, parsed);
    }

    private static double ParseWorkerPressureRatio(string? value, double fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !double.TryParse(value.Trim(), NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsed)
            || !double.IsFinite(parsed))
        {
            return fallback;
        }

        return Math.Clamp(parsed, 0d, 1d);
    }

    private static float ParseWorkerPressureTolerance(string? value, float fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !float.TryParse(value.Trim(), NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsed)
            || !float.IsFinite(parsed))
        {
            return fallback;
        }

        return Math.Max(0f, parsed);
    }

    private static float ParsePositiveFiniteFloatSetting(string? value, float fallback)
    {
        if (string.IsNullOrWhiteSpace(value)
            || !float.TryParse(value.Trim(), NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var parsed)
            || !float.IsFinite(parsed)
            || parsed <= 0f)
        {
            return fallback;
        }

        return parsed;
    }

    private static ProtoSeverity ParseDebugSeveritySetting(string? value, ProtoSeverity fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        if (Enum.TryParse<ProtoSeverity>(value, ignoreCase: true, out var direct))
        {
            return NormalizeDebugSeverity(direct);
        }

        var normalized = value.Trim().ToLowerInvariant();
        return normalized switch
        {
            "trace" or "sev_trace" => ProtoSeverity.SevTrace,
            "debug" or "sev_debug" => ProtoSeverity.SevDebug,
            "info" or "sev_info" => ProtoSeverity.SevInfo,
            "warn" or "warning" or "sev_warn" => ProtoSeverity.SevWarn,
            "error" or "sev_error" => ProtoSeverity.SevError,
            "fatal" or "sev_fatal" => ProtoSeverity.SevFatal,
            _ => fallback
        };
    }

    private static ProtoSeverity NormalizeDebugSeverity(ProtoSeverity severity)
    {
        return severity switch
        {
            ProtoSeverity.SevTrace or ProtoSeverity.SevDebug or ProtoSeverity.SevInfo or ProtoSeverity.SevWarn or ProtoSeverity.SevError or ProtoSeverity.SevFatal => severity,
            _ => ProtoSeverity.SevDebug
        };
    }
}
