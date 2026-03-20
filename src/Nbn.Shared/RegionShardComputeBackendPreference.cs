namespace Nbn.Shared;

public enum RegionShardComputeBackendPreference
{
    Auto = 0,
    Cpu = 1,
    Gpu = 2
}

public static class RegionShardComputeBackendPreferenceResolver
{
    public const string EnvironmentVariableName = "NBN_REGIONSHARD_BACKEND";

    public static RegionShardComputeBackendPreference Resolve(
        RegionShardComputeBackendPreference fallback = RegionShardComputeBackendPreference.Auto,
        string? rawValue = null)
    {
        var value = string.IsNullOrWhiteSpace(rawValue)
            ? Environment.GetEnvironmentVariable(EnvironmentVariableName)
            : rawValue;
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "auto" => RegionShardComputeBackendPreference.Auto,
            "cpu" => RegionShardComputeBackendPreference.Cpu,
            "gpu" => RegionShardComputeBackendPreference.Gpu,
            "cuda" => RegionShardComputeBackendPreference.Gpu,
            "opencl" => RegionShardComputeBackendPreference.Gpu,
            _ => fallback
        };
    }

    public static bool IsGpuExecutionEnabled(RegionShardComputeBackendPreference preference)
        => preference != RegionShardComputeBackendPreference.Cpu;
}
