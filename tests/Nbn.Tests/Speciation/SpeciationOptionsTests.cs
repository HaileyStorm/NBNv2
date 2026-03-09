using Nbn.Runtime.Speciation;

namespace Nbn.Tests.Speciation;

public sealed class SpeciationOptionsTests
{
    [Fact]
    public void FromArgs_UsesLocalAppDataDefaultDatabasePath_WhenDbNotSpecified()
    {
        var options = SpeciationOptions.FromArgs(Array.Empty<string>());

        var expected = SpeciationOptions.GetDefaultDatabasePath();
        Assert.Equal(expected, options.DatabasePath);
    }

    [Fact]
    public void FromArgs_UsesExplicitDatabasePath_WhenDbArgumentProvided()
    {
        var explicitPath = Path.Combine(Path.GetTempPath(), "nbn-tests", Guid.NewGuid().ToString("N"), "speciation.db");
        var options = SpeciationOptions.FromArgs(new[] { "--db", explicitPath });

        Assert.Equal(explicitPath, options.DatabasePath);
    }

    [Fact]
    public void ToRuntimeConfig_UsesSettingsBackedFallbackDefaults()
    {
        var options = SpeciationOptions.FromArgs(Array.Empty<string>());

        var runtimeConfig = options.ToRuntimeConfig();
        Assert.Equal(SpeciationOptions.DefaultPolicyVersion, runtimeConfig.PolicyVersion);
        Assert.Equal(SpeciationOptions.DefaultSpeciesId, runtimeConfig.DefaultSpeciesId);
        Assert.Equal(SpeciationOptions.DefaultSpeciesDisplayName, runtimeConfig.DefaultSpeciesDisplayName);
        Assert.Equal(SpeciationOptions.DefaultStartupReconcileDecisionReason, runtimeConfig.StartupReconcileDecisionReason);
        Assert.Equal(SpeciationOptions.DefaultConfigSnapshotJson, runtimeConfig.ConfigSnapshotJson);
    }

    [Fact]
    public void FromArgs_ParsesOpenTelemetryOptions()
    {
        var options = SpeciationOptions.FromArgs(
        [
            "--enable-otel",
            "--otel-metrics",
            "--otel-traces",
            "--otel-console",
            "--otel-endpoint", "http://127.0.0.1:4317",
            "--otel-service-name", "nbn.speciation.tests"
        ]);

        Assert.True(options.EnableOpenTelemetry);
        Assert.True(options.EnableOtelMetrics);
        Assert.True(options.EnableOtelTraces);
        Assert.True(options.EnableOtelConsoleExporter);
        Assert.Equal("http://127.0.0.1:4317", options.OtlpEndpoint);
        Assert.Equal("nbn.speciation.tests", options.OtelServiceName);
    }
}
