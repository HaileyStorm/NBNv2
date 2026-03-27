using System.Reflection;
using Nbn.Runtime.Brain;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;

namespace Nbn.Runtime.BrainHost;

/// <summary>
/// Owns Brain meter-export resources for the BrainHost process lifetime.
/// </summary>
public sealed class BrainTelemetrySession : IDisposable
{
    private readonly MeterProvider? _meterProvider;

    private BrainTelemetrySession(MeterProvider? meterProvider)
    {
        _meterProvider = meterProvider;
    }

    /// <summary>
    /// Starts BrainHost telemetry exporters when Brain metrics are enabled.
    /// </summary>
    /// <param name="enableTelemetry">Whether OpenTelemetry support is enabled overall.</param>
    /// <param name="enableMetrics">Whether Brain metrics export is enabled.</param>
    /// <param name="enableConsoleExporter">Whether to add a console exporter.</param>
    /// <param name="otlpEndpoint">Optional OTLP endpoint for metric export.</param>
    /// <param name="serviceName">Service name reported to OpenTelemetry resources.</param>
    /// <returns>A session that owns any created exporter resources.</returns>
    public static BrainTelemetrySession Start(bool enableTelemetry, bool enableMetrics, bool enableConsoleExporter, string? otlpEndpoint, string serviceName)
    {
        if (!enableTelemetry || !enableMetrics)
        {
            return new BrainTelemetrySession(null);
        }

        var version = Assembly.GetExecutingAssembly().GetName().Version?.ToString();
        var resource = ResourceBuilder.CreateDefault()
            .AddService(serviceName, serviceVersion: version);

        var meterBuilder = Sdk.CreateMeterProviderBuilder()
            .SetResourceBuilder(resource)
            .AddMeter(BrainTelemetry.MeterNameValue);

        ConfigureExporters(meterBuilder, enableConsoleExporter, otlpEndpoint);
        return new BrainTelemetrySession(meterBuilder.Build());
    }

    /// <summary>
    /// Disposes any active telemetry exporters owned by the session.
    /// </summary>
    public void Dispose()
    {
        _meterProvider?.Dispose();
    }

    private static void ConfigureExporters(MeterProviderBuilder meterBuilder, bool enableConsoleExporter, string? otlpEndpoint)
    {
        if (!string.IsNullOrWhiteSpace(otlpEndpoint))
        {
            meterBuilder.AddOtlpExporter(otlp =>
            {
                otlp.Endpoint = new Uri(otlpEndpoint);
            });
        }

        if (enableConsoleExporter)
        {
            meterBuilder.AddConsoleExporter();
        }
    }
}
