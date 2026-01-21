using System.Reflection;
using Nbn.Runtime.Brain;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;

namespace Nbn.Runtime.BrainHost;

public sealed class BrainTelemetrySession : IDisposable
{
    private readonly MeterProvider? _meterProvider;

    private BrainTelemetrySession(MeterProvider? meterProvider)
    {
        _meterProvider = meterProvider;
    }

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

        return new BrainTelemetrySession(meterBuilder.Build());
    }

    public void Dispose()
    {
        _meterProvider?.Dispose();
    }
}
