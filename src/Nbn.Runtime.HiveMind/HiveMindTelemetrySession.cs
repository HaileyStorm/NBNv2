using System.Reflection;
using Nbn.Runtime.Brain;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Nbn.Runtime.HiveMind;

/// <summary>
/// Owns the optional OpenTelemetry providers configured for a HiveMind process lifetime.
/// </summary>
public sealed class HiveMindTelemetrySession : IDisposable
{
    private readonly TracerProvider? _tracerProvider;
    private readonly MeterProvider? _meterProvider;

    private HiveMindTelemetrySession(TracerProvider? tracerProvider, MeterProvider? meterProvider)
    {
        _tracerProvider = tracerProvider;
        _meterProvider = meterProvider;
    }

    /// <summary>
    /// Starts the telemetry providers enabled by the supplied HiveMind options.
    /// </summary>
    /// <param name="options">The resolved HiveMind runtime options.</param>
    /// <returns>A disposable session that owns the configured providers.</returns>
    public static HiveMindTelemetrySession Start(HiveMindOptions options)
    {
        if (!options.EnableOpenTelemetry)
        {
            return new HiveMindTelemetrySession(null, null);
        }

        var version = Assembly.GetExecutingAssembly().GetName().Version?.ToString();
        var resource = ResourceBuilder.CreateDefault()
            .AddService(options.ServiceName, serviceVersion: version);

        TracerProvider? tracerProvider = null;
        if (options.EnableOtelTraces)
        {
            var tracerBuilder = Sdk.CreateTracerProviderBuilder()
                .SetResourceBuilder(resource)
                .AddSource(HiveMindTelemetry.ActivitySource.Name);

            ConfigureExporters(tracerBuilder, options);
            tracerProvider = tracerBuilder.Build();
        }

        MeterProvider? meterProvider = null;
        if (options.EnableOtelMetrics)
        {
            var meterBuilder = Sdk.CreateMeterProviderBuilder()
                .SetResourceBuilder(resource)
                .AddMeter(HiveMindTelemetry.MeterNameValue)
                .AddMeter(BrainTelemetry.MeterNameValue);

            ConfigureExporters(meterBuilder, options);
            meterProvider = meterBuilder.Build();
        }

        return new HiveMindTelemetrySession(tracerProvider, meterProvider);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _meterProvider?.Dispose();
        _tracerProvider?.Dispose();
    }

    private static void ConfigureExporters(TracerProviderBuilder builder, HiveMindOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.OtlpEndpoint))
        {
            builder.AddOtlpExporter(otlp =>
            {
                otlp.Endpoint = new Uri(options.OtlpEndpoint);
            });
        }

        if (options.EnableOtelConsoleExporter)
        {
            builder.AddConsoleExporter();
        }
    }

    private static void ConfigureExporters(MeterProviderBuilder builder, HiveMindOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.OtlpEndpoint))
        {
            builder.AddOtlpExporter(otlp =>
            {
                otlp.Endpoint = new Uri(options.OtlpEndpoint);
            });
        }

        if (options.EnableOtelConsoleExporter)
        {
            builder.AddConsoleExporter();
        }
    }
}
