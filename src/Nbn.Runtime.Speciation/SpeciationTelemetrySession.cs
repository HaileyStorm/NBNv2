using System.Reflection;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Nbn.Runtime.Speciation;

public sealed class SpeciationTelemetrySession : IDisposable
{
    private readonly TracerProvider? _tracerProvider;
    private readonly MeterProvider? _meterProvider;

    private SpeciationTelemetrySession(TracerProvider? tracerProvider, MeterProvider? meterProvider)
    {
        _tracerProvider = tracerProvider;
        _meterProvider = meterProvider;
    }

    public static SpeciationTelemetrySession Start(SpeciationOptions options)
    {
        if (!options.EnableOpenTelemetry)
        {
            return new SpeciationTelemetrySession(null, null);
        }

        var version = Assembly.GetExecutingAssembly().GetName().Version?.ToString();
        var resource = ResourceBuilder.CreateDefault()
            .AddService(options.OtelServiceName, serviceVersion: version);

        TracerProvider? tracerProvider = null;
        if (options.EnableOtelTraces)
        {
            var tracerBuilder = Sdk.CreateTracerProviderBuilder()
                .SetResourceBuilder(resource)
                .AddSource(SpeciationTelemetry.ActivitySource.Name);

            ConfigureExporters(tracerBuilder, options);
            tracerProvider = tracerBuilder.Build();
        }

        MeterProvider? meterProvider = null;
        if (options.EnableOtelMetrics)
        {
            var meterBuilder = Sdk.CreateMeterProviderBuilder()
                .SetResourceBuilder(resource)
                .AddMeter(SpeciationTelemetry.MeterNameValue);

            ConfigureExporters(meterBuilder, options);
            meterProvider = meterBuilder.Build();
        }

        return new SpeciationTelemetrySession(tracerProvider, meterProvider);
    }

    public void Dispose()
    {
        _meterProvider?.Dispose();
        _tracerProvider?.Dispose();
    }

    private static void ConfigureExporters(TracerProviderBuilder builder, SpeciationOptions options)
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

    private static void ConfigureExporters(MeterProviderBuilder builder, SpeciationOptions options)
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
