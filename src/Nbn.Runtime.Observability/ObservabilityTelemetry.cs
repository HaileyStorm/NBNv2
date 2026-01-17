using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Reflection;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Nbn.Runtime.Observability;

public sealed class ObservabilityTelemetry : IDisposable
{
    public static readonly ActivitySource ActivitySource = new("Nbn.Runtime.Observability");
    public static readonly Meter Meter = new("Nbn.Runtime.Observability");
    public static readonly ObservabilityMetrics Metrics = new(Meter);

    private readonly TracerProvider? _tracerProvider;
    private readonly MeterProvider? _meterProvider;

    private ObservabilityTelemetry(TracerProvider? tracerProvider, MeterProvider? meterProvider)
    {
        _tracerProvider = tracerProvider;
        _meterProvider = meterProvider;
    }

    public static ObservabilityTelemetry Start(ObservabilityOptions options)
    {
        if (!options.EnableOpenTelemetry)
        {
            return new ObservabilityTelemetry(null, null);
        }

        var version = Assembly.GetExecutingAssembly().GetName().Version?.ToString();
        var resource = ResourceBuilder.CreateDefault()
            .AddService(options.ServiceName, serviceVersion: version);

        TracerProvider? tracerProvider = null;
        if (options.EnableOtelTraces)
        {
            var tracerBuilder = Sdk.CreateTracerProviderBuilder()
                .SetResourceBuilder(resource)
                .AddSource(ActivitySource.Name);

            ConfigureExporters(tracerBuilder, options);
            tracerProvider = tracerBuilder.Build();
        }

        MeterProvider? meterProvider = null;
        if (options.EnableOtelMetrics)
        {
            var meterBuilder = Sdk.CreateMeterProviderBuilder()
                .SetResourceBuilder(resource)
                .AddMeter(Meter.Name);

            ConfigureExporters(meterBuilder, options);
            meterProvider = meterBuilder.Build();
        }

        return new ObservabilityTelemetry(tracerProvider, meterProvider);
    }

    public void Dispose()
    {
        _meterProvider?.Dispose();
        _tracerProvider?.Dispose();
    }

    private static void ConfigureExporters(TracerProviderBuilder builder, ObservabilityOptions options)
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

    private static void ConfigureExporters(MeterProviderBuilder builder, ObservabilityOptions options)
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

public sealed class ObservabilityMetrics
{
    public Counter<long> DebugInboundTotal { get; }
    public Counter<long> DebugDeliveredTotal { get; }
    public UpDownCounter<long> DebugSubscribers { get; }
    public Counter<long> VizEventTotal { get; }
    public Counter<long> VizDeliveredTotal { get; }
    public UpDownCounter<long> VizSubscribers { get; }

    public ObservabilityMetrics(Meter meter)
    {
        DebugInboundTotal = meter.CreateCounter<long>("nbn.debug.inbound");
        DebugDeliveredTotal = meter.CreateCounter<long>("nbn.debug.delivered");
        DebugSubscribers = meter.CreateUpDownCounter<long>("nbn.debug.subscribers");

        VizEventTotal = meter.CreateCounter<long>("nbn.viz.events");
        VizDeliveredTotal = meter.CreateCounter<long>("nbn.viz.delivered");
        VizSubscribers = meter.CreateUpDownCounter<long>("nbn.viz.subscribers");
    }
}
