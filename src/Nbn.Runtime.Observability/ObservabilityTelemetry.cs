using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Reflection;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Nbn.Runtime.Observability;

/// <summary>
/// Owns OpenTelemetry activity and meter providers for the observability service.
/// </summary>
public sealed class ObservabilityTelemetry : IDisposable
{
    /// <summary>
    /// Gets the activity source used for debug and visualization publish spans.
    /// </summary>
    public static readonly ActivitySource ActivitySource = new("Nbn.Runtime.Observability");

    /// <summary>
    /// Gets the meter that backs observability counters and gauges.
    /// </summary>
    public static readonly Meter Meter = new("Nbn.Runtime.Observability");

    /// <summary>
    /// Gets the named metrics emitted by the observability service.
    /// </summary>
    public static readonly ObservabilityMetrics Metrics = new(Meter);

    private readonly TracerProvider? _tracerProvider;
    private readonly MeterProvider? _meterProvider;

    private ObservabilityTelemetry(TracerProvider? tracerProvider, MeterProvider? meterProvider)
    {
        _tracerProvider = tracerProvider;
        _meterProvider = meterProvider;
    }

    /// <summary>
    /// Starts observability tracing and metrics exporters from the supplied options.
    /// </summary>
    /// <param name="options">Telemetry configuration for the observability service.</param>
    /// <returns>A session that owns any created tracer and meter providers.</returns>
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

    /// <summary>
    /// Disposes any active tracer and meter providers owned by the session.
    /// </summary>
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

/// <summary>
/// Named counters and gauges emitted by the observability service.
/// </summary>
public sealed class ObservabilityMetrics
{
    /// <summary>
    /// Gets the total number of inbound debug events observed by the service.
    /// </summary>
    public Counter<long> DebugInboundTotal { get; }

    /// <summary>
    /// Gets the total number of debug deliveries sent to subscribers.
    /// </summary>
    public Counter<long> DebugDeliveredTotal { get; }

    /// <summary>
    /// Gets the current number of active debug subscribers.
    /// </summary>
    public UpDownCounter<long> DebugSubscribers { get; }

    /// <summary>
    /// Gets the total number of visualization events observed by the service.
    /// </summary>
    public Counter<long> VizEventTotal { get; }

    /// <summary>
    /// Gets the total number of visualization deliveries sent to subscribers.
    /// </summary>
    public Counter<long> VizDeliveredTotal { get; }

    /// <summary>
    /// Gets the current number of active visualization subscribers.
    /// </summary>
    public UpDownCounter<long> VizSubscribers { get; }

    /// <summary>
    /// Creates the observability metrics bound to the supplied meter.
    /// </summary>
    /// <param name="meter">Meter used to create the counters and gauges.</param>
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
