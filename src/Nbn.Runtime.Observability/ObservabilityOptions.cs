namespace Nbn.Runtime.Observability;

public sealed record ObservabilityOptions(
    string BindHost,
    int Port,
    string? AdvertisedHost,
    int? AdvertisedPort,
    bool EnableDebugHub,
    bool EnableVizHub,
    bool EnableOpenTelemetry,
    bool EnableOtelMetrics,
    bool EnableOtelTraces,
    bool EnableOtelConsoleExporter,
    string? OtlpEndpoint,
    string ServiceName)
{
    public static ObservabilityOptions FromArgs(string[] args)
    {
        var bindHost = GetEnv("NBN_OBS_BIND_HOST") ?? "127.0.0.1";
        var port = GetEnvInt("NBN_OBS_PORT") ?? 12040;
        var advertisedHost = GetEnv("NBN_OBS_ADVERTISE_HOST");
        var advertisedPort = GetEnvInt("NBN_OBS_ADVERTISE_PORT");

        var enableDebug = GetEnvBool("NBN_OBS_DEBUG_ENABLED") ?? true;
        var enableViz = GetEnvBool("NBN_OBS_VIZ_ENABLED") ?? true;
        var enableOtel = GetEnvBool("NBN_OBS_OTEL_ENABLED") ?? false;
        var enableOtelMetrics = GetEnvBool("NBN_OBS_OTEL_METRICS_ENABLED");
        var enableOtelTraces = GetEnvBool("NBN_OBS_OTEL_TRACES_ENABLED");
        var enableOtelConsole = GetEnvBool("NBN_OBS_OTEL_CONSOLE") ?? false;

        var otlpEndpoint = GetEnv("NBN_OBS_OTEL_ENDPOINT") ?? GetEnv("OTEL_EXPORTER_OTLP_ENDPOINT");
        var serviceName = GetEnv("NBN_OBS_OTEL_SERVICE_NAME") ?? GetEnv("OTEL_SERVICE_NAME") ?? "nbn.observability";

        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            if (arg.Equals("--help", StringComparison.OrdinalIgnoreCase) || arg.Equals("-h", StringComparison.OrdinalIgnoreCase))
            {
                PrintHelp();
                Environment.Exit(0);
            }

            switch (arg)
            {
                case "--bind":
                case "--bind-host":
                    if (i + 1 < args.Length)
                    {
                        bindHost = args[++i];
                    }
                    continue;
                case "--port":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var portValue))
                    {
                        port = portValue;
                    }
                    continue;
                case "--advertise":
                case "--advertise-host":
                    if (i + 1 < args.Length)
                    {
                        advertisedHost = args[++i];
                    }
                    continue;
                case "--advertise-port":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var advertisedPortValue))
                    {
                        advertisedPort = advertisedPortValue;
                    }
                    continue;
                case "--enable-debug":
                case "--debug":
                    enableDebug = true;
                    continue;
                case "--disable-debug":
                case "--no-debug":
                    enableDebug = false;
                    continue;
                case "--enable-viz":
                case "--viz":
                    enableViz = true;
                    continue;
                case "--disable-viz":
                case "--no-viz":
                    enableViz = false;
                    continue;
                case "--enable-otel":
                case "--otel":
                    enableOtel = true;
                    continue;
                case "--disable-otel":
                case "--no-otel":
                    enableOtel = false;
                    continue;
                case "--otel-metrics":
                    enableOtelMetrics = true;
                    continue;
                case "--otel-traces":
                    enableOtelTraces = true;
                    continue;
                case "--otel-console":
                    enableOtelConsole = true;
                    continue;
                case "--otel-endpoint":
                    if (i + 1 < args.Length)
                    {
                        otlpEndpoint = args[++i];
                    }
                    continue;
                case "--otel-service-name":
                    if (i + 1 < args.Length)
                    {
                        serviceName = args[++i];
                    }
                    continue;
            }

            if (arg.StartsWith("--bind=", StringComparison.OrdinalIgnoreCase))
            {
                bindHost = arg.Substring("--bind=".Length);
                continue;
            }

            if (arg.StartsWith("--bind-host=", StringComparison.OrdinalIgnoreCase))
            {
                bindHost = arg.Substring("--bind-host=".Length);
                continue;
            }

            if (arg.StartsWith("--port=", StringComparison.OrdinalIgnoreCase) && int.TryParse(arg.Substring("--port=".Length), out var portValueInline))
            {
                port = portValueInline;
                continue;
            }

            if (arg.StartsWith("--advertise=", StringComparison.OrdinalIgnoreCase))
            {
                advertisedHost = arg.Substring("--advertise=".Length);
                continue;
            }

            if (arg.StartsWith("--advertise-host=", StringComparison.OrdinalIgnoreCase))
            {
                advertisedHost = arg.Substring("--advertise-host=".Length);
                continue;
            }

            if (arg.StartsWith("--advertise-port=", StringComparison.OrdinalIgnoreCase) && int.TryParse(arg.Substring("--advertise-port=".Length), out var advertisedPortInline))
            {
                advertisedPort = advertisedPortInline;
                continue;
            }

            if (arg.StartsWith("--otel-endpoint=", StringComparison.OrdinalIgnoreCase))
            {
                otlpEndpoint = arg.Substring("--otel-endpoint=".Length);
                continue;
            }

            if (arg.StartsWith("--otel-service-name=", StringComparison.OrdinalIgnoreCase))
            {
                serviceName = arg.Substring("--otel-service-name=".Length);
            }
        }

        if (enableOtelMetrics == true || enableOtelTraces == true)
        {
            enableOtel = true;
        }

        enableOtelMetrics ??= enableOtel;
        enableOtelTraces ??= enableOtel;

        return new ObservabilityOptions(
            bindHost,
            port,
            advertisedHost,
            advertisedPort,
            enableDebug,
            enableViz,
            enableOtel,
            enableOtelMetrics.Value,
            enableOtelTraces.Value,
            enableOtelConsole,
            otlpEndpoint,
            serviceName);
    }

    private static string? GetEnv(string key) => Environment.GetEnvironmentVariable(key);

    private static int? GetEnvInt(string key)
    {
        var value = GetEnv(key);
        return int.TryParse(value, out var parsed) ? parsed : null;
    }

    private static bool? GetEnvBool(string key)
    {
        var value = GetEnv(key);
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        if (bool.TryParse(value, out var parsed))
        {
            return parsed;
        }

        return value.Equals("1", StringComparison.OrdinalIgnoreCase)
            || value.Equals("yes", StringComparison.OrdinalIgnoreCase)
            || value.Equals("y", StringComparison.OrdinalIgnoreCase);
    }

    private static void PrintHelp()
    {
        Console.WriteLine("NBN Observability options:");
        Console.WriteLine("  --bind, --bind-host <host>       Host/interface to bind (default 127.0.0.1)");
        Console.WriteLine("  --port <port>                    Port to bind (default 12040)");
        Console.WriteLine("  --advertise, --advertise-host    Advertised host for remoting");
        Console.WriteLine("  --advertise-port <port>          Advertised port for remoting");
        Console.WriteLine("  --enable-debug | --disable-debug Toggle DebugHub (default on)");
        Console.WriteLine("  --enable-viz | --disable-viz     Toggle VizHub (default on)");
        Console.WriteLine("  --enable-otel | --disable-otel   Toggle OpenTelemetry (default off)");
        Console.WriteLine("  --otel-metrics                   Enable OTel metrics");
        Console.WriteLine("  --otel-traces                    Enable OTel traces");
        Console.WriteLine("  --otel-console                   Enable OTel console exporter");
        Console.WriteLine("  --otel-endpoint <uri>            OTLP endpoint (env OTEL_EXPORTER_OTLP_ENDPOINT)");
        Console.WriteLine("  --otel-service-name <name>       Service name (env OTEL_SERVICE_NAME)");
    }
}
