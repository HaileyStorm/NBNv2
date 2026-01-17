namespace Nbn.Runtime.HiveMind;

public sealed record HiveMindOptions(
    string BindHost,
    int Port,
    string? AdvertisedHost,
    int? AdvertisedPort,
    float TargetTickHz,
    float MinTickHz,
    int ComputeTimeoutMs,
    int DeliverTimeoutMs,
    float BackpressureDecay,
    float BackpressureRecovery,
    int LateBackpressureThreshold,
    int TimeoutRescheduleThreshold,
    int TimeoutPauseThreshold,
    int RescheduleMinTicks,
    int RescheduleMinMinutes,
    int RescheduleQuietMs,
    int RescheduleSimulatedMs,
    bool AutoStart,
    bool EnableOpenTelemetry,
    bool EnableOtelMetrics,
    bool EnableOtelTraces,
    bool EnableOtelConsoleExporter,
    string? OtlpEndpoint,
    string ServiceName)
{
    public static HiveMindOptions FromArgs(string[] args)
    {
        var bindHost = GetEnv("NBN_HIVE_BIND_HOST") ?? "127.0.0.1";
        var port = GetEnvInt("NBN_HIVE_PORT") ?? 12020;
        var advertisedHost = GetEnv("NBN_HIVE_ADVERTISE_HOST");
        var advertisedPort = GetEnvInt("NBN_HIVE_ADVERTISE_PORT");

        var targetTickHz = GetEnvFloat("NBN_HIVE_TICK_HZ") ?? 30f;
        var minTickHz = GetEnvFloat("NBN_HIVE_MIN_TICK_HZ") ?? 5f;

        var computeTimeoutMs = GetEnvInt("NBN_HIVE_COMPUTE_TIMEOUT_MS");
        var deliverTimeoutMs = GetEnvInt("NBN_HIVE_DELIVER_TIMEOUT_MS");

        var backpressureDecay = GetEnvFloat("NBN_HIVE_BACKPRESSURE_DECAY") ?? 0.8f;
        var backpressureRecovery = GetEnvFloat("NBN_HIVE_BACKPRESSURE_RECOVERY") ?? 1.05f;
        var lateBackpressureThreshold = GetEnvInt("NBN_HIVE_LATE_BACKPRESSURE_TICKS") ?? 2;

        var rescheduleThreshold = GetEnvInt("NBN_HIVE_RESCHEDULE_TIMEOUTS") ?? 3;
        var pauseThreshold = GetEnvInt("NBN_HIVE_PAUSE_TIMEOUTS") ?? 6;

        var rescheduleMinTicks = GetEnvInt("NBN_HIVE_RESCHEDULE_MIN_TICKS") ?? 10;
        var rescheduleMinMinutes = GetEnvInt("NBN_HIVE_RESCHEDULE_MIN_MINUTES") ?? 1;
        var rescheduleQuietMs = GetEnvInt("NBN_HIVE_RESCHEDULE_QUIET_MS") ?? 250;
        var rescheduleSimulatedMs = GetEnvInt("NBN_HIVE_RESCHEDULE_SIM_MS") ?? 1000;

        var autoStart = GetEnvBool("NBN_HIVE_AUTOSTART") ?? true;

        var enableOtel = GetEnvBool("NBN_HIVE_OTEL_ENABLED") ?? false;
        var enableOtelMetrics = GetEnvBool("NBN_HIVE_OTEL_METRICS_ENABLED");
        var enableOtelTraces = GetEnvBool("NBN_HIVE_OTEL_TRACES_ENABLED");
        var enableOtelConsole = GetEnvBool("NBN_HIVE_OTEL_CONSOLE") ?? false;
        var otlpEndpoint = GetEnv("NBN_HIVE_OTEL_ENDPOINT") ?? GetEnv("OTEL_EXPORTER_OTLP_ENDPOINT");
        var serviceName = GetEnv("NBN_HIVE_OTEL_SERVICE_NAME") ?? GetEnv("OTEL_SERVICE_NAME") ?? "nbn.hivemind";

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
                case "--tick-hz":
                    if (i + 1 < args.Length && float.TryParse(args[++i], out var tickHzValue))
                    {
                        targetTickHz = tickHzValue;
                    }
                    continue;
                case "--min-tick-hz":
                    if (i + 1 < args.Length && float.TryParse(args[++i], out var minTickHzValue))
                    {
                        minTickHz = minTickHzValue;
                    }
                    continue;
                case "--compute-timeout-ms":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var computeTimeoutValue))
                    {
                        computeTimeoutMs = computeTimeoutValue;
                    }
                    continue;
                case "--deliver-timeout-ms":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var deliverTimeoutValue))
                    {
                        deliverTimeoutMs = deliverTimeoutValue;
                    }
                    continue;
                case "--backpressure-decay":
                    if (i + 1 < args.Length && float.TryParse(args[++i], out var decayValue))
                    {
                        backpressureDecay = decayValue;
                    }
                    continue;
                case "--backpressure-recovery":
                    if (i + 1 < args.Length && float.TryParse(args[++i], out var recoveryValue))
                    {
                        backpressureRecovery = recoveryValue;
                    }
                    continue;
                case "--late-backpressure-ticks":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var lateBackpressureValue))
                    {
                        lateBackpressureThreshold = lateBackpressureValue;
                    }
                    continue;
                case "--reschedule-timeouts":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var rescheduleValue))
                    {
                        rescheduleThreshold = rescheduleValue;
                    }
                    continue;
                case "--pause-timeouts":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var pauseValue))
                    {
                        pauseThreshold = pauseValue;
                    }
                    continue;
                case "--reschedule-min-ticks":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var reschedTicksValue))
                    {
                        rescheduleMinTicks = reschedTicksValue;
                    }
                    continue;
                case "--reschedule-min-minutes":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var reschedMinutesValue))
                    {
                        rescheduleMinMinutes = reschedMinutesValue;
                    }
                    continue;
                case "--reschedule-quiet-ms":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var quietValue))
                    {
                        rescheduleQuietMs = quietValue;
                    }
                    continue;
                case "--reschedule-sim-ms":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var simValue))
                    {
                        rescheduleSimulatedMs = simValue;
                    }
                    continue;
                case "--auto-start":
                    autoStart = true;
                    continue;
                case "--no-auto-start":
                    autoStart = false;
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

            if (arg.StartsWith("--tick-hz=", StringComparison.OrdinalIgnoreCase) && float.TryParse(arg.Substring("--tick-hz=".Length), out var tickHzValueInline))
            {
                targetTickHz = tickHzValueInline;
                continue;
            }

            if (arg.StartsWith("--min-tick-hz=", StringComparison.OrdinalIgnoreCase) && float.TryParse(arg.Substring("--min-tick-hz=".Length), out var minTickHzValueInline))
            {
                minTickHz = minTickHzValueInline;
                continue;
            }

            if (arg.StartsWith("--compute-timeout-ms=", StringComparison.OrdinalIgnoreCase) && int.TryParse(arg.Substring("--compute-timeout-ms=".Length), out var computeTimeoutInline))
            {
                computeTimeoutMs = computeTimeoutInline;
                continue;
            }

            if (arg.StartsWith("--deliver-timeout-ms=", StringComparison.OrdinalIgnoreCase) && int.TryParse(arg.Substring("--deliver-timeout-ms=".Length), out var deliverTimeoutInline))
            {
                deliverTimeoutMs = deliverTimeoutInline;
                continue;
            }

            if (arg.StartsWith("--backpressure-decay=", StringComparison.OrdinalIgnoreCase) && float.TryParse(arg.Substring("--backpressure-decay=".Length), out var decayInline))
            {
                backpressureDecay = decayInline;
                continue;
            }

            if (arg.StartsWith("--backpressure-recovery=", StringComparison.OrdinalIgnoreCase) && float.TryParse(arg.Substring("--backpressure-recovery=".Length), out var recoveryInline))
            {
                backpressureRecovery = recoveryInline;
                continue;
            }

            if (arg.StartsWith("--late-backpressure-ticks=", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(arg.Substring("--late-backpressure-ticks=".Length), out var lateBackpressureInline))
            {
                lateBackpressureThreshold = lateBackpressureInline;
                continue;
            }

            if (arg.StartsWith("--reschedule-timeouts=", StringComparison.OrdinalIgnoreCase) && int.TryParse(arg.Substring("--reschedule-timeouts=".Length), out var rescheduleInline))
            {
                rescheduleThreshold = rescheduleInline;
                continue;
            }

            if (arg.StartsWith("--pause-timeouts=", StringComparison.OrdinalIgnoreCase) && int.TryParse(arg.Substring("--pause-timeouts=".Length), out var pauseInline))
            {
                pauseThreshold = pauseInline;
                continue;
            }

            if (arg.StartsWith("--reschedule-min-ticks=", StringComparison.OrdinalIgnoreCase) && int.TryParse(arg.Substring("--reschedule-min-ticks=".Length), out var reschedTicksInline))
            {
                rescheduleMinTicks = reschedTicksInline;
                continue;
            }

            if (arg.StartsWith("--reschedule-min-minutes=", StringComparison.OrdinalIgnoreCase) && int.TryParse(arg.Substring("--reschedule-min-minutes=".Length), out var reschedMinutesInline))
            {
                rescheduleMinMinutes = reschedMinutesInline;
                continue;
            }

            if (arg.StartsWith("--reschedule-quiet-ms=", StringComparison.OrdinalIgnoreCase) && int.TryParse(arg.Substring("--reschedule-quiet-ms=".Length), out var quietInline))
            {
                rescheduleQuietMs = quietInline;
                continue;
            }

            if (arg.StartsWith("--reschedule-sim-ms=", StringComparison.OrdinalIgnoreCase) && int.TryParse(arg.Substring("--reschedule-sim-ms=".Length), out var simInline))
            {
                rescheduleSimulatedMs = simInline;
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

        if (minTickHz <= 0)
        {
            minTickHz = 1f;
        }

        if (targetTickHz < minTickHz)
        {
            targetTickHz = minTickHz;
        }

        if (lateBackpressureThreshold < 1)
        {
            lateBackpressureThreshold = 1;
        }

        computeTimeoutMs ??= (int)Math.Ceiling(1000d / minTickHz);
        deliverTimeoutMs ??= (int)Math.Ceiling(1000d / minTickHz);

        if (enableOtelMetrics == true || enableOtelTraces == true)
        {
            enableOtel = true;
        }

        enableOtelMetrics ??= enableOtel;
        enableOtelTraces ??= enableOtel;

        return new HiveMindOptions(
            bindHost,
            port,
            advertisedHost,
            advertisedPort,
            targetTickHz,
            minTickHz,
            computeTimeoutMs.Value,
            deliverTimeoutMs.Value,
            backpressureDecay,
            backpressureRecovery,
            lateBackpressureThreshold,
            rescheduleThreshold,
            pauseThreshold,
            rescheduleMinTicks,
            rescheduleMinMinutes,
            rescheduleQuietMs,
            rescheduleSimulatedMs,
            autoStart,
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

    private static float? GetEnvFloat(string key)
    {
        var value = GetEnv(key);
        return float.TryParse(value, out var parsed) ? parsed : null;
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
        Console.WriteLine("NBN HiveMind options:");
        Console.WriteLine("  --bind, --bind-host <host>          Host/interface to bind (default 127.0.0.1)");
        Console.WriteLine("  --port <port>                       Port to bind (default 12020)");
        Console.WriteLine("  --advertise, --advertise-host       Advertised host for remoting");
        Console.WriteLine("  --advertise-port <port>             Advertised port for remoting");
        Console.WriteLine("  --tick-hz <hz>                      Target tick rate (default 30)");
        Console.WriteLine("  --min-tick-hz <hz>                  Minimum tick rate (default 5)");
        Console.WriteLine("  --compute-timeout-ms <ms>           Compute phase timeout (default 1000/min_tick_hz)");
        Console.WriteLine("  --deliver-timeout-ms <ms>           Deliver phase timeout (default 1000/min_tick_hz)");
        Console.WriteLine("  --backpressure-decay <ratio>        Target tick decay multiplier (default 0.8)");
        Console.WriteLine("  --backpressure-recovery <ratio>     Target tick recovery multiplier (default 1.05)");
        Console.WriteLine("  --late-backpressure-ticks <count>   Late ticks before backpressure (default 2)");
        Console.WriteLine("  --reschedule-timeouts <count>       Timeout streak before reschedule (default 3)");
        Console.WriteLine("  --pause-timeouts <count>            Timeout streak before pause (default 6)");
        Console.WriteLine("  --reschedule-min-ticks <count>      Minimum ticks between reschedules (default 10)");
        Console.WriteLine("  --reschedule-min-minutes <count>    Minimum minutes between reschedules (default 1)");
        Console.WriteLine("  --reschedule-quiet-ms <ms>          Quiet period before reschedule (default 250)");
        Console.WriteLine("  --reschedule-sim-ms <ms>            Simulated reschedule duration (default 1000)");
        Console.WriteLine("  --auto-start | --no-auto-start      Auto start tick loop (default on)");
        Console.WriteLine("  --enable-otel | --disable-otel      Toggle OpenTelemetry (default off)");
        Console.WriteLine("  --otel-metrics                       Enable OTel metrics");
        Console.WriteLine("  --otel-traces                        Enable OTel traces");
        Console.WriteLine("  --otel-console                       Enable OTel console exporter");
        Console.WriteLine("  --otel-endpoint <uri>                OTLP endpoint (env OTEL_EXPORTER_OTLP_ENDPOINT)");
        Console.WriteLine("  --otel-service-name <name>           Service name (env OTEL_SERVICE_NAME)");
    }
}
