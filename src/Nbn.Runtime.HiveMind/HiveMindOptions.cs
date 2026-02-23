using Nbn.Runtime.SettingsMonitor;

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
    string ServiceName,
    string? SettingsDbPath,
    string? SettingsHost,
    int SettingsPort,
    string SettingsName,
    string? IoAddress,
    string? IoName,
    int WorkerInventoryRefreshMs = 2000,
    int WorkerInventoryStaleAfterMs = 15000,
    int PlacementAssignmentTimeoutMs = 10000,
    int PlacementAssignmentRetryBackoffMs = 250,
    int PlacementAssignmentMaxRetries = 2,
    int PlacementReconcileTimeoutMs = 10000)
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

        var settingsHost = GetEnv("NBN_SETTINGS_HOST") ?? "127.0.0.1";
        var settingsPort = GetEnvInt("NBN_SETTINGS_PORT") ?? 12010;
        var settingsName = GetEnv("NBN_SETTINGS_NAME") ?? SettingsMonitorNames.SettingsMonitor;
        var workerInventoryRefreshMs = GetEnvInt("NBN_HIVE_WORKER_INVENTORY_REFRESH_MS") ?? 2000;
        var workerInventoryStaleAfterMs = GetEnvInt("NBN_HIVE_WORKER_INVENTORY_STALE_AFTER_MS") ?? 15000;
        var placementAssignmentTimeoutMs = GetEnvInt("NBN_HIVE_PLACEMENT_ASSIGNMENT_TIMEOUT_MS") ?? 10000;
        var placementAssignmentRetryBackoffMs = GetEnvInt("NBN_HIVE_PLACEMENT_ASSIGNMENT_RETRY_BACKOFF_MS") ?? 250;
        var placementAssignmentMaxRetries = GetEnvInt("NBN_HIVE_PLACEMENT_ASSIGNMENT_MAX_RETRIES") ?? 2;
        var placementReconcileTimeoutMs = GetEnvInt("NBN_HIVE_PLACEMENT_RECONCILE_TIMEOUT_MS") ?? 10000;
        var ioAddress = GetEnv("NBN_HIVE_IO_ADDRESS");
        var ioName = GetEnv("NBN_HIVE_IO_NAME") ?? "io-gateway";

        var settingsDbPath = GetEnv("NBN_SETTINGS_DB");
        if (string.IsNullOrWhiteSpace(settingsDbPath))
        {
            settingsDbPath = "settingsmonitor.db";
        }

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
                case "--settings-db":
                case "--settings-db-path":
                    if (i + 1 < args.Length)
                    {
                        settingsDbPath = args[++i];
                    }
                    continue;
                case "--no-settings-db":
                    settingsDbPath = null;
                    continue;
                case "--settings-host":
                    if (i + 1 < args.Length)
                    {
                        settingsHost = args[++i];
                    }
                    continue;
                case "--settings-port":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var settingsPortValue))
                    {
                        settingsPort = settingsPortValue;
                    }
                    continue;
                case "--settings-name":
                    if (i + 1 < args.Length)
                    {
                        settingsName = args[++i];
                    }
                    continue;
                case "--worker-inventory-refresh-ms":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var workerInventoryRefreshValue))
                    {
                        workerInventoryRefreshMs = workerInventoryRefreshValue;
                    }
                    continue;
                case "--worker-inventory-stale-after-ms":
                case "--worker-inventory-stale-ms":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var workerInventoryStaleValue))
                    {
                        workerInventoryStaleAfterMs = workerInventoryStaleValue;
                    }
                    continue;
                case "--placement-assignment-timeout-ms":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var placementAssignmentTimeoutValue))
                    {
                        placementAssignmentTimeoutMs = placementAssignmentTimeoutValue;
                    }
                    continue;
                case "--placement-assignment-retry-backoff-ms":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var placementAssignmentRetryBackoffValue))
                    {
                        placementAssignmentRetryBackoffMs = placementAssignmentRetryBackoffValue;
                    }
                    continue;
                case "--placement-assignment-max-retries":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var placementAssignmentMaxRetriesValue))
                    {
                        placementAssignmentMaxRetries = placementAssignmentMaxRetriesValue;
                    }
                    continue;
                case "--placement-reconcile-timeout-ms":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var placementReconcileTimeoutValue))
                    {
                        placementReconcileTimeoutMs = placementReconcileTimeoutValue;
                    }
                    continue;
                case "--io-address":
                    if (i + 1 < args.Length)
                    {
                        ioAddress = args[++i];
                    }
                    continue;
                case "--io-name":
                case "--io-gateway":
                    if (i + 1 < args.Length)
                    {
                        ioName = args[++i];
                    }
                    continue;
                case "--no-settings-monitor":
                    settingsHost = null;
                    settingsPort = 0;
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

            if (arg.StartsWith("--settings-db=", StringComparison.OrdinalIgnoreCase))
            {
                settingsDbPath = arg.Substring("--settings-db=".Length);
                continue;
            }

            if (arg.StartsWith("--settings-db-path=", StringComparison.OrdinalIgnoreCase))
            {
                settingsDbPath = arg.Substring("--settings-db-path=".Length);
            }

            if (arg.StartsWith("--settings-host=", StringComparison.OrdinalIgnoreCase))
            {
                settingsHost = arg.Substring("--settings-host=".Length);
                continue;
            }

            if (arg.StartsWith("--settings-port=", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(arg.Substring("--settings-port=".Length), out var settingsPortInline))
            {
                settingsPort = settingsPortInline;
                continue;
            }

            if (arg.StartsWith("--settings-name=", StringComparison.OrdinalIgnoreCase))
            {
                settingsName = arg.Substring("--settings-name=".Length);
            }

            if (arg.StartsWith("--worker-inventory-refresh-ms=", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(arg.Substring("--worker-inventory-refresh-ms=".Length), out var workerInventoryRefreshInline))
            {
                workerInventoryRefreshMs = workerInventoryRefreshInline;
                continue;
            }

            if (arg.StartsWith("--worker-inventory-stale-after-ms=", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(arg.Substring("--worker-inventory-stale-after-ms=".Length), out var workerInventoryStaleInline))
            {
                workerInventoryStaleAfterMs = workerInventoryStaleInline;
                continue;
            }

            if (arg.StartsWith("--worker-inventory-stale-ms=", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(arg.Substring("--worker-inventory-stale-ms=".Length), out var workerInventoryStaleAliasInline))
            {
                workerInventoryStaleAfterMs = workerInventoryStaleAliasInline;
                continue;
            }

            if (arg.StartsWith("--placement-assignment-timeout-ms=", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(arg.Substring("--placement-assignment-timeout-ms=".Length), out var placementAssignmentTimeoutInline))
            {
                placementAssignmentTimeoutMs = placementAssignmentTimeoutInline;
                continue;
            }

            if (arg.StartsWith("--placement-assignment-retry-backoff-ms=", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(arg.Substring("--placement-assignment-retry-backoff-ms=".Length), out var placementAssignmentRetryBackoffInline))
            {
                placementAssignmentRetryBackoffMs = placementAssignmentRetryBackoffInline;
                continue;
            }

            if (arg.StartsWith("--placement-assignment-max-retries=", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(arg.Substring("--placement-assignment-max-retries=".Length), out var placementAssignmentMaxRetriesInline))
            {
                placementAssignmentMaxRetries = placementAssignmentMaxRetriesInline;
                continue;
            }

            if (arg.StartsWith("--placement-reconcile-timeout-ms=", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(arg.Substring("--placement-reconcile-timeout-ms=".Length), out var placementReconcileTimeoutInline))
            {
                placementReconcileTimeoutMs = placementReconcileTimeoutInline;
                continue;
            }

            if (arg.StartsWith("--io-address=", StringComparison.OrdinalIgnoreCase))
            {
                ioAddress = arg.Substring("--io-address=".Length);
                continue;
            }

            if (arg.StartsWith("--io-name=", StringComparison.OrdinalIgnoreCase))
            {
                ioName = arg.Substring("--io-name=".Length);
                continue;
            }

            if (arg.StartsWith("--io-gateway=", StringComparison.OrdinalIgnoreCase))
            {
                ioName = arg.Substring("--io-gateway=".Length);
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

        if (workerInventoryRefreshMs < 100)
        {
            workerInventoryRefreshMs = 100;
        }

        if (workerInventoryStaleAfterMs < workerInventoryRefreshMs)
        {
            workerInventoryStaleAfterMs = workerInventoryRefreshMs;
        }

        if (placementAssignmentTimeoutMs < 100)
        {
            placementAssignmentTimeoutMs = 100;
        }

        if (placementAssignmentRetryBackoffMs < 0)
        {
            placementAssignmentRetryBackoffMs = 0;
        }

        if (placementAssignmentMaxRetries < 0)
        {
            placementAssignmentMaxRetries = 0;
        }

        if (placementReconcileTimeoutMs < 100)
        {
            placementReconcileTimeoutMs = 100;
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
            serviceName,
            string.IsNullOrWhiteSpace(settingsDbPath) ? null : settingsDbPath,
            string.IsNullOrWhiteSpace(settingsHost) ? null : settingsHost,
            settingsPort,
            settingsName,
            string.IsNullOrWhiteSpace(ioAddress) ? null : ioAddress,
            ioName,
            workerInventoryRefreshMs,
            workerInventoryStaleAfterMs,
            placementAssignmentTimeoutMs,
            placementAssignmentRetryBackoffMs,
            placementAssignmentMaxRetries,
            placementReconcileTimeoutMs);
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
        Console.WriteLine("  --settings-db <path>                 Deprecated (HiveMind no longer writes SettingsMonitor DB)");
        Console.WriteLine("  --no-settings-db                     Deprecated (no-op; use --no-settings-monitor)");
        Console.WriteLine("  --settings-host <host>               SettingsMonitor host (default 127.0.0.1)");
        Console.WriteLine("  --settings-port <port>               SettingsMonitor port (default 12010)");
        Console.WriteLine("  --settings-name <name>               SettingsMonitor actor name (default SettingsMonitor)");
        Console.WriteLine("  --worker-inventory-refresh-ms <ms>   Settings snapshot pull interval (default 2000)");
        Console.WriteLine("  --worker-inventory-stale-after-ms <ms> Worker freshness threshold (default 15000)");
        Console.WriteLine("  --placement-assignment-timeout-ms <ms> Assignment ack timeout (default 10000)");
        Console.WriteLine("  --placement-assignment-retry-backoff-ms <ms> Retry backoff for retryable assignment failures (default 250)");
        Console.WriteLine("  --placement-assignment-max-retries <count> Max assignment retries after initial attempt (default 2)");
        Console.WriteLine("  --placement-reconcile-timeout-ms <ms> Reconcile report timeout after assignments are ready (default 10000)");
        Console.WriteLine("  --no-settings-monitor                Disable SettingsMonitor reporting");
        Console.WriteLine("  --io-address <host:port>             IO Gateway remote address");
        Console.WriteLine("  --io-name <name>                     IO Gateway actor name (default io-gateway)");
    }
}
