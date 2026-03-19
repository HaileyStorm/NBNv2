namespace Nbn.Runtime.Speciation;

public sealed record SpeciationOptions(
    string DatabasePath,
    string BindHost,
    int Port,
    string? AdvertisedHost,
    int? AdvertisedPort,
    string ManagerName,
    string ServiceName,
    bool EnableOpenTelemetry,
    bool EnableOtelMetrics,
    bool EnableOtelTraces,
    bool EnableOtelConsoleExporter,
    string? OtlpEndpoint,
    string OtelServiceName,
    string? SettingsHost,
    int SettingsPort,
    string SettingsName)
{
    public const string DefaultPolicyVersion = "default";
    public const string DefaultSpeciesId = "species.default";
    public const string DefaultSpeciesDisplayName = "Default species";
    public const string DefaultStartupReconcileDecisionReason = "startup_reconcile";
    public const string DefaultConfigSnapshotJson = "{}";

    public SpeciationRuntimeConfig ToRuntimeConfig()
        => new(
            DefaultPolicyVersion,
            DefaultConfigSnapshotJson,
            DefaultSpeciesId,
            DefaultSpeciesDisplayName,
            DefaultStartupReconcileDecisionReason);

    public static SpeciationOptions FromArgs(string[] args)
    {
        var dbPath = GetEnv("NBN_SPECIATION_DB") ?? GetDefaultDatabasePath();
        var bindHost = GetEnv("NBN_SPECIATION_BIND_HOST") ?? Nbn.Shared.NetworkAddressDefaults.DefaultBindHost;
        var port = GetEnvInt("NBN_SPECIATION_PORT") ?? 12080;
        var advertisedHost = GetEnv("NBN_SPECIATION_ADVERTISE_HOST");
        var advertisedPort = GetEnvInt("NBN_SPECIATION_ADVERTISE_PORT");
        var managerName = GetEnv("NBN_SPECIATION_NAME") ?? SpeciationNames.Manager;
        var serviceName = GetEnv("NBN_SPECIATION_SERVER_NAME") ?? "nbn.speciation";
        var enableOtel = GetEnvBool("NBN_SPECIATION_OTEL_ENABLED") ?? false;
        var enableOtelMetrics = GetEnvBool("NBN_SPECIATION_OTEL_METRICS_ENABLED");
        var enableOtelTraces = GetEnvBool("NBN_SPECIATION_OTEL_TRACES_ENABLED");
        var enableOtelConsole = GetEnvBool("NBN_SPECIATION_OTEL_CONSOLE") ?? false;
        var otlpEndpoint = GetEnv("NBN_SPECIATION_OTEL_ENDPOINT") ?? GetEnv("OTEL_EXPORTER_OTLP_ENDPOINT");
        var otelServiceName = GetEnv("NBN_SPECIATION_OTEL_SERVICE_NAME") ?? GetEnv("OTEL_SERVICE_NAME") ?? serviceName;
        var settingsHost = GetEnv("NBN_SETTINGS_HOST") ?? "127.0.0.1";
        var settingsPort = GetEnvInt("NBN_SETTINGS_PORT") ?? 12010;
        var settingsName = GetEnv("NBN_SETTINGS_NAME") ?? "SettingsMonitor";

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
                case "--db":
                    if (i + 1 < args.Length)
                    {
                        dbPath = args[++i];
                    }
                    continue;
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
                case "--manager-name":
                    if (i + 1 < args.Length)
                    {
                        managerName = args[++i];
                    }
                    continue;
                case "--server-name":
                    if (i + 1 < args.Length)
                    {
                        serviceName = args[++i];
                    }
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
                        otelServiceName = args[++i];
                    }
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
            }

            if (arg.StartsWith("--db=", StringComparison.OrdinalIgnoreCase))
            {
                dbPath = arg.Substring("--db=".Length);
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

            if (arg.StartsWith("--port=", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(arg.Substring("--port=".Length), out var portInline))
            {
                port = portInline;
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

            if (arg.StartsWith("--advertise-port=", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(arg.Substring("--advertise-port=".Length), out var advertisedPortInline))
            {
                advertisedPort = advertisedPortInline;
                continue;
            }

            if (arg.StartsWith("--manager-name=", StringComparison.OrdinalIgnoreCase))
            {
                managerName = arg.Substring("--manager-name=".Length);
                continue;
            }

            if (arg.StartsWith("--server-name=", StringComparison.OrdinalIgnoreCase))
            {
                serviceName = arg.Substring("--server-name=".Length);
                continue;
            }

            if (arg.StartsWith("--otel-endpoint=", StringComparison.OrdinalIgnoreCase))
            {
                otlpEndpoint = arg.Substring("--otel-endpoint=".Length);
                continue;
            }

            if (arg.StartsWith("--otel-service-name=", StringComparison.OrdinalIgnoreCase))
            {
                otelServiceName = arg.Substring("--otel-service-name=".Length);
                continue;
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
                continue;
            }
        }

        if (enableOtelMetrics == true || enableOtelTraces == true)
        {
            enableOtel = true;
        }

        enableOtelMetrics ??= enableOtel;
        enableOtelTraces ??= enableOtel;

        return new SpeciationOptions(
            dbPath,
            bindHost,
            port,
            advertisedHost,
            advertisedPort,
            managerName,
            serviceName,
            enableOtel,
            enableOtelMetrics.Value,
            enableOtelTraces.Value,
            enableOtelConsole,
            string.IsNullOrWhiteSpace(otlpEndpoint) ? null : otlpEndpoint.Trim(),
            string.IsNullOrWhiteSpace(otelServiceName) ? serviceName : otelServiceName.Trim(),
            string.IsNullOrWhiteSpace(settingsHost) ? null : settingsHost,
            settingsPort,
            settingsName);
    }

    public static string GetDefaultDatabasePath()
    {
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (string.IsNullOrWhiteSpace(localAppData))
        {
            return "speciation.db";
        }

        var root = Path.Combine(localAppData, "Nbn.Workbench");
        Directory.CreateDirectory(root);
        return Path.Combine(root, "speciation.db");
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
        return bool.TryParse(value, out var parsed) ? parsed : null;
    }

    private static void PrintHelp()
    {
        var defaultPath = GetDefaultDatabasePath();
        Console.WriteLine("NBN Speciation options:");
        Console.WriteLine($"  --db <path>                          SQLite DB path (default {defaultPath})");
        Console.WriteLine($"  --bind, --bind-host <host>           Host/interface to bind (default {Nbn.Shared.NetworkAddressDefaults.DefaultBindHost})");
        Console.WriteLine("  --port <port>                        Port to bind (default 12080)");
        Console.WriteLine("  --advertise, --advertise-host        Advertised host for remoting");
        Console.WriteLine("  --advertise-port <port>              Advertised port for remoting");
        Console.WriteLine("  --manager-name <name>                Speciation actor name (default SpeciationManager)");
        Console.WriteLine("  --server-name <name>                 Service name for SettingsMonitor registration");
        Console.WriteLine("  --enable-otel | --disable-otel       Toggle OpenTelemetry (default off)");
        Console.WriteLine("  --otel-metrics                       Enable OTel metrics");
        Console.WriteLine("  --otel-traces                        Enable OTel traces");
        Console.WriteLine("  --otel-console                       Enable OTel console exporter");
        Console.WriteLine("  --otel-endpoint <uri>                OTLP endpoint (env OTEL_EXPORTER_OTLP_ENDPOINT)");
        Console.WriteLine("  --otel-service-name <name>           OTel service name (default nbn.speciation)");
        Console.WriteLine("  --settings-host <host>               SettingsMonitor host (default 127.0.0.1)");
        Console.WriteLine("  --settings-port <port>               SettingsMonitor port (default 12010)");
        Console.WriteLine("  --settings-name <name>               SettingsMonitor actor name (default SettingsMonitor)");
        Console.WriteLine("  Speciation policy defaults/config are loaded from SettingsMonitor keys (workbench.speciation.*).");
    }
}
