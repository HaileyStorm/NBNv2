namespace Nbn.Runtime.Speciation;

public sealed record SpeciationOptions(
    string DatabasePath,
    string BindHost,
    int Port,
    string? AdvertisedHost,
    int? AdvertisedPort,
    string ManagerName,
    string ServiceName,
    string? SettingsHost,
    int SettingsPort,
    string SettingsName,
    string PolicyVersion,
    string DefaultSpeciesId,
    string DefaultSpeciesDisplayName,
    string StartupReconcileDecisionReason,
    string ConfigSnapshotJson)
{
    public SpeciationRuntimeConfig ToRuntimeConfig()
        => new(
            PolicyVersion,
            ConfigSnapshotJson,
            DefaultSpeciesId,
            DefaultSpeciesDisplayName,
            StartupReconcileDecisionReason);

    public static SpeciationOptions FromArgs(string[] args)
    {
        var dbPath = GetEnv("NBN_SPECIATION_DB") ?? GetDefaultDatabasePath();
        var bindHost = GetEnv("NBN_SPECIATION_BIND_HOST") ?? "127.0.0.1";
        var port = GetEnvInt("NBN_SPECIATION_PORT") ?? 12080;
        var advertisedHost = GetEnv("NBN_SPECIATION_ADVERTISE_HOST");
        var advertisedPort = GetEnvInt("NBN_SPECIATION_ADVERTISE_PORT");
        var managerName = GetEnv("NBN_SPECIATION_NAME") ?? SpeciationNames.Manager;
        var serviceName = GetEnv("NBN_SPECIATION_SERVER_NAME") ?? "nbn.speciation";
        var settingsHost = GetEnv("NBN_SETTINGS_HOST") ?? "127.0.0.1";
        var settingsPort = GetEnvInt("NBN_SETTINGS_PORT") ?? 12010;
        var settingsName = GetEnv("NBN_SETTINGS_NAME") ?? "SettingsMonitor";
        var policyVersion = GetEnv("NBN_SPECIATION_POLICY_VERSION") ?? "v1";
        var defaultSpeciesId = GetEnv("NBN_SPECIATION_DEFAULT_SPECIES_ID") ?? "unclassified";
        var defaultSpeciesName = GetEnv("NBN_SPECIATION_DEFAULT_SPECIES_NAME") ?? "Unclassified";
        var startupReconcileReason = GetEnv("NBN_SPECIATION_RECONCILE_REASON") ?? "startup_reconcile";
        var configSnapshotJson = GetEnv("NBN_SPECIATION_CONFIG_JSON") ?? "{}";

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
                case "--policy-version":
                    if (i + 1 < args.Length)
                    {
                        policyVersion = args[++i];
                    }
                    continue;
                case "--default-species-id":
                    if (i + 1 < args.Length)
                    {
                        defaultSpeciesId = args[++i];
                    }
                    continue;
                case "--default-species-name":
                    if (i + 1 < args.Length)
                    {
                        defaultSpeciesName = args[++i];
                    }
                    continue;
                case "--reconcile-reason":
                    if (i + 1 < args.Length)
                    {
                        startupReconcileReason = args[++i];
                    }
                    continue;
                case "--config-json":
                    if (i + 1 < args.Length)
                    {
                        configSnapshotJson = args[++i];
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

            if (arg.StartsWith("--policy-version=", StringComparison.OrdinalIgnoreCase))
            {
                policyVersion = arg.Substring("--policy-version=".Length);
                continue;
            }

            if (arg.StartsWith("--default-species-id=", StringComparison.OrdinalIgnoreCase))
            {
                defaultSpeciesId = arg.Substring("--default-species-id=".Length);
                continue;
            }

            if (arg.StartsWith("--default-species-name=", StringComparison.OrdinalIgnoreCase))
            {
                defaultSpeciesName = arg.Substring("--default-species-name=".Length);
                continue;
            }

            if (arg.StartsWith("--reconcile-reason=", StringComparison.OrdinalIgnoreCase))
            {
                startupReconcileReason = arg.Substring("--reconcile-reason=".Length);
                continue;
            }

            if (arg.StartsWith("--config-json=", StringComparison.OrdinalIgnoreCase))
            {
                configSnapshotJson = arg.Substring("--config-json=".Length);
            }
        }

        return new SpeciationOptions(
            dbPath,
            bindHost,
            port,
            advertisedHost,
            advertisedPort,
            managerName,
            serviceName,
            string.IsNullOrWhiteSpace(settingsHost) ? null : settingsHost,
            settingsPort,
            settingsName,
            policyVersion,
            defaultSpeciesId,
            defaultSpeciesName,
            startupReconcileReason,
            configSnapshotJson);
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

    private static void PrintHelp()
    {
        var defaultPath = GetDefaultDatabasePath();
        Console.WriteLine("NBN Speciation options:");
        Console.WriteLine($"  --db <path>                          SQLite DB path (default {defaultPath})");
        Console.WriteLine("  --bind, --bind-host <host>           Host/interface to bind (default 127.0.0.1)");
        Console.WriteLine("  --port <port>                        Port to bind (default 12080)");
        Console.WriteLine("  --advertise, --advertise-host        Advertised host for remoting");
        Console.WriteLine("  --advertise-port <port>              Advertised port for remoting");
        Console.WriteLine("  --manager-name <name>                Speciation actor name (default SpeciationManager)");
        Console.WriteLine("  --server-name <name>                 Service name for SettingsMonitor registration");
        Console.WriteLine("  --settings-host <host>               SettingsMonitor host (default 127.0.0.1)");
        Console.WriteLine("  --settings-port <port>               SettingsMonitor port (default 12010)");
        Console.WriteLine("  --settings-name <name>               SettingsMonitor actor name (default SettingsMonitor)");
        Console.WriteLine("  --policy-version <version>           Policy version for epoch decisions (default v1)");
        Console.WriteLine("  --default-species-id <id>            Default species id (default unclassified)");
        Console.WriteLine("  --default-species-name <name>        Default species name (default Unclassified)");
        Console.WriteLine("  --reconcile-reason <reason>          Decision reason for startup reconcile");
        Console.WriteLine("  --config-json <json>                 Epoch config snapshot JSON");
    }
}
