namespace Nbn.Runtime.WorkerNode;

public sealed record WorkerNodeOptions(
    string BindHost,
    int Port,
    string? AdvertisedHost,
    int? AdvertisedPort,
    string LogicalName,
    string RootActorName,
    string? SettingsHost,
    int SettingsPort,
    string SettingsName,
    Guid? WorkerNodeId,
    WorkerServiceRole ServiceRoles)
{
    public static WorkerNodeOptions FromArgs(string[] args)
    {
        var bindHost = GetEnv("NBN_WORKER_BIND_HOST") ?? "127.0.0.1";
        var port = GetEnvInt("NBN_WORKER_PORT") ?? 12041;
        var advertisedHost = GetEnv("NBN_WORKER_ADVERTISE_HOST");
        var advertisedPort = GetEnvInt("NBN_WORKER_ADVERTISE_PORT");
        var logicalName = GetEnv("NBN_WORKER_LOGICAL_NAME") ?? "nbn.worker";
        var rootActorName = GetEnv("NBN_WORKER_ROOT_NAME") ?? "worker-node";
        var settingsHost = GetEnv("NBN_SETTINGS_HOST") ?? "127.0.0.1";
        var settingsPort = GetEnvInt("NBN_SETTINGS_PORT") ?? 12010;
        var settingsName = GetEnv("NBN_SETTINGS_NAME") ?? "SettingsMonitor";
        var workerNodeId = GetEnvGuid("NBN_WORKER_NODE_ID");
        var serviceRoles = WorkerServiceRole.All;
        var serviceRolesRaw = GetEnv("NBN_WORKER_SERVICE_ROLES");
        if (!string.IsNullOrWhiteSpace(serviceRolesRaw))
        {
            serviceRoles = WorkerServiceRoles.ParseRoleSet(serviceRolesRaw, "NBN_WORKER_SERVICE_ROLES");
        }

        for (var index = 0; index < args.Length; index++)
        {
            var arg = args[index];
            if (arg.Equals("--help", StringComparison.OrdinalIgnoreCase) || arg.Equals("-h", StringComparison.OrdinalIgnoreCase))
            {
                PrintHelp();
                Environment.Exit(0);
            }

            switch (arg)
            {
                case "--bind":
                case "--bind-host":
                    if (index + 1 < args.Length)
                    {
                        bindHost = args[++index];
                    }
                    continue;
                case "--port":
                    if (index + 1 < args.Length && int.TryParse(args[++index], out var portValue))
                    {
                        port = portValue;
                    }
                    continue;
                case "--advertise":
                case "--advertise-host":
                    if (index + 1 < args.Length)
                    {
                        advertisedHost = args[++index];
                    }
                    continue;
                case "--advertise-port":
                    if (index + 1 < args.Length && int.TryParse(args[++index], out var advertisedPortValue))
                    {
                        advertisedPort = advertisedPortValue;
                    }
                    continue;
                case "--logical-name":
                    if (index + 1 < args.Length)
                    {
                        logicalName = args[++index];
                    }
                    continue;
                case "--root-name":
                case "--root-actor-name":
                    if (index + 1 < args.Length)
                    {
                        rootActorName = args[++index];
                    }
                    continue;
                case "--settings-host":
                    if (index + 1 < args.Length)
                    {
                        settingsHost = args[++index];
                    }
                    continue;
                case "--settings-port":
                    if (index + 1 < args.Length && int.TryParse(args[++index], out var settingsPortValue))
                    {
                        settingsPort = settingsPortValue;
                    }
                    continue;
                case "--settings-name":
                    if (index + 1 < args.Length)
                    {
                        settingsName = args[++index];
                    }
                    continue;
                case "--worker-node-id":
                    if (index + 1 < args.Length && Guid.TryParse(args[++index], out var workerNodeIdValue))
                    {
                        workerNodeId = workerNodeIdValue;
                    }
                    continue;
                case "--service-roles":
                    if (index + 1 < args.Length)
                    {
                        serviceRoles = WorkerServiceRoles.ParseRoleSet(args[++index], "--service-roles");
                    }
                    continue;
                case "--service-role":
                case "--enable-service-role":
                    if (index + 1 < args.Length)
                    {
                        serviceRoles |= WorkerServiceRoles.ParseRoleSet(args[++index], arg);
                    }
                    continue;
                case "--disable-service-role":
                    if (index + 1 < args.Length)
                    {
                        serviceRoles &= ~WorkerServiceRoles.ParseRoleSet(args[++index], "--disable-service-role");
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

            if (arg.StartsWith("--logical-name=", StringComparison.OrdinalIgnoreCase))
            {
                logicalName = arg.Substring("--logical-name=".Length);
                continue;
            }

            if (arg.StartsWith("--root-name=", StringComparison.OrdinalIgnoreCase))
            {
                rootActorName = arg.Substring("--root-name=".Length);
                continue;
            }

            if (arg.StartsWith("--root-actor-name=", StringComparison.OrdinalIgnoreCase))
            {
                rootActorName = arg.Substring("--root-actor-name=".Length);
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

            if (arg.StartsWith("--worker-node-id=", StringComparison.OrdinalIgnoreCase)
                && Guid.TryParse(arg.Substring("--worker-node-id=".Length), out var workerNodeIdInline))
            {
                workerNodeId = workerNodeIdInline;
                continue;
            }

            if (arg.StartsWith("--service-roles=", StringComparison.OrdinalIgnoreCase))
            {
                serviceRoles = WorkerServiceRoles.ParseRoleSet(
                    arg.Substring("--service-roles=".Length),
                    "--service-roles");
                continue;
            }

            if (arg.StartsWith("--service-role=", StringComparison.OrdinalIgnoreCase))
            {
                serviceRoles |= WorkerServiceRoles.ParseRoleSet(
                    arg.Substring("--service-role=".Length),
                    "--service-role");
                continue;
            }

            if (arg.StartsWith("--enable-service-role=", StringComparison.OrdinalIgnoreCase))
            {
                serviceRoles |= WorkerServiceRoles.ParseRoleSet(
                    arg.Substring("--enable-service-role=".Length),
                    "--enable-service-role");
                continue;
            }

            if (arg.StartsWith("--disable-service-role=", StringComparison.OrdinalIgnoreCase))
            {
                serviceRoles &= ~WorkerServiceRoles.ParseRoleSet(
                    arg.Substring("--disable-service-role=".Length),
                    "--disable-service-role");
            }
        }

        if (port <= 0)
        {
            port = 12041;
        }

        if (settingsPort <= 0)
        {
            settingsPort = 12010;
        }

        if (string.IsNullOrWhiteSpace(logicalName))
        {
            logicalName = "nbn.worker";
        }

        if (string.IsNullOrWhiteSpace(rootActorName))
        {
            rootActorName = "worker-node";
        }

        if (string.IsNullOrWhiteSpace(settingsName))
        {
            settingsName = "SettingsMonitor";
        }

        return new WorkerNodeOptions(
            bindHost,
            port,
            advertisedHost,
            advertisedPort,
            logicalName,
            rootActorName,
            string.IsNullOrWhiteSpace(settingsHost) ? null : settingsHost,
            settingsPort,
            settingsName,
            workerNodeId,
            WorkerServiceRoles.Sanitize(serviceRoles));
    }

    private static string? GetEnv(string key) => Environment.GetEnvironmentVariable(key);

    private static int? GetEnvInt(string key)
    {
        var value = GetEnv(key);
        return int.TryParse(value, out var parsed) ? parsed : null;
    }

    private static Guid? GetEnvGuid(string key)
    {
        var value = GetEnv(key);
        return Guid.TryParse(value, out var parsed) ? parsed : null;
    }

    public static void PrintHelp()
    {
        Console.WriteLine("NBN WorkerNode options:");
        Console.WriteLine("  --bind, --bind-host <host>       Host/interface to bind (default 127.0.0.1)");
        Console.WriteLine("  --port <port>                    Port to bind (default 12041)");
        Console.WriteLine("  --advertise, --advertise-host    Advertised host for remoting");
        Console.WriteLine("  --advertise-port <port>          Advertised port for remoting");
        Console.WriteLine("  --logical-name <name>            Node logical name (default nbn.worker)");
        Console.WriteLine("  --root-name <name>               Worker root actor name (default worker-node)");
        Console.WriteLine("  --settings-host <host>           SettingsMonitor host (default 127.0.0.1)");
        Console.WriteLine("  --settings-port <port>           SettingsMonitor port (default 12010)");
        Console.WriteLine("  --settings-name <name>           SettingsMonitor actor name (default SettingsMonitor)");
        Console.WriteLine("  --worker-node-id <guid>          Optional stable worker node id (default derives from advertised address)");
        Console.WriteLine("  --service-roles <list>           Enabled worker roles (default all)");
        Console.WriteLine("  --service-role <role>            Enable one or more roles (repeatable)");
        Console.WriteLine("  --disable-service-role <role>    Disable one or more roles (repeatable)");
        Console.WriteLine("                                   Role tokens: all, none, brain-root, signal-router,");
        Console.WriteLine("                                                input-coordinator, output-coordinator, region-shard");
        Console.WriteLine("  env: NBN_WORKER_SERVICE_ROLES    Same role token list as --service-roles");
    }
}
