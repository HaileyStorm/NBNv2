namespace Nbn.Runtime.SettingsMonitor;

public sealed record SettingsMonitorOptions(
    string DatabasePath,
    string BindHost,
    int Port,
    string? AdvertisedHost,
    int? AdvertisedPort)
{
    public static SettingsMonitorOptions FromArgs(string[] args)
    {
        var dbPath = GetDefaultDatabasePath();
        var bindHost = GetEnv("NBN_SETTINGS_BIND_HOST") ?? "127.0.0.1";
        var port = GetEnvInt("NBN_SETTINGS_PORT") ?? 12010;
        var advertisedHost = GetEnv("NBN_SETTINGS_ADVERTISE_HOST");
        var advertisedPort = GetEnvInt("NBN_SETTINGS_ADVERTISE_PORT");

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

            if (arg.StartsWith("--port=", StringComparison.OrdinalIgnoreCase) && int.TryParse(arg.Substring("--port=".Length), out var portInline))
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
            }
        }

        return new SettingsMonitorOptions(dbPath, bindHost, port, advertisedHost, advertisedPort);
    }

    private static string? GetEnv(string key) => Environment.GetEnvironmentVariable(key);

    public static string GetDefaultDatabasePath()
    {
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (string.IsNullOrWhiteSpace(localAppData))
        {
            return "settingsmonitor.db";
        }

        var root = Path.Combine(localAppData, "Nbn.Workbench");
        Directory.CreateDirectory(root);
        return Path.Combine(root, "settingsmonitor.db");
    }

    private static int? GetEnvInt(string key)
    {
        var value = GetEnv(key);
        return int.TryParse(value, out var parsed) ? parsed : null;
    }

    private static void PrintHelp()
    {
        var defaultPath = GetDefaultDatabasePath();
        Console.WriteLine("NBN SettingsMonitor options:");
        Console.WriteLine($"  --db <path>                         SQLite DB path (default {defaultPath})");
        Console.WriteLine("  --bind, --bind-host <host>          Host/interface to bind (default 127.0.0.1)");
        Console.WriteLine("  --port <port>                       Port to bind (default 12010)");
        Console.WriteLine("  --advertise, --advertise-host       Advertised host for remoting");
        Console.WriteLine("  --advertise-port <port>             Advertised port for remoting");
    }
}
