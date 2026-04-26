namespace Nbn.Runtime.Ppo;

/// <summary>
/// Configures the PPO runtime host and required runtime-service dependencies.
/// </summary>
public sealed record PpoOptions(
    string BindHost,
    int Port,
    string? AdvertisedHost,
    int? AdvertisedPort,
    string ManagerName,
    string ServiceName,
    string? SettingsHost,
    int SettingsPort,
    string SettingsName,
    string? IoAddress,
    string? IoName,
    string? ReproductionAddress,
    string? ReproductionName,
    string? SpeciationAddress,
    string? SpeciationName)
{
    public static PpoOptions FromArgs(string[] args)
    {
        var bindHost = GetEnv("NBN_PPO_BIND_HOST") ?? Nbn.Shared.NetworkAddressDefaults.DefaultBindHost;
        var port = GetEnvInt("NBN_PPO_PORT") ?? 12090;
        var advertisedHost = GetEnv("NBN_PPO_ADVERTISE_HOST");
        var advertisedPort = GetEnvInt("NBN_PPO_ADVERTISE_PORT");
        var managerName = GetEnv("NBN_PPO_NAME") ?? PpoNames.Manager;
        var serviceName = GetEnv("NBN_PPO_SERVER_NAME") ?? "nbn.ppo";
        var settingsHost = GetEnv("NBN_SETTINGS_HOST") ?? "127.0.0.1";
        var settingsPort = GetEnvInt("NBN_SETTINGS_PORT") ?? 12010;
        var settingsName = GetEnv("NBN_SETTINGS_NAME") ?? "SettingsMonitor";
        var ioAddress = GetEnv("NBN_PPO_IO_ADDRESS");
        var ioName = GetEnv("NBN_PPO_IO_NAME") ?? "IoGateway";
        var reproductionAddress = GetEnv("NBN_PPO_REPRO_ADDRESS");
        var reproductionName = GetEnv("NBN_PPO_REPRO_NAME") ?? "ReproductionManager";
        var speciationAddress = GetEnv("NBN_PPO_SPECIATION_ADDRESS");
        var speciationName = GetEnv("NBN_PPO_SPECIATION_NAME") ?? "SpeciationManager";

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
                case "--io-address":
                    if (i + 1 < args.Length)
                    {
                        ioAddress = args[++i];
                    }
                    continue;
                case "--io-name":
                    if (i + 1 < args.Length)
                    {
                        ioName = args[++i];
                    }
                    continue;
                case "--repro-address":
                case "--reproduction-address":
                    if (i + 1 < args.Length)
                    {
                        reproductionAddress = args[++i];
                    }
                    continue;
                case "--repro-name":
                case "--reproduction-name":
                    if (i + 1 < args.Length)
                    {
                        reproductionName = args[++i];
                    }
                    continue;
                case "--speciation-address":
                    if (i + 1 < args.Length)
                    {
                        speciationAddress = args[++i];
                    }
                    continue;
                case "--speciation-name":
                    if (i + 1 < args.Length)
                    {
                        speciationName = args[++i];
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

            if (arg.StartsWith("--repro-address=", StringComparison.OrdinalIgnoreCase))
            {
                reproductionAddress = arg.Substring("--repro-address=".Length);
                continue;
            }

            if (arg.StartsWith("--reproduction-address=", StringComparison.OrdinalIgnoreCase))
            {
                reproductionAddress = arg.Substring("--reproduction-address=".Length);
                continue;
            }

            if (arg.StartsWith("--repro-name=", StringComparison.OrdinalIgnoreCase))
            {
                reproductionName = arg.Substring("--repro-name=".Length);
                continue;
            }

            if (arg.StartsWith("--reproduction-name=", StringComparison.OrdinalIgnoreCase))
            {
                reproductionName = arg.Substring("--reproduction-name=".Length);
                continue;
            }

            if (arg.StartsWith("--speciation-address=", StringComparison.OrdinalIgnoreCase))
            {
                speciationAddress = arg.Substring("--speciation-address=".Length);
                continue;
            }

            if (arg.StartsWith("--speciation-name=", StringComparison.OrdinalIgnoreCase))
            {
                speciationName = arg.Substring("--speciation-name=".Length);
            }
        }

        return new PpoOptions(
            bindHost,
            port,
            advertisedHost,
            advertisedPort,
            managerName,
            serviceName,
            string.IsNullOrWhiteSpace(settingsHost) ? null : settingsHost,
            settingsPort,
            settingsName,
            string.IsNullOrWhiteSpace(ioAddress) ? null : ioAddress,
            string.IsNullOrWhiteSpace(ioName) ? null : ioName,
            string.IsNullOrWhiteSpace(reproductionAddress) ? null : reproductionAddress,
            string.IsNullOrWhiteSpace(reproductionName) ? null : reproductionName,
            string.IsNullOrWhiteSpace(speciationAddress) ? null : speciationAddress,
            string.IsNullOrWhiteSpace(speciationName) ? null : speciationName);
    }

    private static string? GetEnv(string key) => Environment.GetEnvironmentVariable(key);

    private static int? GetEnvInt(string key)
    {
        var value = GetEnv(key);
        return int.TryParse(value, out var parsed) ? parsed : null;
    }

    private static void PrintHelp()
    {
        Console.WriteLine("NBN PPO options:");
        Console.WriteLine($"  --bind, --bind-host <host>       Host/interface to bind (default {Nbn.Shared.NetworkAddressDefaults.DefaultBindHost})");
        Console.WriteLine("  --port <port>                    Port to bind (default 12090)");
        Console.WriteLine("  --advertise, --advertise-host    Advertised host for remoting");
        Console.WriteLine("  --advertise-port <port>          Advertised port for remoting");
        Console.WriteLine("  --manager-name <name>            PPO actor name (default PpoManager)");
        Console.WriteLine("  --server-name <name>             Service name for SettingsMonitor registration");
        Console.WriteLine("  --settings-host <host>           SettingsMonitor host (default 127.0.0.1)");
        Console.WriteLine("  --settings-port <port>           SettingsMonitor port (default 12010)");
        Console.WriteLine("  --settings-name <name>           SettingsMonitor actor name (default SettingsMonitor)");
        Console.WriteLine("  --io-address <host:port>         IO Gateway address");
        Console.WriteLine("  --io-name <name>                 IO Gateway actor name (default IoGateway)");
        Console.WriteLine("  --repro-address <host:port>      Reproduction manager address");
        Console.WriteLine("  --repro-name <name>              Reproduction manager actor name (default ReproductionManager)");
        Console.WriteLine("  --speciation-address <host:port> Speciation manager address");
        Console.WriteLine("  --speciation-name <name>         Speciation manager actor name (default SpeciationManager)");
    }
}
