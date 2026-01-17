namespace Nbn.Runtime.IO;

public sealed record IoOptions(
    string BindHost,
    int Port,
    string? AdvertisedHost,
    int? AdvertisedPort,
    string GatewayName,
    string ServerName,
    string? HiveMindAddress,
    string? HiveMindName,
    string? ReproAddress,
    string? ReproName)
{
    public static IoOptions FromArgs(string[] args)
    {
        var bindHost = GetEnv("NBN_IO_BIND_HOST") ?? "127.0.0.1";
        var port = GetEnvInt("NBN_IO_PORT") ?? 12020;
        var advertisedHost = GetEnv("NBN_IO_ADVERTISE_HOST");
        var advertisedPort = GetEnvInt("NBN_IO_ADVERTISE_PORT");
        var gatewayName = GetEnv("NBN_IO_GATEWAY_NAME") ?? IoNames.Gateway;
        var serverName = GetEnv("NBN_IO_SERVER_NAME") ?? "nbn.io";
        var hiveMindAddress = GetEnv("NBN_IO_HIVEMIND_ADDRESS");
        var hiveMindName = GetEnv("NBN_IO_HIVEMIND_NAME");
        var reproAddress = GetEnv("NBN_IO_REPRO_ADDRESS");
        var reproName = GetEnv("NBN_IO_REPRO_NAME");

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
                case "--gateway-name":
                    if (i + 1 < args.Length)
                    {
                        gatewayName = args[++i];
                    }
                    continue;
                case "--server-name":
                    if (i + 1 < args.Length)
                    {
                        serverName = args[++i];
                    }
                    continue;
                case "--hivemind-address":
                    if (i + 1 < args.Length)
                    {
                        hiveMindAddress = args[++i];
                    }
                    continue;
                case "--hivemind-name":
                    if (i + 1 < args.Length)
                    {
                        hiveMindName = args[++i];
                    }
                    continue;
                case "--repro-address":
                    if (i + 1 < args.Length)
                    {
                        reproAddress = args[++i];
                    }
                    continue;
                case "--repro-name":
                    if (i + 1 < args.Length)
                    {
                        reproName = args[++i];
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
                && int.TryParse(arg.Substring("--port=".Length), out var portValueInline))
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

            if (arg.StartsWith("--advertise-port=", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(arg.Substring("--advertise-port=".Length), out var advertisedPortInline))
            {
                advertisedPort = advertisedPortInline;
                continue;
            }

            if (arg.StartsWith("--gateway-name=", StringComparison.OrdinalIgnoreCase))
            {
                gatewayName = arg.Substring("--gateway-name=".Length);
                continue;
            }

            if (arg.StartsWith("--server-name=", StringComparison.OrdinalIgnoreCase))
            {
                serverName = arg.Substring("--server-name=".Length);
                continue;
            }

            if (arg.StartsWith("--hivemind-address=", StringComparison.OrdinalIgnoreCase))
            {
                hiveMindAddress = arg.Substring("--hivemind-address=".Length);
                continue;
            }

            if (arg.StartsWith("--hivemind-name=", StringComparison.OrdinalIgnoreCase))
            {
                hiveMindName = arg.Substring("--hivemind-name=".Length);
                continue;
            }

            if (arg.StartsWith("--repro-address=", StringComparison.OrdinalIgnoreCase))
            {
                reproAddress = arg.Substring("--repro-address=".Length);
                continue;
            }

            if (arg.StartsWith("--repro-name=", StringComparison.OrdinalIgnoreCase))
            {
                reproName = arg.Substring("--repro-name=".Length);
            }
        }

        return new IoOptions(
            bindHost,
            port,
            advertisedHost,
            advertisedPort,
            gatewayName,
            serverName,
            hiveMindAddress,
            hiveMindName,
            reproAddress,
            reproName);
    }

    private static string? GetEnv(string key) => Environment.GetEnvironmentVariable(key);

    private static int? GetEnvInt(string key)
    {
        var value = GetEnv(key);
        return int.TryParse(value, out var parsed) ? parsed : null;
    }

    private static void PrintHelp()
    {
        Console.WriteLine("NBN IO Gateway options:");
        Console.WriteLine("  --bind, --bind-host <host>       Host/interface to bind (default 127.0.0.1)");
        Console.WriteLine("  --port <port>                    Port to bind (default 12020)");
        Console.WriteLine("  --advertise, --advertise-host    Advertised host for remoting");
        Console.WriteLine("  --advertise-port <port>          Advertised port for remoting");
        Console.WriteLine("  --gateway-name <name>            Gateway actor name (default io-gateway)");
        Console.WriteLine("  --server-name <name>             Name returned in ConnectAck (default nbn.io)");
        Console.WriteLine("  --hivemind-address <host:port>   HiveMind remote address");
        Console.WriteLine("  --hivemind-name <name>           HiveMind actor name");
        Console.WriteLine("  --repro-address <host:port>      Reproduction manager remote address");
        Console.WriteLine("  --repro-name <name>              Reproduction manager actor name");
    }
}
