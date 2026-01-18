using Nbn.Shared;

namespace Nbn.Runtime.RegionHost;

public sealed record RegionHostOptions(
    string BindHost,
    int Port,
    string? AdvertisedHost,
    int? AdvertisedPort,
    string ArtifactRootPath,
    string? SettingsHost,
    int SettingsPort,
    string SettingsName,
    Guid BrainId,
    int RegionId,
    int NeuronStart,
    int NeuronCount,
    int ShardIndex,
    string ShardName,
    string? RouterAddress,
    string? RouterId,
    string? OutputAddress,
    string? OutputId,
    string? TickAddress,
    string? TickId,
    string NbnSha256,
    long NbnSize,
    string? NbsSha256,
    long? NbsSize,
    string? StoreUri)
{
    public static RegionHostOptions FromArgs(string[] args)
    {
        var bindHost = GetEnv("NBN_REGIONHOST_BIND_HOST") ?? "127.0.0.1";
        var port = GetEnvInt("NBN_REGIONHOST_PORT") ?? 12040;
        var advertisedHost = GetEnv("NBN_REGIONHOST_ADVERTISE_HOST");
        var advertisedPort = GetEnvInt("NBN_REGIONHOST_ADVERTISE_PORT");
        var artifactRoot = GetEnv("NBN_REGIONHOST_ARTIFACT_ROOT")
                           ?? Path.Combine(Environment.CurrentDirectory, "artifacts");
        var settingsHost = GetEnv("NBN_SETTINGS_HOST") ?? "127.0.0.1";
        var settingsPort = GetEnvInt("NBN_SETTINGS_PORT") ?? 12010;
        var settingsName = GetEnv("NBN_SETTINGS_NAME") ?? "SettingsMonitor";
        var brainId = GetEnvGuid("NBN_REGIONHOST_BRAIN_ID") ?? Guid.Empty;
        var regionId = GetEnvInt("NBN_REGIONHOST_REGION") ?? -1;
        var neuronStart = GetEnvInt("NBN_REGIONHOST_NEURON_START") ?? 0;
        var neuronCount = GetEnvInt("NBN_REGIONHOST_NEURON_COUNT") ?? 0;
        var shardIndex = GetEnvInt("NBN_REGIONHOST_SHARD_INDEX") ?? 0;
        var shardName = GetEnv("NBN_REGIONHOST_SHARD_NAME") ?? string.Empty;
        var routerAddress = GetEnv("NBN_REGIONHOST_ROUTER_ADDRESS");
        var routerId = GetEnv("NBN_REGIONHOST_ROUTER_ID");
        var outputAddress = GetEnv("NBN_REGIONHOST_OUTPUT_ADDRESS");
        var outputId = GetEnv("NBN_REGIONHOST_OUTPUT_ID");
        var tickAddress = GetEnv("NBN_REGIONHOST_TICK_ADDRESS");
        var tickId = GetEnv("NBN_REGIONHOST_TICK_ID");
        var nbnSha = GetEnv("NBN_REGIONHOST_NBN_SHA256") ?? string.Empty;
        var nbnSize = GetEnvLong("NBN_REGIONHOST_NBN_SIZE") ?? 0L;
        var nbsSha = GetEnv("NBN_REGIONHOST_NBS_SHA256");
        var nbsSize = GetEnvLong("NBN_REGIONHOST_NBS_SIZE");
        var storeUri = GetEnv("NBN_REGIONHOST_STORE_URI");

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
                case "--artifact-root":
                    if (i + 1 < args.Length)
                    {
                        artifactRoot = args[++i];
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
                case "--brain-id":
                    if (i + 1 < args.Length && Guid.TryParse(args[++i], out var brainValue))
                    {
                        brainId = brainValue;
                    }
                    continue;
                case "--region":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var regionValue))
                    {
                        regionId = regionValue;
                    }
                    continue;
                case "--neuron-start":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var neuronStartValue))
                    {
                        neuronStart = neuronStartValue;
                    }
                    continue;
                case "--neuron-count":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var neuronCountValue))
                    {
                        neuronCount = neuronCountValue;
                    }
                    continue;
                case "--shard-index":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out var shardIndexValue))
                    {
                        shardIndex = shardIndexValue;
                    }
                    continue;
                case "--shard-name":
                    if (i + 1 < args.Length)
                    {
                        shardName = args[++i];
                    }
                    continue;
                case "--router-address":
                    if (i + 1 < args.Length)
                    {
                        routerAddress = args[++i];
                    }
                    continue;
                case "--router-id":
                    if (i + 1 < args.Length)
                    {
                        routerId = args[++i];
                    }
                    continue;
                case "--output-address":
                    if (i + 1 < args.Length)
                    {
                        outputAddress = args[++i];
                    }
                    continue;
                case "--output-id":
                    if (i + 1 < args.Length)
                    {
                        outputId = args[++i];
                    }
                    continue;
                case "--tick-address":
                    if (i + 1 < args.Length)
                    {
                        tickAddress = args[++i];
                    }
                    continue;
                case "--tick-id":
                    if (i + 1 < args.Length)
                    {
                        tickId = args[++i];
                    }
                    continue;
                case "--nbn-sha256":
                case "--nbn":
                    if (i + 1 < args.Length)
                    {
                        nbnSha = args[++i];
                    }
                    continue;
                case "--nbn-size":
                    if (i + 1 < args.Length && long.TryParse(args[++i], out var nbnSizeValue))
                    {
                        nbnSize = nbnSizeValue;
                    }
                    continue;
                case "--nbs-sha256":
                case "--nbs":
                    if (i + 1 < args.Length)
                    {
                        nbsSha = args[++i];
                    }
                    continue;
                case "--nbs-size":
                    if (i + 1 < args.Length && long.TryParse(args[++i], out var nbsSizeValue))
                    {
                        nbsSize = nbsSizeValue;
                    }
                    continue;
                case "--store-uri":
                    if (i + 1 < args.Length)
                    {
                        storeUri = args[++i];
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

            if (arg.StartsWith("--artifact-root=", StringComparison.OrdinalIgnoreCase))
            {
                artifactRoot = arg.Substring("--artifact-root=".Length);
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

            if (arg.StartsWith("--brain-id=", StringComparison.OrdinalIgnoreCase)
                && Guid.TryParse(arg.Substring("--brain-id=".Length), out var brainInline))
            {
                brainId = brainInline;
                continue;
            }

            if (arg.StartsWith("--region=", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(arg.Substring("--region=".Length), out var regionInline))
            {
                regionId = regionInline;
                continue;
            }

            if (arg.StartsWith("--neuron-start=", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(arg.Substring("--neuron-start=".Length), out var neuronStartInline))
            {
                neuronStart = neuronStartInline;
                continue;
            }

            if (arg.StartsWith("--neuron-count=", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(arg.Substring("--neuron-count=".Length), out var neuronCountInline))
            {
                neuronCount = neuronCountInline;
                continue;
            }

            if (arg.StartsWith("--shard-index=", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(arg.Substring("--shard-index=".Length), out var shardIndexInline))
            {
                shardIndex = shardIndexInline;
                continue;
            }

            if (arg.StartsWith("--shard-name=", StringComparison.OrdinalIgnoreCase))
            {
                shardName = arg.Substring("--shard-name=".Length);
                continue;
            }

            if (arg.StartsWith("--router-address=", StringComparison.OrdinalIgnoreCase))
            {
                routerAddress = arg.Substring("--router-address=".Length);
                continue;
            }

            if (arg.StartsWith("--router-id=", StringComparison.OrdinalIgnoreCase))
            {
                routerId = arg.Substring("--router-id=".Length);
                continue;
            }

            if (arg.StartsWith("--output-address=", StringComparison.OrdinalIgnoreCase))
            {
                outputAddress = arg.Substring("--output-address=".Length);
                continue;
            }

            if (arg.StartsWith("--output-id=", StringComparison.OrdinalIgnoreCase))
            {
                outputId = arg.Substring("--output-id=".Length);
                continue;
            }

            if (arg.StartsWith("--tick-address=", StringComparison.OrdinalIgnoreCase))
            {
                tickAddress = arg.Substring("--tick-address=".Length);
                continue;
            }

            if (arg.StartsWith("--tick-id=", StringComparison.OrdinalIgnoreCase))
            {
                tickId = arg.Substring("--tick-id=".Length);
                continue;
            }

            if (arg.StartsWith("--nbn-sha256=", StringComparison.OrdinalIgnoreCase))
            {
                nbnSha = arg.Substring("--nbn-sha256=".Length);
                continue;
            }

            if (arg.StartsWith("--nbn=", StringComparison.OrdinalIgnoreCase))
            {
                nbnSha = arg.Substring("--nbn=".Length);
                continue;
            }

            if (arg.StartsWith("--nbn-size=", StringComparison.OrdinalIgnoreCase)
                && long.TryParse(arg.Substring("--nbn-size=".Length), out var nbnSizeInline))
            {
                nbnSize = nbnSizeInline;
                continue;
            }

            if (arg.StartsWith("--nbs-sha256=", StringComparison.OrdinalIgnoreCase))
            {
                nbsSha = arg.Substring("--nbs-sha256=".Length);
                continue;
            }

            if (arg.StartsWith("--nbs=", StringComparison.OrdinalIgnoreCase))
            {
                nbsSha = arg.Substring("--nbs=".Length);
                continue;
            }

            if (arg.StartsWith("--nbs-size=", StringComparison.OrdinalIgnoreCase)
                && long.TryParse(arg.Substring("--nbs-size=".Length), out var nbsSizeInline))
            {
                nbsSize = nbsSizeInline;
                continue;
            }

            if (arg.StartsWith("--store-uri=", StringComparison.OrdinalIgnoreCase))
            {
                storeUri = arg.Substring("--store-uri=".Length);
            }
        }

        if (string.IsNullOrWhiteSpace(shardName) && regionId >= 0)
        {
            shardName = $"region-{regionId}-shard-{shardIndex}";
        }

        return new RegionHostOptions(
            bindHost,
            port,
            advertisedHost,
            advertisedPort,
            artifactRoot,
            string.IsNullOrWhiteSpace(settingsHost) ? null : settingsHost,
            settingsPort,
            settingsName,
            brainId,
            regionId,
            neuronStart,
            neuronCount,
            shardIndex,
            shardName,
            routerAddress,
            routerId,
            outputAddress,
            outputId,
            tickAddress,
            tickId,
            nbnSha,
            nbnSize,
            nbsSha,
            nbsSize,
            storeUri);
    }

    private static string? GetEnv(string key) => Environment.GetEnvironmentVariable(key);

    private static int? GetEnvInt(string key)
    {
        var value = GetEnv(key);
        return int.TryParse(value, out var parsed) ? parsed : null;
    }

    private static long? GetEnvLong(string key)
    {
        var value = GetEnv(key);
        return long.TryParse(value, out var parsed) ? parsed : null;
    }

    private static Guid? GetEnvGuid(string key)
    {
        var value = GetEnv(key);
        return Guid.TryParse(value, out var parsed) ? parsed : null;
    }

    private static void PrintHelp()
    {
        Console.WriteLine("NBN RegionHost options:");
        Console.WriteLine("  --bind, --bind-host <host>       Host/interface to bind (default 127.0.0.1)");
        Console.WriteLine("  --port <port>                    Port to bind (default 12040)");
        Console.WriteLine("  --advertise, --advertise-host    Advertised host for remoting");
        Console.WriteLine("  --advertise-port <port>          Advertised port for remoting");
        Console.WriteLine("  --artifact-root <path>           Artifact store root path");
        Console.WriteLine("  --settings-host <host>           SettingsMonitor host (default 127.0.0.1)");
        Console.WriteLine("  --settings-port <port>           SettingsMonitor port (default 12010)");
        Console.WriteLine("  --settings-name <name>           SettingsMonitor actor name (default SettingsMonitor)");
        Console.WriteLine("  --brain-id <guid>                BrainId for shard messages");
        Console.WriteLine("  --region <id>                    Region id (0-31)");
        Console.WriteLine("  --neuron-start <index>           Neuron start offset within region");
        Console.WriteLine("  --neuron-count <count>           Neuron count (0 = to end)");
        Console.WriteLine("  --shard-index <index>            Shard index (default 0)");
        Console.WriteLine("  --shard-name <name>              Spawn name for shard actor");
        Console.WriteLine("  --router-address <host:port>     BrainSignalRouter address");
        Console.WriteLine("  --router-id <name>               BrainSignalRouter actor id/name");
        Console.WriteLine("  --output-address <host:port>     Output sink address");
        Console.WriteLine("  --output-id <name>               Output sink actor id/name");
        Console.WriteLine("  --tick-address <host:port>       Tick sink address (HiveMind)");
        Console.WriteLine("  --tick-id <name>                 Tick sink actor id/name");
        Console.WriteLine("  --nbn-sha256 <hex>               NBN artifact sha256 (required)");
        Console.WriteLine("  --nbn-size <bytes>               NBN artifact size");
        Console.WriteLine("  --nbs-sha256 <hex>               NBS artifact sha256 (optional)");
        Console.WriteLine("  --nbs-size <bytes>               NBS artifact size");
        Console.WriteLine("  --store-uri <uri>                Store URI label for artifact refs");
    }
}
