using Google.Protobuf;
using Nbn.Proto;
using Nbn.Runtime.RegionHost;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Shared.HiveMind;
using Nbn.Shared.Validation;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;
using ShardId32 = Nbn.Shared.Addressing.ShardId32;

Console.SetOut(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true });
Console.SetError(new StreamWriter(Console.OpenStandardError()) { AutoFlush = true });

var options = RegionHostOptions.FromArgs(args);
ValidateOptions(options);

var system = new ActorSystem();
var remoteConfig = RegionHostRemote.BuildConfig(options);
system.WithRemote(remoteConfig);
await system.Remote().StartAsync();

var store = new LocalArtifactStore(new ArtifactStoreOptions(options.ArtifactRootPath));
var nbnRef = BuildArtifactRef(options.NbnSha256, options.NbnSize, "application/x-nbn", options.StoreUri);
ArtifactRef? nbsRef = null;
if (!string.IsNullOrWhiteSpace(options.NbsSha256))
{
    nbsRef = BuildArtifactRef(options.NbsSha256, options.NbsSize ?? 0, "application/x-nbs", options.StoreUri);
}

var routerPid = RequirePid(options.RouterAddress, options.RouterId, "router");
var tickPid = RequirePid(options.TickAddress, options.TickId, "tick");
var outputPid = TryCreatePid(options.OutputAddress, options.OutputId);
if (options.RegionId == NbnConstants.OutputRegionId && outputPid is null)
{
    throw new InvalidOperationException("Output sink PID is required for output region shards.");
}

var shardId = ShardId32.From(options.RegionId, options.ShardIndex);
var load = await RegionShardArtifactLoader.LoadAsync(
    store,
    nbnRef,
    nbsRef,
    options.RegionId,
    options.NeuronStart,
    options.NeuronCount);

var routing = RegionShardRoutingTable.CreateSingleShard(load.Header.Regions);
var config = new RegionShardActorConfig(options.BrainId, shardId, routerPid, outputPid, tickPid, routing);
var shardProps = Props.FromProducer(() => new RegionShardActor(load.State, config));
var shardPid = system.Root.SpawnNamed(shardProps, options.ShardName);

system.Root.Send(tickPid, new RegisterShard(options.BrainId, options.RegionId, shardId, shardPid));

Console.WriteLine("NBN RegionHost online.");
Console.WriteLine($"Bind: {remoteConfig.Host}:{remoteConfig.Port}");
Console.WriteLine($"Advertised: {remoteConfig.AdvertisedHost ?? remoteConfig.Host}:{remoteConfig.AdvertisedPort ?? remoteConfig.Port}");
Console.WriteLine($"Shard: {PidLabel(shardPid)}");
Console.WriteLine($"Brain: {options.BrainId}");
Console.WriteLine($"Region: {options.RegionId} ({options.NeuronStart}-{options.NeuronStart + load.State.NeuronCount - 1})");
Console.WriteLine("Press Ctrl+C to shut down.");

var shutdown = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    shutdown.TrySetResult();
};

AppDomain.CurrentDomain.ProcessExit += (_, _) => shutdown.TrySetResult();

await shutdown.Task;

system.Root.Send(tickPid, new UnregisterShard(options.BrainId, shardId));

await system.Remote().ShutdownAsync(true);
await system.ShutdownAsync();

static ArtifactRef BuildArtifactRef(string shaHex, long sizeBytes, string mediaType, string? storeUri)
{
    if (string.IsNullOrWhiteSpace(shaHex))
    {
        throw new ArgumentException("Artifact sha256 is required.", nameof(shaHex));
    }

    byte[] bytes;
    try
    {
        bytes = Convert.FromHexString(shaHex.Trim());
    }
    catch (FormatException)
    {
        throw new ArgumentException("Artifact sha256 must be a hex string.", nameof(shaHex));
    }

    if (bytes.Length != 32)
    {
        throw new ArgumentException("Artifact sha256 must be 32 bytes.", nameof(shaHex));
    }

    if (sizeBytes < 0)
    {
        throw new ArgumentOutOfRangeException(nameof(sizeBytes), sizeBytes, "Artifact size must be non-negative.");
    }

    var reference = new ArtifactRef
    {
        Sha256 = new Sha256 { Value = ByteString.CopyFrom(bytes) },
        MediaType = mediaType,
        SizeBytes = (ulong)sizeBytes
    };

    if (!string.IsNullOrWhiteSpace(storeUri))
    {
        reference.StoreUri = storeUri;
    }

    return reference;
}

static PID RequirePid(string? address, string? name, string label)
{
    if (string.IsNullOrWhiteSpace(address) || string.IsNullOrWhiteSpace(name))
    {
        throw new InvalidOperationException($"{label} PID requires both address and id.");
    }

    return new PID(address, name);
}

static PID? TryCreatePid(string? address, string? name)
{
    if (string.IsNullOrWhiteSpace(address) || string.IsNullOrWhiteSpace(name))
    {
        return null;
    }

    return new PID(address, name);
}

static void ValidateOptions(RegionHostOptions options)
{
    if (options.BrainId == Guid.Empty)
    {
        throw new InvalidOperationException("BrainId is required (--brain-id or NBN_REGIONHOST_BRAIN_ID).");
    }

    if (!NbnInvariants.IsValidRegionId(options.RegionId))
    {
        throw new InvalidOperationException("Region id must be between 0 and 31.");
    }

    if (string.IsNullOrWhiteSpace(options.NbnSha256))
    {
        throw new InvalidOperationException("NBN artifact sha256 is required (--nbn-sha256).");
    }

    if (options.NeuronStart < 0)
    {
        throw new InvalidOperationException("Neuron start must be >= 0.");
    }

    if (options.NeuronCount < 0)
    {
        throw new InvalidOperationException("Neuron count must be >= 0.");
    }

    if (options.ShardIndex < 0 || (uint)options.ShardIndex > ShardId32.ShardIndexMask)
    {
        throw new InvalidOperationException("Shard index must fit in 16 bits.");
    }

    if (string.IsNullOrWhiteSpace(options.RouterAddress) || string.IsNullOrWhiteSpace(options.RouterId))
    {
        throw new InvalidOperationException("Router PID is required (--router-address/--router-id).");
    }

    if (string.IsNullOrWhiteSpace(options.TickAddress) || string.IsNullOrWhiteSpace(options.TickId))
    {
        throw new InvalidOperationException("Tick PID is required (--tick-address/--tick-id).");
    }
}

static string PidLabel(PID pid)
    => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";
