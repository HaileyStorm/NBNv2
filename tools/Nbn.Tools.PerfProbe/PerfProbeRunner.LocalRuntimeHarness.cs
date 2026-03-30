using Microsoft.Data.Sqlite;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Debug;
using Nbn.Proto.Io;
using Nbn.Proto.Repro;
using Nbn.Proto.Settings;
using Nbn.Proto.Signal;
using Nbn.Proto.Viz;
using Nbn.Runtime.Artifacts;
using Nbn.Runtime.Brain;
using Nbn.Runtime.HiveMind;
using Nbn.Runtime.IO;
using Nbn.Runtime.RegionHost;
using Nbn.Runtime.WorkerNode;
using Nbn.Shared;
using Nbn.Shared.HiveMind;
using Proto;
using Proto.Remote;
using Proto.Remote.GrpcNet;
using ProtoIo = Nbn.Proto.Io;
using ProtoControl = Nbn.Proto.Control;
using ProtoSettings = Nbn.Proto.Settings;

namespace Nbn.Tools.PerfProbe;

public static partial class PerfProbeRunner
{
    internal enum LocalRuntimeInputLoadProfile
    {
        None = 0,
        Continuous = 1,
        SeedOnce = 2
    }

    internal enum LocalRuntimeWorkloadProfile
    {
        StandardInputDriven = 0,
        ComputeDominantRecurrent = 1
    }

    internal sealed class LocalRuntimeHarness : IAsyncDisposable
    {
        private static readonly SemaphoreSlim BackendPreferenceGate = new(1, 1);
        private static readonly TimeSpan WorkerEndpointBootstrapTimeout = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan PlacementAssignmentTimeout = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan BrainInfoRegistrationTimeout = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan RoutingStabilizationTimeout = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan BackendProbeTimeout = TimeSpan.FromSeconds(2);
        private static readonly TimeSpan SpawnBrainTimeout = TimeSpan.FromSeconds(70);
        private static readonly TimeSpan PerfRuntimeConfigTimeout = TimeSpan.FromSeconds(2);

        private LocalRuntimeHarness(
            ActorSystem system,
            PID hiveMind,
            PID ioGateway,
            PID worker,
            string artifactRoot,
            int hiddenNeuronCount,
            uint inputWidth,
            ArtifactRef brainDef,
            string bootstrapIoEndpointActorName,
            RegionShardComputeBackendPreference computeBackendPreference,
            LocalRuntimeWorkloadProfile workloadProfile)
        {
            System = system;
            HiveMind = hiveMind;
            IoGateway = ioGateway;
            Worker = worker;
            ArtifactRoot = artifactRoot;
            HiddenNeuronCount = hiddenNeuronCount;
            InputWidth = inputWidth;
            BrainDef = brainDef;
            BootstrapIoEndpointActorName = bootstrapIoEndpointActorName;
            ComputeBackendPreference = computeBackendPreference;
            WorkloadProfile = workloadProfile;
        }

        public ActorSystem System { get; }
        public IRootContext Root => System.Root;
        public PID HiveMind { get; }
        public PID IoGateway { get; }
        public PID Worker { get; }
        public string ArtifactRoot { get; }
        public int HiddenNeuronCount { get; }
        public uint InputWidth { get; }
        public ArtifactRef BrainDef { get; }
        public string BootstrapIoEndpointActorName { get; }
        public RegionShardComputeBackendPreference ComputeBackendPreference { get; }
        public LocalRuntimeWorkloadProfile WorkloadProfile { get; }

        /// <summary>
        /// Creates an isolated in-process runtime harness with stable actor names and perf-safe defaults.
        /// </summary>
        public static async Task<LocalRuntimeHarness> CreateAsync(
            int hiddenNeuronCount,
            float targetTickHz,
            RegionShardComputeBackendPreference computeBackendPreference,
            CancellationToken cancellationToken,
            LocalRuntimeWorkloadProfile workloadProfile = LocalRuntimeWorkloadProfile.StandardInputDriven)
        {
            var benchmarkAvailability = new WorkerResourceAvailability(
                cpuPercent: 100,
                ramPercent: 100,
                storagePercent: 100,
                gpuComputePercent: 100,
                gpuVramPercent: 100);
            var artifactRoot = Path.Combine(Path.GetTempPath(), "nbn-perf-probe", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(artifactRoot);

            var inputWidth = workloadProfile == LocalRuntimeWorkloadProfile.ComputeDominantRecurrent
                ? 0
                : ResolvePerformanceInputWidth(hiddenNeuronCount);
            var nbnBytes = BuildPerformanceNbn(
                hiddenNeuronCount,
                inputWidth,
                DefaultOutputWidth,
                includeInputAssignments: workloadProfile != LocalRuntimeWorkloadProfile.ComputeDominantRecurrent);
            var store = new LocalArtifactStore(new ArtifactStoreOptions(artifactRoot));
            var manifest = await store.StoreAsync(new MemoryStream(nbnBytes), "application/x-nbn").ConfigureAwait(false);
            var brainDef = manifest.ArtifactId.Bytes.ToArray().ToArtifactRef((ulong)manifest.ByteLength, "application/x-nbn", artifactRoot);

            var system = new ActorSystem();
            system.WithRemote(
                RemoteConfig.BindToLocalhost(0).WithProtoMessages(
                    NbnCommonReflection.Descriptor,
                    NbnControlReflection.Descriptor,
                    NbnIoReflection.Descriptor,
                    NbnReproReflection.Descriptor,
                    NbnSignalsReflection.Descriptor,
                    NbnDebugReflection.Descriptor,
                    NbnVizReflection.Descriptor,
                    NbnSettingsReflection.Descriptor));
            await system.Remote().StartAsync().ConfigureAwait(false);
            var root = system.Root;
            var workerId = Guid.NewGuid();
            var actorNames = CreateHarnessActorNames();
            var ioPid = new PID(string.Empty, actorNames.IoGateway);

            var hiveMind = root.SpawnNamed(
                Props.FromProducer(() => new HiveMindActor(CreateHiveOptions(targetTickHz), ioPid: ioPid)),
                actorNames.HiveMind);
            var ioGateway = root.SpawnNamed(
                Props.FromProducer(() => new IoGatewayActor(CreateIoOptions(), hiveMindPid: hiveMind)),
                actorNames.IoGateway);
            _ = root.SpawnNamed(
                Props.FromProducer(() => new FixedBrainInfoActor(brainDef, (uint)inputWidth, DefaultOutputWidth)),
                actorNames.Metadata);
            var capabilityProvider = new WorkerNodeCapabilityProvider(availability: benchmarkAvailability);
            var worker = root.SpawnNamed(
                Props.FromProducer(() => new WorkerNodeActor(
                    workerId,
                    string.Empty,
                    artifactRootPath: artifactRoot,
                    resourceAvailability: benchmarkAvailability)),
                actorNames.Worker);

            PrimeWorkerDiscoveryEndpoints(root, worker, actorNames.HiveMind, actorNames.Metadata);
            PrimeWorkers(root, hiveMind, worker, workerId, capabilityProvider.GetCapabilities());
            await WaitForWorkerEndpointBootstrapAsync(root, worker, cancellationToken).ConfigureAwait(false);

            return new LocalRuntimeHarness(
                system,
                hiveMind,
                ioGateway,
                worker,
                artifactRoot,
                hiddenNeuronCount,
                (uint)inputWidth,
                brainDef,
                actorNames.Metadata,
                computeBackendPreference,
                workloadProfile);
        }

        /// <summary>
        /// Spawns a perf brain through IO, applies benchmark-safe runtime config, and waits for routing to settle.
        /// </summary>
        public async Task<Guid> SpawnBrainAsync(CancellationToken cancellationToken)
            => await WithScopedBackendPreferenceAsync(
                    ComputeBackendPreference,
                    async () =>
                    {
                        // The worker advertises the fixed metadata actor until the real IO gateway is ready for BrainInfo traffic.
                        PrimeWorkerDiscoveryEndpoints(Root, Worker, HiveMind.Id, BootstrapIoEndpointActorName);

                        var response = await RequestSpawnBrainAsync().ConfigureAwait(false);
                        var brainId = ResolveSpawnedBrainId(response);
                        await AwaitSpawnPlacementReadyAsync(brainId).ConfigureAwait(false);
                        PrimeWorkerDiscoveryEndpoints(Root, Worker, HiveMind.Id, IoGateway.Id);

                        if (WorkloadProfile == LocalRuntimeWorkloadProfile.ComputeDominantRecurrent)
                        {
                            await PrimeComputeDominantActivityAsync(brainId, cancellationToken).ConfigureAwait(false);
                        }

                        await ApplyBenchmarkSafeRuntimeConfigAsync(brainId).ConfigureAwait(false);
                        await WaitForRoutingStabilizationAsync(brainId, cancellationToken).ConfigureAwait(false);
                        return brainId;
                    })
                .ConfigureAwait(false);

        private static HarnessActorNames CreateHarnessActorNames()
            => new(
                HiveMind: $"hive-{Guid.NewGuid():N}",
                IoGateway: $"io-{Guid.NewGuid():N}",
                Metadata: $"brain-info-{Guid.NewGuid():N}",
                Worker: $"worker-{Guid.NewGuid():N}");

        private static async Task WaitForWorkerEndpointBootstrapAsync(
            IRootContext root,
            PID worker,
            CancellationToken cancellationToken)
            => await WaitForAsync(
                    async () =>
                    {
                        var snapshot = await root.RequestAsync<WorkerNodeActor.WorkerNodeSnapshot>(
                                worker,
                                new WorkerNodeActor.GetWorkerNodeSnapshot())
                            .ConfigureAwait(false);
                        return snapshot.HiveMindEndpoint.HasValue && snapshot.IoGatewayEndpoint.HasValue;
                    },
                    WorkerEndpointBootstrapTimeout,
                    cancellationToken)
                .ConfigureAwait(false);

        private Task<ProtoIo.SpawnBrainViaIOAck> RequestSpawnBrainAsync()
            => Root.RequestAsync<ProtoIo.SpawnBrainViaIOAck>(
                IoGateway,
                new ProtoIo.SpawnBrainViaIO
                {
                    Request = new ProtoControl.SpawnBrain
                    {
                        BrainDef = BrainDef
                    }
                },
                SpawnBrainTimeout);

        private static Guid ResolveSpawnedBrainId(ProtoIo.SpawnBrainViaIOAck response)
        {
            if (response.Ack is null || !response.Ack.BrainId.TryToGuid(out var brainId) || brainId == Guid.Empty)
            {
                throw new InvalidOperationException(
                    $"SpawnBrainViaIOAck did not return a brain id. failure={response.Ack?.FailureReasonCode} message={response.Ack?.FailureMessage}");
            }

            return brainId;
        }

        private async Task AwaitSpawnPlacementReadyAsync(Guid brainId)
        {
            var response = await Root.RequestAsync<ProtoIo.AwaitSpawnPlacementViaIOAck>(
                    IoGateway,
                    new ProtoIo.AwaitSpawnPlacementViaIO
                    {
                        BrainId = brainId.ToProtoUuid(),
                        TimeoutMs = (ulong)PlacementAssignmentTimeout.TotalMilliseconds
                    },
                    SpawnBrainTimeout)
                .ConfigureAwait(false);
            if (response.Ack is null
                || !response.Ack.BrainId.TryToGuid(out var awaitedBrainId)
                || awaitedBrainId == Guid.Empty
                || awaitedBrainId != brainId
                || !response.Ack.AcceptedForPlacement
                || !response.Ack.PlacementReady
                || !string.IsNullOrWhiteSpace(response.Ack.FailureReasonCode))
            {
                throw new InvalidOperationException(
                    $"AwaitSpawnPlacementViaIOAck did not confirm placement for brain {brainId:N}. failure={response.Ack?.FailureReasonCode} message={response.Ack?.FailureMessage}");
            }
        }

        private async Task ApplyBenchmarkSafeRuntimeConfigAsync(Guid brainId)
        {
            var plasticityAck = await RequestIoCommandAckAsync(
                    new ProtoIo.SetPlasticityEnabled
                    {
                        BrainId = brainId.ToProtoUuid(),
                        PlasticityEnabled = false,
                        PlasticityRate = 0f,
                        ProbabilisticUpdates = false,
                        PlasticityDelta = 0f,
                        PlasticityRebaseThreshold = 0,
                        PlasticityRebaseThresholdPct = 0f,
                        PlasticityEnergyCostModulationEnabled = false,
                        PlasticityEnergyCostReferenceTickCost = 0,
                        PlasticityEnergyCostResponseStrength = 1f,
                        PlasticityEnergyCostMinScale = 1f,
                        PlasticityEnergyCostMaxScale = 1f
                    },
                    $"Timed out applying perf plasticity config for brain {brainId:N}.")
                .ConfigureAwait(false);
            EnsureIoCommandSucceeded(
                plasticityAck,
                $"Failed to disable plasticity for perf brain {brainId:N}: {plasticityAck.Message}");

            var homeostasisAck = await RequestIoCommandAckAsync(
                    new ProtoIo.SetHomeostasisEnabled
                    {
                        BrainId = brainId.ToProtoUuid(),
                        HomeostasisEnabled = false,
                        HomeostasisTargetMode = ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero,
                        HomeostasisUpdateMode = ProtoControl.HomeostasisUpdateMode.HomeostasisUpdateProbabilisticQuantizedStep,
                        HomeostasisBaseProbability = 0f,
                        HomeostasisMinStepCodes = 1,
                        HomeostasisEnergyCouplingEnabled = false,
                        HomeostasisEnergyTargetScale = 1f,
                        HomeostasisEnergyProbabilityScale = 1f
                    },
                    $"Timed out applying perf homeostasis config for brain {brainId:N}.")
                .ConfigureAwait(false);
            EnsureIoCommandSucceeded(
                homeostasisAck,
                $"Failed to disable homeostasis for perf brain {brainId:N}: {homeostasisAck.Message}");
        }

        private async Task<ProtoIo.IoCommandAck> RequestIoCommandAckAsync(object command, string timeoutMessage)
        {
            try
            {
                return await Root.RequestAsync<ProtoIo.IoCommandAck>(
                        IoGateway,
                        command,
                        PerfRuntimeConfigTimeout)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(timeoutMessage, ex);
            }
        }

        private static void EnsureIoCommandSucceeded(ProtoIo.IoCommandAck ack, string failureMessage)
        {
            if (!ack.Success)
            {
                throw new InvalidOperationException(failureMessage);
            }
        }

        private async Task WaitForRoutingStabilizationAsync(Guid brainId, CancellationToken cancellationToken)
        {
            var observation = new RoutingReadinessObservation();
            var deadline = DateTime.UtcNow + RoutingStabilizationTimeout;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (DateTime.UtcNow >= deadline)
                {
                    throw new InvalidOperationException(observation.BuildFailureMessage(brainId));
                }

                observation.LastHosted = await Root.RequestAsync<WorkerNodeActor.HostedBrainSnapshot>(
                        Worker,
                        new WorkerNodeActor.GetHostedBrainSnapshot(brainId),
                        BackendProbeTimeout)
                    .ConfigureAwait(false);
                if (observation.LastHosted.RegionShardCount == 0 || observation.LastHosted.HiddenShardPid is null)
                {
                    await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var controlPid = observation.LastHosted.SignalRouterPid ?? observation.LastHosted.BrainRootPid;
                if (controlPid is null)
                {
                    await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                observation.LastControlPid = FormatPid(controlPid);
                observation.LastHiveRouting = await Root.RequestAsync<ProtoControl.BrainRoutingInfo>(
                        HiveMind,
                        new ProtoControl.GetBrainRouting
                        {
                            BrainId = brainId.ToProtoUuid()
                        },
                        BackendProbeTimeout)
                    .ConfigureAwait(false);
                var controlKnownByHiveMind =
                    PidLabelRefersToActor(observation.LastHiveRouting.SignalRouterPid, controlPid)
                    || PidLabelRefersToActor(observation.LastHiveRouting.BrainRootPid, controlPid);
                if (!controlKnownByHiveMind
                    || observation.LastHiveRouting.ShardCount == 0
                    || observation.LastHiveRouting.RoutingCount == 0)
                {
                    await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var routing = await Root.RequestAsync<RoutingTableSnapshot>(
                        controlPid,
                        new GetRoutingTable(),
                        BackendProbeTimeout)
                    .ConfigureAwait(false);
                observation.LastControllerRouteCount = routing.Count;
                observation.LastHiddenRoutePresent = routing.Routes.Any(static route => route.ShardId.RegionId == 1 && route.ShardId.ShardIndex == 0);
                if (observation.LastHiddenRoutePresent)
                {
                    return;
                }

                await Task.Delay(50, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task PrimeComputeDominantActivityAsync(Guid brainId, CancellationToken cancellationToken)
        {
            if (HiddenNeuronCount <= 0)
            {
                return;
            }

            foreach (var neuronId in BuildComputeDominantSeedNeuronIds(HiddenNeuronCount))
            {
                Root.Send(IoGateway, new ProtoIo.RuntimeNeuronPulse
                {
                    BrainId = brainId.ToProtoUuid(),
                    TargetRegionId = 1,
                    TargetNeuronId = (uint)neuronId,
                    Value = 1f
                });
            }

            await Task.Delay(50, cancellationToken).ConfigureAwait(false);
        }

        private static IReadOnlyList<int> BuildComputeDominantSeedNeuronIds(int hiddenNeuronCount)
        {
            var seedCount = Math.Min(16, Math.Max(1, hiddenNeuronCount));
            var seeds = new int[seedCount];
            var stride = Math.Max(1, hiddenNeuronCount / seedCount);
            for (var i = 0; i < seedCount; i++)
            {
                seeds[i] = Math.Min(hiddenNeuronCount - 1, i * stride);
            }

            return seeds;
        }

        /// <summary>
        /// Confirms that the hidden shard executed on the requested backend when backend forcing is enabled.
        /// </summary>
        public async Task<string?> VerifyBackendExecutionAsync(
            IReadOnlyCollection<Guid> brainIds,
            CancellationToken cancellationToken)
        {
            if (ComputeBackendPreference == RegionShardComputeBackendPreference.Auto || brainIds.Count == 0)
            {
                return null;
            }

            foreach (var brainId in brainIds)
            {
                RegionShardBackendExecutionInfo info = default;
                var observedExecution = false;
                for (var attempt = 0; attempt < 20; attempt++)
                {
                    info = await Root.RequestAsync<RegionShardBackendExecutionInfo>(
                            Worker,
                            new WorkerNodeActor.GetHostedRegionShardBackendExecutionInfo(brainId, RegionId: 1, ShardIndex: 0),
                            BackendProbeTimeout)
                        .ConfigureAwait(false);
                    if (info.HasExecuted)
                    {
                        observedExecution = true;
                        break;
                    }

                    await Task.Delay(50, cancellationToken).ConfigureAwait(false);
                }

                if (string.Equals(info.BackendName, "unavailable", StringComparison.Ordinal))
                {
                    return string.IsNullOrWhiteSpace(info.FallbackReason)
                        ? $"backend_execution_query_failed:region_shard_unavailable:{brainId:N}"
                        : info.FallbackReason;
                }

                if (!observedExecution)
                {
                    return $"backend_execution_query_failed:region_shard_never_executed:{brainId:N}";
                }

                var shouldUseGpu = ComputeBackendPreference == RegionShardComputeBackendPreference.Gpu;
                if (info.UsedGpu != shouldUseGpu)
                {
                    var mode = shouldUseGpu ? "gpu" : "cpu";
                    var reason = string.IsNullOrWhiteSpace(info.FallbackReason) ? "none" : info.FallbackReason;
                    return $"expected_{mode}_backend_but_observed_{info.BackendName}:fallback={reason}";
                }
            }

            return null;
        }

        /// <summary>
        /// Measures observed HiveMind tick throughput while the harness applies the requested input load profile.
        /// </summary>
        public async Task<double> MeasureTickRateAsync(
            IReadOnlyCollection<Guid> brainIds,
            float targetTickHz,
            TimeSpan duration,
            CancellationToken cancellationToken,
            LocalRuntimeInputLoadProfile inputLoadProfile = LocalRuntimeInputLoadProfile.Continuous)
        {
            var before = await Root.RequestAsync<ProtoControl.HiveMindStatus>(
                    HiveMind,
                    new ProtoControl.GetHiveMindStatus())
                .ConfigureAwait(false);

            StartTickLoop();
            using var inputLoadCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var inputLoadTask = RunInputLoadAsync(brainIds, targetTickHz, inputLoadCancellation.Token, inputLoadProfile);
            try
            {
                await Task.Delay(duration, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                inputLoadCancellation.Cancel();
                try
                {
                    await inputLoadTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }

                StopTickLoop();
            }

            var after = await Root.RequestAsync<ProtoControl.HiveMindStatus>(
                    HiveMind,
                    new ProtoControl.GetHiveMindStatus())
                .ConfigureAwait(false);
            var completedTicks = after.LastCompletedTickId >= before.LastCompletedTickId
                ? after.LastCompletedTickId - before.LastCompletedTickId
                : 0UL;
            return completedTicks / Math.Max(duration.TotalSeconds, 0.001d);
        }

        public void StartTickLoop()
            => Root.Send(HiveMind, new StartTickLoop());

        public void StopTickLoop()
            => Root.Send(HiveMind, new StopTickLoop());

        public async Task RunInputLoadAsync(
            IReadOnlyCollection<Guid> brainIds,
            float targetTickHz,
            CancellationToken cancellationToken,
            LocalRuntimeInputLoadProfile inputLoadProfile = LocalRuntimeInputLoadProfile.Continuous)
        {
            if (brainIds.Count == 0 || inputLoadProfile == LocalRuntimeInputLoadProfile.None)
            {
                return;
            }

            var values = Enumerable.Repeat(1f, (int)InputWidth).ToArray();
            void SendBurst()
            {
                foreach (var brainId in brainIds)
                {
                    Root.Send(IoGateway, new ProtoIo.InputVector
                    {
                        BrainId = brainId.ToProtoUuid(),
                        Values = { values }
                    });
                }
            }

            SendBurst();
            if (inputLoadProfile == LocalRuntimeInputLoadProfile.SeedOnce)
            {
                return;
            }

            var delayMs = Math.Max(5, (int)Math.Round(1000d / Math.Max(targetTickHz * 2d, 1d)));
            while (!cancellationToken.IsCancellationRequested)
            {
                SendBurst();
                await Task.Delay(delayMs, cancellationToken).ConfigureAwait(false);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await System.Remote().ShutdownAsync(true).ConfigureAwait(false);
            await System.ShutdownAsync().ConfigureAwait(false);
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(ArtifactRoot))
            {
                Directory.Delete(ArtifactRoot, recursive: true);
            }
        }

        private static string FormatPid(PID? pid)
            => pid is null
                ? "<none>"
                : string.IsNullOrWhiteSpace(pid.Address)
                    ? pid.Id
                    : $"{pid.Address}/{pid.Id}";

        private static bool PidLabelRefersToActor(string? pidLabel, PID pid)
            => string.Equals(
                NormalizeObservedActorId(ExtractActorId(pidLabel)),
                NormalizeObservedActorId(pid.Id),
                StringComparison.Ordinal);

        private static string ExtractActorId(string? pidLabel)
        {
            if (string.IsNullOrWhiteSpace(pidLabel))
            {
                return string.Empty;
            }

            var slash = pidLabel.IndexOf('/');
            return slash >= 0 && slash < pidLabel.Length - 1
                ? pidLabel[(slash + 1)..]
                : pidLabel;
        }

        private static string NormalizeObservedActorId(string actorId)
        {
            if (string.IsNullOrWhiteSpace(actorId) || !actorId.Contains('/', StringComparison.Ordinal))
            {
                return actorId;
            }

            var parts = actorId
                .Split('/', StringSplitOptions.RemoveEmptyEntries)
                .Where(static part => !part.StartsWith("$", StringComparison.Ordinal))
                .ToArray();
            return parts.Length == 0 ? actorId : string.Join("/", parts);
        }

        private static async Task<T> WithScopedBackendPreferenceAsync<T>(
            RegionShardComputeBackendPreference preference,
            Func<Task<T>> action)
        {
            if (preference == RegionShardComputeBackendPreference.Auto)
            {
                return await action().ConfigureAwait(false);
            }

            await BackendPreferenceGate.WaitAsync().ConfigureAwait(false);
            var previous = Environment.GetEnvironmentVariable(RegionShardComputeBackendPreferenceResolver.EnvironmentVariableName);
            try
            {
                Environment.SetEnvironmentVariable(
                    RegionShardComputeBackendPreferenceResolver.EnvironmentVariableName,
                    preference.ToString().ToLowerInvariant());
                return await action().ConfigureAwait(false);
            }
            finally
            {
                Environment.SetEnvironmentVariable(
                    RegionShardComputeBackendPreferenceResolver.EnvironmentVariableName,
                    previous);
                BackendPreferenceGate.Release();
            }
        }

        private readonly record struct HarnessActorNames(
            string HiveMind,
            string IoGateway,
            string Metadata,
            string Worker);

        private sealed class RoutingReadinessObservation
        {
            public WorkerNodeActor.HostedBrainSnapshot? LastHosted { get; set; }
            public ProtoControl.BrainRoutingInfo? LastHiveRouting { get; set; }
            public string LastControlPid { get; set; } = "<none>";
            public bool LastHiddenRoutePresent { get; set; }
            public int LastControllerRouteCount { get; set; } = -1;

            public string BuildFailureMessage(Guid brainId)
                => $"Local runtime routing did not stabilize for brain {brainId:N}. " +
                   $"hostedControl={LastControlPid} regionShards={LastHosted?.RegionShardCount ?? 0} hiddenShard={FormatPid(LastHosted?.HiddenShardPid)} " +
                   $"hiveRoot={LastHiveRouting?.BrainRootPid ?? "<none>"} hiveRouter={LastHiveRouting?.SignalRouterPid ?? "<none>"} " +
                   $"hiveShardCount={LastHiveRouting?.ShardCount ?? 0} hiveRoutingCount={LastHiveRouting?.RoutingCount ?? 0} " +
                   $"controllerRouteCount={LastControllerRouteCount} hiddenRoutePresent={LastHiddenRoutePresent}";
        }
    }

    private sealed class FixedBrainInfoActor : IActor
    {
        private readonly ArtifactRef _brainDef;
        private readonly uint _inputWidth;
        private readonly uint _outputWidth;

        public FixedBrainInfoActor(ArtifactRef brainDef, uint inputWidth, uint outputWidth)
        {
            _brainDef = brainDef.Clone();
            _inputWidth = inputWidth;
            _outputWidth = outputWidth;
        }

        public Task ReceiveAsync(IContext context)
        {
            if (context.Message is not ProtoIo.BrainInfoRequest request)
            {
                return Task.CompletedTask;
            }

            context.Respond(new ProtoIo.BrainInfo
            {
                BrainId = request.BrainId,
                InputWidth = _inputWidth,
                OutputWidth = _outputWidth,
                BaseDefinition = _brainDef.Clone(),
                LastSnapshot = new ArtifactRef()
            });

            return Task.CompletedTask;
        }
    }

    private static IoOptions CreateIoOptions()
        => new(
            BindHost: "127.0.0.1",
            Port: 0,
            AdvertisedHost: null,
            AdvertisedPort: null,
            GatewayName: IoNames.Gateway,
            ServerName: "nbn.io.perf-probe",
            SettingsHost: null,
            SettingsPort: 0,
            SettingsName: "SettingsMonitor",
            HiveMindAddress: null,
            HiveMindName: null,
            ReproAddress: null,
            ReproName: null);

    private static HiveMindOptions CreateHiveOptions(float targetTickHz)
        => new(
            BindHost: "127.0.0.1",
            Port: 0,
            AdvertisedHost: null,
            AdvertisedPort: null,
            TargetTickHz: targetTickHz,
            MinTickHz: Math.Max(1f, targetTickHz * 0.5f),
            ComputeTimeoutMs: 1_000,
            DeliverTimeoutMs: 1_000,
            BackpressureDecay: 0.9f,
            BackpressureRecovery: 1.1f,
            LateBackpressureThreshold: 2,
            TimeoutRescheduleThreshold: 3,
            TimeoutPauseThreshold: 6,
            RescheduleMinTicks: 10,
            RescheduleMinMinutes: 1,
            RescheduleQuietMs: 50,
            RescheduleSimulatedMs: 50,
            AutoStart: false,
            EnableOpenTelemetry: false,
            EnableOtelMetrics: false,
            EnableOtelTraces: false,
            EnableOtelConsoleExporter: false,
            OtlpEndpoint: null,
            ServiceName: "nbn.hivemind.perf-probe",
            SettingsDbPath: null,
            SettingsHost: null,
            SettingsPort: 0,
            SettingsName: "SettingsMonitor",
            IoAddress: null,
            IoName: null,
            WorkerInventoryRefreshMs: 2_000,
            WorkerInventoryStaleAfterMs: 10_000,
            PlacementAssignmentTimeoutMs: 1_000,
            PlacementAssignmentRetryBackoffMs: 10,
            PlacementAssignmentMaxRetries: 1,
            PlacementReconcileTimeoutMs: 1_000);

    private static void PrimeWorkerDiscoveryEndpoints(IRootContext root, PID workerPid, string hiveName, string ioName)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var known = new Dictionary<string, ServiceEndpointRegistration>(StringComparer.Ordinal)
        {
            [ServiceEndpointSettings.HiveMindKey] = new ServiceEndpointRegistration(
                ServiceEndpointSettings.HiveMindKey,
                new ServiceEndpoint(string.Empty, hiveName),
                nowMs),
            [ServiceEndpointSettings.IoGatewayKey] = new ServiceEndpointRegistration(
                ServiceEndpointSettings.IoGatewayKey,
                new ServiceEndpoint(string.Empty, ioName),
                nowMs)
        };

        root.Send(workerPid, new WorkerNodeActor.DiscoverySnapshotApplied(known));
    }

    private static void PrimeWorkers(
        IRootContext root,
        PID hiveMind,
        PID workerPid,
        Guid workerId,
        ProtoSettings.NodeCapabilities capabilities)
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        root.Send(hiveMind, new ProtoSettings.WorkerInventorySnapshotResponse
        {
            SnapshotMs = (ulong)nowMs,
            Workers =
            {
                new ProtoSettings.WorkerReadinessCapability
                {
                    NodeId = workerId.ToProtoUuid(),
                    Address = string.Empty,
                    RootActorName = workerPid.Id,
                    IsAlive = true,
                    IsReady = true,
                    LastSeenMs = (ulong)nowMs,
                    HasCapabilities = true,
                    CapabilityTimeMs = (ulong)nowMs,
                    Capabilities = capabilities.Clone()
                }
            }
        });
    }

    private static async Task WaitForAsync(
        Func<Task<bool>> predicate,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        using var timeoutCancellation = new CancellationTokenSource(timeout);
        using var linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCancellation.Token);
        while (true)
        {
            if (await predicate().ConfigureAwait(false))
            {
                return;
            }

            await Task.Delay(20, linkedCancellation.Token).ConfigureAwait(false);
        }
    }
}
