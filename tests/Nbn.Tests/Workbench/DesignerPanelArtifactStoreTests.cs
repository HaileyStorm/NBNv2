using Microsoft.Data.Sqlite;
using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Proto.Settings;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Tests.TestSupport;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tests.Workbench;

[Collection("ArtifactEnvSerial")]
public sealed class DesignerPanelArtifactStoreTests
{
    [Fact]
    public async Task WorkbenchArtifactPublisher_PublishesHttpArtifactsReadableByHttpArtifactStore()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-workbench-publisher-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var payload = Enumerable.Range(0, 256).Select(static value => (byte)value).ToArray();
            await using var publisher = new WorkbenchArtifactPublisher();
            Assert.True(
                LocalPortAllocator.TryFindAvailablePort(
                    NetworkAddressDefaults.LoopbackHost,
                    preferredStartPort: 19100,
                    reservedPorts: null,
                    out var port,
                    out var error),
                error);

            var published = await publisher.PublishAsync(
                payload,
                "application/x-nbn",
                artifactRoot,
                NetworkAddressDefaults.LoopbackHost,
                preferredPort: port);

            Assert.StartsWith($"http://127.0.0.1:{port}/", published.ArtifactRef.StoreUri, StringComparison.OrdinalIgnoreCase);
            Assert.Null(published.AttentionMessage);

            var store = new HttpArtifactStore(published.ArtifactRef.StoreUri);
            var hash = new Sha256Hash(published.ArtifactRef.ToSha256Bytes());
            var manifest = await store.TryGetManifestAsync(hash);
            Assert.NotNull(manifest);
            Assert.Equal(payload.LongLength, manifest!.ByteLength);
            Assert.Equal("application/x-nbn", manifest.MediaType);

            await using var fullStream = await store.TryOpenArtifactAsync(hash);
            Assert.NotNull(fullStream);
            using var fullBuffer = new MemoryStream();
            await fullStream!.CopyToAsync(fullBuffer);
            Assert.Equal(payload, fullBuffer.ToArray());

            await using var rangeStream = await store.TryOpenArtifactRangeAsync(hash, offset: 32, length: 40);
            Assert.NotNull(rangeStream);
            using var rangeBuffer = new MemoryStream();
            await rangeStream!.CopyToAsync(rangeBuffer);
            Assert.Equal(payload.Skip(32).Take(40).ToArray(), rangeBuffer.ToArray());
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task WorkbenchArtifactPublisher_PromotesRegisteredArtifactsToHttpArtifactRefs()
    {
        using var remoteScope = new RegisteredArtifactStoreScope();
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-workbench-promote-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        Assert.True(
            LocalPortAllocator.TryFindAvailablePort(
                NetworkAddressDefaults.LoopbackHost,
                preferredStartPort: 19120,
                reservedPorts: null,
                out var port,
                out var error),
            error);

        try
        {
            var payload = Enumerable.Range(0, 128).Select(static value => (byte)value).ToArray();
            var manifest = await remoteScope.Store.StoreAsync(new MemoryStream(payload, writable: false), "application/x-nbn");
            var storedRef = manifest.ArtifactId.Bytes.ToArray().ToArtifactRef(
                (ulong)manifest.ByteLength,
                "application/x-nbn",
                remoteScope.StoreUri);

            await using var publisher = new WorkbenchArtifactPublisher();
            var promoted = await publisher.PromoteAsync(
                storedRef,
                artifactRoot,
                NetworkAddressDefaults.LoopbackHost,
                preferredPort: port);

            Assert.Equal(storedRef.ToSha256Hex(), promoted.ArtifactRef.ToSha256Hex());
            Assert.StartsWith($"http://127.0.0.1:{port}/", promoted.ArtifactRef.StoreUri, StringComparison.OrdinalIgnoreCase);
            Assert.Null(promoted.AttentionMessage);
            await AssertHttpArtifactReadableAsync(promoted.ArtifactRef, payload);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task WorkbenchArtifactPublisher_ReusesStablePreferredPortAcrossDifferentBackingRoots()
    {
        var firstRoot = Path.Combine(Path.GetTempPath(), $"nbn-workbench-publisher-a-{Guid.NewGuid():N}");
        var secondRoot = Path.Combine(Path.GetTempPath(), $"nbn-workbench-publisher-b-{Guid.NewGuid():N}");
        Directory.CreateDirectory(firstRoot);
        Directory.CreateDirectory(secondRoot);

        Assert.True(
            LocalPortAllocator.TryFindAvailablePort(
                NetworkAddressDefaults.LoopbackHost,
                preferredStartPort: 19150,
                reservedPorts: null,
                out var port,
                out var error),
            error);

        try
        {
            var firstPayload = Enumerable.Range(0, 64).Select(static value => (byte)value).ToArray();
            var secondPayload = Enumerable.Range(64, 64).Select(static value => (byte)value).ToArray();
            await using var publisher = new WorkbenchArtifactPublisher();

            var first = await publisher.PublishAsync(
                firstPayload,
                "application/x-nbn",
                firstRoot,
                NetworkAddressDefaults.LoopbackHost,
                preferredPort: port);
            var second = await publisher.PublishAsync(
                secondPayload,
                "application/x-nbn",
                secondRoot,
                NetworkAddressDefaults.LoopbackHost,
                preferredPort: port);

            Assert.StartsWith($"http://127.0.0.1:{port}/", first.ArtifactRef.StoreUri, StringComparison.OrdinalIgnoreCase);
            Assert.StartsWith($"http://127.0.0.1:{port}/", second.ArtifactRef.StoreUri, StringComparison.OrdinalIgnoreCase);

            await AssertHttpArtifactReadableAsync(first.ArtifactRef, firstPayload);
            await AssertHttpArtifactReadableAsync(second.ArtifactRef, secondPayload);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(firstRoot))
            {
                Directory.Delete(firstRoot, recursive: true);
            }

            if (Directory.Exists(secondRoot))
            {
                Directory.Delete(secondRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task WorkbenchArtifactPublisher_ReusesStablePreferredPortAcrossPublisherInstances()
    {
        var firstRoot = Path.Combine(Path.GetTempPath(), $"nbn-workbench-publisher-inst-a-{Guid.NewGuid():N}");
        var secondRoot = Path.Combine(Path.GetTempPath(), $"nbn-workbench-publisher-inst-b-{Guid.NewGuid():N}");
        Directory.CreateDirectory(firstRoot);
        Directory.CreateDirectory(secondRoot);

        Assert.True(
            LocalPortAllocator.TryFindAvailablePort(
                NetworkAddressDefaults.LoopbackHost,
                preferredStartPort: 19180,
                reservedPorts: null,
                out var port,
                out var error),
            error);

        var firstPublisher = new WorkbenchArtifactPublisher();
        var secondPublisher = new WorkbenchArtifactPublisher();
        try
        {
            var firstPayload = Enumerable.Range(0, 32).Select(static value => (byte)value).ToArray();
            var secondPayload = Enumerable.Range(32, 32).Select(static value => (byte)value).ToArray();
            var thirdPayload = Enumerable.Range(64, 32).Select(static value => (byte)value).ToArray();

            var first = await firstPublisher.PublishAsync(
                firstPayload,
                "application/x-nbn",
                firstRoot,
                NetworkAddressDefaults.LoopbackHost,
                preferredPort: port);
            var second = await secondPublisher.PublishAsync(
                secondPayload,
                "application/x-nbn",
                secondRoot,
                NetworkAddressDefaults.LoopbackHost,
                preferredPort: port);

            Assert.StartsWith($"http://127.0.0.1:{port}/", first.ArtifactRef.StoreUri, StringComparison.OrdinalIgnoreCase);
            Assert.StartsWith($"http://127.0.0.1:{port}/", second.ArtifactRef.StoreUri, StringComparison.OrdinalIgnoreCase);
            await AssertHttpArtifactReadableAsync(first.ArtifactRef, firstPayload);
            await AssertHttpArtifactReadableAsync(second.ArtifactRef, secondPayload);

            await firstPublisher.DisposeAsync();

            var third = await secondPublisher.PublishAsync(
                thirdPayload,
                "application/x-nbn",
                secondRoot,
                NetworkAddressDefaults.LoopbackHost,
                preferredPort: port);

            Assert.StartsWith($"http://127.0.0.1:{port}/", third.ArtifactRef.StoreUri, StringComparison.OrdinalIgnoreCase);
            await AssertHttpArtifactReadableAsync(third.ArtifactRef, thirdPayload);
        }
        finally
        {
            await secondPublisher.DisposeAsync();
            await firstPublisher.DisposeAsync();
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(firstRoot))
            {
                Directory.Delete(firstRoot, recursive: true);
            }

            if (Directory.Exists(secondRoot))
            {
                Directory.Delete(secondRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task StoreCurrentDefinitionArtifactAsync_RegisteredNonFileStore_UsesResolver_AndReloadsFromArtifactRef()
    {
        using var remoteScope = new RegisteredArtifactStoreScope();
        var vm = CreateViewModel();
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            vm.NewBrainCommand.Execute(null);
            vm.ArtifactStoreUri = remoteScope.StoreUri;
        });

        var artifactRef = await vm.StoreCurrentDefinitionArtifactAsync();

        Assert.NotNull(artifactRef);
        Assert.Equal(remoteScope.StoreUri, artifactRef!.StoreUri);
        Assert.Equal("application/x-nbn", artifactRef.MediaType);
        Assert.Equal(1, remoteScope.Store.StoreCalls);
        Assert.Equal(artifactRef.ToSha256Hex(), vm.DefinitionArtifactShaText);
        Assert.Equal(remoteScope.StoreUri, vm.DefinitionArtifactStoreUriText);
        Assert.Contains("memory+test://", vm.DefinitionArtifactSummary, StringComparison.OrdinalIgnoreCase);

        AvaloniaTestHost.RunOnUiThread(() => vm.ResetBrainCommand.Execute(null));
        var loaded = await vm.LoadDefinitionArtifactFromCurrentReferenceAsync();

        Assert.True(loaded);
        Assert.NotNull(vm.Brain);
        Assert.Equal("NBN imported.", vm.Status);
        Assert.Equal(1, remoteScope.Store.OpenCalls);
    }

    private static async Task AssertHttpArtifactReadableAsync(ArtifactRef artifactRef, byte[] expectedPayload)
    {
        var store = new HttpArtifactStore(artifactRef.StoreUri);
        var hash = new Sha256Hash(artifactRef.ToSha256Bytes());
        await using var stream = await store.TryOpenArtifactAsync(hash);
        Assert.NotNull(stream);
        using var buffer = new MemoryStream();
        await stream!.CopyToAsync(buffer);
        Assert.Equal(expectedPayload, buffer.ToArray());
    }

    [Fact]
    public async Task LoadDefinitionArtifactFromCurrentReferenceAsync_UnregisteredNonFileStore_ReturnsActionableFailure()
    {
        var vm = CreateViewModel();
        AvaloniaTestHost.RunOnUiThread(() =>
        {
            vm.DefinitionArtifactShaText = new string('a', 64);
            vm.DefinitionArtifactStoreUriText = "memory+missing://artifact-store/main";
        });

        var loaded = await vm.LoadDefinitionArtifactFromCurrentReferenceAsync();

        Assert.False(loaded);
        Assert.Contains("No artifact store adapter is registered", vm.Status, StringComparison.Ordinal);
        Assert.Contains("memory+missing://artifact-store/main", vm.Status, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StoreCurrentSnapshotArtifactAsync_LocalStore_AllowsRoundTripReload()
    {
        var artifactRoot = Path.Combine(Path.GetTempPath(), $"nbn-designer-snapshot-{Guid.NewGuid():N}");
        Directory.CreateDirectory(artifactRoot);

        try
        {
            var vm = CreateViewModel();
            AvaloniaTestHost.RunOnUiThread(() =>
            {
                vm.NewBrainCommand.Execute(null);
                vm.ArtifactStoreUri = artifactRoot;
            });

            var artifactRef = await vm.StoreCurrentSnapshotArtifactAsync();

            Assert.NotNull(artifactRef);
            Assert.Equal(artifactRoot, artifactRef!.StoreUri);
            Assert.Equal("application/x-nbs", artifactRef.MediaType);
            Assert.Equal(artifactRef.ToSha256Hex(), vm.SnapshotArtifactShaText);
            Assert.Equal(artifactRoot, vm.SnapshotArtifactStoreUriText);
            Assert.Contains("Snapshot:", vm.SnapshotArtifactSummary, StringComparison.Ordinal);

            var reloaded = await vm.LoadSnapshotArtifactFromCurrentReferenceAsync();

            Assert.True(reloaded);
            Assert.True(vm.IsSnapshotLoaded);
            Assert.Contains("NBS imported from artifact reference.", vm.Status, StringComparison.Ordinal);
            Assert.Contains("Loaded NBS", vm.LoadedSummary, StringComparison.Ordinal);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(artifactRoot))
            {
                Directory.Delete(artifactRoot, recursive: true);
            }
        }
    }

    [Fact]
    public async Task RestoreBrainFromCurrentArtifactReferencesAsync_ForwardsDefinitionAndSnapshotRefs_ToRuntime()
    {
        using var remoteScope = new RegisteredArtifactStoreScope();
        Assert.True(
            LocalPortAllocator.TryFindAvailablePort(
                NetworkAddressDefaults.LoopbackHost,
                preferredStartPort: 19210,
                reservedPorts: null,
                out var artifactPort,
                out var error),
            error);
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true,
            HiveMindConnected = true,
            IoConnected = true,
            LocalBindHost = NetworkAddressDefaults.LoopbackHost,
            LocalPortText = (artifactPort - 1).ToString(),
            SettingsPortText = "bad",
            HiveMindPortText = "bad",
            IoPortText = "bad"
        };
        var spawnedBrainId = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            PlacementAck = new PlacementAck { Accepted = true, Message = "accepted" },
            AwaitSpawnPlacementFactory = (brainId, timeoutMs) => new SpawnBrainAck
            {
                BrainId = brainId.ToProtoUuid(),
                AcceptedForPlacement = true,
                PlacementReady = true
            },
            BrainListFactory = () => BuildBrainList(spawnedBrainId, "Active"),
            PlacementLifecycleFactory = requestedBrainId => requestedBrainId == spawnedBrainId
                ? BuildPlacementLifecycle(requestedBrainId, PlacementLifecycleState.PlacementLifecycleRunning, registeredShards: 3)
                : null
        };
        var artifactPublisher = new FakeWorkbenchArtifactPublisher();
        var vm = CreateViewModel(connections, client, artifactPublisher);
        Guid designBrainId = Guid.Empty;

        AvaloniaTestHost.RunOnUiThread(() =>
        {
            vm.NewBrainCommand.Execute(null);
            designBrainId = vm.Brain!.BrainId;
            spawnedBrainId = designBrainId;
            vm.ArtifactStoreUri = remoteScope.StoreUri;
        });

        var definitionRef = await vm.StoreCurrentDefinitionArtifactAsync();
        var snapshotRef = await vm.StoreCurrentSnapshotArtifactAsync();

        Assert.NotNull(definitionRef);
        Assert.NotNull(snapshotRef);

        var ack = await vm.RestoreBrainFromCurrentArtifactReferencesAsync();

        Assert.NotNull(ack);
        Assert.Equal(2, artifactPublisher.PromoteCallCount);
        Assert.Equal(1, client.RequestPlacementCallCount);
        Assert.NotNull(client.LastPlacementRequest);
        Assert.True(client.LastPlacementRequest!.BrainId.TryToGuid(out var requestedBrainId));
        Assert.Equal(designBrainId, requestedBrainId);
        Assert.Equal(definitionRef!.ToSha256Hex(), client.LastPlacementRequest.BaseDef.ToSha256Hex());
        Assert.Equal(snapshotRef!.ToSha256Hex(), client.LastPlacementRequest.LastSnapshot.ToSha256Hex());
        Assert.StartsWith("http://", client.LastPlacementRequest.BaseDef.StoreUri, StringComparison.OrdinalIgnoreCase);
        Assert.StartsWith("http://", client.LastPlacementRequest.LastSnapshot.StoreUri, StringComparison.OrdinalIgnoreCase);
        Assert.True(client.LastPlacementRequest.IsRecovery);
        Assert.Equal(5_000UL, client.LastAwaitSpawnPlacementTimeoutMs);
        Assert.Equal(0, client.KillBrainCallCount);
        Assert.Contains("Brain restored from artifact refs", vm.Status, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RestoreBrainFromCurrentArtifactReferencesAsync_InvalidDefinitionSha_RejectsBeforeRuntimeRequest()
    {
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true,
            HiveMindConnected = true,
            IoConnected = true,
            SettingsPortText = "bad",
            HiveMindPortText = "bad",
            IoPortText = "bad"
        };
        var client = new FakeWorkbenchClient();
        var vm = CreateViewModel(connections, client);

        AvaloniaTestHost.RunOnUiThread(() =>
        {
            vm.DefinitionArtifactShaText = "not-a-sha";
            vm.DefinitionArtifactStoreUriText = Path.Combine(Path.GetTempPath(), "nbn-artifact-tests");
        });

        var ack = await vm.RestoreBrainFromCurrentArtifactReferencesAsync();

        Assert.Null(ack);
        Assert.Equal(0, client.RequestPlacementCallCount);
        Assert.Contains("Artifact reference is invalid", vm.Status, StringComparison.Ordinal);
    }

    private static DesignerPanelViewModel CreateViewModel(
        ConnectionViewModel? connections = null,
        WorkbenchClient? client = null,
        IWorkbenchArtifactPublisher? artifactPublisher = null)
    {
        return AvaloniaTestHost.RunOnUiThread(() =>
        {
            var resolvedConnections = connections ?? new ConnectionViewModel();
            var resolvedClient = client ?? new FakeWorkbenchClient();
            return new DesignerPanelViewModel(resolvedConnections, resolvedClient, artifactPublisher: artifactPublisher);
        });
    }

    private static BrainListResponse BuildBrainList(Guid brainId, string state)
    {
        var nowMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return new BrainListResponse
        {
            Brains =
            {
                new BrainStatus
                {
                    BrainId = brainId.ToProtoUuid(),
                    SpawnedMs = nowMs,
                    LastTickId = 0,
                    State = state
                }
            },
            Controllers =
            {
                new BrainControllerStatus
                {
                    BrainId = brainId.ToProtoUuid(),
                    LastSeenMs = nowMs,
                    IsAlive = true
                }
            }
        };
    }

    private static PlacementLifecycleInfo BuildPlacementLifecycle(
        Guid brainId,
        PlacementLifecycleState state,
        uint registeredShards)
    {
        return new PlacementLifecycleInfo
        {
            BrainId = brainId.ToProtoUuid(),
            PlacementEpoch = 1,
            LifecycleState = state,
            RegisteredShards = registeredShards
        };
    }

    private sealed class FakeWorkbenchClient : WorkbenchClient
    {
        public PlacementAck? PlacementAck { get; set; }
        public Func<Guid, ulong, SpawnBrainAck?>? AwaitSpawnPlacementFactory { get; init; }
        public Func<BrainListResponse?>? BrainListFactory { get; set; }
        public Func<Guid, PlacementLifecycleInfo?>? PlacementLifecycleFactory { get; init; }
        public ulong LastAwaitSpawnPlacementTimeoutMs { get; private set; }
        public int RequestPlacementCallCount { get; private set; }
        public int KillBrainCallCount { get; private set; }
        public RequestPlacement? LastPlacementRequest { get; private set; }

        public FakeWorkbenchClient()
            : base(new NullWorkbenchEventSink())
        {
        }

        public override Task<PlacementAck?> RequestPlacementAsync(RequestPlacement request)
        {
            RequestPlacementCallCount++;
            LastPlacementRequest = request;
            return Task.FromResult(PlacementAck);
        }

        public override Task<BrainListResponse?> ListBrainsAsync()
            => Task.FromResult(BrainListFactory?.Invoke());

        public override Task<SpawnBrainAck?> AwaitSpawnPlacementAsync(Guid brainId, ulong timeoutMs = 0)
        {
            LastAwaitSpawnPlacementTimeoutMs = timeoutMs;
            return Task.FromResult(AwaitSpawnPlacementFactory?.Invoke(brainId, timeoutMs));
        }

        public override Task<PlacementLifecycleInfo?> GetPlacementLifecycleAsync(Guid brainId)
            => Task.FromResult(PlacementLifecycleFactory?.Invoke(brainId));

        public override Task<bool> KillBrainAsync(Guid brainId, string reason)
        {
            KillBrainCallCount++;
            return Task.FromResult(true);
        }
    }

    private sealed class FakeWorkbenchArtifactPublisher : IWorkbenchArtifactPublisher
    {
        private readonly List<ArtifactRef> _promotedRefs = new();

        public int PromoteCallCount => _promotedRefs.Count;

        public Task<PublishedArtifact> PublishAsync(
            byte[] bytes,
            string mediaType,
            string backingStoreRoot,
            string bindHost,
            string? advertisedHost = null,
            string? label = null,
            int? preferredPort = null,
            CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("PublishAsync is not used by this test double.");
        }

        public Task<PublishedArtifact> PromoteAsync(
            ArtifactRef artifactRef,
            string defaultLocalStoreRootPath,
            string bindHost,
            string? advertisedHost = null,
            string? label = null,
            int? preferredPort = null,
            CancellationToken cancellationToken = default)
        {
            var promoted = artifactRef.Clone();
            var port = preferredPort ?? WorkbenchArtifactPublisher.DefaultReachableArtifactPort;
            promoted.StoreUri = $"http://127.0.0.1:{port}/";
            _promotedRefs.Add(artifactRef.Clone());
            return Task.FromResult(new PublishedArtifact(promoted, null));
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class NullWorkbenchEventSink : IWorkbenchEventSink
    {
        public void OnOutputEvent(OutputEventItem item) { }
        public void OnOutputVectorEvent(OutputVectorEventItem item) { }
        public void OnDebugEvent(DebugEventItem item) { }
        public void OnVizEvent(VizEventItem item) { }
        public void OnBrainTerminated(BrainTerminatedItem item) { }
        public void OnIoStatus(string status, bool connected) { }
        public void OnObsStatus(string status, bool connected) { }
        public void OnSettingsStatus(string status, bool connected) { }
        public void OnHiveMindStatus(string status, bool connected) { }
        public void OnSettingChanged(SettingItem item) { }
    }
}
