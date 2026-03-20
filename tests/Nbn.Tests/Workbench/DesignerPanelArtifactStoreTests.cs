using Microsoft.Data.Sqlite;
using Nbn.Proto.Control;
using Nbn.Proto.Settings;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Nbn.Tests.TestSupport;
using Nbn.Tools.Workbench.Models;
using Nbn.Tools.Workbench.Services;
using Nbn.Tools.Workbench.ViewModels;

namespace Nbn.Tests.Workbench;

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

            var published = await publisher.PublishAsync(
                payload,
                "application/x-nbn",
                artifactRoot,
                NetworkAddressDefaults.LoopbackHost);

            Assert.StartsWith("http://127.0.0.1:", published.ArtifactRef.StoreUri, StringComparison.OrdinalIgnoreCase);
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
        var connections = new ConnectionViewModel
        {
            SettingsConnected = true,
            HiveMindConnected = true,
            IoConnected = true,
            SettingsPortText = "bad",
            HiveMindPortText = "bad",
            IoPortText = "bad"
        };
        var spawnedBrainId = Guid.NewGuid();
        var client = new FakeWorkbenchClient
        {
            PlacementAck = new PlacementAck { Accepted = true, Message = "accepted" },
            BrainListFactory = () => BuildBrainList(spawnedBrainId, "Active"),
            PlacementLifecycleFactory = requestedBrainId => requestedBrainId == spawnedBrainId
                ? BuildPlacementLifecycle(requestedBrainId, PlacementLifecycleState.PlacementLifecycleRunning, registeredShards: 3)
                : null
        };
        var vm = CreateViewModel(connections, client);
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
        Assert.Equal(1, client.RequestPlacementCallCount);
        Assert.NotNull(client.LastPlacementRequest);
        Assert.True(client.LastPlacementRequest!.BrainId.TryToGuid(out var requestedBrainId));
        Assert.Equal(designBrainId, requestedBrainId);
        Assert.Equal(definitionRef!.ToSha256Hex(), client.LastPlacementRequest.BaseDef.ToSha256Hex());
        Assert.Equal(snapshotRef!.ToSha256Hex(), client.LastPlacementRequest.LastSnapshot.ToSha256Hex());
        Assert.Equal(remoteScope.StoreUri, client.LastPlacementRequest.BaseDef.StoreUri);
        Assert.Equal(remoteScope.StoreUri, client.LastPlacementRequest.LastSnapshot.StoreUri);
        Assert.True(client.LastPlacementRequest.IsRecovery);
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

    private static DesignerPanelViewModel CreateViewModel(ConnectionViewModel? connections = null, WorkbenchClient? client = null)
    {
        return AvaloniaTestHost.RunOnUiThread(() =>
        {
            var resolvedConnections = connections ?? new ConnectionViewModel();
            var resolvedClient = client ?? new FakeWorkbenchClient();
            return new DesignerPanelViewModel(resolvedConnections, resolvedClient);
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
        public Func<BrainListResponse?>? BrainListFactory { get; set; }
        public Func<Guid, PlacementLifecycleInfo?>? PlacementLifecycleFactory { get; init; }
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

        public override Task<PlacementLifecycleInfo?> GetPlacementLifecycleAsync(Guid brainId)
            => Task.FromResult(PlacementLifecycleFactory?.Invoke(brainId));

        public override Task<bool> KillBrainAsync(Guid brainId, string reason)
        {
            KillBrainCallCount++;
            return Task.FromResult(true);
        }
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
