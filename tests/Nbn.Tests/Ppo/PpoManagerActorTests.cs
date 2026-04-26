using Nbn.Proto.Ppo;
using Nbn.Runtime.Ppo;
using Nbn.Shared;
using Nbn.Tests.TestSupport;
using Proto;
using ProtoIo = Nbn.Proto.Io;
using ProtoRepro = Nbn.Proto.Repro;
using ProtoSpec = Nbn.Proto.Speciation;

namespace Nbn.Tests.Ppo;

public sealed class PpoManagerActorTests
{
    [Fact]
    public async Task Status_ReportsDependencyReadiness()
    {
        await using var system = new ActorSystem();
        var manager = system.Root.Spawn(Props.FromProducer(() => new PpoManagerActor()));

        var initial = await system.Root.RequestAsync<PpoStatusResponse>(
            manager,
            new PpoStatusRequest(),
            TimeSpan.FromSeconds(5));

        Assert.False(initial.Dependencies.ReproductionAvailable);
        Assert.False(initial.Dependencies.SpeciationAvailable);

        system.Root.Send(
            manager,
            new PpoManagerActor.DiscoverySnapshotApplied(
                new Dictionary<string, ServiceEndpointRegistration>
                {
                    [ServiceEndpointSettings.IoGatewayKey] = new(
                        ServiceEndpointSettings.IoGatewayKey,
                        new ServiceEndpoint("127.0.0.1:12050", "IoGateway"),
                        1),
                    [ServiceEndpointSettings.ReproductionManagerKey] = new(
                        ServiceEndpointSettings.ReproductionManagerKey,
                        new ServiceEndpoint("127.0.0.1:12070", "ReproductionManager"),
                        1),
                    [ServiceEndpointSettings.SpeciationManagerKey] = new(
                        ServiceEndpointSettings.SpeciationManagerKey,
                        new ServiceEndpoint("127.0.0.1:12080", "SpeciationManager"),
                        1)
                }));

        var ready = await system.Root.RequestAsync<PpoStatusResponse>(
            manager,
            new PpoStatusRequest(),
            TimeSpan.FromSeconds(5));

        Assert.True(ready.Dependencies.ReproductionAvailable);
        Assert.True(ready.Dependencies.SpeciationAvailable);
        Assert.True(ready.Dependencies.IoAvailable);
        Assert.Equal("127.0.0.1:12050/IoGateway", ready.Dependencies.IoEndpoint);
        Assert.Equal("127.0.0.1:12070/ReproductionManager", ready.Dependencies.ReproductionEndpoint);
        Assert.Equal("127.0.0.1:12080/SpeciationManager", ready.Dependencies.SpeciationEndpoint);
    }

    [Fact]
    public async Task StartRun_RequiresIoReproductionAndSpeciation()
    {
        await using var system = new ActorSystem();
        var manager = system.Root.Spawn(Props.FromProducer(() => new PpoManagerActor()));

        var response = await system.Root.RequestAsync<PpoStartRunResponse>(
            manager,
            new PpoStartRunRequest
            {
                Hyperparameters = CreateValidHyperparameters()
            },
            TimeSpan.FromSeconds(5));

        Assert.False(response.Accepted);
        Assert.Equal(PpoFailureReason.PpoFailureIoUnavailable, response.FailureReason);

        var ioOnly = system.Root.Spawn(
            Props.FromProducer(() => new PpoManagerActor(
                new PID("127.0.0.1:12050", "IoGateway"),
                reproductionPid: null,
                speciationPid: null)));

        var missingReproduction = await system.Root.RequestAsync<PpoStartRunResponse>(
            ioOnly,
            new PpoStartRunRequest
            {
                Hyperparameters = CreateValidHyperparameters()
            },
            TimeSpan.FromSeconds(5));

        Assert.False(missingReproduction.Accepted);
        Assert.Equal(PpoFailureReason.PpoFailureReproductionUnavailable, missingReproduction.FailureReason);
    }

    [Fact]
    public async Task StartRun_RequiresSpeciationWhenIoAndReproductionAreAvailable()
    {
        await using var system = new ActorSystem();
        var manager = system.Root.Spawn(
            Props.FromProducer(() => new PpoManagerActor(
                new PID("127.0.0.1:12050", "IoGateway"),
                new PID("127.0.0.1:12070", "ReproductionManager"),
                speciationPid: null)));

        var response = await system.Root.RequestAsync<PpoStartRunResponse>(
            manager,
            new PpoStartRunRequest
            {
                Hyperparameters = CreateValidHyperparameters()
            },
            TimeSpan.FromSeconds(5));

        Assert.False(response.Accepted);
        Assert.Equal(PpoFailureReason.PpoFailureSpeciationUnavailable, response.FailureReason);
        Assert.Equal("ppo_speciation_unavailable", response.FailureDetail);
    }

    [Fact]
    public async Task StartRun_RequiresTwoParentBrainIdsForExecution()
    {
        await using var system = new ActorSystem();
        var manager = system.Root.Spawn(
            Props.FromProducer(() => new PpoManagerActor(
                new PID("127.0.0.1:12050", "IoGateway"),
                new PID("127.0.0.1:12070", "ReproductionManager"),
                new PID("127.0.0.1:12080", "SpeciationManager"))));

        var response = await system.Root.RequestAsync<PpoStartRunResponse>(
            manager,
            new PpoStartRunRequest
            {
                Hyperparameters = CreateValidHyperparameters()
            },
            TimeSpan.FromSeconds(5));

        Assert.False(response.Accepted);
        Assert.Equal(PpoFailureReason.PpoFailureInvalidRequest, response.FailureReason);
        Assert.Equal("ppo_parent_brain_ids_required", response.FailureDetail);
    }

    [Fact]
    public async Task StartRun_ExecutesRolloutThroughIoReproductionAndSpeciation()
    {
        await using var system = new ActorSystem();
        var parentA = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        var parentB = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        var child = CreateArtifact("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", "child");
        var ioProbe = system.Root.Spawn(Props.FromProducer(() => new PpoIoProbe(parentA, parentB)));
        var reproductionProbe = system.Root.Spawn(Props.FromProducer(() => new PpoReproductionProbe(child)));
        var speciationProbe = system.Root.Spawn(Props.FromProducer(() => new PpoSpeciationProbe()));
        var manager = system.Root.Spawn(
            Props.FromProducer(() => new PpoManagerActor(
                ioProbe,
                reproductionProbe,
                speciationProbe)));

        var started = await system.Root.RequestAsync<PpoStartRunResponse>(
            manager,
            CreateValidStartRequest(parentA, parentB, "ppo-test-run"),
            TimeSpan.FromSeconds(5));

        Assert.True(started.Accepted);
        Assert.Equal(PpoFailureReason.PpoFailureNone, started.FailureReason);
        Assert.Equal("ppo-test-run", started.Run.RunId);
        Assert.Equal(PpoRunState.Running, started.Run.State);

        await AsyncTestHelpers.WaitForAsync(
            async () =>
            {
                var current = await system.Root.RequestAsync<PpoStatusResponse>(
                    manager,
                    new PpoStatusRequest(),
                    TimeSpan.FromSeconds(5));
                return current.LastRun?.State == PpoRunState.Completed;
            },
            timeoutMs: 5000,
            failureMessage: "PPO rollout did not complete.");

        var status = await system.Root.RequestAsync<PpoStatusResponse>(
            manager,
            new PpoStatusRequest(),
            TimeSpan.FromSeconds(5));

        Assert.Equal(1UL, status.CompletedRunCount);
        Assert.Null(status.ActiveRun);
        Assert.NotNull(status.LastRun);
        Assert.Equal(PpoRunState.Completed, status.LastRun.State);
        Assert.Equal("completed", status.LastRun.StatusDetail);
        Assert.Single(status.LastRun.ExecutionReport.Candidates);
        Assert.Equal(child.StoreUri, status.LastRun.ExecutionReport.Candidates[0].ChildDef.StoreUri);

        var ioSnapshot = await system.Root.RequestAsync<PpoIoProbe.Snapshot>(
            ioProbe,
            new PpoIoProbe.GetSnapshot(),
            TimeSpan.FromSeconds(5));
        Assert.Equal(2, ioSnapshot.BrainInfoRequests);
        Assert.Equal(2, ioSnapshot.SnapshotRequests);
        Assert.Equal(0, ioSnapshot.LiveMutationMessages);

        var reproduction = await system.Root.RequestAsync<PpoReproductionProbe.Snapshot>(
            reproductionProbe,
            new PpoReproductionProbe.GetSnapshot(),
            TimeSpan.FromSeconds(5));
        Assert.NotNull(reproduction.LastRequest);
        Assert.Equal(ProtoRepro.SpawnChildPolicy.SpawnChildNever, reproduction.LastRequest.Config.SpawnChild);
        Assert.True(reproduction.LastRequest.Config.ProtectIoRegionNeuronCounts);
        Assert.Equal(4u, reproduction.LastRequest.RunCount);
        Assert.Equal("artifact://ppo/parent-a.nbn", reproduction.LastRequest.ParentADef.StoreUri);
        Assert.Equal("artifact://ppo/parent-a.nbs", reproduction.LastRequest.ParentAState.StoreUri);

        var speciation = await system.Root.RequestAsync<PpoSpeciationProbe.Snapshot>(
            speciationProbe,
            new PpoSpeciationProbe.GetSnapshot(),
            TimeSpan.FromSeconds(5));
        Assert.NotNull(speciation.LastRequest);
        Assert.Equal(ProtoSpec.SpeciationApplyMode.Commit, speciation.LastRequest.ApplyMode);
        var item = Assert.Single(speciation.LastRequest.Items);
        Assert.Equal("ppo-test-run:0", item.ItemId);
        Assert.Equal(child.StoreUri, item.Candidate.ArtifactRef.StoreUri);
        Assert.Contains("\"ppo_run_id\":\"ppo-test-run\"", item.DecisionMetadataJson);
        Assert.Contains("\"observation_source\":\"io_request_snapshot\"", item.DecisionMetadataJson);
        Assert.Contains("\"post_deliver_output_fence\":false", item.DecisionMetadataJson);
    }

    [Fact]
    public async Task StopRun_RejectsMissingOrMismatchedRun()
    {
        await using var system = new ActorSystem();
        var parentA = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        var parentB = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        var ioProbe = system.Root.Spawn(Props.FromProducer(() => new PpoIoProbe(parentA, parentB)));
        var reproductionProbe = system.Root.Spawn(
            Props.FromProducer(() => new PpoReproductionProbe(
                CreateArtifact("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", "slow-child"),
                responseDelay: TimeSpan.FromSeconds(2))));
        var speciationProbe = system.Root.Spawn(Props.FromProducer(() => new PpoSpeciationProbe()));
        var manager = system.Root.Spawn(
            Props.FromProducer(() => new PpoManagerActor(
                ioProbe,
                reproductionProbe,
                speciationProbe)));

        var missing = await system.Root.RequestAsync<PpoStopRunResponse>(
            manager,
            new PpoStopRunRequest(),
            TimeSpan.FromSeconds(5));

        Assert.False(missing.Stopped);
        Assert.Equal(PpoFailureReason.PpoFailureRunNotActive, missing.FailureReason);
        Assert.Equal("ppo_run_not_active", missing.FailureDetail);

        var started = await system.Root.RequestAsync<PpoStartRunResponse>(
            manager,
            CreateValidStartRequest(parentA, parentB, "active-run"),
            TimeSpan.FromSeconds(5));

        Assert.True(started.Accepted);

        var duplicate = await system.Root.RequestAsync<PpoStartRunResponse>(
            manager,
            CreateValidStartRequest(parentA, parentB, "duplicate-run"),
            TimeSpan.FromSeconds(5));

        Assert.False(duplicate.Accepted);
        Assert.Equal(PpoFailureReason.PpoFailureRunAlreadyActive, duplicate.FailureReason);

        var mismatch = await system.Root.RequestAsync<PpoStopRunResponse>(
            manager,
            new PpoStopRunRequest
            {
                RunId = "other-run"
            },
            TimeSpan.FromSeconds(5));

        Assert.False(mismatch.Stopped);
        Assert.Equal(PpoFailureReason.PpoFailureInvalidRequest, mismatch.FailureReason);
        Assert.Equal("ppo_run_id_mismatch", mismatch.FailureDetail);

        var stopped = await system.Root.RequestAsync<PpoStopRunResponse>(
            manager,
            new PpoStopRunRequest
            {
                RunId = " "
            },
            TimeSpan.FromSeconds(5));

        Assert.True(stopped.Stopped);
        Assert.Equal("active-run", stopped.Run.RunId);
        Assert.Equal(PpoRunState.Cancelled, stopped.Run.State);
    }

    [Fact]
    public async Task StopRun_RejectsCancellationAfterSpeciationCommitDispatch()
    {
        await using var system = new ActorSystem();
        var parentA = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        var parentB = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        var ioProbe = system.Root.Spawn(Props.FromProducer(() => new PpoIoProbe(parentA, parentB)));
        var reproductionProbe = system.Root.Spawn(
            Props.FromProducer(() => new PpoReproductionProbe(
                CreateArtifact("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "commit-child"))));
        var speciationProbe = system.Root.Spawn(
            Props.FromProducer(() => new PpoSpeciationProbe(responseDelay: TimeSpan.FromSeconds(2))));
        var manager = system.Root.Spawn(
            Props.FromProducer(() => new PpoManagerActor(ioProbe, reproductionProbe, speciationProbe)));

        var started = await system.Root.RequestAsync<PpoStartRunResponse>(
            manager,
            CreateValidStartRequest(parentA, parentB, "commit-dispatched-run"),
            TimeSpan.FromSeconds(5));

        Assert.True(started.Accepted);

        await AsyncTestHelpers.WaitForAsync(
            async () =>
            {
                var snapshot = await system.Root.RequestAsync<PpoSpeciationProbe.Snapshot>(
                    speciationProbe,
                    new PpoSpeciationProbe.GetSnapshot(),
                    TimeSpan.FromSeconds(5));
                return snapshot.LastRequest is not null;
            },
            timeoutMs: 5000,
            failureMessage: "PPO did not dispatch the speciation commit request.");

        var stopped = await system.Root.RequestAsync<PpoStopRunResponse>(
            manager,
            new PpoStopRunRequest { RunId = "commit-dispatched-run" },
            TimeSpan.FromSeconds(5));

        Assert.False(stopped.Stopped);
        Assert.Equal(PpoFailureReason.PpoFailureInvalidRequest, stopped.FailureReason);
        Assert.Equal("ppo_run_commit_dispatched", stopped.FailureDetail);
    }

    [Fact]
    public async Task StartRun_FailsWhenIoReturnsCachedSnapshotInsteadOfLiveGeneratedSnapshot()
    {
        await using var system = new ActorSystem();
        var parentA = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        var parentB = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        var ioProbe = system.Root.Spawn(Props.FromProducer(() => new PpoIoProbe(parentA, parentB, generatedFromLiveState: false)));
        var reproductionProbe = system.Root.Spawn(
            Props.FromProducer(() => new PpoReproductionProbe(
                CreateArtifact("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "unused-child"))));
        var speciationProbe = system.Root.Spawn(Props.FromProducer(() => new PpoSpeciationProbe()));
        var manager = system.Root.Spawn(
            Props.FromProducer(() => new PpoManagerActor(ioProbe, reproductionProbe, speciationProbe)));

        var started = await system.Root.RequestAsync<PpoStartRunResponse>(
            manager,
            CreateValidStartRequest(parentA, parentB, "cached-snapshot-run"),
            TimeSpan.FromSeconds(5));

        Assert.True(started.Accepted);

        await AsyncTestHelpers.WaitForAsync(
            async () =>
            {
                var status = await system.Root.RequestAsync<PpoStatusResponse>(
                    manager,
                    new PpoStatusRequest(),
                    TimeSpan.FromSeconds(5));
                return status.LastRun?.State == PpoRunState.Failed;
            },
            timeoutMs: 5000,
            failureMessage: "PPO did not fail the cached-snapshot run.");

        var finalStatus = await system.Root.RequestAsync<PpoStatusResponse>(
            manager,
            new PpoStatusRequest(),
            TimeSpan.FromSeconds(5));

        Assert.Equal("ppo_parent_live_snapshot_unavailable", finalStatus.LastRun.StatusDetail);

        var reproduction = await system.Root.RequestAsync<PpoReproductionProbe.Snapshot>(
            reproductionProbe,
            new PpoReproductionProbe.GetSnapshot(),
            TimeSpan.FromSeconds(5));
        Assert.Null(reproduction.LastRequest);
    }

    [Fact]
    public async Task DiscoveryObservation_InvalidOrRemovedEndpointFallsBackToConfiguredHint()
    {
        await using var system = new ActorSystem();
        var configuredIo = new PID("127.0.0.1:12050", "ConfiguredIoGateway");
        var configuredRepro = new PID("127.0.0.1:12070", "ConfiguredReproduction");
        var configuredSpeciation = new PID("127.0.0.1:12080", "ConfiguredSpeciation");
        var manager = system.Root.Spawn(
            Props.FromProducer(() => new PpoManagerActor(configuredIo, configuredRepro, configuredSpeciation)));

        system.Root.Send(
            manager,
            new PpoManagerActor.EndpointStateObserved(
                new ServiceEndpointObservation(
                    ServiceEndpointSettings.IoGatewayKey,
                    ServiceEndpointObservationKind.Upserted,
                    new ServiceEndpointRegistration(
                        ServiceEndpointSettings.IoGatewayKey,
                        new ServiceEndpoint("127.0.0.1:13050", "DiscoveredIoGateway"),
                        2),
                    string.Empty,
                    2)));
        system.Root.Send(
            manager,
            new PpoManagerActor.EndpointStateObserved(
                new ServiceEndpointObservation(
                    ServiceEndpointSettings.ReproductionManagerKey,
                    ServiceEndpointObservationKind.Upserted,
                    new ServiceEndpointRegistration(
                        ServiceEndpointSettings.ReproductionManagerKey,
                        new ServiceEndpoint("127.0.0.1:13070", "DiscoveredReproduction"),
                        2),
                    string.Empty,
                    2)));
        system.Root.Send(
            manager,
            new PpoManagerActor.EndpointStateObserved(
                new ServiceEndpointObservation(
                    ServiceEndpointSettings.SpeciationManagerKey,
                    ServiceEndpointObservationKind.Upserted,
                    new ServiceEndpointRegistration(
                        ServiceEndpointSettings.SpeciationManagerKey,
                        new ServiceEndpoint("127.0.0.1:13080", "DiscoveredSpeciation"),
                        2),
                    string.Empty,
                    2)));

        var discovered = await system.Root.RequestAsync<PpoStatusResponse>(
            manager,
            new PpoStatusRequest(),
            TimeSpan.FromSeconds(5));

        Assert.Equal("127.0.0.1:13050/DiscoveredIoGateway", discovered.Dependencies.IoEndpoint);
        Assert.Equal("127.0.0.1:13070/DiscoveredReproduction", discovered.Dependencies.ReproductionEndpoint);
        Assert.Equal("127.0.0.1:13080/DiscoveredSpeciation", discovered.Dependencies.SpeciationEndpoint);

        system.Root.Send(
            manager,
            new PpoManagerActor.EndpointStateObserved(
                new ServiceEndpointObservation(
                    ServiceEndpointSettings.IoGatewayKey,
                    ServiceEndpointObservationKind.Removed,
                    null,
                    "endpoint_removed",
                    3)));
        system.Root.Send(
            manager,
            new PpoManagerActor.EndpointStateObserved(
                new ServiceEndpointObservation(
                    ServiceEndpointSettings.ReproductionManagerKey,
                    ServiceEndpointObservationKind.Invalid,
                    null,
                    "endpoint_parse_failed",
                    3)));
        system.Root.Send(
            manager,
            new PpoManagerActor.EndpointStateObserved(
                new ServiceEndpointObservation(
                    ServiceEndpointSettings.SpeciationManagerKey,
                    ServiceEndpointObservationKind.Removed,
                    null,
                    "endpoint_removed",
                    3)));

        var fallback = await system.Root.RequestAsync<PpoStatusResponse>(
            manager,
            new PpoStatusRequest(),
            TimeSpan.FromSeconds(5));

        Assert.Equal("127.0.0.1:12050/ConfiguredIoGateway", fallback.Dependencies.IoEndpoint);
        Assert.Equal("127.0.0.1:12070/ConfiguredReproduction", fallback.Dependencies.ReproductionEndpoint);
        Assert.Equal("127.0.0.1:12080/ConfiguredSpeciation", fallback.Dependencies.SpeciationEndpoint);
    }

    [Fact]
    public async Task StartRun_RejectsInvalidHyperparameters()
    {
        await using var system = new ActorSystem();
        var manager = system.Root.Spawn(
            Props.FromProducer(() => new PpoManagerActor(
                new PID("127.0.0.1:12050", "IoGateway"),
                new PID("127.0.0.1:12070", "ReproductionManager"),
                new PID("127.0.0.1:12080", "SpeciationManager"))));

        var hyperparameters = CreateValidHyperparameters();
        hyperparameters.RolloutTickCount = 0;

        var response = await system.Root.RequestAsync<PpoStartRunResponse>(
            manager,
            new PpoStartRunRequest { Hyperparameters = hyperparameters },
            TimeSpan.FromSeconds(5));

        Assert.False(response.Accepted);
        Assert.Equal(PpoFailureReason.PpoFailureInvalidRequest, response.FailureReason);
        Assert.Equal("ppo_rollout_tick_count_invalid", response.FailureDetail);
    }

    private static PpoHyperparameters CreateValidHyperparameters()
        => new()
        {
            RolloutTickCount = 128,
            RolloutBatchCount = 4,
            ClipEpsilon = 0.2f,
            DiscountGamma = 0.99f,
            GaeLambda = 0.95f,
            LearningRate = 0.0003f,
            OptimizationEpochCount = 4,
            MinibatchSize = 32,
            Seed = 42,
            RewardSignal = "output.reward"
        };

    private static PpoStartRunRequest CreateValidStartRequest(Guid parentA, Guid parentB, string runId)
    {
        var request = new PpoStartRunRequest
        {
            RunId = runId,
            ObjectiveName = "reward",
            MetadataJson = "{\"source\":\"test\"}",
            Hyperparameters = CreateValidHyperparameters(),
            ReproduceConfig = new ProtoRepro.ReproduceConfig
            {
                SpawnChild = ProtoRepro.SpawnChildPolicy.SpawnChildAlways,
                ProtectIoRegionNeuronCounts = false
            },
            StrengthSource = ProtoRepro.StrengthSource.StrengthLiveCodes,
        };
        request.ParentBrainIds.Add(parentA.ToProtoUuid());
        request.ParentBrainIds.Add(parentB.ToProtoUuid());
        return request;
    }

    private static Nbn.Proto.ArtifactRef CreateArtifact(string sha, string label)
        => sha.ToArtifactRef(
            sizeBytes: 128,
            mediaType: label.EndsWith(".nbs", StringComparison.Ordinal) ? "application/x-nbs" : "application/x-nbn",
            storeUri: $"artifact://ppo/{label}");

    private sealed class PpoIoProbe : IActor
    {
        private readonly Guid _parentA;
        private readonly Guid _parentB;
        private readonly bool _generatedFromLiveState;
        private int _brainInfoRequests;
        private int _snapshotRequests;
        private int _liveMutationMessages;

        public PpoIoProbe(Guid parentA, Guid parentB, bool generatedFromLiveState = true)
        {
            _parentA = parentA;
            _parentB = parentB;
            _generatedFromLiveState = generatedFromLiveState;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case ProtoIo.BrainInfoRequest request:
                    _brainInfoRequests++;
                    context.Respond(CreateBrainInfo(request.BrainId.TryToGuid(out var brainId) ? brainId : Guid.Empty));
                    break;
                case ProtoIo.RequestSnapshot request:
                    _snapshotRequests++;
                    context.Respond(CreateSnapshot(request.BrainId.TryToGuid(out var snapshotBrainId) ? snapshotBrainId : Guid.Empty));
                    break;
                case ProtoIo.InputWrite
                    or ProtoIo.InputVector
                    or ProtoIo.RuntimeNeuronPulse
                    or ProtoIo.RuntimeNeuronStateWrite
                    or ProtoIo.ResetBrainRuntimeState
                    or ProtoIo.SpawnBrainViaIO:
                    _liveMutationMessages++;
                    break;
                case GetSnapshot:
                    context.Respond(new Snapshot(_brainInfoRequests, _snapshotRequests, _liveMutationMessages));
                    break;
            }

            return Task.CompletedTask;
        }

        private ProtoIo.BrainInfo CreateBrainInfo(Guid brainId)
            => new()
            {
                BrainId = brainId.ToProtoUuid(),
                InputWidth = 2,
                OutputWidth = 1,
                BaseDefinition = CreateArtifact(
                    brainId == _parentA
                        ? "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        : "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    brainId == _parentA ? "parent-a.nbn" : "parent-b.nbn"),
                LastSnapshot = CreateSnapshot(brainId).Snapshot.Clone()
            };

        private ProtoIo.SnapshotReady CreateSnapshot(Guid brainId)
            => new()
            {
                BrainId = brainId.ToProtoUuid(),
                Snapshot = CreateArtifact(
                    brainId == _parentA
                        ? "1111111111111111111111111111111111111111111111111111111111111111"
                        : "2222222222222222222222222222222222222222222222222222222222222222",
                    brainId == _parentA ? "parent-a.nbs" : "parent-b.nbs"),
                SnapshotTickId = 12,
                GeneratedFromLiveState = _generatedFromLiveState,
                SnapshotSource = _generatedFromLiveState ? "live_tick_boundary" : "cached_last_snapshot"
            };

        public sealed record GetSnapshot;

        public sealed record Snapshot(int BrainInfoRequests, int SnapshotRequests, int LiveMutationMessages);
    }

    private sealed class PpoReproductionProbe : IActor
    {
        private readonly Nbn.Proto.ArtifactRef _child;
        private readonly TimeSpan _responseDelay;
        private ProtoRepro.ReproduceByArtifactsRequest? _lastRequest;

        public PpoReproductionProbe(Nbn.Proto.ArtifactRef child, TimeSpan responseDelay = default)
        {
            _child = child;
            _responseDelay = responseDelay;
        }

        public async Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case ProtoRepro.ReproduceByArtifactsRequest request:
                    _lastRequest = request.Clone();
                    if (_responseDelay > TimeSpan.Zero)
                    {
                        await Task.Delay(_responseDelay).ConfigureAwait(false);
                    }

                    context.Respond(new ProtoRepro.ReproduceResult
                    {
                        Report = new ProtoRepro.SimilarityReport
                        {
                            Compatible = true,
                            SimilarityScore = 0.9f,
                            LineageSimilarityScore = 0.9f
                        },
                        Summary = new ProtoRepro.MutationSummary
                        {
                            AxonsAdded = 1
                        },
                        ChildDef = _child.Clone(),
                        RequestedRunCount = request.RunCount,
                        Runs =
                        {
                            new ProtoRepro.ReproduceRunOutcome
                            {
                                RunIndex = 0,
                                Seed = request.Seed,
                                ChildDef = _child.Clone(),
                                Report = new ProtoRepro.SimilarityReport
                                {
                                    Compatible = true,
                                    SimilarityScore = 0.9f,
                                    LineageSimilarityScore = 0.9f
                                },
                                Summary = new ProtoRepro.MutationSummary
                                {
                                    AxonsAdded = 1
                                }
                            }
                        }
                    });
                    break;
                case GetSnapshot:
                    context.Respond(new Snapshot(_lastRequest?.Clone()));
                    break;
            }
        }

        public sealed record GetSnapshot;

        public sealed record Snapshot(ProtoRepro.ReproduceByArtifactsRequest? LastRequest);
    }

    private sealed class PpoSpeciationProbe : IActor
    {
        private readonly TimeSpan _responseDelay;
        private ProtoSpec.SpeciationBatchEvaluateApplyRequest? _lastRequest;

        public PpoSpeciationProbe(TimeSpan responseDelay = default)
        {
            _responseDelay = responseDelay;
        }

        public async Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case ProtoSpec.SpeciationBatchEvaluateApplyRequest request:
                    _lastRequest = request.Clone();
                    if (_responseDelay > TimeSpan.Zero)
                    {
                        await Task.Delay(_responseDelay).ConfigureAwait(false);
                    }

                    var response = new ProtoSpec.SpeciationBatchEvaluateApplyResponse
                    {
                        FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureNone,
                        ApplyMode = request.ApplyMode,
                        RequestedCount = (uint)request.Items.Count,
                        ProcessedCount = (uint)request.Items.Count,
                        CommittedCount = (uint)request.Items.Count
                    };

                    foreach (var item in request.Items)
                    {
                        response.Results.Add(new ProtoSpec.SpeciationBatchItemResult
                        {
                            ItemId = item.ItemId,
                            Decision = new ProtoSpec.SpeciationDecision
                            {
                                ApplyMode = request.ApplyMode,
                                CandidateMode = ProtoSpec.SpeciationCandidateMode.ArtifactRef,
                                Success = true,
                                Created = true,
                                FailureReason = ProtoSpec.SpeciationFailureReason.SpeciationFailureNone,
                                SpeciesId = string.IsNullOrWhiteSpace(item.SpeciesId) ? "ppo-candidates" : item.SpeciesId,
                                SpeciesDisplayName = string.IsNullOrWhiteSpace(item.SpeciesDisplayName)
                                    ? "PPO Candidates"
                                    : item.SpeciesDisplayName,
                                DecisionReason = item.DecisionReason,
                                DecisionMetadataJson = item.DecisionMetadataJson,
                                Committed = request.ApplyMode == ProtoSpec.SpeciationApplyMode.Commit
                            }
                        });
                    }

                    context.Respond(response);
                    break;
                case GetSnapshot:
                    context.Respond(new Snapshot(_lastRequest?.Clone()));
                    break;
            }
        }

        public sealed record GetSnapshot;

        public sealed record Snapshot(ProtoSpec.SpeciationBatchEvaluateApplyRequest? LastRequest);
    }
}
