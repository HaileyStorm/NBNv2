using Nbn.Proto.Ppo;
using Nbn.Runtime.Ppo;
using Nbn.Shared;
using Proto;

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
        Assert.Equal("127.0.0.1:12070/ReproductionManager", ready.Dependencies.ReproductionEndpoint);
        Assert.Equal("127.0.0.1:12080/SpeciationManager", ready.Dependencies.SpeciationEndpoint);
    }

    [Fact]
    public async Task StartRun_RequiresReproductionAndSpeciation()
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
        Assert.Equal(PpoFailureReason.PpoFailureReproductionUnavailable, response.FailureReason);
    }

    [Fact]
    public async Task StartAndStopRun_TracksLifecycleWithoutRuntimeMutation()
    {
        await using var system = new ActorSystem();
        var manager = system.Root.Spawn(
            Props.FromProducer(() => new PpoManagerActor(
                new PID("127.0.0.1:12070", "ReproductionManager"),
                new PID("127.0.0.1:12080", "SpeciationManager"))));

        var started = await system.Root.RequestAsync<PpoStartRunResponse>(
            manager,
            new PpoStartRunRequest
            {
                RunId = "ppo-test-run",
                ObjectiveName = "reward",
                MetadataJson = "{\"source\":\"test\"}",
                Hyperparameters = CreateValidHyperparameters()
            },
            TimeSpan.FromSeconds(5));

        Assert.True(started.Accepted);
        Assert.Equal(PpoFailureReason.PpoFailureNone, started.FailureReason);
        Assert.Equal("ppo-test-run", started.Run.RunId);
        Assert.Equal(PpoRunState.Running, started.Run.State);

        var duplicate = await system.Root.RequestAsync<PpoStartRunResponse>(
            manager,
            new PpoStartRunRequest { Hyperparameters = CreateValidHyperparameters() },
            TimeSpan.FromSeconds(5));

        Assert.False(duplicate.Accepted);
        Assert.Equal(PpoFailureReason.PpoFailureRunAlreadyActive, duplicate.FailureReason);

        var stopped = await system.Root.RequestAsync<PpoStopRunResponse>(
            manager,
            new PpoStopRunRequest
            {
                RunId = "ppo-test-run",
                Reason = "operator_cancelled"
            },
            TimeSpan.FromSeconds(5));

        Assert.True(stopped.Stopped);
        Assert.Equal(PpoRunState.Cancelled, stopped.Run.State);
        Assert.Equal("operator_cancelled", stopped.Run.StatusDetail);

        var status = await system.Root.RequestAsync<PpoStatusResponse>(
            manager,
            new PpoStatusRequest(),
            TimeSpan.FromSeconds(5));

        Assert.Equal(1UL, status.CompletedRunCount);
        Assert.Null(status.ActiveRun);
    }

    [Fact]
    public async Task DiscoveryObservation_InvalidOrRemovedEndpointFallsBackToConfiguredHint()
    {
        await using var system = new ActorSystem();
        var configuredRepro = new PID("127.0.0.1:12070", "ConfiguredReproduction");
        var configuredSpeciation = new PID("127.0.0.1:12080", "ConfiguredSpeciation");
        var manager = system.Root.Spawn(
            Props.FromProducer(() => new PpoManagerActor(configuredRepro, configuredSpeciation)));

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

        Assert.Equal("127.0.0.1:13070/DiscoveredReproduction", discovered.Dependencies.ReproductionEndpoint);
        Assert.Equal("127.0.0.1:13080/DiscoveredSpeciation", discovered.Dependencies.SpeciationEndpoint);

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

        Assert.Equal("127.0.0.1:12070/ConfiguredReproduction", fallback.Dependencies.ReproductionEndpoint);
        Assert.Equal("127.0.0.1:12080/ConfiguredSpeciation", fallback.Dependencies.SpeciationEndpoint);
    }

    [Fact]
    public async Task StartRun_RejectsInvalidHyperparameters()
    {
        await using var system = new ActorSystem();
        var manager = system.Root.Spawn(
            Props.FromProducer(() => new PpoManagerActor(
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
}
