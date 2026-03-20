using Nbn.Proto.Io;
using Nbn.Runtime.IO;
using Nbn.Shared;
using Proto;
using ProtoControl = Nbn.Proto.Control;
using Xunit.Sdk;

namespace Nbn.Tests.Integration;

public sealed class IoGatewayDistributedCoordinatorTests
{
    [Fact]
    public async Task EnsureBrainEntry_Bootstraps_RemoteCoordinators_From_HiveMindMetadata()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPid = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 4)));
        var outputPid = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 2)));
        var routerPid = root.Spawn(Props.FromProducer(() => new IoGatewayRegistrationProbeActor(brainId)));
        var hivePid = root.Spawn(Props.FromProducer(() => new BrainIoInfoHiveProbeActor(
            brainId,
            inputPid,
            outputPid,
            routerPid,
            inputWidth: 4,
            outputWidth: 2,
            inputMode: ProtoControl.InputCoordinatorMode.DirtyOnChange)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hivePid)));
        var subscriberPid = root.Spawn(Props.FromProducer(() => new OutputSubscriberProbeActor()));

        root.Send(subscriberPid, new OutputSubscriberProbeActor.SubscribeGateway(gateway, brainId));

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<BrainIoInfoHiveProbeActor.Snapshot>(
                    hivePid,
                    new BrainIoInfoHiveProbeActor.GetSnapshot());
                return snapshot.RegisterOutputSinkCount > 0;
            },
            timeoutMs: 2_000);

        root.Send(gateway, new InputWrite
        {
            BrainId = brainId.ToProtoUuid(),
            InputIndex = 1,
            Value = 0.75f
        });

        var drain = await root.RequestAsync<InputDrain>(
            gateway,
            new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 7
            });

        var contribution = Assert.Single(drain.Contribs);
        Assert.Equal(1u, contribution.TargetNeuronId);
        Assert.Equal(0.75f, contribution.Value);

        var hiveSnapshot = await root.RequestAsync<BrainIoInfoHiveProbeActor.Snapshot>(
            hivePid,
            new BrainIoInfoHiveProbeActor.GetSnapshot());
        Assert.Equal(PidLabel(outputPid), hiveSnapshot.LastOutputSinkPid);

        var routerSnapshot = await root.RequestAsync<IoGatewayRegistrationProbeActor.Snapshot>(
            routerPid,
            new IoGatewayRegistrationProbeActor.GetSnapshot());
        Assert.Equal(2, routerSnapshot.RegistrationCount);
        Assert.False(string.IsNullOrWhiteSpace(routerSnapshot.LastIoGatewayPid));

        var bootstrapOutputTick = 8UL;
        await WaitForAsync(
            async () =>
            {
                root.Send(outputPid, new OutputEvent
                {
                    BrainId = brainId.ToProtoUuid(),
                    OutputIndex = 1,
                    Value = 0.9f,
                    TickId = bootstrapOutputTick
                });
                root.Send(outputPid, new OutputVectorEvent
                {
                    BrainId = brainId.ToProtoUuid(),
                    TickId = bootstrapOutputTick,
                    Values = { 0.1f, 0.9f }
                });
                bootstrapOutputTick++;

                var snapshot = await root.RequestAsync<OutputSubscriberProbeActor.Snapshot>(
                    subscriberPid,
                    new OutputSubscriberProbeActor.GetSnapshot());
                return snapshot.SingleCount >= 1 && snapshot.VectorCount >= 1;
            },
            timeoutMs: 2_000);

        var subscriberSnapshot = await root.RequestAsync<OutputSubscriberProbeActor.Snapshot>(
            subscriberPid,
            new OutputSubscriberProbeActor.GetSnapshot());
        Assert.NotNull(subscriberSnapshot.LastSingle);
        Assert.NotNull(subscriberSnapshot.LastVector);
        Assert.Equal(1u, subscriberSnapshot.LastSingle!.OutputIndex);
        Assert.Equal(0.9f, subscriberSnapshot.LastSingle.Value);
        Assert.Equal([0.1f, 0.9f], subscriberSnapshot.LastVector!.Values.ToArray());

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task BrainInfoRequest_Tolerates_Slow_HiveMindMetadataBootstrap()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPid = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 4)));
        var outputPid = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 2)));
        var routerPid = root.Spawn(Props.FromProducer(() => new IoGatewayRegistrationProbeActor(brainId)));
        var hivePid = root.Spawn(Props.FromProducer(() => new BrainIoInfoHiveProbeActor(
            brainId,
            inputPid,
            outputPid,
            routerPid,
            inputWidth: 4,
            outputWidth: 2,
            inputMode: ProtoControl.InputCoordinatorMode.DirtyOnChange,
            brainIoInfoDelay: TimeSpan.FromMilliseconds(1500))));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hivePid)));

        var info = await root.RequestAsync<BrainInfo>(
            gateway,
            new BrainInfoRequest { BrainId = brainId.ToProtoUuid() },
            TimeSpan.FromSeconds(5));

        Assert.Equal((uint)4, info.InputWidth);
        Assert.Equal((uint)2, info.OutputWidth);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RegisterBrain_Skips_Placeholder_OutputSink_Until_RemoteCoordinator_Is_Known()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPid = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 1)));
        var outputPid = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 1)));
        var routerPid = root.Spawn(Props.FromProducer(() => new IoGatewayRegistrationProbeActor(brainId)));
        var hivePid = root.Spawn(Props.FromProducer(() => new BrainIoInfoHiveProbeActor(
            brainId,
            inputPid,
            outputPid,
            routerPid,
            inputWidth: 1,
            outputWidth: 1,
            inputMode: ProtoControl.InputCoordinatorMode.DirtyOnChange)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hivePid)));

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            InputCoordinatorPid = PidLabel(inputPid),
            IoGatewayOwnsInputCoordinator = false,
            IoGatewayOwnsOutputCoordinator = false
        });

        await Task.Delay(100);

        var placeholderSnapshot = await root.RequestAsync<BrainIoInfoHiveProbeActor.Snapshot>(
            hivePid,
            new BrainIoInfoHiveProbeActor.GetSnapshot());
        Assert.Equal(0, placeholderSnapshot.RegisterOutputSinkCount);
        Assert.Equal(string.Empty, placeholderSnapshot.LastOutputSinkPid);

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            InputCoordinatorPid = PidLabel(inputPid),
            OutputCoordinatorPid = PidLabel(outputPid),
            IoGatewayOwnsInputCoordinator = false,
            IoGatewayOwnsOutputCoordinator = false
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<BrainIoInfoHiveProbeActor.Snapshot>(
                    hivePid,
                    new BrainIoInfoHiveProbeActor.GetSnapshot());
                return snapshot.RegisterOutputSinkCount > 0;
            },
            timeoutMs: 2_000);

        var remoteSnapshot = await root.RequestAsync<BrainIoInfoHiveProbeActor.Snapshot>(
            hivePid,
            new BrainIoInfoHiveProbeActor.GetSnapshot());
        Assert.Equal(PidLabel(outputPid), remoteSnapshot.LastOutputSinkPid);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RegisterBrain_Registers_Local_OutputSink_When_GatewayOwnsCoordinator()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPid = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 1)));
        var outputPid = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 1)));
        var routerPid = root.Spawn(Props.FromProducer(() => new IoGatewayRegistrationProbeActor(brainId)));
        var hivePid = root.Spawn(Props.FromProducer(() => new BrainIoInfoHiveProbeActor(
            brainId,
            inputPid,
            outputPid,
            routerPid,
            inputWidth: 1,
            outputWidth: 1,
            inputMode: ProtoControl.InputCoordinatorMode.DirtyOnChange)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hivePid)));

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 1,
            OutputWidth = 1,
            IoGatewayOwnsInputCoordinator = true,
            IoGatewayOwnsOutputCoordinator = true
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<BrainIoInfoHiveProbeActor.Snapshot>(
                    hivePid,
                    new BrainIoInfoHiveProbeActor.GetSnapshot());
                return snapshot.RegisterOutputSinkCount > 0;
            },
            timeoutMs: 2_000);

        var snapshot = await root.RequestAsync<BrainIoInfoHiveProbeActor.Snapshot>(
            hivePid,
            new BrainIoInfoHiveProbeActor.GetSnapshot());
        Assert.Contains(IoNames.OutputCoordinatorPrefix, snapshot.LastOutputSinkPid);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task RegisterBrain_Replays_InputState_And_OutputSubscriptions_Across_CoordinatorMoves()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPidA = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 3)));
        var outputPidA = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 3)));
        var inputPidB = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 3)));
        var outputPidB = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 3)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions())));
        var subscriberPid = root.Spawn(Props.FromProducer(() => new OutputSubscriberProbeActor()));

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 3,
            OutputWidth = 3,
            InputCoordinatorMode = ProtoControl.InputCoordinatorMode.ReplayLatestVector,
            InputCoordinatorPid = PidLabel(inputPidA),
            OutputCoordinatorPid = PidLabel(outputPidA),
            IoGatewayOwnsInputCoordinator = false,
            IoGatewayOwnsOutputCoordinator = false
        });
        root.Send(subscriberPid, new OutputSubscriberProbeActor.SubscribeGateway(gateway, brainId));
        root.Send(gateway, new InputVector
        {
            BrainId = brainId.ToProtoUuid(),
            Values = { 0.25f, 0.5f, 0.75f }
        });

        root.Send(gateway, new RegisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            InputWidth = 3,
            OutputWidth = 3,
            InputCoordinatorMode = ProtoControl.InputCoordinatorMode.ReplayLatestVector,
            InputCoordinatorPid = PidLabel(inputPidB),
            OutputCoordinatorPid = PidLabel(outputPidB),
            IoGatewayOwnsInputCoordinator = false,
            IoGatewayOwnsOutputCoordinator = false
        });

        var drain = await root.RequestAsync<InputDrain>(
            gateway,
            new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 42
            });

        Assert.Equal(3, drain.Contribs.Count);
        Assert.Equal([0.25f, 0.5f, 0.75f], drain.Contribs.Select(static contrib => contrib.Value).ToArray());

        var movedOutputTick = 43UL;
        await WaitForAsync(
            async () =>
            {
                root.Send(outputPidB, new OutputEvent
                {
                    BrainId = brainId.ToProtoUuid(),
                    OutputIndex = 2,
                    Value = 0.6f,
                    TickId = movedOutputTick
                });
                root.Send(outputPidB, new OutputVectorEvent
                {
                    BrainId = brainId.ToProtoUuid(),
                    TickId = movedOutputTick,
                    Values = { 0.2f, 0.4f, 0.6f }
                });
                movedOutputTick++;

                var snapshot = await root.RequestAsync<OutputSubscriberProbeActor.Snapshot>(
                    subscriberPid,
                    new OutputSubscriberProbeActor.GetSnapshot());
                return snapshot.SingleCount >= 1 && snapshot.VectorCount >= 1;
            },
            timeoutMs: 2_000);

        var subscriberSnapshot = await root.RequestAsync<OutputSubscriberProbeActor.Snapshot>(
            subscriberPid,
            new OutputSubscriberProbeActor.GetSnapshot());
        Assert.Equal(2u, subscriberSnapshot.LastSingle!.OutputIndex);
        Assert.Equal(0.6f, subscriberSnapshot.LastSingle.Value);
        Assert.Equal([0.2f, 0.4f, 0.6f], subscriberSnapshot.LastVector!.Values.ToArray());

        root.Send(gateway, new UnregisterBrain
        {
            BrainId = brainId.ToProtoUuid(),
            Reason = "cleanup"
        });

        root.Send(inputPidB, new InputWrite
        {
            BrainId = brainId.ToProtoUuid(),
            InputIndex = 0,
            Value = 1f
        });

        var directDrain = await root.RequestAsync<InputDrain>(
            inputPidB,
            new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 99
            },
            TimeSpan.FromSeconds(1));
        Assert.Equal(3, directDrain.Contribs.Count);
        Assert.Equal(1f, directDrain.Contribs[0].Value);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ForwardInput_Refreshes_RouterRegistration_When_BrainRouting_Changes()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPid = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 1)));
        var outputPid = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 1)));
        var routerA = root.Spawn(Props.FromProducer(() => new InputRouterProbeActor(brainId)));
        var routerB = root.Spawn(Props.FromProducer(() => new InputRouterProbeActor(brainId)));
        var hivePid = root.Spawn(Props.FromProducer(() => new BrainIoInfoHiveProbeActor(
            brainId,
            inputPid,
            outputPid,
            routerA,
            inputWidth: 1,
            outputWidth: 1,
            inputMode: ProtoControl.InputCoordinatorMode.ReplayLatestVector)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hivePid)));

        root.Send(gateway, new InputVector
        {
            BrainId = brainId.ToProtoUuid(),
            Values = { 0.25f }
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
                    routerA,
                    new InputRouterProbeActor.GetSnapshot());
                return snapshot.InputVectorCount >= 1 && snapshot.RegisterIoGatewayCount >= 1;
            },
            timeoutMs: 2_000);

        root.Send(hivePid, new BrainIoInfoHiveProbeActor.UpdateRouter(routerB));

        root.Send(gateway, new InputVector
        {
            BrainId = brainId.ToProtoUuid(),
            Values = { 0.5f }
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
                    routerB,
                    new InputRouterProbeActor.GetSnapshot());
                return snapshot.InputVectorCount >= 1 && snapshot.RegisterIoGatewayCount >= 1;
            },
            timeoutMs: 2_000);

        var routerASnapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
            routerA,
            new InputRouterProbeActor.GetSnapshot());
        var routerBSnapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
            routerB,
            new InputRouterProbeActor.GetSnapshot());

        Assert.Equal(1, routerASnapshot.InputVectorCount);
        Assert.Equal(1, routerBSnapshot.InputVectorCount);
        Assert.Equal(1, routerASnapshot.RegisterIoGatewayCount);
        Assert.Equal(1, routerBSnapshot.RegisterIoGatewayCount);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ForwardInput_ReRegisters_IoGateway_When_Same_Router_Forgets_Registration()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        var inputPid = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(brainId, 1)));
        var outputPid = root.Spawn(Props.FromProducer(() => new OutputCoordinatorActor(brainId, 1)));
        var router = root.Spawn(Props.FromProducer(() => new InputRouterProbeActor(brainId)));
        var hivePid = root.Spawn(Props.FromProducer(() => new BrainIoInfoHiveProbeActor(
            brainId,
            inputPid,
            outputPid,
            router,
            inputWidth: 1,
            outputWidth: 1,
            inputMode: ProtoControl.InputCoordinatorMode.ReplayLatestVector)));
        var gateway = root.Spawn(Props.FromProducer(() => new IoGatewayActor(CreateOptions(), hiveMindPid: hivePid)));

        root.Send(gateway, new InputVector
        {
            BrainId = brainId.ToProtoUuid(),
            Values = { 0.25f }
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
                    router,
                    new InputRouterProbeActor.GetSnapshot());
                return snapshot.InputVectorCount >= 1
                    && snapshot.RegisterIoGatewayCount >= 1
                    && snapshot.HasIoGatewayRegistration;
            },
            timeoutMs: 2_000);

        root.Send(router, new InputRouterProbeActor.ForgetIoGatewayRegistration());

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
                    router,
                    new InputRouterProbeActor.GetSnapshot());
                return !snapshot.HasIoGatewayRegistration;
            },
            timeoutMs: 2_000);

        root.Send(gateway, new InputVector
        {
            BrainId = brainId.ToProtoUuid(),
            Values = { 0.5f }
        });

        await WaitForAsync(
            async () =>
            {
                var snapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
                    router,
                    new InputRouterProbeActor.GetSnapshot());
                return snapshot.InputVectorCount >= 2
                    && snapshot.RegisterIoGatewayCount >= 2
                    && snapshot.HasIoGatewayRegistration;
            },
            timeoutMs: 2_000);

        var routerSnapshot = await root.RequestAsync<InputRouterProbeActor.Snapshot>(
            router,
            new InputRouterProbeActor.GetSnapshot());
        Assert.Equal(2, routerSnapshot.InputVectorCount);
        Assert.Equal(2, routerSnapshot.RegisterIoGatewayCount);
        Assert.True(routerSnapshot.HasIoGatewayRegistration);

        await system.ShutdownAsync();
    }

    private static IoOptions CreateOptions()
        => new(
            BindHost: "127.0.0.1",
            Port: 0,
            AdvertisedHost: null,
            AdvertisedPort: null,
            GatewayName: "io-gateway",
            ServerName: "nbn.io.tests",
            SettingsHost: null,
            SettingsPort: 0,
            SettingsName: "SettingsMonitor",
            HiveMindAddress: null,
            HiveMindName: null,
            ReproAddress: null,
            ReproName: null,
            SpeciationAddress: null,
            SpeciationName: null);

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private static async Task WaitForAsync(Func<Task<bool>> predicate, int timeoutMs)
    {
        using var cts = new CancellationTokenSource(timeoutMs);
        while (true)
        {
            if (await predicate().ConfigureAwait(false))
            {
                return;
            }

            try
            {
                await Task.Delay(20, cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        throw new XunitException($"Condition was not met within {timeoutMs} ms.");
    }

    private sealed class BrainIoInfoHiveProbeActor : IActor
    {
        private readonly Guid _brainId;
        private readonly PID _inputCoordinatorPid;
        private readonly PID _outputCoordinatorPid;
        private PID _routerPid;
        private readonly uint _inputWidth;
        private readonly uint _outputWidth;
        private readonly ProtoControl.InputCoordinatorMode _inputMode;
        private readonly TimeSpan _brainIoInfoDelay;
        private int _registerOutputSinkCount;
        private string _lastOutputSinkPid = string.Empty;

        public BrainIoInfoHiveProbeActor(
            Guid brainId,
            PID inputCoordinatorPid,
            PID outputCoordinatorPid,
            PID routerPid,
            uint inputWidth,
            uint outputWidth,
            ProtoControl.InputCoordinatorMode inputMode,
            TimeSpan? brainIoInfoDelay = null)
        {
            _brainId = brainId;
            _inputCoordinatorPid = inputCoordinatorPid;
            _outputCoordinatorPid = outputCoordinatorPid;
            _routerPid = routerPid;
            _inputWidth = inputWidth;
            _outputWidth = outputWidth;
            _inputMode = inputMode;
            _brainIoInfoDelay = brainIoInfoDelay ?? TimeSpan.Zero;
        }

        public sealed record GetSnapshot;

        public sealed record UpdateRouter(PID RouterPid);

        public sealed record Snapshot(int RegisterOutputSinkCount, string LastOutputSinkPid);

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case ProtoControl.GetBrainIoInfo request when request.BrainId.TryToGuid(out var requestBrainId) && requestBrainId == _brainId:
                    if (_brainIoInfoDelay > TimeSpan.Zero)
                    {
                        return RespondBrainIoInfoAsync(context, request);
                    }

                    context.Respond(BuildBrainIoInfo(request.BrainId));
                    break;
                case ProtoControl.GetBrainRouting request when request.BrainId.TryToGuid(out var routingBrainId) && routingBrainId == _brainId:
                    context.Respond(new ProtoControl.BrainRoutingInfo
                    {
                        BrainId = request.BrainId,
                        SignalRouterPid = PidLabel(_routerPid)
                    });
                    break;
                case UpdateRouter update:
                    _routerPid = update.RouterPid;
                    break;
                case ProtoControl.RegisterOutputSink register when register.BrainId.TryToGuid(out var registeredBrainId) && registeredBrainId == _brainId:
                    _registerOutputSinkCount++;
                    _lastOutputSinkPid = register.OutputPid ?? string.Empty;
                    break;
                case GetSnapshot:
                    context.Respond(new Snapshot(_registerOutputSinkCount, _lastOutputSinkPid));
                    break;
            }

            return Task.CompletedTask;
        }

        private async Task RespondBrainIoInfoAsync(IContext context, ProtoControl.GetBrainIoInfo request)
        {
            await Task.Delay(_brainIoInfoDelay).ConfigureAwait(false);
            context.Respond(BuildBrainIoInfo(request.BrainId));
        }

        private ProtoControl.BrainIoInfo BuildBrainIoInfo(Nbn.Proto.Uuid brainId)
            => new()
            {
                BrainId = brainId,
                InputWidth = _inputWidth,
                OutputWidth = _outputWidth,
                InputCoordinatorMode = _inputMode,
                OutputVectorSource = ProtoControl.OutputVectorSource.Potential,
                InputCoordinatorPid = PidLabel(_inputCoordinatorPid),
                OutputCoordinatorPid = PidLabel(_outputCoordinatorPid),
                IoGatewayOwnsInputCoordinator = false,
                IoGatewayOwnsOutputCoordinator = false
            };
    }

    private sealed class InputRouterProbeActor : IActor
    {
        private readonly Guid _brainId;
        private int _inputVectorCount;
        private int _registerIoGatewayCount;
        private bool _hasIoGatewayRegistration;

        public InputRouterProbeActor(Guid brainId)
        {
            _brainId = brainId;
        }

        public sealed record GetSnapshot;
        public sealed record ForgetIoGatewayRegistration;

        public sealed record Snapshot(int InputVectorCount, int RegisterIoGatewayCount, bool HasIoGatewayRegistration);

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case InputVector input when input.BrainId.TryToGuid(out var brainId) && brainId == _brainId:
                    _inputVectorCount++;
                    break;
                case RegisterIoGateway register when register.BrainId.TryToGuid(out var registeredBrainId) && registeredBrainId == _brainId:
                    _registerIoGatewayCount++;
                    _hasIoGatewayRegistration = true;
                    break;
                case ForgetIoGatewayRegistration:
                    _hasIoGatewayRegistration = false;
                    break;
                case GetSnapshot:
                    context.Respond(new Snapshot(_inputVectorCount, _registerIoGatewayCount, _hasIoGatewayRegistration));
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class IoGatewayRegistrationProbeActor : IActor
    {
        private readonly Guid _brainId;
        private int _registrationCount;
        private string _lastIoGatewayPid = string.Empty;

        public IoGatewayRegistrationProbeActor(Guid brainId)
        {
            _brainId = brainId;
        }

        public sealed record GetSnapshot;

        public sealed record Snapshot(int RegistrationCount, string LastIoGatewayPid);

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case RegisterIoGateway register
                    when register.BrainId is not null
                         && register.BrainId.TryToGuid(out var brainId)
                         && brainId == _brainId:
                    _registrationCount++;
                    _lastIoGatewayPid = register.IoGatewayPid ?? string.Empty;
                    break;
                case GetSnapshot:
                    context.Respond(new Snapshot(_registrationCount, _lastIoGatewayPid));
                    break;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class OutputSubscriberProbeActor : IActor
    {
        private int _singleCount;
        private int _vectorCount;
        private OutputEvent? _lastSingle;
        private OutputVectorEvent? _lastVector;

        public sealed record SubscribeGateway(PID GatewayPid, Guid BrainId, bool SubscribeSingles = true, bool SubscribeVectors = true);

        public sealed record GetSnapshot;

        public sealed record Snapshot(int SingleCount, int VectorCount, OutputEvent? LastSingle, OutputVectorEvent? LastVector);

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case SubscribeGateway subscribe:
                    var subscriberActor = PidLabel(context.Self);
                    if (subscribe.SubscribeSingles)
                    {
                        context.Send(subscribe.GatewayPid, new SubscribeOutputs
                        {
                            BrainId = subscribe.BrainId.ToProtoUuid(),
                            SubscriberActor = subscriberActor
                        });
                    }

                    if (subscribe.SubscribeVectors)
                    {
                        context.Send(subscribe.GatewayPid, new SubscribeOutputsVector
                        {
                            BrainId = subscribe.BrainId.ToProtoUuid(),
                            SubscriberActor = subscriberActor
                        });
                    }

                    break;
                case OutputEvent output:
                    _singleCount++;
                    _lastSingle = output.Clone();
                    break;
                case OutputVectorEvent vector:
                    _vectorCount++;
                    _lastVector = vector.Clone();
                    break;
                case GetSnapshot:
                    context.Respond(new Snapshot(_singleCount, _vectorCount, _lastSingle?.Clone(), _lastVector?.Clone()));
                    break;
            }

            return Task.CompletedTask;
        }
    }
}
