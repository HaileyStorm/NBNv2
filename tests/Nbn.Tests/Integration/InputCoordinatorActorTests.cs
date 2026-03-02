using Nbn.Proto.Control;
using Nbn.Proto.Io;
using Nbn.Runtime.IO;
using Nbn.Shared;
using Proto;

namespace Nbn.Tests.Integration;

public sealed class InputCoordinatorActorTests
{
    [Fact]
    public async Task DirtyOnChange_Drain_EmitsOnlyDirtyIndices_Once()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        try
        {
            var coordinator = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(
                brainId,
                inputWidth: 4,
                InputCoordinatorMode.DirtyOnChange)));

            root.Send(coordinator, new InputWrite
            {
                BrainId = brainId.ToProtoUuid(),
                InputIndex = 1,
                Value = 0.5f
            });
            root.Send(coordinator, new InputWrite
            {
                BrainId = brainId.ToProtoUuid(),
                InputIndex = 3,
                Value = -0.2f
            });

            var firstDrain = await root.RequestAsync<InputDrain>(coordinator, new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 10
            });
            Assert.Equal((ulong)10, firstDrain.TickId);
            Assert.Equal(2, firstDrain.Contribs.Count);
            Assert.Contains(firstDrain.Contribs, contrib => contrib.TargetNeuronId == 1 && Math.Abs(contrib.Value - 0.5f) < 1e-6f);
            Assert.Contains(firstDrain.Contribs, contrib => contrib.TargetNeuronId == 3 && Math.Abs(contrib.Value - (-0.2f)) < 1e-6f);

            var secondDrain = await root.RequestAsync<InputDrain>(coordinator, new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 11
            });
            Assert.Equal((ulong)11, secondDrain.TickId);
            Assert.Empty(secondDrain.Contribs);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ReplayLatestVector_Drain_EmitsFullVector_EveryTick()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        try
        {
            var coordinator = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(
                brainId,
                inputWidth: 4,
                InputCoordinatorMode.ReplayLatestVector)));

            var firstDrain = await root.RequestAsync<InputDrain>(coordinator, new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 1
            });
            Assert.Equal(new[] { 0f, 0f, 0f, 0f }, firstDrain.Contribs.Select(c => c.Value).ToArray());

            var vector = new InputVector { BrainId = brainId.ToProtoUuid() };
            vector.Values.Add(new[] { 1f, 2f, 3f, 4f });
            root.Send(coordinator, vector);

            var secondDrain = await root.RequestAsync<InputDrain>(coordinator, new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 2
            });
            Assert.Equal(new[] { 1f, 2f, 3f, 4f }, secondDrain.Contribs.Select(c => c.Value).ToArray());

            root.Send(coordinator, new InputWrite
            {
                BrainId = brainId.ToProtoUuid(),
                InputIndex = 2,
                Value = 9f
            });

            var thirdDrain = await root.RequestAsync<InputDrain>(coordinator, new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 3
            });
            Assert.Equal(new[] { 1f, 2f, 9f, 4f }, thirdDrain.Contribs.Select(c => c.Value).ToArray());

            var fourthDrain = await root.RequestAsync<InputDrain>(coordinator, new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 4
            });
            Assert.Equal(new[] { 1f, 2f, 9f, 4f }, fourthDrain.Contribs.Select(c => c.Value).ToArray());
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }

    [Fact]
    public async Task ModeUpdate_TogglesDrainBehavior()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var brainId = Guid.NewGuid();

        try
        {
            var coordinator = root.Spawn(Props.FromProducer(() => new InputCoordinatorActor(
                brainId,
                inputWidth: 2,
                InputCoordinatorMode.DirtyOnChange)));

            root.Send(coordinator, new InputWrite
            {
                BrainId = brainId.ToProtoUuid(),
                InputIndex = 0,
                Value = 1f
            });

            var firstDrain = await root.RequestAsync<InputDrain>(coordinator, new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 1
            });
            Assert.Single(firstDrain.Contribs);

            root.Send(coordinator, new UpdateInputCoordinatorMode(InputCoordinatorMode.ReplayLatestVector));
            var secondDrain = await root.RequestAsync<InputDrain>(coordinator, new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 2
            });
            Assert.Equal(new[] { 1f, 0f }, secondDrain.Contribs.Select(c => c.Value).ToArray());

            root.Send(coordinator, new UpdateInputCoordinatorMode(InputCoordinatorMode.DirtyOnChange));
            var thirdDrain = await root.RequestAsync<InputDrain>(coordinator, new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 3
            });
            Assert.Empty(thirdDrain.Contribs);

            root.Send(coordinator, new InputWrite
            {
                BrainId = brainId.ToProtoUuid(),
                InputIndex = 1,
                Value = 2f
            });
            var fourthDrain = await root.RequestAsync<InputDrain>(coordinator, new DrainInputs
            {
                BrainId = brainId.ToProtoUuid(),
                TickId = 4
            });
            Assert.Single(fourthDrain.Contribs);
            Assert.Equal((uint)1, fourthDrain.Contribs[0].TargetNeuronId);
            Assert.Equal(2f, fourthDrain.Contribs[0].Value);
        }
        finally
        {
            await system.ShutdownAsync();
        }
    }
}

