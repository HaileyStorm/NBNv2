using Nbn.Runtime.Reproduction;
using Nbn.Shared;
using Proto;
using Repro = Nbn.Proto.Repro;

namespace Nbn.Tests.Reproduction;

public class ReproductionManagerActorTests
{
    [Fact]
    public async Task ReproduceByBrainIds_Returns_NotImplemented_Report()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor()));

        var response = await root.RequestAsync<Repro.ReproduceResult>(
            manager,
            new Repro.ReproduceByBrainIdsRequest
            {
                ParentA = Guid.NewGuid().ToProtoUuid(),
                ParentB = Guid.NewGuid().ToProtoUuid(),
                Config = new Repro.ReproduceConfig()
            });

        Assert.NotNull(response.Report);
        Assert.False(response.Report.Compatible);
        Assert.Equal("repro_not_implemented:brain_ids", response.Report.AbortReason);
        Assert.False(response.Spawned);

        await system.ShutdownAsync();
    }

    [Fact]
    public async Task ReproduceByArtifacts_Returns_NotImplemented_Report()
    {
        var system = new ActorSystem();
        var root = system.Root;
        var manager = root.Spawn(Props.FromProducer(() => new ReproductionManagerActor()));

        var response = await root.RequestAsync<Repro.ReproduceResult>(
            manager,
            new Repro.ReproduceByArtifactsRequest());

        Assert.NotNull(response.Report);
        Assert.False(response.Report.Compatible);
        Assert.Equal("repro_not_implemented:artifacts", response.Report.AbortReason);
        Assert.False(response.Spawned);

        await system.ShutdownAsync();
    }
}
