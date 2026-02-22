using Nbn.Proto.Repro;
using Proto;

namespace Nbn.Runtime.Reproduction;

public sealed class ReproductionManagerActor : IActor
{
    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case ReproduceByBrainIdsRequest:
                context.Respond(CreateNotImplementedResult("repro_not_implemented:brain_ids"));
                break;
            case ReproduceByArtifactsRequest:
                context.Respond(CreateNotImplementedResult("repro_not_implemented:artifacts"));
                break;
        }

        return Task.CompletedTask;
    }

    private static ReproduceResult CreateNotImplementedResult(string reason)
        => new()
        {
            Report = new SimilarityReport
            {
                Compatible = false,
                AbortReason = reason,
                SimilarityScore = 0f,
                RegionSpanScore = 0f,
                FunctionScore = 0f,
                ConnectivityScore = 0f
            },
            Summary = new MutationSummary(),
            Spawned = false
        };
}
