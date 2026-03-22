using Nbn.Runtime.Artifacts;
using Proto;

namespace Nbn.Runtime.Reproduction;

/// <summary>
/// Coordinates parent resolution, compatibility evaluation, child artifact creation, and optional child spawn requests.
/// </summary>
public sealed partial class ReproductionManagerActor : IActor
{
    private static readonly TimeSpan DefaultRequestTimeout = TimeSpan.FromSeconds(15);
    private const string NbnMediaType = "application/x-nbn";
    private const string NbsMediaType = "application/x-nbs";
    private const int SpotCheckSampleCount = 32;
    private const float MinRequiredSpotOverlap = 0.35f;
    private const float MaxRequiredSpotOverlap = 0.95f;
    private const ulong DefaultSpotCheckSeed = 0x9E3779B97F4A7C15UL;
    private const uint DefaultRunCount = 1;
    private const uint MaxRunCount = 64;
    private const int ParsedParentCacheCapacity = 128;
    private const float PreferredActivationMutationBias = 0.80f;
    private const float PreferredResetMutationBias = 0.75f;
    private const float PreferredAccumulationMutationBias = 0.85f;
    private static readonly byte[] PreferredActivationFunctionIds = { 1, 5, 6, 7, 8, 9, 11, 18, 28 };
    private static readonly byte[] PreferredResetFunctionIds = { 0, 1, 3, 17, 30, 43, 44, 45, 47, 48, 49, 58 };
    private static readonly byte[] PreferredAccumulationFunctionIds = { 0, 0, 2, 2, 1 };

    private readonly PID? _configuredIoGatewayPid;
    private readonly ArtifactStoreResolver _artifactStoreResolver;
    private readonly object _parsedParentCacheGate = new();
    private readonly Dictionary<string, ParsedParent> _parsedParentCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Queue<string> _parsedParentCacheOrder = new();
    private PID? _ioGatewayPid;

    /// <summary>
    /// Initializes a reproduction manager with an optional preconfigured IO Gateway endpoint.
    /// </summary>
    public ReproductionManagerActor(PID? ioGatewayPid = null)
    {
        _configuredIoGatewayPid = ioGatewayPid;
        _ioGatewayPid = ioGatewayPid;
        _artifactStoreResolver = new ArtifactStoreResolver();
    }

    /// <inheritdoc />
    public Task ReceiveAsync(IContext context)
    {
        switch (context.Message)
        {
            case Nbn.Proto.Repro.ReproduceByBrainIdsRequest message:
                RespondAfter(context, HandleReproduceByBrainIdsAsync(context, message));
                break;
            case Nbn.Proto.Repro.ReproduceByArtifactsRequest message:
                RespondAfter(context, HandleReproduceByArtifactsAsync(context, message));
                break;
            case Nbn.Proto.Repro.AssessCompatibilityByBrainIdsRequest message:
                RespondAfter(context, HandleAssessCompatibilityByBrainIdsAsync(context, message));
                break;
            case Nbn.Proto.Repro.AssessCompatibilityByArtifactsRequest message:
                RespondAfter(context, HandleAssessCompatibilityByArtifactsAsync(context, message));
                break;
            case DiscoverySnapshotApplied snapshot:
                ApplyDiscoverySnapshot(snapshot);
                break;
            case EndpointStateObserved observed:
                ApplyObservedEndpoint(observed.Observation);
                break;
        }

        return Task.CompletedTask;
    }

    private static void RespondAfter(IContext context, Task<Nbn.Proto.Repro.ReproduceResult> responseTask)
    {
        context.ReenterAfter(responseTask, completed =>
        {
            if (completed.IsCompletedSuccessfully)
            {
                context.Respond(completed.Result ?? CreateAbortResult("repro_internal_error"));
                return Task.CompletedTask;
            }

            context.Respond(CreateAbortResult("repro_internal_error"));
            return Task.CompletedTask;
        });
    }

}
