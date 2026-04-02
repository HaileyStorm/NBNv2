using System.Diagnostics.Metrics;

namespace Nbn.Runtime.IO;

public static class IoTelemetry
{
    private const string MeterName = "Nbn.Runtime.IO";
    private static readonly Meter Meter = new(MeterName);

    public static string MeterNameValue => Meter.Name;

    private static readonly Counter<long> OutputVectorPublished =
        Meter.CreateCounter<long>("nbn.io.output.vector.published");

    private static readonly Counter<long> OutputVectorRejected =
        Meter.CreateCounter<long>("nbn.io.output.vector.rejected");

    private static readonly Counter<long> OutputSingleRejected =
        Meter.CreateCounter<long>("nbn.io.output.single.rejected");
    private static readonly Counter<long> AwaitSpawnPlacementCompleted =
        Meter.CreateCounter<long>("nbn.io.await_spawn_placement.completed");
    private static readonly Counter<long> AwaitSpawnPlacementFailed =
        Meter.CreateCounter<long>("nbn.io.await_spawn_placement.failed");
    private static readonly Histogram<double> AwaitSpawnPlacementTotalMs =
        Meter.CreateHistogram<double>("nbn.io.await_spawn_placement.total.ms");
    private static readonly Histogram<double> AwaitSpawnPlacementHiveMindMs =
        Meter.CreateHistogram<double>("nbn.io.await_spawn_placement.hivemind.ms");
    private static readonly Histogram<double> AwaitSpawnPlacementMetadataMs =
        Meter.CreateHistogram<double>("nbn.io.await_spawn_placement.metadata.ms");

    public static void RecordOutputVectorPublished(Guid brainId, int outputWidth)
    {
        OutputVectorPublished.Add(
            1,
            new KeyValuePair<string, object?>("brain_id", brainId.ToString("D")),
            new KeyValuePair<string, object?>("output_width", outputWidth));
    }

    public static void RecordOutputVectorRejected(Guid brainId, string reason, int outputWidth)
    {
        OutputVectorRejected.Add(
            1,
            new KeyValuePair<string, object?>("brain_id", brainId.ToString("D")),
            new KeyValuePair<string, object?>("reason", reason),
            new KeyValuePair<string, object?>("output_width", outputWidth));
    }

    public static void RecordOutputSingleRejected(Guid brainId, string reason, int outputWidth)
    {
        OutputSingleRejected.Add(
            1,
            new KeyValuePair<string, object?>("brain_id", brainId.ToString("D")),
            new KeyValuePair<string, object?>("reason", reason),
            new KeyValuePair<string, object?>("output_width", outputWidth));
    }

    public static void RecordAwaitSpawnPlacement(
        Guid? brainId,
        string outcome,
        bool placementReady,
        TimeSpan totalWait,
        TimeSpan hiveMindWait,
        TimeSpan metadataVisibilityWait)
    {
        var tags = new List<KeyValuePair<string, object?>>
        {
            new("outcome", outcome),
            new("placement_ready", placementReady)
        };
        if (brainId.HasValue && brainId.Value != Guid.Empty)
        {
            tags.Add(new KeyValuePair<string, object?>("brain_id", brainId.Value.ToString("D")));
        }
        var tagArray = tags.ToArray();

        if (placementReady)
        {
            AwaitSpawnPlacementCompleted.Add(1, tagArray);
        }
        else
        {
            AwaitSpawnPlacementFailed.Add(1, tagArray);
        }

        AwaitSpawnPlacementTotalMs.Record(totalWait.TotalMilliseconds, tagArray);
        AwaitSpawnPlacementHiveMindMs.Record(hiveMindWait.TotalMilliseconds, tagArray);
        AwaitSpawnPlacementMetadataMs.Record(metadataVisibilityWait.TotalMilliseconds, tagArray);
    }
}
