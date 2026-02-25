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
}
