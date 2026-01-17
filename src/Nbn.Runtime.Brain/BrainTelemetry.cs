using System.Diagnostics.Metrics;

namespace Nbn.Runtime.Brain;

public static class BrainTelemetry
{
    private const string MeterName = "Nbn.Runtime.Brain";
    private static readonly Meter Meter = new(MeterName);

    public static string MeterNameValue => Meter.Name;

    private static readonly Counter<long> DeliveryTimeouts =
        Meter.CreateCounter<long>("nbn.brain.signal.deliver.timeouts");

    private static readonly Counter<long> LateAcks =
        Meter.CreateCounter<long>("nbn.brain.signal.ack.late");

    public static void RecordDeliveryTimeout(int count = 1)
    {
        if (count <= 0)
        {
            return;
        }

        DeliveryTimeouts.Add(count);
    }

    public static void RecordLateAck(int count = 1)
    {
        if (count <= 0)
        {
            return;
        }

        LateAcks.Add(count);
    }
}
