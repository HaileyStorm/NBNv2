using System.Diagnostics.Metrics;

namespace Nbn.Runtime.RegionHost;

public static class RegionHostTelemetry
{
    private const string MeterName = "Nbn.Runtime.RegionHost";
    private static readonly Meter Meter = new(MeterName);

    public static string MeterNameValue => Meter.Name;

    private static readonly Counter<long> TickComputeDuplicates =
        Meter.CreateCounter<long>("nbn.regionhost.tick.compute.duplicate");

    private static readonly Counter<long> TickComputeOutOfOrder =
        Meter.CreateCounter<long>("nbn.regionhost.tick.compute.out_of_order");

    private static readonly Counter<long> TickComputeJumps =
        Meter.CreateCounter<long>("nbn.regionhost.tick.compute.jump");

    private static readonly Counter<long> SignalBatchLate =
        Meter.CreateCounter<long>("nbn.regionhost.signal.batch.late");

    private static readonly Counter<long> SignalBatchRejected =
        Meter.CreateCounter<long>("nbn.regionhost.signal.batch.rejected");

    public static void RecordComputeDuplicate(int count = 1)
    {
        if (count <= 0)
        {
            return;
        }

        TickComputeDuplicates.Add(count);
    }

    public static void RecordComputeOutOfOrder(int count = 1)
    {
        if (count <= 0)
        {
            return;
        }

        TickComputeOutOfOrder.Add(count);
    }

    public static void RecordComputeJump(int count = 1)
    {
        if (count <= 0)
        {
            return;
        }

        TickComputeJumps.Add(count);
    }

    public static void RecordSignalBatchLate(int count = 1)
    {
        if (count <= 0)
        {
            return;
        }

        SignalBatchLate.Add(count);
    }

    public static void RecordSignalBatchRejected(int count = 1)
    {
        if (count <= 0)
        {
            return;
        }

        SignalBatchRejected.Add(count);
    }
}
