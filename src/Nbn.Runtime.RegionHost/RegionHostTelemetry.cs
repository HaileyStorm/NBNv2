using System.Diagnostics.Metrics;
using System.Diagnostics;
using System.Collections.Generic;

namespace Nbn.Runtime.RegionHost;

public static class RegionHostTelemetry
{
    private const string MeterName = "Nbn.Runtime.RegionHost";
    private static readonly Meter Meter = new(MeterName);
    private static readonly ActivitySource ActivitySource = new(MeterName);

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

    private static readonly Counter<long> PlasticityStrengthCodeChanges =
        Meter.CreateCounter<long>("nbn.regionhost.plasticity.strength_code.changed");

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

    public static void RecordPlasticityStrengthCodeChanges(long count, int regionId, int shardIndex)
    {
        if (count <= 0)
        {
            return;
        }

        PlasticityStrengthCodeChanges.Add(
            count,
            new KeyValuePair<string, object?>("region_id", regionId),
            new KeyValuePair<string, object?>("shard_index", shardIndex));

        using var activity = ActivitySource.StartActivity("regionhost.plasticity.mutation");
        activity?.SetTag("region.id", regionId);
        activity?.SetTag("shard.index", shardIndex);
        activity?.SetTag("plasticity.strength_code_changes", count);
    }
}
