using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Collections.Generic;

namespace Nbn.Runtime.HiveMind;

public static class HiveMindTelemetry
{
    private const string MeterName = "Nbn.Runtime.HiveMind";

    private static readonly Meter Meter = new(MeterName);
    public static readonly ActivitySource ActivitySource = new(MeterName);
    public static string MeterNameValue => Meter.Name;

    private static readonly Counter<long> TickCompleted = Meter.CreateCounter<long>("nbn.hivemind.tick.completed");
    private static readonly Histogram<double> TickComputeMs = Meter.CreateHistogram<double>("nbn.hivemind.tick.compute.ms");
    private static readonly Histogram<double> TickDeliverMs = Meter.CreateHistogram<double>("nbn.hivemind.tick.deliver.ms");
    private static readonly Counter<long> TickComputeTimeouts = Meter.CreateCounter<long>("nbn.hivemind.tick.compute.timeouts");
    private static readonly Counter<long> TickDeliverTimeouts = Meter.CreateCounter<long>("nbn.hivemind.tick.deliver.timeouts");
    private static readonly Counter<long> LateCompute = Meter.CreateCounter<long>("nbn.hivemind.tick.compute.late");
    private static readonly Counter<long> LateDeliver = Meter.CreateCounter<long>("nbn.hivemind.tick.deliver.late");
    private static readonly Counter<long> LateComputeAfterCompletion = Meter.CreateCounter<long>("nbn.hivemind.tick.compute.late.after");
    private static readonly Counter<long> LateDeliverAfterCompletion = Meter.CreateCounter<long>("nbn.hivemind.tick.deliver.late.after");
    private static readonly Histogram<double> TargetTickHz = Meter.CreateHistogram<double>("nbn.hivemind.tick.target.hz");
    private static readonly Counter<long> RescheduleRequested = Meter.CreateCounter<long>("nbn.hivemind.reschedule.requested");
    private static readonly Counter<long> PauseRequested = Meter.CreateCounter<long>("nbn.hivemind.pause.requested");
    private static readonly Counter<long> BrainTickCostTotal = Meter.CreateCounter<long>("nbn.hivemind.brain.tick_cost.total");
    private static readonly Counter<long> BrainEnergyDepleted = Meter.CreateCounter<long>("nbn.hivemind.brain.energy.depleted");
    private static readonly Counter<long> SnapshotOverlayRecords = Meter.CreateCounter<long>("nbn.hivemind.snapshot.overlay.records");
    private static readonly Counter<long> RebaseOverlayRecords = Meter.CreateCounter<long>("nbn.hivemind.rebase.overlay.records");

    public static void RecordTickOutcome(TickOutcome outcome, float targetTickHz)
    {
        TickCompleted.Add(1);
        TickComputeMs.Record(outcome.ComputeDuration.TotalMilliseconds);
        TickDeliverMs.Record(outcome.DeliverDuration.TotalMilliseconds);
        TargetTickHz.Record(targetTickHz);

        if (outcome.ComputeTimedOut)
        {
            TickComputeTimeouts.Add(1);
        }

        if (outcome.DeliverTimedOut)
        {
            TickDeliverTimeouts.Add(1);
        }

        if (outcome.LateComputeCount > 0)
        {
            LateCompute.Add(outcome.LateComputeCount);
        }

        if (outcome.LateDeliverCount > 0)
        {
            LateDeliver.Add(outcome.LateDeliverCount);
        }
    }

    public static void RecordReschedule(string? reason)
    {
        RescheduleRequested.Add(1);
        using var activity = ActivitySource.StartActivity("hivemind.reschedule");
        activity?.SetTag("reason", reason);
    }

    public static void RecordPause(string? reason)
    {
        PauseRequested.Add(1);
        using var activity = ActivitySource.StartActivity("hivemind.pause");
        activity?.SetTag("reason", reason);
    }

    public static void RecordLateComputeAfterCompletion(int count = 1)
    {
        if (count <= 0)
        {
            return;
        }

        LateComputeAfterCompletion.Add(count);
    }

    public static void RecordLateDeliverAfterCompletion(int count = 1)
    {
        if (count <= 0)
        {
            return;
        }

        LateDeliverAfterCompletion.Add(count);
    }

    public static void RecordBrainTickCost(Guid brainId, long tickCost)
    {
        if (tickCost <= 0)
        {
            return;
        }

        BrainTickCostTotal.Add(
            tickCost,
            new KeyValuePair<string, object?>("brain_id", brainId.ToString("D")));
    }

    public static void RecordEnergyDepleted(Guid brainId)
    {
        BrainEnergyDepleted.Add(
            1,
            new KeyValuePair<string, object?>("brain_id", brainId.ToString("D")));

        using var activity = ActivitySource.StartActivity("hivemind.energy.depleted");
        activity?.SetTag("brain.id", brainId.ToString("D"));
    }

    public static void RecordSnapshotOverlayRecords(Guid brainId, int overlayCount)
    {
        if (overlayCount <= 0)
        {
            return;
        }

        SnapshotOverlayRecords.Add(
            overlayCount,
            new KeyValuePair<string, object?>("brain_id", brainId.ToString("D")));
    }

    public static void RecordRebaseOverlayRecords(Guid brainId, int overlayCount)
    {
        if (overlayCount <= 0)
        {
            return;
        }

        RebaseOverlayRecords.Add(
            overlayCount,
            new KeyValuePair<string, object?>("brain_id", brainId.ToString("D")));
    }
}
