using System.Diagnostics;
using System.Diagnostics.Metrics;

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
    private static readonly Histogram<double> TargetTickHz = Meter.CreateHistogram<double>("nbn.hivemind.tick.target.hz");
    private static readonly Counter<long> RescheduleRequested = Meter.CreateCounter<long>("nbn.hivemind.reschedule.requested");
    private static readonly Counter<long> PauseRequested = Meter.CreateCounter<long>("nbn.hivemind.pause.requested");

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
}
