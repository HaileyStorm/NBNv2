using System.Text;
using Nbn.Shared;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private static TimeSpan SafeDuration(DateTime start, DateTime end)
    {
        if (start == default || end == default || end < start)
        {
            return TimeSpan.Zero;
        }

        return end - start;
    }

    private static TimeSpan ComputeTickDelay(TimeSpan elapsed, float targetTickHz)
    {
        if (targetTickHz <= 0)
        {
            return TimeSpan.Zero;
        }

        var period = TimeSpan.FromSeconds(1d / targetTickHz);
        var delay = period - elapsed;
        return delay <= TimeSpan.Zero ? TimeSpan.Zero : delay;
    }

    private void EmitTickComputeDoneIgnored(
        IContext context,
        ProtoControl.TickComputeDone message,
        string reason,
        PID? expectedSender = null)
    {
        var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
        var expectedLabel = expectedSender is null ? string.Empty : $" expectedSender={PidLabel(expectedSender)}";
        var brainLabel = message.BrainId is null || !message.BrainId.TryToGuid(out var brainId)
            ? "<invalid>"
            : brainId.ToString("D");
        var shardLabel = message.ShardId is null
            ? "<missing>"
            : message.ShardId.ToShardId32().ToString();
        EmitDebug(
            context,
            ProtoSeverity.SevDebug,
            "tick.compute_done.ignored",
            $"Ignored TickComputeDone. reason={reason} tick={message.TickId} brain={brainLabel} shard={shardLabel} sender={senderLabel}{expectedLabel}.");
        if (LogTickBarrier || LogVizDiagnostics)
        {
            Log($"TickComputeDone ignored reason={reason} tick={message.TickId} brain={brainLabel} shard={shardLabel} sender={senderLabel}{expectedLabel}");
        }
    }

    private void EmitTickDeliverDoneIgnored(
        IContext context,
        ProtoControl.TickDeliverDone message,
        string reason,
        PID? expectedSender = null)
    {
        var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
        var expectedLabel = expectedSender is null ? string.Empty : $" expectedSender={PidLabel(expectedSender)}";
        var brainLabel = message.BrainId is null || !message.BrainId.TryToGuid(out var brainId)
            ? "<invalid>"
            : brainId.ToString("D");
        EmitDebug(
            context,
            ProtoSeverity.SevDebug,
            "tick.deliver_done.ignored",
            $"Ignored TickDeliverDone. reason={reason} tick={message.TickId} brain={brainLabel} sender={senderLabel}{expectedLabel}.");
        if (LogTickBarrier || LogVizDiagnostics)
        {
            Log($"TickDeliverDone ignored reason={reason} tick={message.TickId} brain={brainLabel} sender={senderLabel}{expectedLabel}");
        }
    }

    private string DescribePendingCompute(int maxItems = 10)
    {
        if (_pendingCompute.Count == 0)
        {
            return "none";
        }

        var sb = new StringBuilder();
        var index = 0;
        foreach (var key in _pendingCompute)
        {
            if (index > 0)
            {
                sb.Append("; ");
            }

            var senderLabel = _pendingComputeSenders.TryGetValue(key, out var sender)
                ? PidLabel(sender)
                : "<missing>";
            sb.Append($"brain={key.BrainId:D} shard={key.ShardId} sender={senderLabel}");
            index++;
            if (index >= maxItems)
            {
                break;
            }
        }

        if (_pendingCompute.Count > index)
        {
            sb.Append($"; +{_pendingCompute.Count - index} more");
        }

        return sb.ToString();
    }

    private string DescribePendingDeliver(int maxItems = 10)
    {
        if (_pendingDeliver.Count == 0)
        {
            return "none";
        }

        var sb = new StringBuilder();
        var index = 0;
        foreach (var brainId in _pendingDeliver)
        {
            if (index > 0)
            {
                sb.Append("; ");
            }

            var senderLabel = _pendingDeliverSenders.TryGetValue(brainId, out var sender)
                ? PidLabel(sender)
                : "<missing>";
            sb.Append($"brain={brainId:D} sender={senderLabel}");
            index++;
            if (index >= maxItems)
            {
                break;
            }
        }

        if (_pendingDeliver.Count > index)
        {
            sb.Append($"; +{_pendingDeliver.Count - index} more");
        }

        return sb.ToString();
    }
}
