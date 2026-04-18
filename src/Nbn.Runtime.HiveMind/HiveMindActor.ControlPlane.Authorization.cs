using Nbn.Runtime.IO;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private bool IsRegisterBrainAuthorized(
        IContext context,
        Guid brainId,
        PID? brainRootPid,
        PID? routerPid,
        out string reason)
    {
        if (_brains.TryGetValue(brainId, out var brain) && HasTrustedController(brain))
        {
            if (IsTrustedControllerSender(context.Sender, brain))
            {
                reason = string.Empty;
                return true;
            }

            reason = context.Sender is null
                ? "trusted_controller_sender_missing"
                : "sender_not_trusted_controller";
            return false;
        }

        if (context.Sender is null)
        {
            reason = "bootstrap_sender_missing";
            return false;
        }

        if (IsValidControllerBootstrapSender(context, brainRootPid, routerPid))
        {
            reason = string.Empty;
            return true;
        }

        reason = "bootstrap_sender_mismatch";
        return false;
    }

    private bool IsUpdateBrainSignalRouterAuthorized(
        IContext context,
        Guid brainId,
        PID routerPid,
        out string reason)
    {
        if (_brains.TryGetValue(brainId, out var brain) && HasTrustedController(brain))
        {
            if (IsTrustedControllerSender(context.Sender, brain))
            {
                reason = string.Empty;
                return true;
            }

            reason = context.Sender is null
                ? "trusted_controller_sender_missing"
                : "sender_not_trusted_controller";
            return false;
        }

        if (context.Sender is null)
        {
            reason = "bootstrap_sender_missing";
            return false;
        }

        if (IsValidControllerBootstrapSender(context, null, routerPid))
        {
            reason = string.Empty;
            return true;
        }

        reason = "bootstrap_sender_mismatch";
        return false;
    }

    private bool IsRegisterShardAuthorized(
        IContext context,
        Guid brainId,
        ShardId32 shardId,
        PID shardPid,
        out string reason)
    {
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            reason = context.Sender is null
                ? "brain_not_registered_sender_missing"
                : "brain_not_registered_sender_not_allowed";
            return false;
        }

        if (context.Sender is null)
        {
            reason = "sender_missing";
            return false;
        }

        var senderIsTrustedController = IsTrustedControllerSender(context.Sender, brain);
        var senderIsAuthorizedWorker = IsPlacementAuthorizedWorkerSender(context.Sender, brain);
        var senderMatchesShardPid = SenderMatchesPid(context.Sender, shardPid);

        if (brain.Shards.TryGetValue(shardId, out var existingShardPid))
        {
            if (PidEquals(existingShardPid, shardPid))
            {
                reason = string.Empty;
                return true;
            }

            if (senderIsTrustedController
                || senderIsAuthorizedWorker
                || (brain.PlacementEpoch > 0 && senderMatchesShardPid))
            {
                reason = string.Empty;
                return true;
            }

            reason = "overwrite_sender_not_authorized";
            return false;
        }

        if (senderIsTrustedController || senderIsAuthorizedWorker || senderMatchesShardPid)
        {
            reason = string.Empty;
            return true;
        }

        reason = "sender_not_authorized";
        return false;
    }

    private bool IsUnregisterShardAuthorized(
        IContext context,
        Guid brainId,
        ShardId32 shardId,
        out string reason)
    {
        reason = string.Empty;
        if (!_brains.TryGetValue(brainId, out var brain))
        {
            return true;
        }

        if (!brain.Shards.TryGetValue(shardId, out var existingShardPid))
        {
            return true;
        }

        if (context.Sender is null)
        {
            reason = "sender_missing";
            return false;
        }

        if (SenderMatchesPid(context.Sender, existingShardPid)
            || IsTrustedControllerSender(context.Sender, brain)
            || IsPlacementAuthorizedWorkerSender(context.Sender, brain))
        {
            return true;
        }

        reason = "sender_not_authorized";
        return false;
    }

    private bool IsControlPlaneBrainMutationAuthorized(
        IContext context,
        Guid brainId,
        out BrainState brain,
        out string reason)
    {
        if (!_brains.TryGetValue(brainId, out brain!))
        {
            reason = context.Sender is null
                ? "brain_not_registered_sender_missing"
                : "brain_not_registered_sender_not_allowed";
            return false;
        }

        if (context.Sender is null)
        {
            reason = "sender_missing";
            return false;
        }

        if (IsTrustedControllerSender(context.Sender, brain)
            || IsPlacementAuthorizedWorkerSender(context.Sender, brain)
            || IsTrustedIoSender(context))
        {
            reason = string.Empty;
            return true;
        }

        reason = "sender_not_authorized";
        return false;
    }

    private static bool HasTrustedController(BrainState brain)
        => brain.BrainRootPid is not null || brain.SignalRouterPid is not null;

    private static bool IsTrustedControllerSender(PID? sender, BrainState brain)
    {
        if (sender is null)
        {
            return false;
        }

        return (brain.BrainRootPid is not null && SenderMatchesPid(sender, brain.BrainRootPid))
               || (brain.SignalRouterPid is not null && SenderMatchesPid(sender, brain.SignalRouterPid));
    }

    private bool IsPlacementAuthorizedWorkerSender(PID? sender, BrainState brain)
    {
        if (sender is null)
        {
            return false;
        }

        if (brain.PlacementExecution is not null)
        {
            foreach (var workerPid in brain.PlacementExecution.WorkerTargets.Values)
            {
                if (SenderMatchesPid(sender, workerPid))
                {
                    return true;
                }
            }
        }

        foreach (var worker in _workerCatalog.Values)
        {
            if (!worker.IsAlive || !worker.IsReady || !worker.IsFresh || string.IsNullOrWhiteSpace(worker.WorkerRootActorName))
            {
                continue;
            }

            var workerPid = string.IsNullOrWhiteSpace(worker.WorkerAddress)
                ? new PID { Id = worker.WorkerRootActorName }
                : new PID(worker.WorkerAddress, worker.WorkerRootActorName);
            if (SenderMatchesPid(sender, workerPid))
            {
                return true;
            }
        }

        return false;
    }

    private bool IsTrustedIoSender(IContext context)
    {
        var normalizedIoPid = NormalizePid(context, _ioPid);
        return normalizedIoPid is not null && SenderMatchesPid(context.Sender, normalizedIoPid);
    }

    private static bool IsValidControllerBootstrapSender(
        IContext context,
        PID? brainRootPid,
        PID? routerPid)
    {
        var normalizedBrainRoot = NormalizePid(context, brainRootPid);
        if (normalizedBrainRoot is not null && SenderMatchesPid(context.Sender, normalizedBrainRoot))
        {
            return true;
        }

        var normalizedRouter = NormalizePid(context, routerPid);
        return normalizedRouter is not null && SenderMatchesPid(context.Sender, normalizedRouter);
    }

    private void EmitControlPlaneMutationIgnored(
        IContext context,
        string topic,
        Guid brainId,
        string reason,
        ShardId32? shardId = null)
    {
        var senderLabel = context.Sender is null ? "<missing>" : PidLabel(context.Sender);
        var shardLabel = shardId is null ? string.Empty : $" shard={shardId.Value}";
        EmitDebug(
            context,
            ProtoSeverity.SevDebug,
            $"{topic}.ignored",
            $"Ignored {topic}. reason={reason} brain={brainId:D}{shardLabel} sender={senderLabel}.");
    }
}
