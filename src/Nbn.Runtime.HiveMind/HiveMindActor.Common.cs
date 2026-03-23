using Nbn.Runtime.Brain;
using Nbn.Runtime.IO;
using Nbn.Shared;
using Nbn.Shared.Addressing;
using Proto;
using ProtoSeverity = Nbn.Proto.Severity;
using ProtoIo = Nbn.Proto.Io;
using ProtoControl = Nbn.Proto.Control;

namespace Nbn.Runtime.HiveMind;

public sealed partial class HiveMindActor
{
    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Trim().ToLowerInvariant() is "1" or "true" or "yes" or "on";
    }

    private static void Log(string message)
        => Console.WriteLine($"[{DateTime.UtcNow:O}] [HiveMind] {message}");

    private static void LogError(string message)
        => Console.WriteLine($"[{DateTime.UtcNow:O}] [HiveMind][ERROR] {message}");

    private static void SendRoutingTable(IContext context, PID pid, RoutingTableSnapshot snapshot, string label)
    {
        if (!string.IsNullOrWhiteSpace(pid.Address) && string.IsNullOrWhiteSpace(context.System.Address))
        {
            LogError($"Routing table not sent to {label} {PidLabel(pid)} because remoting is not configured.");
            return;
        }

        try
        {
            context.Send(pid, new SetRoutingTable(snapshot));
        }
        catch (Exception ex)
        {
            LogError($"Failed to send routing table to {label} {PidLabel(pid)}: {ex.Message}");
        }
    }

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private static PID? NormalizePid(IContext context, PID? pid)
    {
        if (pid is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(pid.Address))
        {
            return pid;
        }

        var senderAddress = context.Sender?.Address;
        if (!string.IsNullOrWhiteSpace(senderAddress))
        {
            return new PID(senderAddress, pid.Id);
        }

        return pid;
    }

    private static PID ResolveSendTargetPid(IContext context, PID pid)
    {
        if (!string.IsNullOrWhiteSpace(pid.Address))
        {
            return pid;
        }

        var systemAddress = context.System.Address;
        if (string.IsNullOrWhiteSpace(systemAddress))
        {
            return pid;
        }

        return new PID(systemAddress, pid.Id);
    }

    private static bool TryGetGuid(Nbn.Proto.Uuid? uuid, out Guid guid)
    {
        if (uuid is null)
        {
            guid = Guid.Empty;
            return false;
        }

        return uuid.TryToGuid(out guid);
    }

    private static bool IsSupportedHomeostasisTargetMode(ProtoControl.HomeostasisTargetMode mode)
    {
        return mode == ProtoControl.HomeostasisTargetMode.HomeostasisTargetZero
               || mode == ProtoControl.HomeostasisTargetMode.HomeostasisTargetFixed;
    }

    private static bool IsFiniteInRange(float value, float min, float max)
    {
        return float.IsFinite(value) && value >= min && value <= max;
    }

    private static bool TryNormalizePlasticityEnergyCostModulation(
        bool enabled,
        long referenceTickCost,
        float responseStrength,
        float minScale,
        float maxScale,
        out long normalizedReferenceTickCost,
        out float normalizedResponseStrength,
        out float normalizedMinScale,
        out float normalizedMaxScale)
    {
        normalizedReferenceTickCost = DefaultPlasticityEnergyCostReferenceTickCost;
        normalizedResponseStrength = DefaultPlasticityEnergyCostResponseStrength;
        normalizedMinScale = DefaultPlasticityEnergyCostMinScale;
        normalizedMaxScale = DefaultPlasticityEnergyCostMaxScale;

        if (!enabled)
        {
            var hasExplicitConfiguration = referenceTickCost > 0
                                           || (float.IsFinite(responseStrength) && responseStrength > 0f)
                                           || (float.IsFinite(minScale) && minScale > 0f)
                                           || (float.IsFinite(maxScale) && maxScale > 0f);
            if (!hasExplicitConfiguration)
            {
                return true;
            }

            if (referenceTickCost > 0)
            {
                normalizedReferenceTickCost = referenceTickCost;
            }

            if (float.IsFinite(responseStrength) && responseStrength >= 0f)
            {
                normalizedResponseStrength = Math.Clamp(responseStrength, 0f, 8f);
            }

            var hasExplicitScale = float.IsFinite(minScale)
                                   && float.IsFinite(maxScale)
                                   && (minScale > 0f || maxScale > 0f);
            if (hasExplicitScale)
            {
                normalizedMinScale = Math.Clamp(minScale, 0f, 1f);
                normalizedMaxScale = Math.Clamp(maxScale, 0f, 1f);
                if (normalizedMaxScale < normalizedMinScale)
                {
                    normalizedMaxScale = normalizedMinScale;
                }
            }

            return true;
        }

        if (referenceTickCost <= 0
            || !IsFiniteInRange(responseStrength, 0f, 8f)
            || !IsFiniteInRange(minScale, 0f, 1f)
            || !IsFiniteInRange(maxScale, 0f, 1f)
            || maxScale < minScale)
        {
            return false;
        }

        normalizedReferenceTickCost = referenceTickCost;
        normalizedResponseStrength = responseStrength;
        normalizedMinScale = minScale;
        normalizedMaxScale = maxScale;
        return true;
    }

    private static float ResolvePlasticityDelta(float plasticityRate, float plasticityDelta)
    {
        if (plasticityDelta > 0f)
        {
            return plasticityDelta;
        }

        return plasticityRate > 0f ? plasticityRate : 0f;
    }

    private static bool ResolvePerBrainCostEnergyEnabled(BrainState brain)
        => brain.CostEnergyEnabled;

    private bool ResolveEffectiveCostEnergyEnabled(BrainState brain)
        => _systemCostEnergyEnabled && ResolvePerBrainCostEnergyEnabled(brain);

    private bool ResolveEffectivePlasticityEnabled(BrainState brain)
        => _systemPlasticityEnabled && brain.PlasticityEnabled;

    private bool ResolveSnapshotCostEnergyEnabled(BrainState brain, ProtoIo.RequestSnapshot message)
    {
        var requestedCostEnergyEnabled = message.HasRuntimeState
            ? message.CostEnabled && message.EnergyEnabled
            : ResolvePerBrainCostEnergyEnabled(brain);
        return _systemCostEnergyEnabled && requestedCostEnergyEnabled;
    }

    private bool ResolveSnapshotPlasticityEnabled(BrainState brain, ProtoIo.RequestSnapshot message)
    {
        var requestedPlasticityEnabled = message.HasRuntimeState
            ? message.PlasticityEnabled
            : brain.PlasticityEnabled;
        return _systemPlasticityEnabled && requestedPlasticityEnabled;
    }

    private static PID? ParsePid(string? value)
        => TryParsePid(value, out var pid) ? pid : null;

    private static async Task<PID?> ResolvePidAsync(string? value)
        => await RoutablePidReference.ResolveAsync(value).ConfigureAwait(false);

    private static string ResolveActorReference(string? actorReference, PID? pid)
        => !string.IsNullOrWhiteSpace(actorReference)
            ? actorReference.Trim()
            : pid is null
                ? string.Empty
                : PidLabel(pid);

    private static bool TryParsePid(string? value, out PID pid)
    {
        pid = new PID();
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        var slashIndex = trimmed.IndexOf('/');
        if (slashIndex <= 0)
        {
            pid.Id = trimmed;
            return true;
        }

        var address = trimmed[..slashIndex];
        var id = trimmed[(slashIndex + 1)..];

        if (string.IsNullOrWhiteSpace(id))
        {
            return false;
        }

        pid.Address = address;
        pid.Id = id;
        return true;
    }
}
