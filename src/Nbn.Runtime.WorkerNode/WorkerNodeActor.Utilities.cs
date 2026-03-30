using Nbn.Proto;
using Nbn.Proto.Control;
using Nbn.Runtime.Artifacts;
using Nbn.Shared;
using Proto;

namespace Nbn.Runtime.WorkerNode;

public sealed partial class WorkerNodeActor
{
    private void MaybeCaptureHiveMindHint(PID? sender)
    {
        if (sender is null
            || string.IsNullOrWhiteSpace(sender.Id)
            || sender.Id.StartsWith("$", StringComparison.Ordinal))
        {
            return;
        }

        _hiveMindHintPid = sender;
    }

    private PID? ResolveHiveMindPid(IContext context)
    {
        if (TryResolveEndpointPid(ServiceEndpointSettings.HiveMindKey, out var endpointPid))
        {
            if (!string.IsNullOrWhiteSpace(endpointPid.Address))
            {
                return endpointPid;
            }

            if (_hiveMindHintPid is not null
                && !string.IsNullOrWhiteSpace(_hiveMindHintPid.Address)
                && string.Equals(_hiveMindHintPid.Id, endpointPid.Id, StringComparison.Ordinal))
            {
                return _hiveMindHintPid;
            }

            return endpointPid;
        }

        if (_hiveMindHintPid is not null)
        {
            WorkerNodeTelemetry.RecordDiscoveryEndpointResolve(
                _workerNodeId,
                brainId: null,
                placementEpoch: 0,
                target: ServiceEndpointSettings.HiveMindKey,
                outcome: "resolved_hint",
                failureReason: "endpoint_missing");
            return _hiveMindHintPid;
        }

        return null;
    }

    private PID ResolveReplyTarget(IContext context, PID target)
    {
        var knownReplyTargets = _endpoints.Values
            .Select(static registration => registration.Endpoint.ToPid());
        return NormalizeReplyTarget(
            target,
            context.System.Address,
            _hiveMindHintPid,
            knownReplyTargets);
    }

    private PID? ResolvePlacementReplyTarget(IContext context, PlacementAssignmentRequest request)
    {
        if (!string.IsNullOrWhiteSpace(request.ReplyPid)
            && TryParsePid(request.ReplyPid, out var replyPid))
        {
            return ResolvePlacementReplyTarget(context, replyPid);
        }

        return ResolvePlacementReplyTarget(context, context.Sender);
    }

    private PID? ResolvePlacementReplyTarget(IContext context, PID? target)
    {
        if (target is null)
        {
            return null;
        }

        if (IsEphemeralRequestSender(target))
        {
            return target;
        }

        var hiveMindPid = ResolveHiveMindPid(context);
        return ResolvePlacementReplyTarget(target, hiveMindPid);
    }

    private static PID ResolvePlacementReplyTarget(PID target, PID? authoritativeHiveMindPid)
    {
        if (authoritativeHiveMindPid is null)
        {
            return target;
        }

        return string.Equals(authoritativeHiveMindPid.Id ?? string.Empty, target.Id ?? string.Empty, StringComparison.Ordinal)
            ? authoritativeHiveMindPid
            : target;
    }

    private static PID NormalizeReplyTarget(
        PID target,
        string? systemAddress,
        PID? hintedReplyPid,
        IEnumerable<PID> knownReplyTargets)
    {
        if (HasRoutableAddress(target))
        {
            return target;
        }

        if (!string.IsNullOrWhiteSpace(target.Id))
        {
            if (hintedReplyPid is not null
                && string.Equals(hintedReplyPid.Id, target.Id, StringComparison.Ordinal)
                && HasRoutableAddress(hintedReplyPid))
            {
                return hintedReplyPid;
            }

            foreach (var candidate in knownReplyTargets)
            {
                if (string.Equals(candidate.Id, target.Id, StringComparison.Ordinal)
                    && HasRoutableAddress(candidate))
                {
                    return candidate;
                }
            }
        }

        if (!string.IsNullOrWhiteSpace(target.Id)
            && !string.IsNullOrWhiteSpace(systemAddress)
            && !string.Equals(systemAddress, "nonhost", StringComparison.OrdinalIgnoreCase))
        {
            return new PID(systemAddress, target.Id);
        }

        return target;
    }

    private static bool TryParsePid(string? value, out PID pid)
    {
        pid = new PID(string.Empty, string.Empty);
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        var slashIndex = trimmed.IndexOf('/');
        if (slashIndex <= 0)
        {
            pid = new PID(string.Empty, trimmed);
            return true;
        }

        var address = trimmed[..slashIndex];
        var id = trimmed[(slashIndex + 1)..];
        if (string.IsNullOrWhiteSpace(id))
        {
            return false;
        }

        pid = new PID(address, id);
        return true;
    }

    private bool TryResolveEndpointPid(string key, out PID pid)
    {
        if (_endpoints.TryGetValue(key, out var registration))
        {
            pid = registration.Endpoint.ToPid();
            WorkerNodeTelemetry.RecordDiscoveryEndpointResolve(
                _workerNodeId,
                brainId: null,
                placementEpoch: 0,
                target: key,
                outcome: "resolved",
                failureReason: "none");
            return true;
        }

        WorkerNodeTelemetry.RecordDiscoveryEndpointResolve(
            _workerNodeId,
            brainId: null,
            placementEpoch: 0,
            target: key,
            outcome: "missing",
            failureReason: "endpoint_missing");
        pid = new PID();
        return false;
    }

    private bool HasKnownIoGatewayEndpoint()
        => _endpoints.ContainsKey(ServiceEndpointSettings.IoGatewayKey);

    private bool HasHiveMindHint()
        => _hiveMindHintPid is { } hint && !string.IsNullOrWhiteSpace(hint.Id);

    private static bool IsEphemeralRequestSender(PID? sender)
        => sender is { } value
           && !string.IsNullOrWhiteSpace(value.Id)
           && value.Id.StartsWith("$", StringComparison.Ordinal);

    private static bool HasRoutableAddress(PID? pid)
        => pid is { } value
           && !string.IsNullOrWhiteSpace(value.Address)
           && !string.Equals(value.Address, "nonhost", StringComparison.OrdinalIgnoreCase);

    private string ResolveReconcileFailureReason(
        bool requestHasBrainId,
        Guid requestBrainId,
        ulong placementEpoch,
        int assignmentCount)
    {
        if (!requestHasBrainId)
        {
            return "brain_id_invalid";
        }

        if (!_brains.TryGetValue(requestBrainId, out var brain))
        {
            return assignmentCount > 0 ? "none" : "brain_not_hosted";
        }

        if (brain.PlacementEpoch != placementEpoch)
        {
            return "placement_epoch_mismatch";
        }

        return assignmentCount > 0 ? "none" : "no_assignments";
    }

    private static string NormalizeSource(string source)
        => string.IsNullOrWhiteSpace(source) ? "update" : source.Trim().ToLowerInvariant();

    private static string NormalizeFailureReason(string failureReason, string fallback)
        => string.IsNullOrWhiteSpace(failureReason) ? fallback : failureReason.Trim().ToLowerInvariant();

    private static string NormalizeObservedActorId(string actorId)
    {
        if (string.IsNullOrWhiteSpace(actorId) || !actorId.Contains('/', StringComparison.Ordinal))
        {
            return actorId;
        }

        var parts = actorId
            .Split('/', StringSplitOptions.RemoveEmptyEntries)
            .Where(static part => !part.StartsWith("$", StringComparison.Ordinal))
            .ToArray();
        return parts.Length == 0 ? actorId : string.Join("/", parts);
    }

    private PID ToRemotePid(IContext context, PID pid)
    {
        var systemAddress = context.System.Address;

        if (!string.IsNullOrWhiteSpace(_workerAddress))
        {
            if (string.IsNullOrWhiteSpace(pid.Address)
                || string.Equals(pid.Address, systemAddress, StringComparison.Ordinal)
                || string.Equals(pid.Address, "nonhost", StringComparison.OrdinalIgnoreCase))
            {
                return new PID(_workerAddress, pid.Id);
            }
        }

        if (!string.IsNullOrWhiteSpace(pid.Address))
        {
            return pid;
        }

        if (string.IsNullOrWhiteSpace(systemAddress)
            || string.Equals(systemAddress, "nonhost", StringComparison.OrdinalIgnoreCase))
        {
            return pid;
        }

        return new PID(systemAddress, pid.Id);
    }

    private static string PidLabel(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private static string ArtifactLabel(ArtifactRef? reference)
    {
        if (!HasArtifactRef(reference))
        {
            return "missing";
        }

        return reference!.TryToSha256Hex(out var sha)
            ? sha[..Math.Min(12, sha.Length)]
            : "present";
    }

    private static bool HasArtifactRef(ArtifactRef? reference)
        => reference is not null
           && reference.Sha256 is not null
           && reference.Sha256.Value is not null
           && reference.Sha256.Value.Length == 32;

    private static string ResolveArtifactRoot(string? overridePath)
        => ArtifactStoreResolverOptions.ResolveLocalStoreRootPath(overridePath);

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        return !string.IsNullOrWhiteSpace(value)
               && (string.Equals(value, "1", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "yes", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(value, "on", StringComparison.OrdinalIgnoreCase));
    }

    private static bool ResolveDebugStreamEnabled(bool defaultValue)
    {
        var value = Environment.GetEnvironmentVariable("NBN_DEBUG_STREAM_ENABLED");
        if (string.IsNullOrWhiteSpace(value))
        {
            return defaultValue;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "1" or "true" or "yes" or "on" => true,
            "0" or "false" or "no" or "off" => false,
            _ => defaultValue
        };
    }

    private static Severity ResolveDebugMinSeverity(Severity defaultValue)
    {
        var value = Environment.GetEnvironmentVariable("NBN_DEBUG_STREAM_MIN_SEVERITY");
        if (string.IsNullOrWhiteSpace(value))
        {
            return defaultValue;
        }

        if (Enum.TryParse<Severity>(value, ignoreCase: true, out var parsed))
        {
            return NormalizeDebugSeverity(parsed);
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "trace" or "sev_trace" => Severity.SevTrace,
            "debug" or "sev_debug" => Severity.SevDebug,
            "info" or "sev_info" => Severity.SevInfo,
            "warn" or "warning" or "sev_warn" => Severity.SevWarn,
            "error" or "sev_error" => Severity.SevError,
            "fatal" or "sev_fatal" => Severity.SevFatal,
            _ => defaultValue
        };
    }

    private static Severity NormalizeDebugSeverity(Severity severity)
    {
        return severity switch
        {
            Severity.SevTrace or Severity.SevDebug or Severity.SevInfo or Severity.SevWarn or Severity.SevError or Severity.SevFatal => severity,
            _ => Severity.SevInfo
        };
    }
}
