using Nbn.Proto.Control;

namespace Nbn.Runtime.WorkerNode;

[Flags]
public enum WorkerServiceRole
{
    None = 0,
    BrainRoot = 1 << 0,
    SignalRouter = 1 << 1,
    InputCoordinator = 1 << 2,
    OutputCoordinator = 1 << 3,
    RegionShard = 1 << 4,
    All = BrainRoot | SignalRouter | InputCoordinator | OutputCoordinator | RegionShard
}

public static class WorkerServiceRoles
{
    private static readonly char[] RoleSeparators = [',', ';', '|', ' ', '\t', '\r', '\n'];

    private static readonly WorkerServiceRole[] OrderedRoles =
    [
        WorkerServiceRole.BrainRoot,
        WorkerServiceRole.SignalRouter,
        WorkerServiceRole.InputCoordinator,
        WorkerServiceRole.OutputCoordinator,
        WorkerServiceRole.RegionShard
    ];

    public static WorkerServiceRole Sanitize(WorkerServiceRole roles)
        => roles & WorkerServiceRole.All;

    public static bool IsEnabled(WorkerServiceRole enabledRoles, WorkerServiceRole requiredRole)
        => (Sanitize(enabledRoles) & requiredRole) == requiredRole;

    public static bool TryMapAssignmentTarget(PlacementAssignmentTarget target, out WorkerServiceRole role)
    {
        role = target switch
        {
            PlacementAssignmentTarget.PlacementTargetBrainRoot => WorkerServiceRole.BrainRoot,
            PlacementAssignmentTarget.PlacementTargetSignalRouter => WorkerServiceRole.SignalRouter,
            PlacementAssignmentTarget.PlacementTargetInputCoordinator => WorkerServiceRole.InputCoordinator,
            PlacementAssignmentTarget.PlacementTargetOutputCoordinator => WorkerServiceRole.OutputCoordinator,
            PlacementAssignmentTarget.PlacementTargetRegionShard => WorkerServiceRole.RegionShard,
            _ => WorkerServiceRole.None
        };

        return role != WorkerServiceRole.None;
    }

    public static string ToOptionValue(WorkerServiceRole roles)
    {
        var normalized = Sanitize(roles);
        if (normalized == WorkerServiceRole.None)
        {
            return "none";
        }

        if (normalized == WorkerServiceRole.All)
        {
            return "all";
        }

        return string.Join(
            ",",
            OrderedRoles
                .Where(role => IsEnabled(normalized, role))
                .Select(ToRoleToken));
    }

    public static string ToRoleToken(WorkerServiceRole role)
        => role switch
        {
            WorkerServiceRole.BrainRoot => "brain-root",
            WorkerServiceRole.SignalRouter => "signal-router",
            WorkerServiceRole.InputCoordinator => "input-coordinator",
            WorkerServiceRole.OutputCoordinator => "output-coordinator",
            WorkerServiceRole.RegionShard => "region-shard",
            WorkerServiceRole.None => "none",
            WorkerServiceRole.All => "all",
            _ => ToOptionValue(role)
        };

    public static WorkerServiceRole ParseRoleSet(string rawValue, string argumentName)
    {
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            throw new ArgumentException($"{argumentName} requires at least one role token.");
        }

        var roles = WorkerServiceRole.None;
        var tokens = rawValue.Split(RoleSeparators, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (tokens.Length == 0)
        {
            throw new ArgumentException($"{argumentName} requires at least one role token.");
        }

        foreach (var token in tokens)
        {
            if (!TryParseRoleToken(token, out var parsed))
            {
                throw new ArgumentException(
                    $"{argumentName} contains unknown role token '{token}'. Allowed tokens: all, none, brain-root, signal-router, input-coordinator, output-coordinator, region-shard.");
            }

            roles |= parsed;
        }

        return Sanitize(roles);
    }

    public static bool TryParseRoleToken(string rawToken, out WorkerServiceRole role)
    {
        var token = NormalizeRoleToken(rawToken);
        role = token switch
        {
            "all" => WorkerServiceRole.All,
            "none" => WorkerServiceRole.None,
            "brain-root" => WorkerServiceRole.BrainRoot,
            "brainroot" => WorkerServiceRole.BrainRoot,
            "signal-router" => WorkerServiceRole.SignalRouter,
            "signalrouter" => WorkerServiceRole.SignalRouter,
            "input-coordinator" => WorkerServiceRole.InputCoordinator,
            "inputcoordinator" => WorkerServiceRole.InputCoordinator,
            "input" => WorkerServiceRole.InputCoordinator,
            "output-coordinator" => WorkerServiceRole.OutputCoordinator,
            "outputcoordinator" => WorkerServiceRole.OutputCoordinator,
            "output" => WorkerServiceRole.OutputCoordinator,
            "region-shard" => WorkerServiceRole.RegionShard,
            "regionshard" => WorkerServiceRole.RegionShard,
            "shard" => WorkerServiceRole.RegionShard,
            "shard-host" => WorkerServiceRole.RegionShard,
            "shardhost" => WorkerServiceRole.RegionShard,
            "region-host" => WorkerServiceRole.RegionShard,
            "regionhost" => WorkerServiceRole.RegionShard,
            "brain-host" => WorkerServiceRole.BrainRoot | WorkerServiceRole.SignalRouter,
            "brainhost" => WorkerServiceRole.BrainRoot | WorkerServiceRole.SignalRouter,
            "io-coordinator" => WorkerServiceRole.InputCoordinator | WorkerServiceRole.OutputCoordinator,
            "io-coordinators" => WorkerServiceRole.InputCoordinator | WorkerServiceRole.OutputCoordinator,
            "iocoordinator" => WorkerServiceRole.InputCoordinator | WorkerServiceRole.OutputCoordinator,
            "iocoordinators" => WorkerServiceRole.InputCoordinator | WorkerServiceRole.OutputCoordinator,
            _ => WorkerServiceRole.None
        };

        return token.Length > 0
               && (role != WorkerServiceRole.None
                   || token.Equals("none", StringComparison.Ordinal));
    }

    private static string NormalizeRoleToken(string rawToken)
        => (rawToken ?? string.Empty).Trim().ToLowerInvariant().Replace("_", "-", StringComparison.Ordinal);
}
