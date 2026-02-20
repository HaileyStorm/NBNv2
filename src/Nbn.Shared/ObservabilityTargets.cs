using Proto;

namespace Nbn.Shared;

public static class ObservabilityTargets
{
    public const string DefaultDebugHubName = "DebugHub";
    public const string DefaultVizHubName = "VisualizationHub";
    public const int DefaultPort = 12060;

    public static (PID? DebugHub, PID? VizHub) Resolve(string? defaultHost = null, int defaultPort = DefaultPort)
    {
        if (GetEnvBool("NBN_OBS_DISABLED") == true)
        {
            return (null, null);
        }

        var address = Environment.GetEnvironmentVariable("NBN_OBS_ADDRESS");
        if (string.IsNullOrWhiteSpace(address))
        {
            var host = Environment.GetEnvironmentVariable("NBN_OBS_HOST");
            if (string.IsNullOrWhiteSpace(host))
            {
                host = defaultHost;
            }

            var port = GetEnvInt("NBN_OBS_PORT") ?? defaultPort;
            if (string.IsNullOrWhiteSpace(host) || port <= 0)
            {
                return (null, null);
            }

            address = $"{host}:{port}";
        }

        var debugHubName = Environment.GetEnvironmentVariable("NBN_OBS_DEBUG_HUB");
        if (string.IsNullOrWhiteSpace(debugHubName))
        {
            debugHubName = DefaultDebugHubName;
        }

        var vizHubName = Environment.GetEnvironmentVariable("NBN_OBS_VIZ_HUB");
        if (string.IsNullOrWhiteSpace(vizHubName))
        {
            vizHubName = DefaultVizHubName;
        }

        PID? debugHub = string.IsNullOrWhiteSpace(debugHubName) ? null : new PID(address, debugHubName);
        PID? vizHub = string.IsNullOrWhiteSpace(vizHubName) ? null : new PID(address, vizHubName);
        return (debugHub, vizHub);
    }

    public static bool CanSend(IContext context, PID? pid)
    {
        if (pid is null)
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(pid.Address))
        {
            return true;
        }

        return !string.IsNullOrWhiteSpace(context.System.Address);
    }

    private static int? GetEnvInt(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        return int.TryParse(value, out var parsed) ? parsed : null;
    }

    private static bool? GetEnvBool(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "1" or "true" or "yes" or "on" => true,
            "0" or "false" or "no" or "off" => false,
            _ => null
        };
    }
}
