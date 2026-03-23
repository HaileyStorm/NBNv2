using Nbn.Proto.Io;
using Nbn.Shared;
using Proto;

namespace Nbn.Runtime.IO;

public sealed partial class IoGatewayActor
{
    private void HandleConnect(IContext context, Connect message)
    {
        if (context.Sender is null)
        {
            return;
        }

        var key = PidKey(context.Sender);
        if (!_clients.ContainsKey(key))
        {
            context.Watch(context.Sender);
        }

        _clients[key] = new ClientInfo(context.Sender, message.ClientName ?? string.Empty);

        context.Respond(new ConnectAck
        {
            ServerName = _options.ServerName,
            ServerTimeMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        });
    }

    private void HandleClientTerminated(IContext context, Terminated terminated)
    {
        if (terminated.Who is null)
        {
            return;
        }

        var key = PidKey(terminated.Who);
        if (_clients.Remove(key))
        {
            context.Unwatch(terminated.Who);
        }
    }

    private void BroadcastToClients(IContext context, object message)
    {
        if (_clients.Count == 0)
        {
            return;
        }

        foreach (var client in _clients.ToArray())
        {
            try
            {
                context.Send(client.Value.Pid, message);
            }
            catch
            {
                _clients.Remove(client.Key);
                context.Unwatch(client.Value.Pid);
            }
        }
    }

    private static PID? TryCreatePid(string? address, string? name)
    {
        if (string.IsNullOrWhiteSpace(address) || string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        return new PID(address, name);
    }

    private static string PidKey(PID pid)
        => string.IsNullOrWhiteSpace(pid.Address) ? pid.Id : $"{pid.Address}/{pid.Id}";

    private static string PidLabel(PID? pid)
        => pid is null ? "unknown" : PidKey(pid);

    private string BuildLocalActorReference(PID pid)
    {
        if (_localEndpointCandidates is not null && _localEndpointCandidates.Count > 0)
        {
            return RoutablePidReference.Encode(pid, _localEndpointCandidates);
        }

        return PidKey(pid);
    }

    private static bool PidEquals(PID? left, PID right)
    {
        return left is not null
               && string.Equals(left.Id, right.Id, StringComparison.Ordinal)
               && string.Equals(left.Address, right.Address, StringComparison.Ordinal);
    }

    private static bool IsEnvTrue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "1" or "true" or "yes" or "on" => true,
            _ => false
        };
    }
    private static PID ToRemotePid(IContext context, PID pid)
    {
        if (!string.IsNullOrWhiteSpace(pid.Address))
        {
            return pid;
        }

        var address = context.System.Address;
        if (string.IsNullOrWhiteSpace(address))
        {
            return pid;
        }

        return new PID(address, pid.Id);
    }

    private sealed record ClientInfo(PID Pid, string Name);
}
