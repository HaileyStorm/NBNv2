using System;

namespace Nbn.Tools.Workbench.ViewModels;

internal static class WorkbenchWorkerHostGrouping
{
    internal static bool IsWorkerHostCandidate(ConnectionViewModel? connections, string? logicalName, string? rootActorName)
    {
        if (connections is not null)
        {
            if (!string.IsNullOrWhiteSpace(connections.WorkerLogicalName)
                && !string.IsNullOrWhiteSpace(logicalName)
                && string.Equals(logicalName.Trim(), connections.WorkerLogicalName.Trim(), StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (!string.IsNullOrWhiteSpace(connections.WorkerRootName)
                && !string.IsNullOrWhiteSpace(rootActorName)
                && string.Equals(rootActorName.Trim(), connections.WorkerRootName.Trim(), StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        if (!string.IsNullOrWhiteSpace(logicalName)
            && logicalName.Trim().StartsWith("nbn.worker", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (string.IsNullOrWhiteSpace(rootActorName))
        {
            return false;
        }

        var root = rootActorName.Trim();
        return root.StartsWith("worker-node", StringComparison.OrdinalIgnoreCase)
               || root.Equals("regionhost", StringComparison.OrdinalIgnoreCase)
               || root.Equals("region-host", StringComparison.OrdinalIgnoreCase)
               || root.StartsWith("regionhost-", StringComparison.OrdinalIgnoreCase)
               || root.StartsWith("region-host-", StringComparison.OrdinalIgnoreCase);
    }

    internal static string ResolveHostGroupKey(
        string? address,
        string? logicalName,
        string? rootActorName,
        Guid? nodeId = null)
    {
        if (TryParseHostPort(address, out var host, out _))
        {
            return $"host:{host.ToLowerInvariant()}";
        }

        if (!string.IsNullOrWhiteSpace(logicalName))
        {
            return $"logical:{logicalName.Trim().ToLowerInvariant()}";
        }

        if (!string.IsNullOrWhiteSpace(rootActorName))
        {
            return $"root:{rootActorName.Trim().ToLowerInvariant()}";
        }

        return nodeId is { } value && value != Guid.Empty
            ? $"node:{value:N}"
            : "unknown";
    }

    internal static string ResolveHostDisplayName(string? address, string? logicalName, Guid? nodeId = null)
    {
        if (TryParseHostPort(address, out var host, out _))
        {
            return host;
        }

        if (!string.IsNullOrWhiteSpace(logicalName))
        {
            return logicalName.Trim();
        }

        return nodeId is { } value && value != Guid.Empty
            ? value.ToString("N")
            : "Unknown host";
    }

    internal static bool TryParseHostPort(string? address, out string host, out int port)
    {
        host = string.Empty;
        port = 0;

        if (string.IsNullOrWhiteSpace(address))
        {
            return false;
        }

        var trimmed = address.Trim();
        var colonIndex = trimmed.LastIndexOf(':');
        if (colonIndex <= 0 || colonIndex >= trimmed.Length - 1)
        {
            return false;
        }

        var hostPart = trimmed[..colonIndex];
        var portPart = trimmed[(colonIndex + 1)..];
        if (hostPart.StartsWith("[", StringComparison.Ordinal) && hostPart.EndsWith("]", StringComparison.Ordinal))
        {
            hostPart = hostPart[1..^1];
        }

        if (!int.TryParse(portPart, out port))
        {
            return false;
        }

        host = hostPart;
        return !string.IsNullOrWhiteSpace(host);
    }

    internal static string FormatWorkerCountLabel(int workerCount)
        => $"{workerCount} worker{(workerCount == 1 ? string.Empty : "s")}";
}
