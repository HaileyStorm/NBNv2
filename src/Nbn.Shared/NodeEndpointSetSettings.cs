namespace Nbn.Shared;

public readonly record struct NodeEndpointSetRegistration(
    Guid NodeId,
    ServiceEndpointSet EndpointSet,
    long UpdatedMs);

public static class NodeEndpointSetSettings
{
    public const string Prefix = "node.endpoint_set.";

    public static string BuildKey(Guid nodeId)
    {
        if (nodeId == Guid.Empty)
        {
            throw new ArgumentException("Node id is required.", nameof(nodeId));
        }

        return Prefix + nodeId.ToString("D");
    }

    public static bool TryParseKey(string? key, out Guid nodeId)
    {
        nodeId = Guid.Empty;
        if (string.IsNullOrWhiteSpace(key))
        {
            return false;
        }

        var trimmed = key.Trim();
        if (!trimmed.StartsWith(Prefix, StringComparison.Ordinal))
        {
            return false;
        }

        return Guid.TryParse(trimmed.Substring(Prefix.Length), out nodeId) && nodeId != Guid.Empty;
    }

    public static bool TryParseSetting(
        string? key,
        string? value,
        ulong updatedMs,
        out NodeEndpointSetRegistration registration)
    {
        registration = default;
        if (!TryParseKey(key, out var nodeId))
        {
            return false;
        }

        if (!ServiceEndpointSettings.TryParseSetValue(value, out var endpointSet))
        {
            return false;
        }

        registration = new NodeEndpointSetRegistration(
            nodeId,
            endpointSet,
            updatedMs > long.MaxValue ? long.MaxValue : (long)updatedMs);
        return true;
    }
}
