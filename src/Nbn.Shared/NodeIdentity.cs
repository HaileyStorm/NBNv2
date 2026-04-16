using System.Security.Cryptography;
using System.Text;

namespace Nbn.Shared;

public static class NodeIdentity
{
    public static Guid DeriveNodeId(string address)
    {
        if (string.IsNullOrWhiteSpace(address))
        {
            return Guid.Empty;
        }

        var bytes = Encoding.UTF8.GetBytes(address.Trim());
        var hash = MD5.HashData(bytes);
        return new Guid(hash);
    }

    public static Guid DeriveNodeId(string address, string rootActorName)
    {
        if (string.IsNullOrWhiteSpace(address) || string.IsNullOrWhiteSpace(rootActorName))
        {
            return Guid.Empty;
        }

        return DeriveNodeId($"{address.Trim()}/{rootActorName.Trim()}");
    }
}
