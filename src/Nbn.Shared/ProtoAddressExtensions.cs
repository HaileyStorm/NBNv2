using System;
using Nbn.Shared.Addressing;
using Proto = Nbn.Proto;

namespace Nbn.Shared;

public static class ProtoAddressExtensions
{
    public static Address32 ToAddress32(this Proto.Address32 address)
    {
        if (address is null)
        {
            throw new ArgumentNullException(nameof(address));
        }

        return new Address32(address.Value);
    }

    public static ShardId32 ToShardId32(this Proto.ShardId32 shardId)
    {
        if (shardId is null)
        {
            throw new ArgumentNullException(nameof(shardId));
        }

        return new ShardId32(shardId.Value);
    }

    public static Proto.Address32 ToProtoAddress32(this Address32 address)
    {
        return new Proto.Address32 { Value = address.Value };
    }

    public static Proto.ShardId32 ToProtoShardId32(this ShardId32 shardId)
    {
        return new Proto.ShardId32 { Value = shardId.Value };
    }
}
