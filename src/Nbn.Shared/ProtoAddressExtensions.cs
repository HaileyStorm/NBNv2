using System;
using Nbn.Shared.Addressing;
using Proto = Nbn.Proto;

namespace Nbn.Shared;

/// <summary>
/// Converts shared addressing value objects to and from protobuf wrappers.
/// </summary>
public static class ProtoAddressExtensions
{
    /// <summary>
    /// Converts a protobuf address wrapper into a shared <see cref="Address32"/>.
    /// </summary>
    public static Address32 ToAddress32(this Proto.Address32 address)
    {
        if (address is null)
        {
            throw new ArgumentNullException(nameof(address));
        }

        return new Address32(address.Value);
    }

    /// <summary>
    /// Converts a protobuf shard identifier wrapper into a shared <see cref="ShardId32"/>.
    /// </summary>
    public static ShardId32 ToShardId32(this Proto.ShardId32 shardId)
    {
        if (shardId is null)
        {
            throw new ArgumentNullException(nameof(shardId));
        }

        return new ShardId32(shardId.Value);
    }

    /// <summary>
    /// Wraps a shared <see cref="Address32"/> for protobuf transport.
    /// </summary>
    public static Proto.Address32 ToProtoAddress32(this Address32 address)
    {
        return new Proto.Address32 { Value = address.Value };
    }

    /// <summary>
    /// Wraps a shared <see cref="ShardId32"/> for protobuf transport.
    /// </summary>
    public static Proto.ShardId32 ToProtoShardId32(this ShardId32 shardId)
    {
        return new Proto.ShardId32 { Value = shardId.Value };
    }
}
