using System;
using Google.Protobuf;
using Nbn.Proto;

namespace Nbn.Shared;

/// <summary>
/// Converts shared UUID values between protobuf wrappers and RFC 4122 byte order.
/// </summary>
public static class ProtoUuidExtensions
{
    /// <summary>
    /// Converts a protobuf UUID wrapper to a <see cref="Guid"/>.
    /// </summary>
    public static Guid ToGuid(this Uuid uuid)
    {
        if (uuid is null)
        {
            throw new ArgumentNullException(nameof(uuid));
        }

        if (uuid.Value is null || uuid.Value.Length != UuidEncoding.UuidByteLength)
        {
            throw new ArgumentException("Uuid value must be 16 bytes.", nameof(uuid));
        }

        return UuidEncoding.FromRfc4122Bytes(uuid.Value.Span);
    }

    /// <summary>
    /// Attempts to convert a protobuf UUID wrapper to a <see cref="Guid"/>.
    /// </summary>
    public static bool TryToGuid(this Uuid? uuid, out Guid guid)
    {
        if (uuid is null || uuid.Value is null || uuid.Value.Length != UuidEncoding.UuidByteLength)
        {
            guid = Guid.Empty;
            return false;
        }

        guid = UuidEncoding.FromRfc4122Bytes(uuid.Value.Span);
        return true;
    }

    /// <summary>
    /// Wraps a <see cref="Guid"/> for protobuf transport using RFC 4122 byte order.
    /// </summary>
    public static Uuid ToProtoUuid(this Guid guid)
    {
        return new Uuid
        {
            Value = ByteString.CopyFrom(UuidEncoding.ToRfc4122Bytes(guid))
        };
    }

    /// <summary>
    /// Writes a <see cref="Guid"/> into an existing protobuf UUID wrapper.
    /// </summary>
    public static void WriteTo(this Guid guid, Uuid destination)
    {
        if (destination is null)
        {
            throw new ArgumentNullException(nameof(destination));
        }

        destination.Value = ByteString.CopyFrom(UuidEncoding.ToRfc4122Bytes(guid));
    }
}
