using System;
using Google.Protobuf;
using Nbn.Proto;

namespace Nbn.Shared;

public static class ProtoUuidExtensions
{
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

    public static bool TryToGuid(this Uuid uuid, out Guid guid)
    {
        if (uuid is null || uuid.Value is null || uuid.Value.Length != UuidEncoding.UuidByteLength)
        {
            guid = Guid.Empty;
            return false;
        }

        guid = UuidEncoding.FromRfc4122Bytes(uuid.Value.Span);
        return true;
    }

    public static Uuid ToProtoUuid(this Guid guid)
    {
        return new Uuid
        {
            Value = ByteString.CopyFrom(UuidEncoding.ToRfc4122Bytes(guid))
        };
    }

    public static void WriteTo(this Guid guid, Uuid destination)
    {
        if (destination is null)
        {
            throw new ArgumentNullException(nameof(destination));
        }

        destination.Value = ByteString.CopyFrom(UuidEncoding.ToRfc4122Bytes(guid));
    }
}
