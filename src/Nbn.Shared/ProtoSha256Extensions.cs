using System;
using Google.Protobuf;
using Nbn.Proto;

namespace Nbn.Shared;

public static class ProtoSha256Extensions
{
    public const int Sha256Length = 32;

    public static byte[] ToByteArray(this Sha256 sha256)
    {
        if (sha256 is null)
        {
            throw new ArgumentNullException(nameof(sha256));
        }

        if (sha256.Value is null || sha256.Value.Length != Sha256Length)
        {
            throw new ArgumentException("Sha256 value must be 32 bytes.", nameof(sha256));
        }

        return sha256.Value.ToByteArray();
    }

    public static bool TryToByteArray(this Sha256 sha256, out byte[] bytes)
    {
        if (sha256 is null || sha256.Value is null || sha256.Value.Length != Sha256Length)
        {
            bytes = Array.Empty<byte>();
            return false;
        }

        bytes = sha256.Value.ToByteArray();
        return true;
    }

    public static Sha256 ToProtoSha256(this byte[] bytes)
    {
        if (bytes is null)
        {
            throw new ArgumentNullException(nameof(bytes));
        }

        if (bytes.Length != Sha256Length)
        {
            throw new ArgumentException("Sha256 value must be 32 bytes.", nameof(bytes));
        }

        return new Sha256 { Value = ByteString.CopyFrom(bytes) };
    }
}
