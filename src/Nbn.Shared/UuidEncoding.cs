using System;

namespace Nbn.Shared;

public static class UuidEncoding
{
    public const int UuidByteLength = 16;

    public static Guid FromRfc4122Bytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < UuidByteLength)
        {
            throw new ArgumentException("UUID bytes must be at least 16 bytes.", nameof(bytes));
        }

        return new Guid(bytes.Slice(0, UuidByteLength), bigEndian: true);
    }

    public static void WriteRfc4122Bytes(Guid value, Span<byte> destination)
    {
        if (destination.Length < UuidByteLength)
        {
            throw new ArgumentException("Destination span is too small for a UUID.", nameof(destination));
        }

        if (!value.TryWriteBytes(destination, bigEndian: true, out var bytesWritten) || bytesWritten != UuidByteLength)
        {
            throw new InvalidOperationException("Failed to write UUID bytes.");
        }
    }

    public static byte[] ToRfc4122Bytes(Guid value)
    {
        var bytes = new byte[UuidByteLength];
        WriteRfc4122Bytes(value, bytes);
        return bytes;
    }
}
