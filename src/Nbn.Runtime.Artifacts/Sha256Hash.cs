using System.Buffers.Binary;
using System.Security.Cryptography;

namespace Nbn.Runtime.Artifacts;

public readonly struct Sha256Hash : IEquatable<Sha256Hash>
{
    public const int Length = 32;

    private readonly byte[] _bytes;

    public Sha256Hash(byte[] bytes)
    {
        if (bytes is null)
        {
            throw new ArgumentNullException(nameof(bytes));
        }

        if (bytes.Length != Length)
        {
            throw new ArgumentException("SHA-256 hash must be 32 bytes.", nameof(bytes));
        }

        _bytes = bytes;
    }

    public ReadOnlyMemory<byte> Bytes => _bytes ?? ReadOnlyMemory<byte>.Empty;

    public bool IsEmpty => _bytes is null || _bytes.Length == 0;

    public static Sha256Hash Compute(ReadOnlySpan<byte> data)
    {
        var hash = SHA256.HashData(data);
        return new Sha256Hash(hash);
    }

    public static Sha256Hash FromBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != Length)
        {
            throw new ArgumentException("SHA-256 hash must be 32 bytes.", nameof(bytes));
        }

        return new Sha256Hash(bytes.ToArray());
    }

    public static bool TryParseHex(string hex, out Sha256Hash hash)
    {
        hash = default;
        if (string.IsNullOrWhiteSpace(hex) || hex.Length != Length * 2)
        {
            return false;
        }

        try
        {
            var bytes = Convert.FromHexString(hex);
            hash = new Sha256Hash(bytes);
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    public string ToHex()
    {
        if (_bytes is null)
        {
            return string.Empty;
        }

        return Convert.ToHexString(_bytes).ToLowerInvariant();
    }

    public override string ToString() => ToHex();

    public bool Equals(Sha256Hash other)
    {
        if (_bytes is null || other._bytes is null)
        {
            return _bytes == other._bytes;
        }

        return _bytes.AsSpan().SequenceEqual(other._bytes);
    }

    public override bool Equals(object? obj) => obj is Sha256Hash other && Equals(other);

    public override int GetHashCode()
    {
        if (_bytes is null || _bytes.Length < 16)
        {
            return 0;
        }

        var h1 = BinaryPrimitives.ReadInt32LittleEndian(_bytes.AsSpan(0, 4));
        var h2 = BinaryPrimitives.ReadInt32LittleEndian(_bytes.AsSpan(4, 4));
        var h3 = BinaryPrimitives.ReadInt32LittleEndian(_bytes.AsSpan(8, 4));
        var h4 = BinaryPrimitives.ReadInt32LittleEndian(_bytes.AsSpan(12, 4));
        return HashCode.Combine(h1, h2, h3, h4);
    }

    public static bool operator ==(Sha256Hash left, Sha256Hash right) => left.Equals(right);

    public static bool operator !=(Sha256Hash left, Sha256Hash right) => !left.Equals(right);
}
