using System;
using Google.Protobuf;
using Nbn.Proto;

namespace Nbn.Shared;

/// <summary>
/// Converts shared SHA-256 values between protobuf wrappers, raw bytes, and hexadecimal strings.
/// </summary>
public static class ProtoSha256Extensions
{
    /// <summary>
    /// The required byte length of a SHA-256 digest.
    /// </summary>
    public const int Sha256Length = 32;

    /// <summary>
    /// Extracts the raw SHA-256 bytes from a protobuf wrapper.
    /// </summary>
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

    /// <summary>
    /// Attempts to extract the raw SHA-256 bytes from a protobuf wrapper.
    /// </summary>
    public static bool TryToByteArray(this Sha256? sha256, out byte[] bytes)
    {
        if (sha256 is null || sha256.Value is null || sha256.Value.Length != Sha256Length)
        {
            bytes = Array.Empty<byte>();
            return false;
        }

        bytes = sha256.Value.ToByteArray();
        return true;
    }

    /// <summary>
    /// Wraps raw SHA-256 bytes for protobuf transport.
    /// </summary>
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

    /// <summary>
    /// Converts a protobuf SHA-256 wrapper to hexadecimal.
    /// </summary>
    public static string ToHex(this Sha256 sha256, bool lowerCase = true)
    {
        if (sha256 is null)
        {
            throw new ArgumentNullException(nameof(sha256));
        }

        var bytes = sha256.ToByteArray();
        var hex = Convert.ToHexString(bytes);
        return lowerCase ? hex.ToLowerInvariant() : hex;
    }

    /// <summary>
    /// Attempts to convert a protobuf SHA-256 wrapper to hexadecimal.
    /// </summary>
    public static bool TryToHex(this Sha256? sha256, out string hex, bool lowerCase = true)
    {
        if (sha256 is null)
        {
            hex = string.Empty;
            return false;
        }

        if (!sha256.TryToByteArray(out var bytes))
        {
            hex = string.Empty;
            return false;
        }

        var value = Convert.ToHexString(bytes);
        hex = lowerCase ? value.ToLowerInvariant() : value;
        return true;
    }

    /// <summary>
    /// Parses a hexadecimal SHA-256 string into a protobuf wrapper.
    /// </summary>
    public static Sha256 FromHex(string hex)
    {
        if (!TryParseHexBytes(hex, out var bytes))
        {
            throw new ArgumentException("Sha256 hex must be a 64-character hex string.", nameof(hex));
        }

        return bytes.ToProtoSha256();
    }

    /// <summary>
    /// Attempts to parse a hexadecimal SHA-256 string into a protobuf wrapper.
    /// </summary>
    public static bool TryFromHex(string? hex, out Sha256 sha256)
    {
        if (!TryParseHexBytes(hex, out var bytes))
        {
            sha256 = new Sha256();
            return false;
        }

        sha256 = bytes.ToProtoSha256();
        return true;
    }

    /// <summary>
    /// Attempts to parse raw SHA-256 bytes from a hexadecimal string.
    /// </summary>
    public static bool TryParseHexBytes(string? hex, out byte[] bytes)
    {
        bytes = Array.Empty<byte>();
        if (string.IsNullOrWhiteSpace(hex))
        {
            return false;
        }

        var trimmed = hex.Trim();
        if (trimmed.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            trimmed = trimmed[2..];
        }

        if (trimmed.Length != Sha256Length * 2)
        {
            return false;
        }

        try
        {
            bytes = Convert.FromHexString(trimmed);
            return bytes.Length == Sha256Length;
        }
        catch (FormatException)
        {
            bytes = Array.Empty<byte>();
            return false;
        }
    }
}
