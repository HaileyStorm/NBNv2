using System;
using Nbn.Proto;

namespace Nbn.Shared;

/// <summary>
/// Converts artifact references between shared SHA-256 helpers and protobuf models.
/// </summary>
public static class ProtoArtifactExtensions
{
    /// <summary>
    /// Extracts the SHA-256 bytes from an artifact reference.
    /// </summary>
    public static byte[] ToSha256Bytes(this ArtifactRef reference)
    {
        if (reference is null)
        {
            throw new ArgumentNullException(nameof(reference));
        }

        if (reference.Sha256 is null)
        {
            throw new ArgumentException("ArtifactRef is missing sha256.", nameof(reference));
        }

        return reference.Sha256.ToByteArray();
    }

    /// <summary>
    /// Attempts to extract the SHA-256 bytes from an artifact reference.
    /// </summary>
    public static bool TryToSha256Bytes(this ArtifactRef? reference, out byte[] bytes)
    {
        bytes = Array.Empty<byte>();
        if (reference?.Sha256 is null)
        {
            return false;
        }

        return reference.Sha256.TryToByteArray(out bytes);
    }

    /// <summary>
    /// Converts the artifact SHA-256 value to hexadecimal.
    /// </summary>
    public static string ToSha256Hex(this ArtifactRef reference, bool lowerCase = true)
    {
        if (reference is null)
        {
            throw new ArgumentNullException(nameof(reference));
        }

        if (reference.Sha256 is null)
        {
            throw new ArgumentException("ArtifactRef is missing sha256.", nameof(reference));
        }

        return reference.Sha256.ToHex(lowerCase);
    }

    /// <summary>
    /// Attempts to convert the artifact SHA-256 value to hexadecimal.
    /// </summary>
    public static bool TryToSha256Hex(this ArtifactRef? reference, out string hex, bool lowerCase = true)
    {
        hex = string.Empty;
        if (reference?.Sha256 is null)
        {
            return false;
        }

        return reference.Sha256.TryToHex(out hex, lowerCase);
    }

    /// <summary>
    /// Creates an artifact reference from raw SHA-256 bytes.
    /// </summary>
    public static ArtifactRef ToArtifactRef(this byte[] sha256Bytes, ulong sizeBytes = 0, string? mediaType = null, string? storeUri = null)
    {
        var sha256 = sha256Bytes.ToProtoSha256();
        return sha256.ToArtifactRef(sizeBytes, mediaType, storeUri);
    }

    /// <summary>
    /// Creates an artifact reference from a protobuf SHA-256 wrapper.
    /// </summary>
    public static ArtifactRef ToArtifactRef(this Sha256 sha256, ulong sizeBytes = 0, string? mediaType = null, string? storeUri = null)
    {
        if (sha256 is null)
        {
            throw new ArgumentNullException(nameof(sha256));
        }

        var reference = new ArtifactRef
        {
            Sha256 = sha256,
            SizeBytes = sizeBytes
        };

        if (!string.IsNullOrWhiteSpace(mediaType))
        {
            reference.MediaType = mediaType;
        }

        if (!string.IsNullOrWhiteSpace(storeUri))
        {
            reference.StoreUri = storeUri;
        }

        return reference;
    }

    /// <summary>
    /// Creates an artifact reference from a hexadecimal SHA-256 value.
    /// </summary>
    public static ArtifactRef ToArtifactRef(this string sha256Hex, ulong sizeBytes = 0, string? mediaType = null, string? storeUri = null)
    {
        if (!ProtoSha256Extensions.TryFromHex(sha256Hex, out var sha256))
        {
            throw new ArgumentException("Sha256 hex must be a 64-character hex string.", nameof(sha256Hex));
        }

        return sha256.ToArtifactRef(sizeBytes, mediaType, storeUri);
    }
}
